"""租约协调服务：多工作进程间的独占任务协调。

所有租约状态转换均由 Redis Lua 脚本原子完成（见 app/lua.py），
过期判断基于 Redis 服务器时间（TIME 命令）。
"""
from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import Settings, get_settings
from .lua import (
    ACQUIRE_BATCH_LUA,
    ACQUIRE_LUA,
    RELEASE_LUA,
    RENEW_LUA,
    VALIDATE_LUA,
)
from .schemas import (
    AcquireBatchRequest,
    AcquireBatchResponse,
    AcquiredLease,
    AcquireRequest,
    AcquireResponse,
    LeaseStateResponse,
    ReleaseRequest,
    ReleaseResponse,
    RenewRequest,
    RenewResponse,
    ValidateRequest,
    ValidateResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis = redis
    # 启动时注册 Lua 脚本（EVALSHA 执行，脚本不存在时客户端自动回退 EVAL）
    app.state.scripts = SimpleNamespace(
        acquire=redis.register_script(ACQUIRE_LUA),
        acquire_batch=redis.register_script(ACQUIRE_BATCH_LUA),
        renew=redis.register_script(RENEW_LUA),
        release=redis.register_script(RELEASE_LUA),
        validate=redis.register_script(VALIDATE_LUA),
    )
    yield
    await redis.aclose()


app = FastAPI(
    title="Lease API",
    version="1.0.0",
    description="供多个工作进程协调独占任务的租约服务（FastAPI + Redis Lua）",
    lifespan=lifespan,
)


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "redis unavailable"},
    )


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_scripts(request: Request) -> SimpleNamespace:
    return request.app.state.scripts


RedisDep = Annotated[Redis, Depends(get_redis)]
ScriptsDep = Annotated[SimpleNamespace, Depends(get_scripts)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def lease_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:lease:{task_key}"


def fencing_key(settings: Settings, task_key: str) -> str:
    return f"{settings.key_prefix}:fencing:{task_key}"


def resolve_ttl(ttl_ms: int | None, settings: Settings) -> int:
    ttl = ttl_ms if ttl_ms is not None else settings.default_ttl_ms
    if not settings.min_ttl_ms <= ttl <= settings.max_ttl_ms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"ttl_ms must be within "
                f"[{settings.min_ttl_ms}, {settings.max_ttl_ms}]"
            ),
        )
    return ttl


@app.get("/healthz")
async def healthz(redis: RedisDep) -> dict:
    await redis.ping()
    return {"status": "ok"}


@app.get("/")
async def root() -> dict:
    return {"service": "lease-api", "docs": "/docs"}


@app.post("/v1/leases/acquire", response_model=AcquireResponse)
async def acquire(
    req: AcquireRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireResponse:
    """申请租约。仅当任务未被占用或旧租约已过期时成功。

    成功返回 200 及所有权令牌 + 防栅序号；被占用返回 409。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    token = uuid.uuid4().hex
    ok, fencing, expires_at = await scripts.acquire(
        keys=[lease_key(settings, req.task_key), fencing_key(settings, req.task_key)],
        args=[token, ttl],
    )
    if ok:
        return AcquireResponse(
            acquired=True,
            task_key=req.task_key,
            token=token,
            fencing_token=fencing,
            expires_at_ms=expires_at,
        )
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireResponse(
        acquired=False,
        task_key=req.task_key,
        fencing_token=fencing,
        expires_at_ms=expires_at,
    )


@app.post("/v1/leases/acquire-batch", response_model=AcquireBatchResponse)
async def acquire_batch(
    req: AcquireBatchRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> AcquireBatchResponse:
    """批量申请租约（全成全败）。

    全部任务键均未被占用（或旧租约已过期）时，一次性创建所有租约；
    任一任务仍被占用则返回 409 及全部冲突键，且不创建任何租约、
    不递增任何防栅计数器。

    成功时按输入顺序返回每个任务独立的 token 与 fencing_token，
    expires_at_ms 基于同一次服务器 TIME 计算，因此全批相同。
    所得凭据与单个申请接口完全一致，可直接用于续租 / 释放 / 校验。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    task_keys = req.task_keys
    tokens = [uuid.uuid4().hex for _ in task_keys]

    # Lua 约定：KEYS[1..n] 为租约键，KEYS[n+1..2n] 为防栅计数器键，顺序与入参一致
    keys = [lease_key(settings, k) for k in task_keys]
    keys += [fencing_key(settings, k) for k in task_keys]

    result = await scripts.acquire_batch(
        keys=keys,
        args=[*tokens, ttl],
    )

    if result[0]:
        n = len(task_keys)
        fencing_tokens = result[1 : n + 1]
        expires_at = result[n + 1]
        return AcquireBatchResponse(
            acquired=True,
            leases=[
                AcquiredLease(
                    task_key=task_key,
                    token=token,
                    fencing_token=fencing,
                    expires_at_ms=expires_at,
                )
                for task_key, token, fencing in zip(
                    task_keys, tokens, fencing_tokens
                )
            ],
        )

    # 冲突：result[1:] 为冲突任务键的输入下标（1 起）
    conflict_indexes = result[1:]
    response.status_code = status.HTTP_409_CONFLICT
    return AcquireBatchResponse(
        acquired=False,
        conflicts=[task_keys[i - 1] for i in conflict_indexes],
    )


@app.post("/v1/leases/renew", response_model=RenewResponse)
async def renew(
    req: RenewRequest,
    response: Response,
    scripts: ScriptsDep,
    settings: SettingsDep,
) -> RenewResponse:
    """续租。必须同时匹配令牌与防栅序号，且租约尚未过期。

    失败返回 409，reason 取值：not_found / conflict / expired。
    """
    ttl = resolve_ttl(req.ttl_ms, settings)
    ok, reason, expires_at = await scripts.renew(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token), ttl],
    )
    if ok:
        return RenewResponse(renewed=True, expires_at_ms=expires_at)
    response.status_code = status.HTTP_409_CONFLICT
    return RenewResponse(renewed=False, reason=reason)


@app.post("/v1/leases/release", response_model=ReleaseResponse)
async def release(
    req: ReleaseRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ReleaseResponse:
    """释放租约。令牌与防栅序号同时匹配才删除，否则幂等忽略。

    旧持有者在超时后调用本接口不会影响新租约。
    """
    released = await scripts.release(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token)],
    )
    return ReleaseResponse(released=bool(released))


@app.post("/v1/leases/validate", response_model=ValidateResponse)
async def validate(
    req: ValidateRequest, scripts: ScriptsDep, settings: SettingsDep
) -> ValidateResponse:
    """校验 (令牌, 防栅序号) 是否仍对应当前有效租约，供下游判断操作归属。"""
    valid, remaining_ms = await scripts.validate(
        keys=[lease_key(settings, req.task_key)],
        args=[req.token, str(req.fencing_token)],
    )
    return ValidateResponse(valid=bool(valid), remaining_ms=remaining_ms)


@app.get("/v1/leases/{task_key:path}", response_model=LeaseStateResponse)
async def inspect(
    task_key: str, redis: RedisDep, settings: SettingsDep
) -> LeaseStateResponse:
    """查看任务键当前租约状态（只读，不暴露所有权令牌）。"""
    data = await redis.hgetall(lease_key(settings, task_key))
    if not data:
        return LeaseStateResponse(held=False)
    sec, usec = await redis.time()
    now = int(sec) * 1000 + int(usec) // 1000
    expires_at = int(data["expires_at"])
    held = expires_at > now
    return LeaseStateResponse(
        held=held,
        fencing_token=int(data["fencing"]),
        expires_at_ms=expires_at,
        remaining_ms=max(0, expires_at - now),
    )
