"""API 请求 / 响应模型。"""
from typing import Annotated

from pydantic import BaseModel, Field, model_validator

# 与单个申请接口一致的任务键约束
TaskKey = Annotated[str, Field(min_length=1, max_length=512)]


class AcquireRequest(BaseModel):
    task_key: str = Field(
        min_length=1, max_length=512, examples=["video-transcode:order-123"]
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="租约 TTL（毫秒），缺省使用服务端默认值"
    )


class AcquireResponse(BaseModel):
    acquired: bool
    task_key: str
    token: str | None = Field(default=None, description="所有权令牌，仅成功时返回")
    fencing_token: int | None = Field(
        default=None, description="防栅序号，单调递增"
    )
    expires_at_ms: int | None = Field(
        default=None, description="过期时间（Redis 服务器时钟，Unix 毫秒）"
    )


class AcquireBatchRequest(BaseModel):
    task_keys: list[TaskKey] = Field(
        min_length=2,
        max_length=32,
        description="2～32 个互不重复的任务键，响应顺序与此一致",
    )
    ttl_ms: int | None = Field(
        default=None, gt=0, description="全批统一的租约 TTL（毫秒），缺省用默认值"
    )

    @model_validator(mode="after")
    def _ensure_unique(self) -> "AcquireBatchRequest":
        if len(set(self.task_keys)) != len(self.task_keys):
            raise ValueError("task_keys must be unique")
        return self


class AcquiredLease(BaseModel):
    task_key: str
    token: str = Field(description="该任务独立的所有权令牌（UUID）")
    fencing_token: int = Field(description="该任务单调递增的防栅序号")
    expires_at_ms: int = Field(
        description="过期时间（Redis 服务器时钟，Unix 毫秒），全批相同基准"
    )


class AcquireBatchResponse(BaseModel):
    acquired: bool
    leases: list[AcquiredLease] = Field(
        default_factory=list,
        description="成功时按输入顺序给出每个任务的独立凭据；冲突时为空",
    )
    conflicts: list[str] = Field(
        default_factory=list, description="冲突（仍被占用）的任务键，按输入顺序"
    )


class RenewRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)
    ttl_ms: int | None = Field(
        default=None, gt=0, description="新的 TTL（毫秒），缺省使用服务端默认值"
    )


class RenewResponse(BaseModel):
    renewed: bool
    expires_at_ms: int | None = None
    reason: str | None = Field(
        default=None, description="失败原因：not_found / conflict / expired"
    )


class ReleaseRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)


class ReleaseResponse(BaseModel):
    released: bool = Field(description="是否真正删除了租约；不匹配时为 false（幂等）")


class ValidateRequest(BaseModel):
    task_key: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)


class ValidateResponse(BaseModel):
    valid: bool
    remaining_ms: int = Field(default=0, description="租约剩余毫秒数，无效时为 0")


class LeaseStateResponse(BaseModel):
    """租约状态查询（不暴露所有权令牌）。"""

    held: bool
    fencing_token: int | None = None
    expires_at_ms: int | None = None
    remaining_ms: int = 0
