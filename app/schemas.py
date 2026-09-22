"""API 请求 / 响应模型。"""
from pydantic import BaseModel, Field


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
