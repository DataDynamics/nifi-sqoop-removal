from typing import Literal
from uuid import UUID

from pydantic import Field

from load_control.domain import RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class PublishClaimRequest(ApiModel):
    publish_token: UUID


class PublishClaimResponse(ApiModel):
    claimed: bool
    run_status: RunStatus


class PublishResultRequest(ApiModel):
    publish_token: UUID
    outcome: Literal["PUBLISHED", "FAILED_PUBLISH", "PUBLISH_UNKNOWN"]
    error_code: ShortText | None = None
    message: Message = ""


class PublishResultResponse(ApiModel):
    run_status: RunStatus
    changed: bool


class PublishUnknownResolveRequest(ApiModel):
    resolution: Literal["PUBLISHED", "FAILED_PUBLISH"]
    reason: str = Field(min_length=5, max_length=2000)


class DispatchResendResponse(ApiModel):
    dispatch_id: str
    status: str
