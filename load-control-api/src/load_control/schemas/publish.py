"""게시·운영자 엔드포인트 모델."""

from typing import Literal
from uuid import UUID

from pydantic import Field

from load_control.domain import RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class PublishClaimRequest(ApiModel):
    """게시 소유권 요청."""

    publish_token: UUID


class PublishClaimResponse(ApiModel):
    """게시 소유권 결과."""

    claimed: bool
    run_status: RunStatus


class PublishResultRequest(ApiModel):
    """게시 결과 보고."""

    publish_token: UUID
    outcome: Literal["PUBLISHED", "FAILED_PUBLISH", "PUBLISH_UNKNOWN"]
    error_code: ShortText | None = None
    message: Message = ""


class PublishResultResponse(ApiModel):
    """게시 결과 처리 결과."""

    run_status: RunStatus
    changed: bool


class PublishUnknownResolveRequest(ApiModel):
    """운영자의 PUBLISH_UNKNOWN 확정 요청. 사유는 load_event에 남는다."""

    resolution: Literal["PUBLISHED", "FAILED_PUBLISH"]
    reason: str = Field(min_length=5, max_length=2000)


class DispatchResendResponse(ApiModel):
    """재전송 결과."""

    dispatch_id: str
    status: str
