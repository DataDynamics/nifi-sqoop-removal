"""게시·운영자 엔드포인트 모델."""

from typing import Literal
from uuid import UUID

from pydantic import Field

from load_control.domain import RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class PublishClaimRequest(ApiModel):
    """게시 소유권 요청. publishToken은 PG-50이 한 번 만들어 재시도와 결과 보고에 그대로 쓴다."""

    publish_token: UUID


class PublishClaimResponse(ApiModel):
    """게시 소유권 결과."""

    claimed: bool  # true일 때만 INSERT OVERWRITE를 실행한다
    run_status: RunStatus


class PublishResultRequest(ApiModel):
    """게시 결과 보고."""

    publish_token: UUID  # claim 때 보낸 token. 다르면 409 PUBLISH_TOKEN_MISMATCH
    # PUBLISHED: 성공, FAILED_PUBLISH: 반영되지 않았음이 확실한 실패, PUBLISH_UNKNOWN: 반영 여부를 모름
    outcome: Literal["PUBLISHED", "FAILED_PUBLISH", "PUBLISH_UNKNOWN"]
    error_code: ShortText | None = None  # 실패일 때. 없으면 outcome 값을 run error_code로 쓴다
    message: Message = ""


class PublishResultResponse(ApiModel):
    """게시 결과 처리 결과. PUBLISH_UNKNOWN 확정(운영자)의 응답으로도 쓴다."""

    run_status: RunStatus
    changed: bool  # 이미 그 상태였으면(재요청) false


class PublishUnknownResolveRequest(ApiModel):
    """운영자의 PUBLISH_UNKNOWN 확정 요청. 사유는 load_event에 남는다."""

    resolution: Literal["PUBLISHED", "FAILED_PUBLISH"]  # Hive 반영 확인 결과
    # 확인 근거(필수). load_event에 남고, FAILED_PUBLISH면 run error_message로도 남는다
    reason: str = Field(min_length=5, max_length=2000)


class DispatchResendResponse(ApiModel):
    """재전송 결과."""

    dispatch_id: str
    status: str  # 재전송 후 상태(항상 PENDING)
