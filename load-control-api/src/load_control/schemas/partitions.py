"""파티션 엔드포인트 모델."""

from uuid import UUID

from pydantic import Field

from load_control.domain import PartitionStatus, RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class ClaimRequest(ApiModel):
    """claim 요청. claimToken은 Worker가 한 번 만들어 재시도에도 그대로 쓴다."""

    claim_token: UUID
    worker_node: str = Field(min_length=1, max_length=200)


class ClaimResponse(ApiModel):
    """claim 결과. claimed=false면 처리하지 않는다."""

    claimed: bool
    run_status: RunStatus
    partition_status: PartitionStatus
    attempt: int


class ChunkReport(ApiModel):
    """chunk 하나의 보고. NiFi AttributesToJSON의 문자열 값도 받는다."""

    claim_token: UUID
    chunk_index: int = Field(ge=0)
    chunk_count: int = Field(gt=0, le=1_000_000)
    fragment_identifier: str | None = Field(default=None, max_length=100)
    hdfs_path: str = Field(min_length=1, max_length=1500)
    record_count: int = Field(ge=0)
    byte_count: int | None = Field(default=None, ge=0)


class ChunkResult(ApiModel):
    """chunk 판정 결과. NiFi는 로그 수준만 정하고 흐름을 바꾸지 않는다."""

    recorded: bool
    partition_status: PartitionStatus
    run_status: RunStatus
    received_chunks: int
    chunk_count: int
    validation_scheduled: bool


class PartitionFailRequest(ApiModel):
    """파티션 최종 실패 보고."""

    claim_token: UUID
    error_stage: ShortText
    error_class: ShortText
    error_code: ShortText
    message: Message = ""
    attempt: int | None = Field(default=None, ge=0)


class PartitionFailResponse(ApiModel):
    """파티션 실패 처리 결과."""

    partition_status: PartitionStatus
    run_status: RunStatus
    changed: bool
