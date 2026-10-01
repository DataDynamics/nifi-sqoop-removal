from uuid import UUID

from pydantic import Field

from load_control.domain import PartitionStatus, RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class ClaimRequest(ApiModel):
    claim_token: UUID
    worker_node: str = Field(min_length=1, max_length=200)


class ClaimResponse(ApiModel):
    claimed: bool
    run_status: RunStatus
    partition_status: PartitionStatus
    attempt: int


class ChunkReport(ApiModel):
    claim_token: UUID
    chunk_index: int = Field(ge=0)
    chunk_count: int = Field(gt=0, le=1_000_000)
    fragment_identifier: str | None = Field(default=None, max_length=100)
    hdfs_path: str = Field(min_length=1, max_length=1500)
    record_count: int = Field(ge=0)
    byte_count: int | None = Field(default=None, ge=0)


class ChunkResult(ApiModel):
    recorded: bool
    partition_status: PartitionStatus
    run_status: RunStatus
    received_chunks: int
    chunk_count: int
    validation_scheduled: bool


class PartitionFailRequest(ApiModel):
    claim_token: UUID
    error_stage: ShortText
    error_class: ShortText
    error_code: ShortText
    message: Message = ""
    attempt: int | None = Field(default=None, ge=0)


class PartitionFailResponse(ApiModel):
    partition_status: PartitionStatus
    run_status: RunStatus
    changed: bool
