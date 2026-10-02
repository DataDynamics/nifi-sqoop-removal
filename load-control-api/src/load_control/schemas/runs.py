"""run·manifest 엔드포인트 모델."""

from datetime import datetime
from typing import Any

from pydantic import Field

from load_control.domain import PartitionStatus, RunStatus
from load_control.schemas.common import (
    ApiModel,
    BusinessKey,
    DecimalStr,
    HdfsPath,
    JobKey,
    Message,
    PartitionId,
    ShortText,
    TablePrefix,
)


class RunCreateRequest(ApiModel):
    """run 생성 요청(PG-10)."""

    job_key: JobKey
    business_key: BusinessKey
    hdfs_root: HdfsPath
    stage_table_prefix: TablePrefix
    allow_empty_source: bool = False
    parameters: dict[str, Any] = Field(default_factory=dict)


class RunCreateResponse(ApiModel):
    """생성된 run과 NiFi가 쓸 HDFS 경로·stage table."""

    run_id: str
    status: RunStatus
    hdfs_run_path: str
    stage_table: str


class ManifestPartition(ApiModel):
    """파티션 하나의 범위와 예상 건수. 경계값은 정밀도를 위해 문자열."""

    partition_id: PartitionId
    lower_bound: DecimalStr | None = None
    upper_bound: DecimalStr | None = None
    upper_inclusive: bool = False
    is_null_partition: bool = False
    expected_row_count: int = Field(ge=0)


class ManifestRequest(ApiModel):
    """manifest 등록 요청."""

    snapshot_scn: DecimalStr | None = None  # PostgreSQL 원천(불변 마감 조건)이면 null
    source_count: int = Field(ge=0)
    source_null_split_count: int = Field(default=0, ge=0)
    source_min_split: DecimalStr | None = None
    source_max_split: DecimalStr | None = None
    planned_partition_count: int = Field(gt=0, le=10_000)
    source_metrics: dict[str, Message] = Field(default_factory=dict, max_length=200)
    source_metrics_version: ShortText = "v1"
    partitions: list[ManifestPartition] = Field(min_length=1, max_length=10_000)


class ManifestResponse(ApiModel):
    """Worker로 보낼 파티션 목록(0건 파티션 제외)."""

    run_id: str
    status: RunStatus
    dispatch_partitions: list[ManifestPartition]
    empty_partition_count: int
    validation_scheduled: bool


class RunFailRequest(ApiModel):
    """파티션 외 단계 실패 보고."""

    expected_status: RunStatus
    fail_status: RunStatus
    error_stage: ShortText
    error_code: ShortText
    message: Message = ""


class RunFailResponse(ApiModel):
    """실패 처리 결과."""

    run_id: str
    run_status: RunStatus
    changed: bool


class PartitionSummary(ApiModel):
    """조회용 파티션 요약."""

    partition_id: str
    status: PartitionStatus
    expected_row_count: int
    actual_row_count: int | None
    file_count: int | None
    attempt_count: int
    worker_node: str | None
    error_code: str | None


class DispatchSummary(ApiModel):
    """조회용 dispatch 요약."""

    dispatch_id: str
    dispatch_type: str
    partition_id: str | None
    status: str
    attempt_count: int
    sent_at: datetime | None
    acked_at: datetime | None


class RunDetail(ApiModel):
    """run 상세 조회 결과."""

    run_id: str
    job_key: str
    business_key: str
    status: RunStatus
    snapshot_scn: str | None
    source_count: int | None
    expected_partition_count: int | None
    success_partition_count: int
    failed_partition_count: int
    extracted_count: int
    hdfs_run_path: str | None
    stage_table: str | None
    started_at: datetime
    heartbeat_at: datetime
    extract_completed_at: datetime | None
    completed_at: datetime | None
    error_stage: str | None
    error_code: str | None
    error_message: str | None
    partition_counts: dict[str, int]
    partitions: list[PartitionSummary]
    dispatches: list[DispatchSummary]


class RunListItem(ApiModel):
    """run 목록 항목."""

    run_id: str
    job_key: str
    business_key: str
    status: RunStatus
    source_count: int | None
    extracted_count: int
    started_at: datetime
    completed_at: datetime | None
    error_code: str | None
