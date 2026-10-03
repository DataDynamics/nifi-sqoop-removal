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

    job_key: JobKey  # 적재 Job 식별자(예: ORACLE_INSP_DTL_DAILY)
    business_key: BusinessKey  # 업무 키(예: 업무일자 2026-09-28)
    hdfs_root: HdfsPath  # staging 루트. run 경로는 {hdfsRoot}/{jobKey}/run_id={runId}
    stage_table_prefix: TablePrefix  # staging table 이름 = (prefix + runId hex) 소문자
    allow_empty_source: bool = False  # true면 원천 0건 manifest를 허용한다(기본은 FAILED_MANIFEST)
    parameters: dict[str, Any] = Field(default_factory=dict)  # 기록용 추가 값(load_run.parameters jsonb)


class RunCreateResponse(ApiModel):
    """생성된 run과 NiFi가 쓸 HDFS 경로·stage table."""

    run_id: str
    status: RunStatus  # 항상 CREATED
    hdfs_run_path: str  # 이 run의 Parquet 파일을 쓸 HDFS 경로. chunk 보고의 hdfsPath는 이 아래여야 한다
    stage_table: str  # 이 run 전용 staging table 이름


class ManifestPartition(ApiModel):
    """파티션 하나의 범위와 예상 건수. 경계값은 정밀도를 위해 문자열."""

    partition_id: PartitionId  # 0000~9999 또는 NULL
    lower_bound: DecimalStr | None = None  # split 컬럼 하한(포함). NULL 파티션은 null
    upper_bound: DecimalStr | None = None  # split 컬럼 상한. NULL 파티션은 null
    upper_inclusive: bool = False  # 상한 포함 여부. 경계가 가장 큰 마지막 파티션만 true
    is_null_partition: bool = False  # split 컬럼이 NULL인 행을 모은 파티션(ID는 NULL, 최대 1개)
    expected_row_count: int = Field(ge=0)  # 이 범위의 원천 건수. 0이면 등록 즉시 SUCCESS


class ManifestRequest(ApiModel):
    """manifest 등록 요청."""

    snapshot_scn: DecimalStr | None = None  # PostgreSQL 원천(불변 마감 조건)이면 null
    source_count: int = Field(ge=0)  # 같은 SCN 기준 원천 전체 건수. 파티션 예상 건수 합계와 같아야 한다
    source_null_split_count: int = Field(default=0, ge=0)  # split 컬럼이 NULL인 건수 = NULL 파티션 예상 건수
    source_min_split: DecimalStr | None = None  # split 컬럼 최솟값. 주면 첫 파티션 하한과 같아야 한다
    source_max_split: DecimalStr | None = None  # split 컬럼 최댓값. 주면 마지막 파티션 상한과 같아야 한다
    planned_partition_count: int = Field(gt=0, le=10_000)  # 계획한 파티션 수. partitions 개수와 같아야 한다
    # 원천 지표(예: AMOUNT_SUM, MIN_TS, MAX_TS). stage=SOURCE로 저장하고 /validation/start 응답으로 돌려준다
    source_metrics: dict[str, Message] = Field(default_factory=dict, max_length=200)
    source_metrics_version: ShortText = "v1"  # 원천 지표 쿼리 버전(load_validation.query_version)
    partitions: list[ManifestPartition] = Field(min_length=1, max_length=10_000)


class ManifestResponse(ApiModel):
    """Worker로 보낼 파티션 목록(0건 파티션 제외)."""

    run_id: str
    status: RunStatus  # EXTRACTING, 모든 파티션이 0건이면 EXTRACTED_VALIDATED
    dispatch_partitions: list[ManifestPartition]  # Worker로 보낼 파티션(예상 건수 > 0)
    empty_partition_count: int  # 예상 0건이라 바로 SUCCESS로 등록한 파티션 수
    validation_scheduled: bool  # 이 호출로 검증 호출을 예약했으면 true(재요청 응답은 false)


class RunFailRequest(ApiModel):
    """파티션 외 단계 실패 보고."""

    expected_status: RunStatus  # 호출자가 아는 현재 상태. 다르면 409 RUN_STATUS_MISMATCH
    fail_status: RunStatus  # 바꿀 실패 상태. (expected, fail) 조합은 domain.ALLOWED_RUN_FAILURES만 허용
    error_stage: ShortText  # 실패한 단계 이름
    error_code: ShortText
    message: Message = ""


class RunFailResponse(ApiModel):
    """실패 처리 결과."""

    run_id: str
    run_status: RunStatus
    changed: bool  # 이미 fail_status였으면(재요청) false


class PartitionSummary(ApiModel):
    """조회용 파티션 요약."""

    partition_id: str
    status: PartitionStatus
    expected_row_count: int  # manifest의 예상 건수
    actual_row_count: int | None  # SUCCESS 판정 때 기록한 chunk 건수 합계
    file_count: int | None  # SUCCESS 판정 때 기록한 chunk(파일) 수
    attempt_count: int  # claim 횟수
    worker_node: str | None  # 마지막으로 claim한 NiFi 노드
    error_code: str | None


class DispatchSummary(ApiModel):
    """조회용 dispatch 요약."""

    dispatch_id: str
    dispatch_type: str  # VALIDATE_RUN(검증 시작) 또는 REISSUE_PARTITION(파티션 재발행)
    partition_id: str | None  # REISSUE_PARTITION일 때만
    status: str  # PENDING, SENT, ACKED, DEAD
    attempt_count: int  # 전송 시도 횟수(운영자 재전송 때 0으로 초기화)
    sent_at: datetime | None  # NiFi가 2xx로 받은 시각
    acked_at: datetime | None  # 검증 시작·재발행 claim으로 수신이 확인된 시각


class RunDetail(ApiModel):
    """run 상세 조회 결과."""

    run_id: str
    job_key: str
    business_key: str
    status: RunStatus
    snapshot_scn: str | None  # manifest의 Oracle SCN(문자열, 정밀도 보존)
    source_count: int | None  # manifest의 원천 건수(manifest 전에는 null)
    expected_partition_count: int | None  # manifest의 파티션 수
    success_partition_count: int
    failed_partition_count: int
    extracted_count: int  # 추출 완료(EXTRACTED_VALIDATED) 때 기록한 파티션 건수 합계. 그 전에는 0
    staging_count: int | None  # STAGE_COUNT 지표(staging 검증 통과 때 기록)
    target_count: int | None  # SUCCESS 때 기록한 target 건수
    hdfs_run_path: str | None
    stage_table: str | None
    started_at: datetime  # run 생성 시각(run_timeout 기준)
    heartbeat_at: datetime  # 마지막 상태 변화·claim·보고 시각
    extract_completed_at: datetime | None  # EXTRACTED_VALIDATED가 된 시각
    completed_at: datetime | None  # SUCCESS·실패로 끝난 시각
    error_stage: str | None
    error_code: str | None
    error_message: str | None
    partition_counts: dict[str, int]  # 파티션 상태별 개수
    partitions: list[PartitionSummary]
    dispatches: list[DispatchSummary]  # 생성 순


class RunListItem(ApiModel):
    """run 목록 항목. 필드 의미는 RunDetail과 같다(파티션·dispatch 목록 제외)."""

    run_id: str
    job_key: str
    business_key: str
    status: RunStatus
    source_count: int | None
    extracted_count: int
    staging_count: int | None
    target_count: int | None
    expected_partition_count: int | None
    success_partition_count: int
    failed_partition_count: int
    started_at: datetime
    heartbeat_at: datetime
    completed_at: datetime | None
    error_code: str | None
