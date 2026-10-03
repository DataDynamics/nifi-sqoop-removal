"""검증 엔드포인트 모델."""

from typing import Any, Literal
from uuid import UUID

from pydantic import Field

from load_control.domain import RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class ValidationStartRequest(ApiModel):
    """검증 시작 요청. dispatchId는 PG-05가 받은 값."""

    dispatch_id: UUID  # PG-05가 받은 X-Dispatch-Id. 이 run의 VALIDATE_RUN dispatch여야 한다
    node: str = Field(default="", max_length=200)  # 검증을 실행하는 NiFi 노드 이름(추적용)


class ValidationStartResponse(ApiModel):
    """검증 시작 결과. started=true일 때만 검증에 필요한 값이 채워진다."""

    started: bool  # false면 중복 요청이다. NiFi는 FlowFile을 끝낸다
    run_status: RunStatus
    job_key: str | None = None
    business_key: str | None = None
    snapshot_scn: str | None = None
    hdfs_run_path: str | None = None  # 검증할 Parquet 파일 위치
    stage_table: str | None = None  # 만들고 검증할 staging table
    source_count: int | None = None  # 기대 건수(manifest의 원천 건수)
    extracted_count: int | None = None  # chunk 보고 기준 추출 건수 합계
    # manifest 때 저장한 SOURCE 지표(SOURCE_COUNT와 sourceMetrics). staging 지표의 기대값으로 쓴다
    source_metrics: dict[str, str | None] = Field(default_factory=dict)


class Metric(ApiModel):
    """지표 하나. 판정(PASS/FAIL/WARN)은 NiFi가 하고, API는 FAIL이 있는지만 다시 본다."""

    # 지표 이름(예: STAGE_COUNT, TARGET_COUNT, AMOUNT_SUM).
    # STAGE_COUNT·TARGET_COUNT 값은 run의 staging·target 건수로도 기록한다
    metric_name: str = Field(min_length=1, max_length=150, pattern=r"^[A-Za-z0-9_.:\-]+$")
    expected_value: Message | None = None  # NiFi가 비교한 기대값(기록용)
    actual_value: Message | None = None  # 측정값
    tolerance: ShortText | None = None  # 허용 오차 표기(NiFi 판정 근거, 기록용)
    result: Literal["PASS", "FAIL", "WARN"]  # WARN은 통과로 본다. FAIL이 하나라도 있으면 다음 단계 거절
    details: dict[str, Any] = Field(default_factory=dict)  # 추가 정보(jsonb 그대로 저장)


class ValidationsRequest(ApiModel):
    """stage별 지표 묶음."""

    stage: Literal["STAGING", "TARGET"]  # STAGING은 STAGE_VALIDATING, TARGET은 PUBLISHED 상태에서만 받는다
    query_version: ShortText = "v1"  # 검증 쿼리 버전. (stage, metricName, queryVersion)이 같으면 덮어쓴다
    metrics: list[Metric] = Field(min_length=1, max_length=500)


class ValidationsResponse(ApiModel):
    """지표 저장 결과."""

    recorded: int  # 저장한 지표 수
    fail_count: int  # 그중 FAIL 수
    run_status: RunStatus  # 지표 저장은 상태를 바꾸지 않는다


class StageValidatedResponse(ApiModel):
    """staging 통과 판정 결과."""

    stage_validated: bool
    run_status: RunStatus
    # 통과 못 한 이유: NO_METRICS, FAIL <지표>, RUN_STATUS <상태>
    reasons: list[str] = Field(default_factory=list)


class SuccessRequest(ApiModel):
    """최종 성공 요청."""

    target_count: int | None = Field(default=None, ge=0)  # target 건수. 없으면 TARGET_COUNT 지표 값을 쓴다


class SuccessResponse(ApiModel):
    """최종 성공 판정 결과."""

    success: bool
    run_status: RunStatus
    # 통과 못 한 이유: NO_METRICS, FAIL <지표>, RUN_STATUS <상태>
    reasons: list[str] = Field(default_factory=list)
