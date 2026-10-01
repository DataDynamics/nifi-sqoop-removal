"""검증 엔드포인트 모델."""

from typing import Any, Literal
from uuid import UUID

from pydantic import Field

from load_control.domain import RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class ValidationStartRequest(ApiModel):
    """검증 시작 요청. dispatchId는 PG-05가 받은 값."""

    dispatch_id: UUID
    node: str = Field(default="", max_length=200)


class ValidationStartResponse(ApiModel):
    """검증 시작 결과. started=true일 때만 검증에 필요한 값이 채워진다."""

    started: bool
    run_status: RunStatus
    job_key: str | None = None
    business_key: str | None = None
    snapshot_scn: str | None = None
    hdfs_run_path: str | None = None
    stage_table: str | None = None
    source_count: int | None = None
    extracted_count: int | None = None
    source_metrics: dict[str, str | None] = Field(default_factory=dict)


class Metric(ApiModel):
    """지표 하나. 판정(PASS/FAIL/WARN)은 NiFi가 하고, API는 FAIL이 있는지만 다시 본다."""

    metric_name: str = Field(min_length=1, max_length=150, pattern=r"^[A-Za-z0-9_.:\-]+$")
    expected_value: Message | None = None
    actual_value: Message | None = None
    tolerance: ShortText | None = None
    result: Literal["PASS", "FAIL", "WARN"]
    details: dict[str, Any] = Field(default_factory=dict)


class ValidationsRequest(ApiModel):
    """stage별 지표 묶음."""

    stage: Literal["STAGING", "TARGET"]
    query_version: ShortText = "v1"
    metrics: list[Metric] = Field(min_length=1, max_length=500)


class ValidationsResponse(ApiModel):
    """지표 저장 결과."""

    recorded: int
    fail_count: int
    run_status: RunStatus


class StageValidatedResponse(ApiModel):
    """staging 통과 판정 결과."""

    stage_validated: bool
    run_status: RunStatus
    reasons: list[str] = Field(default_factory=list)


class SuccessRequest(ApiModel):
    """최종 성공 요청."""

    target_count: int | None = Field(default=None, ge=0)


class SuccessResponse(ApiModel):
    """최종 성공 판정 결과."""

    success: bool
    run_status: RunStatus
    reasons: list[str] = Field(default_factory=list)
