from typing import Any, Literal
from uuid import UUID

from pydantic import Field

from load_control.domain import RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class ValidationStartRequest(ApiModel):
    dispatch_id: UUID
    node: str = Field(default="", max_length=200)


class ValidationStartResponse(ApiModel):
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
    metric_name: str = Field(min_length=1, max_length=150, pattern=r"^[A-Za-z0-9_.:\-]+$")
    expected_value: Message | None = None
    actual_value: Message | None = None
    tolerance: ShortText | None = None
    result: Literal["PASS", "FAIL", "WARN"]
    details: dict[str, Any] = Field(default_factory=dict)


class ValidationsRequest(ApiModel):
    stage: Literal["STAGING", "TARGET"]
    query_version: ShortText = "v1"
    metrics: list[Metric] = Field(min_length=1, max_length=500)


class ValidationsResponse(ApiModel):
    recorded: int
    fail_count: int
    run_status: RunStatus


class StageValidatedResponse(ApiModel):
    stage_validated: bool
    run_status: RunStatus
    reasons: list[str] = Field(default_factory=list)


class SuccessRequest(ApiModel):
    target_count: int | None = Field(default=None, ge=0)


class SuccessResponse(ApiModel):
    success: bool
    run_status: RunStatus
    reasons: list[str] = Field(default_factory=list)
