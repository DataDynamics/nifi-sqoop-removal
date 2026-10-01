"""검증 flow 연동: 시작, 지표 기록, staging 통과, 최종 성공(API 설계 4.4, 5장)."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.domain import RunStatus
from load_control.errors import Conflict, NotFound
from load_control.repositories import dispatch, events, runs, validations
from load_control.repositories.runs import RunRow
from load_control.schemas.validation import (
    StageValidatedResponse,
    SuccessRequest,
    SuccessResponse,
    ValidationsRequest,
    ValidationsResponse,
    ValidationStartRequest,
    ValidationStartResponse,
)

# stage별로 지표를 받을 수 있는 run 상태
_STAGE_STATUS = {"STAGING": RunStatus.STAGE_VALIDATING, "TARGET": RunStatus.PUBLISHED}


async def _lock(conn: AsyncConnection, run_id: UUID) -> RunRow:
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    return run


async def start(conn: AsyncConnection, run_id: UUID, req: ValidationStartRequest) -> ValidationStartResponse:
    """EXTRACTED_VALIDATED → STAGE_VALIDATING CAS. 성공한 호출만 started=true(중복 dispatch 제거)."""
    run = await _lock(conn, run_id)
    d = await dispatch.get_for_update(conn, req.dispatch_id)
    if d is None or d[0] != run_id or d[1] != "VALIDATE_RUN":
        raise Conflict("DISPATCH_MISMATCH")
    await dispatch.ack(conn, req.dispatch_id)

    if not await runs.cas_status(conn, run_id, expected=RunStatus.EXTRACTED_VALIDATED,
                                 to=RunStatus.STAGE_VALIDATING):
        return ValidationStartResponse(started=False, run_status=RunStatus(run.status))

    metrics = await validations.list_by_stage(conn, run_id, "SOURCE")
    await events.record(conn, "STAGE_VALIDATION_STARTED", run,
                        details={"dispatchId": str(req.dispatch_id), "node": req.node})
    return ValidationStartResponse(
        started=True, run_status=RunStatus.STAGE_VALIDATING, job_key=run.job_key,
        business_key=run.business_key,
        snapshot_scn=str(run.snapshot_scn) if run.snapshot_scn is not None else None,
        hdfs_run_path=run.hdfs_run_path, stage_table=run.stage_table_name,
        source_count=run.source_count, extracted_count=run.extracted_count,
        source_metrics={m["metric_name"]: m["actual_value"] for m in metrics})


async def record(conn: AsyncConnection, run_id: UUID, req: ValidationsRequest) -> ValidationsResponse:
    run = await _lock(conn, run_id)
    if run.status != _STAGE_STATUS[req.stage]:
        raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status, stage=req.stage)
    await validations.upsert_many(conn, run_id, req.stage, req.query_version, [
        m.model_dump() for m in req.metrics])
    await runs.touch(conn, run_id)
    fails = [m.metric_name for m in req.metrics if m.result == "FAIL"]
    if fails:
        await events.record(conn, f"{req.stage}_METRIC_FAILED", run, level="WARN",
                            message=", ".join(fails)[:2000])
    return ValidationsResponse(recorded=len(req.metrics), fail_count=len(fails),
                               run_status=RunStatus(run.status))


def _judge(metrics: list[dict[str, object]]) -> list[str]:
    if not metrics:
        return ["NO_METRICS"]
    return [f"FAIL {m['metric_name']}" for m in metrics if m["result"] == "FAIL"]


def _count(metrics: list[dict[str, object]], name: str) -> int | None:
    for m in metrics:
        value = m.get("actual_value")
        if m["metric_name"] == name and isinstance(value, str) and value.isdigit():
            return int(value)
    return None


async def stage_validated(conn: AsyncConnection, run_id: UUID) -> StageValidatedResponse:
    """저장된 STAGING 지표가 모두 PASS일 때만 STAGING_VALIDATED. NiFi 판정을 그대로 믿지 않는다."""
    run = await _lock(conn, run_id)
    if run.status == RunStatus.STAGING_VALIDATED:
        return StageValidatedResponse(stage_validated=True, run_status=RunStatus.STAGING_VALIDATED)
    if run.status != RunStatus.STAGE_VALIDATING:
        return StageValidatedResponse(stage_validated=False, run_status=RunStatus(run.status),
                                      reasons=[f"RUN_STATUS {run.status}"])
    metrics = await validations.list_by_stage(conn, run_id, "STAGING")
    reasons = _judge(metrics)
    if reasons:
        return StageValidatedResponse(stage_validated=False, run_status=RunStatus(run.status),
                                      reasons=reasons)
    await runs.cas_status(conn, run_id, expected=RunStatus.STAGE_VALIDATING,
                          to=RunStatus.STAGING_VALIDATED,
                          staging_count=_count(metrics, "STAGE_COUNT"))
    await events.record(conn, "STAGE_VALIDATED", run, row_count=_count(metrics, "STAGE_COUNT"))
    return StageValidatedResponse(stage_validated=True, run_status=RunStatus.STAGING_VALIDATED)


async def succeed(conn: AsyncConnection, run_id: UUID, req: SuccessRequest) -> SuccessResponse:
    """저장된 TARGET 지표가 모두 PASS일 때만 PUBLISHED → SUCCESS."""
    run = await _lock(conn, run_id)
    if run.status == RunStatus.SUCCESS:
        return SuccessResponse(success=True, run_status=RunStatus.SUCCESS)
    if run.status != RunStatus.PUBLISHED:
        return SuccessResponse(success=False, run_status=RunStatus(run.status),
                               reasons=[f"RUN_STATUS {run.status}"])
    metrics = await validations.list_by_stage(conn, run_id, "TARGET")
    reasons = _judge(metrics)
    if reasons:
        return SuccessResponse(success=False, run_status=RunStatus(run.status), reasons=reasons)
    target_count = req.target_count if req.target_count is not None else _count(metrics, "TARGET_COUNT")
    await runs.cas_status(conn, run_id, expected=RunStatus.PUBLISHED, to=RunStatus.SUCCESS,
                          target_count=target_count, completed_at=runs.NOW)
    await events.record(conn, "RUN_SUCCESS", run, row_count=target_count)
    return SuccessResponse(success=True, run_status=RunStatus.SUCCESS)
