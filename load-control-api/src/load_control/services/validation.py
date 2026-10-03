"""검증 flow의 시작, 지표 기록, staging 통과 및 최종 성공을 처리한다.

상태는 `EXTRACTED_VALIDATED → STAGE_VALIDATING → STAGING_VALIDATED → (게시) →
PUBLISHED → SUCCESS` 순서로 전이한다. 검증 통과 여부는 NiFi가 보낸 결론을 신뢰하지 않고
`load_validation.result`에 저장된 지표로 다시 판정한다. 상태를 다루는 함수는 먼저 `load_run` 행을
잠근다.
"""

from uuid import UUID

import structlog
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

log = structlog.get_logger(__name__)

# 단계별로 지표를 받을 수 있는 run 상태. STAGING 지표는 검증 중에, TARGET 지표는 게시를 마친 뒤에만
# 기록할 수 있다.
_STAGE_STATUS = {"STAGING": RunStatus.STAGE_VALIDATING, "TARGET": RunStatus.PUBLISHED}


async def _lock(conn: AsyncConnection, run_id: UUID) -> RunRow:
    """run 행을 잠가 돌려준다. 없으면 NotFound(RUN_NOT_FOUND)."""
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    return run


async def start(conn: AsyncConnection, run_id: UUID, req: ValidationStartRequest) -> ValidationStartResponse:
    """검증을 시작하며 `EXTRACTED_VALIDATED → STAGE_VALIDATING`으로 전이한다.

    `load_run → load_dispatch` 순서로 잠근다. 요청의 `dispatchId`가 이 run의 `VALIDATE_RUN`인지
    확인한 뒤, 상태 전이 성공 여부와 관계없이 dispatch를 `ACKED`로 바꾼다. 이는 호출이 NiFi에
    도착했다는 뜻이므로 outbox 재전송을 멈춰도 되기 때문이다.

    첫 호출만 `started=true`를 반환하고 `STAGE_VALIDATION_STARTED` 이벤트를 남긴다. 중복 호출은
    `started=false`와 현재 상태만 반환한다.

    Raises:
        NotFound: RUN_NOT_FOUND.
        Conflict: DISPATCH_MISMATCH. dispatch가 없거나 다른 run 것이거나 VALIDATE_RUN이 아님.
    """
    run = await _lock(conn, run_id)
    d = await dispatch.get_for_update(conn, req.dispatch_id)
    if d is None or d[0] != run_id or d[1] != "VALIDATE_RUN":
        log.warning("validation_start_dispatch_mismatch", runId=str(run_id), dispatchId=str(req.dispatch_id))
        raise Conflict("DISPATCH_MISMATCH")
    await dispatch.ack(conn, req.dispatch_id)

    if not await runs.cas_status(conn, run_id, expected=RunStatus.EXTRACTED_VALIDATED,
                                 to=RunStatus.STAGE_VALIDATING):
        # outbox 재전송·LB 재시도로 같은 run의 검증 요청이 또 왔다. NiFi는 이 FlowFile을 끝낸다.
        log.info("validation_start_duplicate", runId=str(run_id), runStatus=run.status,
                 dispatchId=str(req.dispatch_id), node=req.node)
        return ValidationStartResponse(started=False, run_status=RunStatus(run.status))

    metrics = await validations.list_by_stage(conn, run_id, "SOURCE")
    await events.record(conn, "STAGE_VALIDATION_STARTED", run,
                        details={"dispatchId": str(req.dispatch_id), "node": req.node})
    log.info("validation_started", runId=str(run_id), dispatchId=str(req.dispatch_id), node=req.node)
    return ValidationStartResponse(
        started=True, run_status=RunStatus.STAGE_VALIDATING, job_key=run.job_key,
        business_key=run.business_key,
        snapshot_scn=str(run.snapshot_scn) if run.snapshot_scn is not None else None,
        hdfs_run_path=run.hdfs_run_path, stage_table=run.stage_table_name,
        source_count=run.source_count, extracted_count=run.extracted_count,
        source_metrics={m["metric_name"]: m["actual_value"] for m in metrics})


async def record(conn: AsyncConnection, run_id: UUID, req: ValidationsRequest) -> ValidationsResponse:
    """검증 flow가 측정한 지표를 저장한다. 같은 (stage, metric, queryVersion)은 덮어쓴다(NiFi 재시도).

    run 상태는 바꾸지 않고 heartbeat만 갱신한다. FAIL 지표가 있으면 {stage}_METRIC_FAILED(WARN) 이벤트를
    남긴다. 통과 여부는 이후 stage_validated·succeed가 저장된 지표로 판정한다.

    Raises:
        NotFound: RUN_NOT_FOUND.
        Conflict: RUN_STATUS_MISMATCH. stage에 맞는 상태가 아님(_STAGE_STATUS).
    """
    run = await _lock(conn, run_id)
    if run.status != _STAGE_STATUS[req.stage]:
        log.warning("validations_rejected", runId=str(run_id), stage=req.stage, runStatus=run.status)
        raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status, stage=req.stage)
    await validations.upsert_many(conn, run_id, req.stage, req.query_version, [
        m.model_dump() for m in req.metrics])
    await runs.touch(conn, run_id)
    fails = [m.metric_name for m in req.metrics if m.result == "FAIL"]
    if fails:
        await events.record(conn, f"{req.stage}_METRIC_FAILED", run, level="WARN",
                            message=", ".join(fails)[:2000])
        log.warning("validation_metrics_failed", runId=str(run_id), stage=req.stage, failed=fails)
    log.info("validations_recorded", runId=str(run_id), stage=req.stage, recorded=len(req.metrics),
             failCount=len(fails), queryVersion=req.query_version)
    return ValidationsResponse(recorded=len(req.metrics), fail_count=len(fails),
                               run_status=RunStatus(run.status))


def _judge(metrics: list[dict[str, object]]) -> list[str]:
    """저장된 지표를 검사해 불통과 사유를 반환한다.

    지표가 없으면 검증을 건너뛴 것으로 보고 `NO_METRICS`를 반환한다. 여러 `query_version`의 지표가
    섞여 있어도 모두 검사하며, `FAIL`인 지표는 각각 사유에 포함한다. 빈 목록을 반환하면 통과다.
    """
    if not metrics:
        return ["NO_METRICS"]
    return [f"FAIL {m['metric_name']}" for m in metrics if m["result"] == "FAIL"]


def _count(metrics: list[dict[str, object]], name: str) -> int | None:
    """name 지표의 actual_value를 정수로 돌려준다. 없거나 숫자 문자열이 아니면 None.

    같은 이름이 여러 query_version으로 있으면 목록 순서상 첫 번째 값을 쓴다.
    """
    for m in metrics:
        value = m.get("actual_value")
        if m["metric_name"] == name and isinstance(value, str) and value.isdigit():
            return int(value)
    return None


async def stage_validated(conn: AsyncConnection, run_id: UUID) -> StageValidatedResponse:
    """저장된 STAGING 지표가 모두 통과했을 때만 `STAGING_VALIDATED`로 전이한다.

    통과하면 `STAGE_COUNT`를 `staging_count`에 저장하고 `STAGE_VALIDATED` 이벤트를 남긴다. 이미
    전이된 요청은 성공으로 응답한다. 상태가 맞지 않거나 지표가 통과하지 못하면 예외를 내지 않고
    `stage_validated=false`와 사유를 반환한다. 이 함수는 run을 실패로 바꾸지 않는다. 실패 확정은
    NiFi가 `/fail` 엔드포인트로 요청한다.
    """
    run = await _lock(conn, run_id)
    if run.status == RunStatus.STAGING_VALIDATED:
        return StageValidatedResponse(stage_validated=True, run_status=RunStatus.STAGING_VALIDATED)
    if run.status != RunStatus.STAGE_VALIDATING:
        return StageValidatedResponse(stage_validated=False, run_status=RunStatus(run.status),
                                      reasons=[f"RUN_STATUS {run.status}"])
    metrics = await validations.list_by_stage(conn, run_id, "STAGING")
    reasons = _judge(metrics)
    if reasons:
        log.warning("stage_validation_not_passed", runId=str(run_id), reasons=reasons)
        return StageValidatedResponse(stage_validated=False, run_status=RunStatus(run.status),
                                      reasons=reasons)
    await runs.cas_status(conn, run_id, expected=RunStatus.STAGE_VALIDATING,
                          to=RunStatus.STAGING_VALIDATED,
                          staging_count=_count(metrics, "STAGE_COUNT"))
    await events.record(conn, "STAGE_VALIDATED", run, row_count=_count(metrics, "STAGE_COUNT"))
    log.info("stage_validated", runId=str(run_id), stageCount=_count(metrics, "STAGE_COUNT"),
             metrics=len(metrics))
    return StageValidatedResponse(stage_validated=True, run_status=RunStatus.STAGING_VALIDATED)


async def succeed(conn: AsyncConnection, run_id: UUID, req: SuccessRequest) -> SuccessResponse:
    """저장된 TARGET 지표가 모두 통과했을 때만 `PUBLISHED → SUCCESS`로 전이한다.

    `target_count`는 요청값을 우선 사용하고, 없으면 `TARGET_COUNT` 지표에서 가져온다. 성공 시
    `completed_at`과 `RUN_SUCCESS` 이벤트를 기록한다. 이미 성공한 요청은 성공으로 응답한다. 상태가
    맞지 않거나 지표가 통과하지 못하면 `success=false`와 사유를 반환하며, 실패 확정은 NiFi가
    `/fail` 엔드포인트로 요청한다.
    """
    run = await _lock(conn, run_id)
    if run.status == RunStatus.SUCCESS:
        return SuccessResponse(success=True, run_status=RunStatus.SUCCESS)
    if run.status != RunStatus.PUBLISHED:
        return SuccessResponse(success=False, run_status=RunStatus(run.status),
                               reasons=[f"RUN_STATUS {run.status}"])
    metrics = await validations.list_by_stage(conn, run_id, "TARGET")
    reasons = _judge(metrics)
    if reasons:
        log.warning("target_validation_not_passed", runId=str(run_id), reasons=reasons)
        return SuccessResponse(success=False, run_status=RunStatus(run.status), reasons=reasons)
    target_count = req.target_count if req.target_count is not None else _count(metrics, "TARGET_COUNT")
    await runs.cas_status(conn, run_id, expected=RunStatus.PUBLISHED, to=RunStatus.SUCCESS,
                          target_count=target_count, completed_at=runs.NOW)
    await events.record(conn, "RUN_SUCCESS", run, row_count=target_count)
    log.info("run_success", runId=str(run_id), jobKey=run.job_key, businessKey=run.business_key,
             targetCount=target_count, sourceCount=run.source_count)
    return SuccessResponse(success=True, run_status=RunStatus.SUCCESS)
