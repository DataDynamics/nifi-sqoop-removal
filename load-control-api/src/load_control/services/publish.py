"""게시 소유권과 결과. INSERT OVERWRITE는 token 소유자 1명만 실행한다."""

import logging
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.domain import RunStatus
from load_control.errors import Conflict, NotFound
from load_control.repositories import events, runs
from load_control.repositories.runs import RunRow
from load_control.schemas.publish import (
    PublishClaimRequest,
    PublishClaimResponse,
    PublishResultRequest,
    PublishResultResponse,
    PublishUnknownResolveRequest,
)

log = structlog.get_logger(__name__)

# 게시 결과별 load_event 이름과 수준
_RESULT_EVENTS = {
    RunStatus.PUBLISHED: ("PUBLISH_FINISHED", "INFO"),
    RunStatus.FAILED_PUBLISH: ("RUN_FAILED", "ERROR"),
    RunStatus.PUBLISH_UNKNOWN: ("PUBLISH_UNKNOWN", "ERROR"),
}


async def _lock(conn: AsyncConnection, run_id: UUID) -> RunRow:
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    return run


async def claim(conn: AsyncConnection, run_id: UUID, req: PublishClaimRequest) -> PublishClaimResponse:
    """STAGING_VALIDATED → PUBLISHING. 같은 token 재요청은 claimed=true(응답 유실 재시도)."""
    run = await _lock(conn, run_id)
    if run.status == RunStatus.PUBLISHING:
        token = await runs.get_publish_token(conn, run_id)
        same = token == req.publish_token
        log.info("publish_claim_replayed" if same else "publish_claim_refused", runId=str(run_id),
                 reason=None if same else "other_owner")
        return PublishClaimResponse(claimed=same, run_status=RunStatus.PUBLISHING)
    if not await runs.cas_status(conn, run_id, expected=RunStatus.STAGING_VALIDATED,
                                 to=RunStatus.PUBLISHING, publish_token=req.publish_token,
                                 publish_started_at=runs.NOW):
        log.info("publish_claim_refused", runId=str(run_id), reason="status", runStatus=run.status)
        return PublishClaimResponse(claimed=False, run_status=RunStatus(run.status))
    await events.record(conn, "PUBLISH_STARTED", run, details={"publishToken": str(req.publish_token)})
    log.info("publish_claimed", runId=str(run_id), jobKey=run.job_key, businessKey=run.business_key)
    return PublishClaimResponse(claimed=True, run_status=RunStatus.PUBLISHING)


async def result(conn: AsyncConnection, run_id: UUID, req: PublishResultRequest) -> PublishResultResponse:
    """INSERT OVERWRITE 결과를 기록한다. token 소유자만 보고할 수 있다.

    PUBLISH_UNKNOWN은 이후 자동으로 확정할 수 없다. 운영자가 resolve_unknown으로 확정한다.
    """
    run = await _lock(conn, run_id)
    token = await runs.get_publish_token(conn, run_id)
    if token != req.publish_token:
        log.warning("publish_result_token_mismatch", runId=str(run_id), outcome=req.outcome)
        raise Conflict("PUBLISH_TOKEN_MISMATCH")
    outcome = RunStatus(req.outcome)
    if run.status == outcome:  # 멱등 재요청
        log.debug("publish_result_replayed", runId=str(run_id), outcome=req.outcome)
        return PublishResultResponse(run_status=outcome, changed=False)
    if run.status != RunStatus.PUBLISHING:
        log.warning("publish_result_rejected", runId=str(run_id), outcome=req.outcome, runStatus=run.status)
        raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status)

    sets: dict[str, object] = {}
    if outcome == RunStatus.PUBLISHED:
        sets["published_at"] = runs.NOW
    else:
        sets.update(error_stage="PUBLISH", error_code=req.error_code or req.outcome,
                    error_message=req.message[:2000] or None)
        if outcome == RunStatus.FAILED_PUBLISH:
            sets["completed_at"] = runs.NOW
    await runs.cas_status(conn, run_id, expected=RunStatus.PUBLISHING, to=outcome, **sets)
    name, level = _RESULT_EVENTS[outcome]
    await events.record(conn, name, run, level=level, error_code=req.error_code,
                        message=req.message or None)
    log.log(logging.INFO if outcome == RunStatus.PUBLISHED else logging.ERROR, "publish_result",
            runId=str(run_id), outcome=req.outcome, errorCode=req.error_code, errorMessage=req.message[:300])
    return PublishResultResponse(run_status=outcome, changed=True)


async def resolve_unknown(conn: AsyncConnection, run_id: UUID, req: PublishUnknownResolveRequest,
                          operator: str) -> PublishResultResponse:
    """운영자가 Hive 이력과 target 지표를 확인한 뒤 PUBLISH_UNKNOWN을 확정한다."""
    run = await _lock(conn, run_id)
    to = RunStatus(req.resolution)
    if run.status == to:
        return PublishResultResponse(run_status=to, changed=False)
    if run.status != RunStatus.PUBLISH_UNKNOWN:
        raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status)
    sets: dict[str, object] = ({"published_at": runs.NOW} if to == RunStatus.PUBLISHED
                               else {"completed_at": runs.NOW, "error_code": "PUBLISH_UNKNOWN_RESOLVED",
                                     "error_message": req.reason})
    await runs.cas_status(conn, run_id, expected=RunStatus.PUBLISH_UNKNOWN, to=to, **sets)
    await events.record(conn, "PUBLISH_UNKNOWN_RESOLVED", run, level="WARN", message=req.reason,
                        details={"resolution": req.resolution, "operatorRole": operator})
    log.warning("publish_unknown_resolved", runId=str(run_id), resolution=req.resolution,
                operatorRole=operator, reason=req.reason[:300])
    return PublishResultResponse(run_status=to, changed=True)
