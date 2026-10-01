"""게시 소유권과 결과(가이드 11장, API 설계 5.2). INSERT OVERWRITE는 token 소유자 1명만 실행한다."""

from uuid import UUID

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
        return PublishClaimResponse(claimed=token == req.publish_token, run_status=RunStatus.PUBLISHING)
    if not await runs.cas_status(conn, run_id, expected=RunStatus.STAGING_VALIDATED,
                                 to=RunStatus.PUBLISHING, publish_token=req.publish_token,
                                 publish_started_at=runs.NOW):
        return PublishClaimResponse(claimed=False, run_status=RunStatus(run.status))
    await events.record(conn, "PUBLISH_STARTED", run, details={"publishToken": str(req.publish_token)})
    return PublishClaimResponse(claimed=True, run_status=RunStatus.PUBLISHING)


async def result(conn: AsyncConnection, run_id: UUID, req: PublishResultRequest) -> PublishResultResponse:
    run = await _lock(conn, run_id)
    token = await runs.get_publish_token(conn, run_id)
    if token != req.publish_token:
        raise Conflict("PUBLISH_TOKEN_MISMATCH")
    outcome = RunStatus(req.outcome)
    if run.status == outcome:  # 멱등 재요청
        return PublishResultResponse(run_status=outcome, changed=False)
    if run.status != RunStatus.PUBLISHING:
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
    return PublishResultResponse(run_status=outcome, changed=True)


async def resolve_unknown(conn: AsyncConnection, run_id: UUID, req: PublishUnknownResolveRequest,
                          operator: str) -> PublishResultResponse:
    """운영자가 Hive 이력과 target 지표를 확인한 뒤 PUBLISH_UNKNOWN을 확정한다(가이드 11장)."""
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
    return PublishResultResponse(run_status=to, changed=True)
