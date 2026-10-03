"""게시 소유권과 결과. INSERT OVERWRITE는 token 소유자 1명만 실행한다.

상태 흐름: STAGING_VALIDATED → PUBLISHING(claim) → PUBLISHED | FAILED_PUBLISH | PUBLISH_UNKNOWN(result).
PUBLISH_UNKNOWN은 게시가 반영됐는지 알 수 없는 상태라 자동으로 끝내지 않고 운영자가 resolve_unknown으로
PUBLISHED 또는 FAILED_PUBLISH로 확정한다. sweeper도 publish_stale 동안 결과가 없으면
PUBLISH_UNKNOWN으로 바꾼다.
모든 함수는 load_run 행을 먼저 잠근다.
"""

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
    """run 행을 잠가 돌려준다. 없으면 NotFound(RUN_NOT_FOUND)."""
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    return run


async def claim(conn: AsyncConnection, run_id: UUID, req: PublishClaimRequest) -> PublishClaimResponse:
    """STAGING_VALIDATED → PUBLISHING. 같은 token 재요청은 claimed=true(응답 유실 재시도).

    NiFi가 만든 publish_token을 저장하고 publish_started_at을 기록한 뒤 PUBLISH_STARTED 이벤트를 남긴다.
    이미 PUBLISHING이면 저장된 token과 비교해 같으면 claimed=true, 다르면 claimed=false(다른 소유자)다.
    그 밖의 상태에서는 CAS가 실패해 claimed=false를 돌려준다. 거절은 예외가 아니라 응답 값이며,
    claimed=false를 받은 NiFi는 INSERT OVERWRITE를 실행하지 않는다. 오류는 NotFound(RUN_NOT_FOUND)뿐이다.
    """
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

    PUBLISHING → outcome CAS다. PUBLISHED면 published_at을, 실패·불명이면 error_stage=PUBLISH와 오류 코드·
    메시지를 남긴다. FAILED_PUBLISH만 completed_at을 기록한다(끝난 상태). PUBLISH_UNKNOWN은 아직 끝난 것이
    아니므로 completed_at을 두지 않는다. 결과에 맞는 이벤트(_RESULT_EVENTS)를 남긴다.
    이미 같은 outcome이면 changed=False로 성공 응답한다(멱등).

    Raises:
        NotFound: RUN_NOT_FOUND.
        Conflict: PUBLISH_TOKEN_MISMATCH(claim한 소유자가 아님), RUN_STATUS_MISMATCH(PUBLISHING이 아님.
            예: sweeper가 이미 PUBLISH_UNKNOWN으로 바꾼 뒤 늦게 온 결과).
    """
    run = await _lock(conn, run_id)
    token = await runs.get_publish_token(conn, run_id)
    if token != req.publish_token:
        log.warning("publish_result_token_mismatch", runId=str(run_id), outcome=req.outcome)
        raise Conflict("PUBLISH_TOKEN_MISMATCH")
    outcome = RunStatus(req.outcome)
    # token이 맞는 상태에서만 멱등 판정을 한다. 다른 소유자의 보고는 위에서 이미 거절됐다.
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
    """운영자가 Hive 이력과 target 지표를 확인한 뒤 PUBLISH_UNKNOWN을 확정한다.

    PUBLISH_UNKNOWN → PUBLISHED(published_at 기록, 오류 필드 초기화, 이후 target 검증으로 SUCCESS까지
    진행) 또는
    FAILED_PUBLISH(completed_at, error_code=PUBLISH_UNKNOWN_RESOLVED, error_message=사유) CAS다.
    PUBLISH_UNKNOWN_RESOLVED(WARN) 이벤트에 결정과 operator 역할을 남긴다. 이미 같은 상태면 changed=False.
    publish token은 확인하지 않는다(운영자 권한으로 호출).

    Raises:
        NotFound: RUN_NOT_FOUND.
        Conflict: RUN_STATUS_MISMATCH. PUBLISH_UNKNOWN이 아닌 run.
    """
    run = await _lock(conn, run_id)
    to = RunStatus(req.resolution)
    if run.status == to:
        return PublishResultResponse(run_status=to, changed=False)
    if run.status != RunStatus.PUBLISH_UNKNOWN:
        raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status)
    # PUBLISHED로 확정하면 PUBLISH_UNKNOWN 때 남긴 오류(PUBLISH_STALE 등)를 지운다. 이후 SUCCESS가 된
    # run에 오류 코드가 남아 실패처럼 보이지 않게 하기 위해서다. 경위는 PUBLISH_UNKNOWN과
    # PUBLISH_UNKNOWN_RESOLVED 이벤트에 남는다.
    sets: dict[str, object] = ({"published_at": runs.NOW, "error_stage": None, "error_code": None,
                                "error_message": None} if to == RunStatus.PUBLISHED
                               else {"completed_at": runs.NOW, "error_code": "PUBLISH_UNKNOWN_RESOLVED",
                                     "error_message": req.reason})
    await runs.cas_status(conn, run_id, expected=RunStatus.PUBLISH_UNKNOWN, to=to, **sets)
    await events.record(conn, "PUBLISH_UNKNOWN_RESOLVED", run, level="WARN", message=req.reason,
                        details={"resolution": req.resolution, "operatorRole": operator})
    log.warning("publish_unknown_resolved", runId=str(run_id), resolution=req.resolution,
                operatorRole=operator, reason=req.reason[:300])
    return PublishResultResponse(run_status=to, changed=True)
