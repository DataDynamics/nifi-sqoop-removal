"""게시 소유권과 `INSERT OVERWRITE` 결과를 관리한다.

publish token을 가진 요청 하나만 실제 게시를 수행할 수 있다. 상태 흐름은 다음과 같다.

`STAGING_VALIDATED → PUBLISHING → PUBLISHED | FAILED_PUBLISH | PUBLISH_UNKNOWN`

`PUBLISH_UNKNOWN`은 게시 반영 여부를 알 수 없는 상태다. 자동으로 재시도하거나 종료하지 않으며,
운영자가 확인 후 `PUBLISHED` 또는 `FAILED_PUBLISH`로 확정한다. `publish_stale` 동안 결과가 없어도
sweeper가 같은 상태로 전환한다. 모든 함수는 먼저 `load_run` 행을 잠근다.
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
    """게시 소유권을 부여하며 `STAGING_VALIDATED → PUBLISHING`으로 전이한다.

    NiFi가 만든 token과 시작 시각을 저장하고 `PUBLISH_STARTED` 이벤트를 남긴다. 이미 게시 중이면
    저장된 token이 같은 요청만 `claimed=true`를 받는다. 다른 token이나 다른 run 상태는 정상적인
    경합으로 보고 `claimed=false`를 반환한다. 이 응답을 받은 NiFi는 `INSERT OVERWRITE`를 실행하지
    않는다.
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
    """token 소유자가 보고한 `INSERT OVERWRITE` 결과를 기록한다.

    `PUBLISHING`에서 요청한 결과로 CAS 전이한다. 성공이면 `published_at`을, 실패 또는 결과 불명이면
    오류 정보를 기록한다. 종료 상태인 `FAILED_PUBLISH`에만 `completed_at`을 기록한다.
    `PUBLISH_UNKNOWN`은 운영자가 확정해야 하므로 활성 상태와 `completed_at=NULL`을 유지한다.
    이미 같은 결과가 저장되어 있으면 멱등 재요청으로 보고 `changed=false`를 반환한다.

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
    """운영자가 확인한 게시 결과로 `PUBLISH_UNKNOWN`을 확정한다.

    운영자는 Hive 이력과 target 지표를 먼저 확인해야 한다. 확인 결과에 따라 다음 중 하나로 전이한다.

    - `PUBLISHED`: 게시 시각을 기록하고 이전 오류를 지운 뒤 target 검증을 계속한다.
    - `FAILED_PUBLISH`: 종료 시각과 확인 사유를 기록한다.

    결정과 operator 역할은 `PUBLISH_UNKNOWN_RESOLVED` 이벤트에 남긴다. 운영자 권한으로 확정하므로
    publish token은 확인하지 않는다. 이미 같은 상태면 `changed=false`를 반환한다.

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
    # 성공으로 확정한 run이 실패처럼 보이지 않도록 `PUBLISH_STALE` 등의 오류 필드를 지운다. 판단
    # 과정은 `PUBLISH_UNKNOWN`과 `PUBLISH_UNKNOWN_RESOLVED` 이벤트에 보존된다.
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
