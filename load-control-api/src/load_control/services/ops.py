"""운영자 작업: dispatch 재전송."""

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.errors import Conflict, NotFound
from load_control.repositories import dispatch, events, runs
from load_control.schemas.publish import DispatchResendResponse

log = structlog.get_logger(__name__)


async def resend_dispatch(conn: AsyncConnection, run_id: UUID, dispatch_id: UUID) -> DispatchResendResponse:
    """DEAD(또는 ACK 없는 SENT) dispatch를 PENDING으로 되돌려 다시 보내게 한다."""
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    current = await dispatch.get_for_update(conn, dispatch_id)
    if current is None or current[0] != run_id:
        raise NotFound("DISPATCH_NOT_FOUND")
    if not await dispatch.resend(conn, run_id, dispatch_id):
        raise Conflict("DISPATCH_STATUS_MISMATCH", status=current[2])
    await events.record(conn, "DISPATCH_RESENT", run, level="WARN",
                        details={"dispatchId": str(dispatch_id), "previousStatus": current[2]})
    log.warning("dispatch_resent_by_operator", runId=str(run_id), dispatchId=str(dispatch_id),
                previousStatus=current[2])
    return DispatchResendResponse(dispatch_id=str(dispatch_id), status="PENDING")
