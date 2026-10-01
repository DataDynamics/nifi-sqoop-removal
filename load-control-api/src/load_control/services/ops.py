"""운영자 작업(API 설계 5.2): dispatch 재전송."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.errors import Conflict, NotFound
from load_control.repositories import dispatch, events, runs
from load_control.schemas.publish import DispatchResendResponse


async def resend_dispatch(conn: AsyncConnection, run_id: UUID, dispatch_id: UUID) -> DispatchResendResponse:
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
    return DispatchResendResponse(dispatch_id=str(dispatch_id), status="PENDING")
