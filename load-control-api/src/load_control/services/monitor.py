"""모니터(TUI)용 조회. 상태를 바꾸지 않는다."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.config import Settings
from load_control.errors import NotFound
from load_control.repositories import cleanup, dispatch, monitor, runs
from load_control.schemas.monitor import (
    Alert,
    EventItem,
    EventList,
    MonitorSummary,
    ValidationItem,
    ValidationList,
)

RECENT_WINDOW = timedelta(hours=24)


async def summary(conn: AsyncConnection, settings: Settings, *, alert_limit: int) -> MonitorSummary:
    """진행 중·최근 run 수, dispatch 현황, 정리 대상 수, 경보."""
    alerts = await monitor.alerts(conn, window=RECENT_WINDOW,
                                  validation_stale=settings.recovery.validation_stale,
                                  stale=settings.recovery.stale, limit=alert_limit)
    due = await cleanup.due_runs(conn, job_key=None, success_retention=settings.cleanup.success_retention,
                                 failed_retention=settings.cleanup.failed_retention,
                                 limit=settings.cleanup.max_batch)
    return MonitorSummary(
        server_time=datetime.now(UTC), active_runs=await monitor.active_counts(conn),
        recent_runs=await monitor.recent_finished_counts(conn, RECENT_WINDOW),
        recent_window_hours=int(RECENT_WINDOW.total_seconds() // 3600),
        dispatches=await dispatch.backlog(conn), cleanup_due=len(due),
        alerts=[Alert(kind=a.kind, severity=a.severity, run_id=str(a.run_id) if a.run_id else None,
                      job_key=a.job_key, business_key=a.business_key, status=a.status, at=a.at,
                      message=(a.message or "").strip()[:500] or None) for a in alerts])


async def _require_run(conn: AsyncConnection, run_id: UUID) -> None:
    if await runs.get(conn, run_id) is None:
        raise NotFound("RUN_NOT_FOUND")


async def validations(conn: AsyncConnection, run_id: UUID) -> ValidationList:
    """run의 SOURCE·STAGING·TARGET 지표."""
    await _require_run(conn, run_id)
    return ValidationList(metrics=[ValidationItem(**m) for m in await monitor.validations(conn, run_id)])


async def events(conn: AsyncConnection, run_id: UUID, *, limit: int) -> EventList:
    """run의 이벤트 타임라인(오래된 순, 최근 limit개)."""
    await _require_run(conn, run_id)
    return EventList(events=[EventItem(
        event_time=e["event_time"], level=e["event_level"], name=e["event_name"],
        partition_id=e["partition_id"], process_group=e["process_group"], error_code=e["error_code"],
        message=e["message"]) for e in await monitor.events(conn, run_id, limit)])
