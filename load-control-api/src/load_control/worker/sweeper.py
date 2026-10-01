"""stale·timeout 정리(API 설계 7장, 가이드 13.1). worker마다 돌아도 advisory lock을 얻은 하나만 실행한다."""

import asyncio
import contextlib
from collections import Counter

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from load_control import metrics
from load_control.config import Settings
from load_control.db import in_tx
from load_control.domain import RunStatus
from load_control.repositories import dispatch, events, files, runs
from load_control.repositories import sweeper as repo

log = structlog.get_logger(__name__)


async def _stale_partitions(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    for run_id in await repo.runs_with_stale_partitions(conn, s.recovery_stale):
        run = await runs.get(conn, run_id)
        stale = await repo.stale_partitions(conn, run_id, s.recovery_stale)
        exhausted = any(attempt >= s.recovery_max_attempts for _, attempt in stale)
        if s.recovery_mode == "REISSUE" and not exhausted:
            for pid, attempt in stale:
                await repo.reset_for_reissue(conn, run_id, pid)
                invalidated = await files.invalidate(conn, run_id, pid)
                await dispatch.enqueue_reissue(conn, run_id, pid)
                await events.record(conn, "RECOVERY_REISSUED", run, level="WARN", partition_id=pid,
                                    details={"attempt": attempt, "invalidatedFiles": invalidated})
                done["reissue_partition"] += 1
            await runs.touch(conn, run_id)
            continue
        pids = [pid for pid, _ in stale]
        reissue_exhausted = exhausted and s.recovery_mode == "REISSUE"
        reason = "REISSUE_ATTEMPTS_EXHAUSTED" if reissue_exhausted else "PARTITION_STALE"
        await repo.time_out_partitions(conn, run_id, reason)
        await runs.fail(conn, run_id, expected=RunStatus.EXTRACTING, to=RunStatus.TIMED_OUT,
                        stage="SWEEPER", code=reason, message=f"stale partitions: {', '.join(pids)}")
        await events.record(conn, "RUN_TIMED_OUT", run, level="ERROR", error_code=reason,
                            details={"stalePartitions": pids})
        done["timeout_stale_partition"] += 1


async def _run_deadline(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    for run_id in await repo.runs_past_deadline(conn, s.run_timeout):
        run = await runs.get(conn, run_id)
        assert run is not None
        await repo.time_out_partitions(conn, run_id, "RUN_TIMEOUT")
        await runs.fail(conn, run_id, expected=run.status, to=RunStatus.TIMED_OUT, stage="SWEEPER",
                        code="RUN_TIMEOUT", message=f"exceeded {s.run_timeout} in {run.status}")
        await events.record(conn, "RUN_TIMED_OUT", run, level="ERROR", error_code="RUN_TIMEOUT",
                            details={"from": run.status})
        done["timeout_run_deadline"] += 1


async def _unacked_dispatches(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    requeued = await repo.requeue_unacked_dispatches(conn, s.dispatch_ack_timeout)
    for run_id in set(requeued):
        await events.record(conn, "DISPATCH_REQUEUED", run_id=run_id, level="WARN")
    if requeued:
        await conn.execute(text("SELECT pg_notify(:c, 'sweeper')"), {"c": dispatch.CHANNEL})
    done["requeue_dispatch"] += len(requeued)


async def _stale_publishing(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    for run_id in await repo.stale_publishing_runs(conn, s.publish_stale):
        run = await runs.get(conn, run_id)
        await runs.cas_status(conn, run_id, expected=RunStatus.PUBLISHING, to=RunStatus.PUBLISH_UNKNOWN,
                              error_stage="PUBLISH", error_code="PUBLISH_STALE",
                              error_message=f"no publish result within {s.publish_stale}")
        await events.record(conn, "PUBLISH_UNKNOWN", run, level="ERROR", error_code="PUBLISH_STALE")
        done["publish_unknown"] += 1


async def sweep_once(engine: AsyncEngine, settings: Settings) -> dict[str, int] | None:
    """한 번 정리한다. 다른 worker가 실행 중이면 None."""

    async def fn(conn: AsyncConnection) -> dict[str, int] | None:
        if not await repo.try_lock(conn):
            return None
        done: Counter[str] = Counter()
        await _stale_partitions(conn, settings, done)
        await _run_deadline(conn, settings, done)
        await _unacked_dispatches(conn, settings, done)
        done["stale_alert"] += await repo.alert_stale_runs(conn, settings.validation_stale)
        await _stale_publishing(conn, settings, done)
        return {k: v for k, v in done.items() if v}

    result = await in_tx(engine, fn)
    if result:
        for rule, count in result.items():
            metrics.SWEEPER_ACTIONS.labels(rule).inc(count)
        log.warning("sweeper_actions", **result)
    return result


async def refresh_gauges(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        for status, count in (await dispatch.backlog(conn)).items():
            metrics.DISPATCH_BACKLOG.labels(status).set(count)
        active = await repo.active_run_counts(conn)
    for status in RunStatus:
        metrics.ACTIVE_RUNS.labels(status.value).set(active.get(status.value, 0))


async def run_sweeper(engine: AsyncEngine, settings: Settings, stop: asyncio.Event) -> None:
    interval = settings.sweeper_interval.total_seconds()
    while not stop.is_set():
        try:
            await sweep_once(engine, settings)
            await refresh_gauges(engine)
        except Exception:
            log.exception("sweeper_error")
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), interval)
