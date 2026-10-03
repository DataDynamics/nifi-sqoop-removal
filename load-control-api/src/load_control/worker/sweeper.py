"""요청 없이 정체된 run, 파티션 및 dispatch를 복구한다.

NiFi가 더 이상 요청을 보내지 않는 상황도 복구할 수 있도록 `recovery.sweeper_interval`마다 다음
규칙을 순서대로 적용한다. 자세한 흐름은 설계 문서 8장을 참고한다.

1. 정체된 `RUNNING` 파티션: run을 `TIMED_OUT`으로 끝내거나 파티션을 재발행한다.
2. 전체 제한 시간을 넘긴 `CREATED`/`EXTRACTING` run: `TIMED_OUT`으로 끝낸다.
3. ACK가 오지 않은 `SENT` dispatch: `PENDING`으로 되돌려 재전송한다.
4. 검증이 정체된 run: 상태는 유지하고 오류 이벤트만 남긴다.
5. 게시 결과가 오지 않은 run: `PUBLISH_UNKNOWN`으로 바꿔 운영자 확인을 기다린다.

한 주기의 규칙은 하나의 트랜잭션에서 실행한다. 여러 worker 중 advisory lock을 얻은 하나만 처리하며,
`FOR UPDATE SKIP LOCKED`로 잠긴 run은 건너뛰고 다음 주기에 다시 확인한다.
"""

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
    """heartbeat가 끊긴 파티션을 재발행하거나 run을 시간 초과로 끝낸다.

    잠금 순서는 `run → partition`이다.

    - `REISSUE`이고 모든 파티션에 재시도 횟수가 남아 있으면 기존 claim과 chunk를 무효화하고
      `REISSUE_PARTITION` dispatch를 만든다. run은 `EXTRACTING` 상태를 유지한다.
    - `FAIL`이거나 한 파티션이라도 최대 시도 횟수에 도달했으면 미완료 파티션과 run 전체를
      `TIMED_OUT`으로 끝낸다. 일부 파티션만 재발행하지는 않는다.

    Args:
        conn: advisory lock을 잡은 sweeper 트랜잭션의 연결.
        s: recovery.stale, recovery.mode, recovery.max_attempts를 읽을 설정.
        done: 규칙별 처리 건수 누적기(reissue_partition은 파티션 수, timeout_stale_partition은 run 수).
    """
    for run_id in await repo.runs_with_stale_partitions(conn, s.recovery.stale):
        # 이벤트에 job_key·business_key를 채우기 위해 run 행을 읽는다(이미 위에서 잠갔다).
        run = await runs.get(conn, run_id)
        stale = await repo.stale_partitions(conn, run_id, s.recovery.stale)
        # 파티션 하나라도 최대 시도에 도달했으면 run 전체를 포기한다(일부만 재발행하지 않는다).
        exhausted = any(attempt >= s.recovery.max_attempts for _, attempt in stale)
        if s.recovery.mode == "REISSUE" and not exhausted:
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
        reissue_exhausted = exhausted and s.recovery.mode == "REISSUE"
        reason = "REISSUE_ATTEMPTS_EXHAUSTED" if reissue_exhausted else "PARTITION_STALE"
        await repo.time_out_partitions(conn, run_id, reason)
        await runs.fail(conn, run_id, expected=RunStatus.EXTRACTING, to=RunStatus.TIMED_OUT,
                        stage="SWEEPER", code=reason, message=f"stale partitions: {', '.join(pids)}")
        await events.record(conn, "RUN_TIMED_OUT", run, level="ERROR", error_code=reason,
                            details={"stalePartitions": pids})
        done["timeout_stale_partition"] += 1


async def _run_deadline(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    """전체 실행 제한 시간을 넘긴 `CREATED`/`EXTRACTING` run을 끝낸다.

    manifest가 오지 않거나 추출이 오래 걸리는 경우다. run을 잠근 뒤 미완료 파티션과 run을
    `TIMED_OUT`으로 바꾸고 `RUN_TIMED_OUT` 이벤트를 남긴다. 앞 규칙에서 이미 처리한 run은 같은
    트랜잭션에서 상태가 바뀌었으므로 다시 조회되지 않는다.

    Args:
        conn: sweeper 트랜잭션의 연결.
        s: recovery.run_timeout을 읽을 설정.
        done: timeout_run_deadline 건수를 누적한다.
    """
    for run_id in await repo.runs_past_deadline(conn, s.recovery.run_timeout):
        run = await runs.get(conn, run_id)
        assert run is not None
        await repo.time_out_partitions(conn, run_id, "RUN_TIMEOUT")
        await runs.fail(conn, run_id, expected=run.status, to=RunStatus.TIMED_OUT, stage="SWEEPER",
                        code="RUN_TIMEOUT", message=f"exceeded {s.recovery.run_timeout} in {run.status}")
        await events.record(conn, "RUN_TIMED_OUT", run, level="ERROR", error_code="RUN_TIMEOUT",
                            details={"from": run.status})
        done["timeout_run_deadline"] += 1


async def _unacked_dispatches(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    """제한 시간 안에 ACK가 오지 않은 `SENT` dispatch를 복구한다.

    NiFi가 202를 반환한 직후 노드가 종료되어 flow가 시작되지 않은 경우를 복구한다. run이 여전히 해당
    호출을 기다리는 상태일 때만 `PENDING`으로 되돌리고 dispatcher를 깨운다. `attempt_count`는 유지한다.
    최대 시도 횟수까지 ACK가 없으면 무한 재전송을 막기 위해 `DEAD`로 바꾸고 오류 이벤트를 남긴다.

    Args:
        conn: sweeper 트랜잭션의 연결.
        s: dispatch.ack_timeout을 읽을 설정.
        done: requeue_dispatch(되돌림), dead_dispatch(DEAD) 건수를 누적한다.
    """
    handled = await repo.requeue_unacked_dispatches(conn, s.dispatch.ack_timeout, s.dispatch.max_attempts)
    requeued = [d.run_id for d in handled if d.status == "PENDING"]
    for d in handled:
        if d.status != "DEAD":
            continue
        # dispatcher._dead와 같은 이벤트·메트릭을 남겨 TUI·경보에서 똑같이 DISPATCH_DEAD로 보이게 한다.
        await events.record(conn, "DISPATCH_DEAD", run_id=d.run_id, level="ERROR",
                            partition_id=d.partition_id,
                            message=f"no ack after {d.attempt_count} attempts (ack timeout)",
                            details={"dispatchId": str(d.dispatch_id), "type": d.dispatch_type,
                                     "attempt": d.attempt_count})
        metrics.DISPATCH.labels(d.dispatch_type, "dead").inc()
        done["dead_dispatch"] += 1
    # 같은 run에 여러 dispatch가 되돌려질 수 있으므로 이벤트는 run당 한 번만 남긴다.
    for run_id in set(requeued):
        await events.record(conn, "DISPATCH_REQUEUED", run_id=run_id, level="WARN")
    if requeued:
        await conn.execute(text("SELECT pg_notify(:c, 'sweeper')"), {"c": dispatch.CHANNEL})
    done["requeue_dispatch"] += len(requeued)


async def _stale_publishing(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    """게시 결과가 제한 시간 안에 오지 않은 run을 `PUBLISH_UNKNOWN`으로 바꾼다.

    API는 target 반영 여부를 알 수 없으므로 게시를 자동으로 재실행하거나 실패 처리하지 않는다.
    이 상태는 활성 run으로 취급해 같은 업무 키의 새 run을 막는다. 운영자가 실제 결과를 확인한 뒤
    `/publish-unknown/resolve`로 확정해야 한다.

    Args:
        conn: sweeper 트랜잭션의 연결.
        s: recovery.publish_stale을 읽을 설정.
        done: publish_unknown 건수를 누적한다.
    """
    for run_id in await repo.stale_publishing_runs(conn, s.recovery.publish_stale):
        run = await runs.get(conn, run_id)
        await runs.cas_status(conn, run_id, expected=RunStatus.PUBLISHING, to=RunStatus.PUBLISH_UNKNOWN,
                              error_stage="PUBLISH", error_code="PUBLISH_STALE",
                              error_message=f"no publish result within {s.recovery.publish_stale}")
        await events.record(conn, "PUBLISH_UNKNOWN", run, level="ERROR", error_code="PUBLISH_STALE")
        done["publish_unknown"] += 1


async def sweep_once(engine: AsyncEngine, settings: Settings) -> dict[str, int] | None:
    """복구 규칙을 한 번 실행하고 규칙별 처리 건수를 반환한다.

    트랜잭션 범위 advisory lock을 다른 worker가 보유하고 있으면 `None`을 반환한다. 각 규칙은 별도의
    SAVEPOINT에서 실행한다. 한 규칙이 실패하면 그 규칙의 변경만 rollback하고 나머지는 계속 처리한다.
    따라서 특정 데이터의 지속적인 오류가 다른 복구 작업까지 막지 않으며, 실패한 규칙은 다음 주기에
    다시 시도된다.

    Returns:
        처리 건수가 0이 아닌 규칙만 담은 {규칙 이름: 건수}. 할 일이 없었으면 빈 dict,
        lock을 얻지 못했으면 None. 건수가 있으면 SWEEPER_ACTIONS 메트릭을 올리고 WARN 로그를 남긴다.
    """

    async def fn(conn: AsyncConnection) -> dict[str, int] | None:
        """advisory lock을 얻은 경우에만 규칙을 순서대로 적용한다."""
        if not await repo.try_lock(conn):
            return None
        done: Counter[str] = Counter()

        async def stale_alert(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
            """검증·게시 정체는 상태를 바꾸지 않고 경보 이벤트만 남긴다(같은 run은 stale 기간마다 한 번)."""
            done["stale_alert"] += await repo.alert_stale_runs(conn, s.recovery.validation_stale)

        for rule in (_stale_partitions, _run_deadline, _unacked_dispatches, stale_alert, _stale_publishing):
            rule_done: Counter[str] = Counter()
            try:
                async with conn.begin_nested():   # SAVEPOINT: 이 규칙의 변경만 되돌릴 수 있게 한다
                    await rule(conn, settings, rule_done)
            except Exception:
                log.exception("sweeper_rule_error", rule=rule.__name__.lstrip("_"))
                continue
            done.update(rule_done)   # 롤백된 규칙의 건수는 세지 않는다
        return {k: v for k, v in done.items() if v}

    result = await in_tx(engine, fn)
    if result:
        for rule, count in result.items():
            metrics.SWEEPER_ACTIONS.labels(rule).inc(count)
        log.warning("sweeper_actions", **result)
    return result


async def refresh_gauges(engine: AsyncEngine) -> None:
    """dispatch backlog와 활성 run 수 메트릭을 갱신한다.

    advisory lock과 관계없이 모든 worker가 각자 갱신한다(읽기만 하고 커밋하지 않는 짧은 연결).
    활성 run 수는 조회 결과에 없는 상태도 0으로 덮어써, 사라진 상태의 이전 값이 남지 않게 한다.
    """
    async with engine.connect() as conn:
        for status, count in (await dispatch.backlog(conn)).items():
            metrics.DISPATCH_BACKLOG.labels(status).set(count)
        active = await repo.active_run_counts(conn)
    for status in RunStatus:
        metrics.ACTIVE_RUNS.labels(status.value).set(active.get(status.value, 0))


async def run_sweeper(engine: AsyncEngine, settings: Settings, stop: asyncio.Event) -> None:
    """recovery.sweeper_interval마다 sweep_once를 실행한다. 오류가 나도 루프는 계속된다.

    sweep_once 뒤에 메트릭 게이지를 갱신한다. 예외(DB 장애 등)는 로그만 남기고 다음 주기에 다시
    시도한다. 대기 중 stop이 set되면 interval을 기다리지 않고 바로 끝난다.
    """
    interval = settings.recovery.sweeper_interval.total_seconds()
    while not stop.is_set():
        try:
            await sweep_once(engine, settings)
            await refresh_gauges(engine)
        except Exception:
            log.exception("sweeper_error")
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), interval)
