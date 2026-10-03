"""stale·timeout 정리. worker마다 돌아도 advisory lock을 얻은 하나만 실행한다.

run이 멈추면 NiFi에서 요청이 오지 않으므로, 요청과 관계없이 recovery.sweeper_interval마다 돌며
다음 규칙을 순서대로 적용한다(설계 문서 8장).

1. heartbeat가 recovery.stale보다 오래된 RUNNING 파티션: mode=FAIL이면 run TIMED_OUT,
   mode=REISSUE면 파티션을 RETRY로 되돌리고 재발행 dispatch를 만든다(최대 시도 초과 시 FAIL과 같게).
2. 시작 후 recovery.run_timeout이 지난 CREATED/EXTRACTING run: TIMED_OUT.
3. SENT 후 dispatch.ack_timeout 동안 ACK가 없는 dispatch: PENDING으로 되돌려 재전송.
4. recovery.validation_stale 동안 변화 없는 STAGE_VALIDATING/PUBLISHED run: ERROR 이벤트만 남긴다.
5. recovery.publish_stale이 지난 PUBLISHING run: PUBLISH_UNKNOWN(자동 재실행 없음, 운영자가 확정).

한 바퀴의 모든 규칙은 하나의 트랜잭션에서 실행되며, 트랜잭션 범위 advisory lock
(pg_try_advisory_xact_lock)을 얻은 worker 하나만 실제로 처리한다. 대상 run은 FOR UPDATE SKIP LOCKED로
가져오므로 server가 같은 run을 처리 중이면 이번 바퀴에서는 건너뛰고 다음 바퀴에 다시 본다.
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
    """heartbeat가 끊긴 RUNNING 파티션이 있는 EXTRACTING run을 재발행하거나 TIMED_OUT으로 끝낸다.

    run 행을 SKIP LOCKED로, 그 run의 stale 파티션을 FOR UPDATE로 잠근다(잠금 순서 run → partition).
    - mode=REISSUE이고 stale 파티션 중 attempt_count가 recovery.max_attempts에 도달한 것이 없으면:
      파티션마다 RUNNING → RETRY(claim token 초기화, 이전 Worker의 늦은 보고는 CLAIM_MISMATCH가 됨),
      이전 시도의 WRITTEN chunk를 FAILED로 무효화, REISSUE_PARTITION dispatch 예약(pg_notify 포함),
      RECOVERY_REISSUED 이벤트를 남긴다. run heartbeat도 갱신한다. run 상태는 EXTRACTING 그대로다.
    - 그 밖(mode=FAIL, 또는 REISSUE인데 시도 횟수 소진): run의 미완료 파티션을 모두 TIMED_OUT으로,
      run을 EXTRACTING → TIMED_OUT(CAS)으로 바꾸고 RUN_TIMED_OUT 이벤트를 남긴다. 오류 코드는
      PARTITION_STALE 또는 REISSUE_ATTEMPTS_EXHAUSTED.

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
    """시작 후 recovery.run_timeout이 지난 CREATED/EXTRACTING run을 TIMED_OUT으로 끝낸다.

    manifest가 오지 않거나 추출이 너무 오래 걸리는 run이 대상이다. run을 SKIP LOCKED로 잠근 뒤
    미완료 파티션을 TIMED_OUT(RUN_TIMEOUT)으로 바꾸고, 지금 상태를 기대값으로 한 CAS로 run을
    TIMED_OUT으로 바꾸며, RUN_TIMED_OUT 이벤트를 남긴다. 앞 규칙에서 이미 TIMED_OUT이 된 run은
    같은 트랜잭션 안에서 상태가 바뀌었으므로 다시 잡히지 않는다.

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
    """SENT 후 dispatch.ack_timeout 동안 ACK가 없는 dispatch를 PENDING으로 되돌려 다시 보내게 한다.

    NiFi가 202로 받은 직후 노드가 죽어 검증·재발행 flow가 시작되지 않은 경우를 복구한다. run이 아직
    그 호출을 기다리는 상태일 때만 되돌린다(VALIDATE_RUN은 run EXTRACTED_VALIDATED, REISSUE_PARTITION은
    run EXTRACTING이고 파티션 RETRY). 되돌린 run마다 DISPATCH_REQUEUED 이벤트를 한 번 남기고,
    하나라도 있으면 pg_notify로 dispatcher를 깨운다(NOTIFY는 이 트랜잭션이 커밋될 때 전달된다).
    attempt_count는 초기화하지 않으므로 반복되면 결국 max_attempts에서 DEAD가 된다.

    Args:
        conn: sweeper 트랜잭션의 연결.
        s: dispatch.ack_timeout을 읽을 설정.
        done: requeue_dispatch에 되돌린 dispatch 수를 누적한다.
    """
    requeued = await repo.requeue_unacked_dispatches(conn, s.dispatch.ack_timeout)
    # 같은 run에 여러 dispatch가 되돌려질 수 있으므로 이벤트는 run당 한 번만 남긴다.
    for run_id in set(requeued):
        await events.record(conn, "DISPATCH_REQUEUED", run_id=run_id, level="WARN")
    if requeued:
        await conn.execute(text("SELECT pg_notify(:c, 'sweeper')"), {"c": dispatch.CHANNEL})
    done["requeue_dispatch"] += len(requeued)


async def _stale_publishing(conn: AsyncConnection, s: Settings, done: Counter[str]) -> None:
    """recovery.publish_stale 동안 게시 결과가 오지 않은 PUBLISHING run을 PUBLISH_UNKNOWN으로 바꾼다.

    게시(target 반영)가 실제로 됐는지 API가 알 수 없으므로 자동으로 재실행하거나 실패 처리하지 않는다.
    PUBLISH_UNKNOWN은 진행 중인 run으로 취급되어(uq_load_run_active) 같은 business_key의 새 run을 막고,
    운영자가 /publish-unknown/resolve로 PUBLISHED 또는 FAILED_PUBLISH로 확정한다.
    PUBLISH_UNKNOWN(ERROR) 이벤트를 남긴다.

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
    """한 번 정리한다. 다른 worker가 실행 중이면 None.

    모든 규칙을 하나의 트랜잭션(in_tx)에서 실행한다. 먼저 트랜잭션 범위 advisory lock을 시도하고,
    다른 worker가 잡고 있으면 아무것도 하지 않고 None을 돌려준다. lock은 커밋·롤백 때 자동으로 풀린다.
    규칙 하나에서 예외가 나면 그 바퀴의 변경 전체가 롤백된다. deadlock·serialization 실패는 in_tx가
    트랜잭션 전체를 다시 실행한다.

    Returns:
        처리 건수가 0이 아닌 규칙만 담은 {규칙 이름: 건수}. 할 일이 없었으면 빈 dict,
        lock을 얻지 못했으면 None. 건수가 있으면 SWEEPER_ACTIONS 메트릭을 올리고 WARN 로그를 남긴다.
    """

    async def fn(conn: AsyncConnection) -> dict[str, int] | None:
        """advisory lock을 얻은 경우에만 규칙을 순서대로 적용한다."""
        if not await repo.try_lock(conn):
            return None
        done: Counter[str] = Counter()
        await _stale_partitions(conn, settings, done)
        await _run_deadline(conn, settings, done)
        await _unacked_dispatches(conn, settings, done)
        # 검증·게시 정체는 상태를 바꾸지 않고 경보 이벤트만 남긴다(같은 run은 stale 기간마다 한 번).
        done["stale_alert"] += await repo.alert_stale_runs(conn, settings.recovery.validation_stale)
        await _stale_publishing(conn, settings, done)
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
