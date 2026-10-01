import asyncio
import uuid
from datetime import timedelta

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from load_control.config import Settings
from load_control.repositories.sweeper import LOCK_KEY
from load_control.worker.sweeper import refresh_gauges, sweep_once
from tests.conftest import Db
from tests.helpers import claim, complete_run, create_run, report, start_run, to_staging_validated


async def age_partition(db: Db, run_id: str, pid: str, minutes: int = 120) -> None:
    await db.execute("UPDATE nifi_ops.load_partition SET heartbeat_at = clock_timestamp() - "
                     "make_interval(mins => :m) WHERE run_id = CAST(:r AS uuid) AND partition_id = :p",
                     m=minutes, r=run_id, p=pid)


async def run_status(db: Db, run_id: str) -> str:
    return str(await db.scalar("SELECT status FROM nifi_ops.load_run WHERE run_id = CAST(:r AS uuid)",
                               r=run_id))


def test_stale_must_exceed_query_timeout(migrated_url: str) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=migrated_url, recovery_stale=timedelta(minutes=15),  # type: ignore[arg-type]
                 extract_query_timeout=timedelta(minutes=60))


async def test_fresh_partitions_untouched(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                                          settings: Settings) -> None:
    run = await start_run(client, [3, 4])
    await claim(client, run, "0000")
    assert await sweep_once(engine, settings) == {}
    assert await run_status(db, run.run_id) == "EXTRACTING"


async def test_stale_partition_fails_run(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                                         settings: Settings) -> None:
    run = await start_run(client, [3, 4])
    t0 = await claim(client, run, "0000")
    await claim(client, run, "0001")
    await age_partition(db, run.run_id, "0001")
    assert await sweep_once(engine, settings) == {"timeout_stale_partition": 1}
    assert await run_status(db, run.run_id) == "TIMED_OUT"
    parts = await db.all("SELECT partition_id, status FROM nifi_ops.load_partition "
                         "WHERE run_id = CAST(:r AS uuid) ORDER BY partition_id", r=run.run_id)
    assert [p["status"] for p in parts] == ["TIMED_OUT", "TIMED_OUT"]
    late = await report(client, run, "0000", t0, 0, 1, 3)  # 늦게 온 보고는 기록만
    assert late.status_code == 200 and late.json()["runStatus"] == "TIMED_OUT"
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_event WHERE event_name = 'RUN_TIMED_OUT'") == 1
    # active lock 해제: 같은 업무키로 다시 실행 가능
    bk = await db.scalar("SELECT business_key FROM nifi_ops.load_run WHERE run_id = CAST(:r AS uuid)",
                         r=run.run_id)
    await create_run(client, business_key=bk)


async def test_reissue_mode(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                            settings: Settings) -> None:
    s = settings.model_copy(update={"recovery_mode": "REISSUE"})
    run = await start_run(client, [3, 4])
    t0 = await claim(client, run, "0000")
    old = await claim(client, run, "0001")
    await report(client, run, "0001", old, 0, 2, 2)  # 이전 시도가 chunk 하나만 남기고 멈춤
    await age_partition(db, run.run_id, "0001")

    assert await sweep_once(engine, s) == {"reissue_partition": 1}
    assert await run_status(db, run.run_id) == "EXTRACTING"
    part = await db.one("SELECT status, claim_token FROM nifi_ops.load_partition "
                        "WHERE run_id = CAST(:r AS uuid) AND partition_id = '0001'", r=run.run_id)
    assert part == {"status": "RETRY", "claim_token": None}
    assert await db.scalar("SELECT status FROM nifi_ops.load_file") == "FAILED"
    d = await db.one("SELECT dispatch_type, partition_id, status FROM nifi_ops.load_dispatch")
    assert d == {"dispatch_type": "REISSUE_PARTITION", "partition_id": "0001", "status": "PENDING"}

    stale_report = await report(client, run, "0001", old, 1, 2, 2)  # 이전 Worker가 늦게 살아남
    assert stale_report.status_code == 409 and stale_report.json()["code"] == "CLAIM_MISMATCH"

    new = await claim(client, run, "0001")  # 재발행 수신 → claim이 ACK 역할
    assert await db.scalar("SELECT status FROM nifi_ops.load_dispatch") == "ACKED"
    assert await db.scalar("SELECT attempt_count FROM nifi_ops.load_partition "
                           "WHERE run_id = CAST(:r AS uuid) AND partition_id = '0001'", r=run.run_id) == 2
    await report(client, run, "0000", t0, 0, 1, 3)
    await report(client, run, "0001", new, 0, 2, 2)
    done = await report(client, run, "0001", new, 1, 2, 2)
    assert done.json()["validationScheduled"] is True
    assert await db.scalar("SELECT extracted_count FROM nifi_ops.load_run WHERE run_id = CAST(:r AS uuid)",
                           r=run.run_id) == 7


async def test_reissue_attempts_exhausted(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                                          settings: Settings) -> None:
    s = settings.model_copy(update={"recovery_mode": "REISSUE", "recovery_max_attempts": 1})
    run = await start_run(client, [3])
    await claim(client, run, "0000")
    await age_partition(db, run.run_id, "0000")
    assert await sweep_once(engine, s) == {"timeout_stale_partition": 1}
    assert await db.scalar("SELECT error_code FROM nifi_ops.load_run") == "REISSUE_ATTEMPTS_EXHAUSTED"


async def test_run_deadline(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                            settings: Settings) -> None:
    created = await create_run(client)
    running = await start_run(client, [3])
    await db.execute("UPDATE nifi_ops.load_run SET started_at = clock_timestamp() - interval '7 hours'")
    assert await sweep_once(engine, settings) == {"timeout_run_deadline": 2}
    assert await run_status(db, created.run_id) == "TIMED_OUT"
    assert await run_status(db, running.run_id) == "TIMED_OUT"
    assert await db.scalar("SELECT status FROM nifi_ops.load_partition") == "TIMED_OUT"


async def test_requeue_unacked_validation_dispatch(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                                                   settings: Settings) -> None:
    waiting, _ = await complete_run(client)
    started, started_dispatch = await complete_run(client)
    await db.execute("UPDATE nifi_ops.load_dispatch SET status = 'SENT', "
                     "sent_at = clock_timestamp() - interval '11 minutes'")
    await client.post(f"/v1/runs/{started.run_id}/validation/start", json={"dispatchId": started_dispatch})
    await db.execute("UPDATE nifi_ops.load_dispatch SET status = 'SENT' WHERE run_id = CAST(:r AS uuid)",
                     r=started.run_id)  # ACK 후 상태가 이미 진행된 run은 재전송하지 않는다
    assert await sweep_once(engine, settings) == {"requeue_dispatch": 1}
    rows = {r["run_id"]: r["status"] for r in await db.all(
        "SELECT CAST(run_id AS text) AS run_id, status FROM nifi_ops.load_dispatch")}
    assert rows == {waiting.run_id: "PENDING", started.run_id: "SENT"}


async def test_stale_validation_alert_once(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                                           settings: Settings) -> None:
    run = await to_staging_validated(client)
    await db.execute("UPDATE nifi_ops.load_run SET status = 'STAGE_VALIDATING', "
                     "heartbeat_at = clock_timestamp() - interval '3 hours'")
    assert await sweep_once(engine, settings) == {"stale_alert": 1}
    assert await sweep_once(engine, settings) == {}
    assert await run_status(db, run.run_id) == "STAGE_VALIDATING"  # 자동 전이 없음


async def test_stale_publishing_becomes_unknown(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                                                settings: Settings) -> None:
    run = await to_staging_validated(client)
    await client.post(f"/v1/runs/{run.run_id}/publish/claim", json={"publishToken": str(uuid.uuid4())})
    await db.execute("UPDATE nifi_ops.load_run "
                     "SET publish_started_at = clock_timestamp() - interval '3 hours'")
    assert await sweep_once(engine, settings) == {"publish_unknown": 1}
    assert await run_status(db, run.run_id) == "PUBLISH_UNKNOWN"


async def test_skips_when_other_sweeper_holds_lock(client: httpx.AsyncClient, engine: AsyncEngine,
                                                   settings: Settings) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": LOCK_KEY})
        assert await sweep_once(engine, settings) is None


async def test_concurrent_sweepers_act_once(client: httpx.AsyncClient, engine: AsyncEngine, db: Db,
                                            settings: Settings) -> None:
    for _ in range(3):
        run = await start_run(client, [3])
        await claim(client, run, "0000")
        await age_partition(db, run.run_id, "0000")
    results = await asyncio.gather(*(sweep_once(engine, settings) for _ in range(4)))
    assert sum((r or {}).get("timeout_stale_partition", 0) for r in results) == 3


async def test_refresh_gauges(client: httpx.AsyncClient, engine: AsyncEngine) -> None:
    await complete_run(client)
    await refresh_gauges(engine)
    from load_control import metrics

    assert metrics.DISPATCH_BACKLOG.labels("PENDING")._value.get() == 1
    assert metrics.ACTIVE_RUNS.labels("EXTRACTED_VALIDATED")._value.get() == 1
