import asyncio
import json
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
import respx
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine

from load_control.config import Settings
from load_control.db import in_tx
from load_control.repositories import dispatch
from load_control.worker.dispatcher import Dispatcher, backoff
from tests.conftest import Db
from tests.helpers import claim, complete_run, start_run

NIFI = "https://nifi.test:9443"
VALIDATE = f"{NIFI}/validate/ORACLE_INSP_DTL_DAILY"


@pytest.fixture
def worker_settings(settings: Settings, migrated_url: str) -> Settings:
    return settings.model_copy(update={
        "nifi_receiver_url": NIFI, "dispatch_max_attempts": 3,
        "dispatch_backoff_min": timedelta(seconds=5), "dispatch_backoff_max": timedelta(minutes=1),
        "dispatch_poll_interval": timedelta(seconds=60),
        "listen_dsn": SecretStr(migrated_url.replace("+asyncpg", "")),
    })


async def make_dispatcher(worker_settings: Settings, engine: AsyncEngine) -> Dispatcher:
    return Dispatcher(worker_settings, engine, httpx.AsyncClient(timeout=2))


async def dispatch_row(db: Db, dispatch_id: str) -> dict[str, object]:
    return await db.one("SELECT status, attempt_count, last_http_status, last_error, "
                        "next_attempt_at > clock_timestamp() AS deferred "
                        "FROM nifi_ops.load_dispatch WHERE dispatch_id = CAST(:d AS uuid)", d=dispatch_id)


def test_backoff(worker_settings: Settings) -> None:
    assert backoff(worker_settings, 1) == timedelta(seconds=5)
    assert backoff(worker_settings, 3) == timedelta(seconds=20)
    assert backoff(worker_settings, 10) == timedelta(minutes=1)


@respx.mock
async def test_sends_validation_and_marks_sent(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                               worker_settings: Settings) -> None:
    run, dispatch_id = await complete_run(client)
    route = respx.post(VALIDATE).mock(return_value=httpx.Response(202))
    assert await (await make_dispatcher(worker_settings, engine)).dispatch_once() == 1
    request = route.calls.last.request
    assert json.loads(request.content) == {"runId": run.run_id, "dispatchId": dispatch_id}
    assert request.headers["X-Run-Id"] == run.run_id
    assert request.headers["X-Dispatch-Id"] == dispatch_id
    row = await dispatch_row(db, dispatch_id)
    assert row["status"] == "SENT" and row["attempt_count"] == 1 and row["last_http_status"] == 202
    assert await (await make_dispatcher(worker_settings, engine)).dispatch_once() == 0  # 다시 보내지 않음


@respx.mock
async def test_server_error_is_retried_later(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                             worker_settings: Settings) -> None:
    _, dispatch_id = await complete_run(client)
    respx.post(VALIDATE).mock(return_value=httpx.Response(503, text="busy"))
    d = await make_dispatcher(worker_settings, engine)
    await d.dispatch_once()
    row = await dispatch_row(db, dispatch_id)
    assert row["status"] == "PENDING" and row["deferred"] is True
    assert row["last_http_status"] == 503 and row["last_error"] == "busy"
    assert await d.dispatch_once() == 0  # backoff 중


@respx.mock
async def test_connection_error_is_retried(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                           worker_settings: Settings) -> None:
    _, dispatch_id = await complete_run(client)
    respx.post(VALIDATE).mock(side_effect=httpx.ConnectError("refused"))
    await (await make_dispatcher(worker_settings, engine)).dispatch_once()
    row = await dispatch_row(db, dispatch_id)
    assert row["status"] == "PENDING" and row["last_http_status"] is None
    assert "refused" in str(row["last_error"])


@respx.mock
async def test_max_attempts_marks_dead(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                       worker_settings: Settings) -> None:
    run, dispatch_id = await complete_run(client)
    respx.post(VALIDATE).mock(return_value=httpx.Response(500))
    d = await make_dispatcher(worker_settings, engine)
    for _ in range(3):
        await d.dispatch_once()
        await db.execute("UPDATE nifi_ops.load_dispatch SET next_attempt_at = clock_timestamp() "
                         "WHERE status = 'PENDING'")
    row = await dispatch_row(db, dispatch_id)
    assert row["status"] == "DEAD" and row["attempt_count"] == 3
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_event WHERE event_name = 'DISPATCH_DEAD' "
                           "AND run_id = CAST(:r AS uuid)", r=run.run_id) == 1


@respx.mock
async def test_client_error_marks_dead_immediately(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                                   worker_settings: Settings) -> None:
    _, dispatch_id = await complete_run(client)
    respx.post(VALIDATE).mock(return_value=httpx.Response(400))
    await (await make_dispatcher(worker_settings, engine)).dispatch_once()
    assert (await dispatch_row(db, dispatch_id))["status"] == "DEAD"


async def test_ack_before_mark_sent_keeps_acked(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                                worker_settings: Settings) -> None:
    """NiFi가 202 직후 /validation/start를 먼저 호출해도 ACKED가 SENT로 덮이지 않는다(API 설계 9.8)."""
    run, dispatch_id = await complete_run(client)
    leased = await in_tx(engine, lambda c: dispatch.lease_due(c, batch=10, lease=timedelta(seconds=60)))
    assert [str(d.dispatch_id) for d in leased] == [dispatch_id]
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
    assert r.json()["started"] is True
    assert await in_tx(engine, lambda c: dispatch.mark_sent(c, UUID(dispatch_id), 202)) is False
    assert (await dispatch_row(db, dispatch_id))["status"] == "ACKED"


@respx.mock
async def test_concurrent_dispatchers_send_once(client: httpx.AsyncClient, engine: AsyncEngine,
                                                worker_settings: Settings) -> None:
    for _ in range(5):
        await complete_run(client)
    route = respx.post(VALIDATE).mock(return_value=httpx.Response(202))
    workers = [await make_dispatcher(worker_settings, engine) for _ in range(4)]
    counts = await asyncio.gather(*(w.dispatch_once() for w in workers))
    assert sum(counts) == 5 and route.call_count == 5


@respx.mock
async def test_expired_lease_is_resent(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                       worker_settings: Settings) -> None:
    """전송 중 worker가 죽으면 lease 만료 후 다른 worker가 다시 보낸다."""
    _, dispatch_id = await complete_run(client)
    await in_tx(engine, lambda c: dispatch.lease_due(c, batch=10, lease=timedelta(seconds=60)))
    route = respx.post(VALIDATE).mock(return_value=httpx.Response(202))
    d = await make_dispatcher(worker_settings, engine)
    assert await d.dispatch_once() == 0  # lease 유효
    await db.execute("UPDATE nifi_ops.load_dispatch SET next_attempt_at = clock_timestamp()")
    assert await d.dispatch_once() == 1 and route.call_count == 1
    assert (await dispatch_row(db, dispatch_id))["attempt_count"] == 2


@respx.mock
async def test_reissue_body(client: httpx.AsyncClient, engine: AsyncEngine,
                            worker_settings: Settings) -> None:
    run = await start_run(client, [3, 4])
    await claim(client, run, "0001")
    await in_tx(engine, lambda c: dispatch.enqueue_reissue(c, UUID(run.run_id), "0001"))
    route = respx.post(f"{NIFI}/reissue/ORACLE_INSP_DTL_DAILY").mock(return_value=httpx.Response(202))
    await (await make_dispatcher(worker_settings, engine)).dispatch_once()
    body = json.loads(route.calls.last.request.content)
    assert body | {"dispatchId": None} == {
        "runId": run.run_id, "dispatchId": None, "partitionId": "0001", "businessKey": body["businessKey"],
        "snapshotScn": "1234567890", "hdfsRunPath": run.hdfs_run_path, "lowerBound": "1001",
        "upperBound": "2001", "upperInclusive": True, "isNullPartition": False, "expectedRowCount": 4}


@respx.mock
async def test_listen_wakes_dispatcher(client: httpx.AsyncClient, engine: AsyncEngine,
                                       worker_settings: Settings) -> None:
    """poll 주기(60초)를 기다리지 않고 NOTIFY로 바로 보낸다."""
    route = respx.post(VALIDATE).mock(return_value=httpx.Response(202))
    d = await make_dispatcher(worker_settings, engine)
    stop = asyncio.Event()
    task = asyncio.create_task(d.run(stop))
    try:
        await asyncio.sleep(0.5)  # 첫 dispatch_once와 LISTEN 연결
        await complete_run(client)
        for _ in range(50):
            if route.call_count:
                break
            await asyncio.sleep(0.1)
        assert route.call_count == 1
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


@respx.mock
async def test_listen_reconnects_after_connection_loss(client: httpx.AsyncClient, db: Db,
                                                       engine: AsyncEngine,
                                                       worker_settings: Settings) -> None:
    route = respx.post(VALIDATE).mock(return_value=httpx.Response(202))
    d = await make_dispatcher(worker_settings, engine)
    stop = asyncio.Event()
    task = asyncio.create_task(d.run(stop))
    try:
        await asyncio.sleep(0.5)
        killed = await db.scalar("SELECT COUNT(pg_terminate_backend(pid)) FROM pg_stat_activity "
                                 "WHERE query LIKE 'LISTEN%'")
        assert killed == 1
        await asyncio.sleep(0.5)  # 재연결
        await complete_run(client)
        for _ in range(50):
            if route.call_count:
                break
            await asyncio.sleep(0.1)
        assert route.call_count == 1
        assert await db.scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE query LIKE 'LISTEN%'") == 1
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
