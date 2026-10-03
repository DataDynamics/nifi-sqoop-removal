"""outbox dispatcher(worker)가 load_dispatch를 NiFi로 보내는 규칙을 검증한다.

NiFi 수신 endpoint는 respx로 흉내 낸다. 재시도(backoff), DEAD 처리, lease 기반 중복 방지,
LISTEN/NOTIFY 즉시 깨우기를 다룬다.
"""

import asyncio
import json
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncEngine

from load_control.config import Settings
from load_control.db import in_tx
from load_control.repositories import dispatch
from load_control.worker.dispatcher import Dispatcher, backoff
from tests.conftest import Db, override
from tests.helpers import claim, complete_run, start_run

NIFI = "https://nifi.test:9443"
VALIDATE = f"{NIFI}/validate/ORACLE_INSP_DTL_DAILY"


@pytest.fixture
def worker_settings(settings: Settings, migrated_url: str) -> Settings:
    """dispatcher 테스트용 설정.

    NiFi 수신 주소를 가짜 주소로 두고, 최대 3회·5초~1분 backoff로 줄이며,
    poll 주기를 60초로 길게 잡아 NOTIFY로만 깨어나는지 구분할 수 있게 한다.
    LISTEN 연결은 asyncpg 드라이버 표기를 뺀 DSN으로 연다.
    """
    return override(
        settings,
        nifi={"receiver_url": NIFI},
        dispatch={"max_attempts": 3, "backoff_min": timedelta(seconds=5),
                  "backoff_max": timedelta(minutes=1), "poll_interval": timedelta(seconds=60)},
        database={"listen_dsn": migrated_url.replace("+asyncpg", "")},
    )


async def make_dispatcher(worker_settings: Settings, engine: AsyncEngine) -> Dispatcher:
    """테스트 엔진과 짧은 timeout의 HTTP 클라이언트로 Dispatcher를 만든다."""
    return Dispatcher(worker_settings, engine, httpx.AsyncClient(timeout=2))


async def dispatch_row(db: Db, dispatch_id: str) -> dict[str, object]:
    """dispatch 행의 상태·시도 횟수·마지막 오류와, 다음 시도가 미래로 미뤄졌는지(deferred)를 읽는다."""
    return await db.one("SELECT status, attempt_count, last_http_status, last_error, "
                        "next_attempt_at > clock_timestamp() AS deferred "
                        "FROM nifi_ops.load_dispatch WHERE dispatch_id = CAST(:d AS uuid)", d=dispatch_id)


def test_backoff(worker_settings: Settings) -> None:
    """backoff는 최솟값에서 시작해 시도마다 두 배로 늘고 최댓값에서 멈춘다."""
    assert backoff(worker_settings, 1) == timedelta(seconds=5)
    assert backoff(worker_settings, 3) == timedelta(seconds=20)
    assert backoff(worker_settings, 10) == timedelta(minutes=1)


@respx.mock
async def test_sends_validation_and_marks_sent(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                               worker_settings: Settings) -> None:
    """PENDING VALIDATE_RUN을 NiFi로 보내고 202를 받으면 SENT로 기록한다.

    본문과 X-Run-Id·X-Dispatch-Id 헤더를 확인하고, SENT는 다시 보내지 않는다.
    """
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
    """5xx 응답은 PENDING으로 되돌리고 backoff만큼 다음 시도를 미룬다."""
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
    """연결 오류도 재시도 대상이다. HTTP 상태 없이 오류 메시지만 남긴다."""
    _, dispatch_id = await complete_run(client)
    respx.post(VALIDATE).mock(side_effect=httpx.ConnectError("refused"))
    await (await make_dispatcher(worker_settings, engine)).dispatch_once()
    row = await dispatch_row(db, dispatch_id)
    assert row["status"] == "PENDING" and row["last_http_status"] is None
    assert "refused" in str(row["last_error"])


@respx.mock
async def test_max_attempts_marks_dead(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                       worker_settings: Settings) -> None:
    """최대 시도 횟수만큼 실패하면 DEAD가 되고 DISPATCH_DEAD 이벤트가 한 번 남는다."""
    run, dispatch_id = await complete_run(client)
    respx.post(VALIDATE).mock(return_value=httpx.Response(500))
    d = await make_dispatcher(worker_settings, engine)
    for _ in range(3):
        await d.dispatch_once()
        # backoff를 기다리지 않도록 다음 시도 시각을 지금으로 당긴다.
        await db.execute("UPDATE nifi_ops.load_dispatch SET next_attempt_at = clock_timestamp() "
                         "WHERE status = 'PENDING'")
    row = await dispatch_row(db, dispatch_id)
    assert row["status"] == "DEAD" and row["attempt_count"] == 3
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_event WHERE event_name = 'DISPATCH_DEAD' "
                           "AND run_id = CAST(:r AS uuid)", r=run.run_id) == 1


@respx.mock
async def test_client_error_marks_dead_immediately(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                                   worker_settings: Settings) -> None:
    """4xx 응답은 다시 보내도 성공할 수 없으므로 첫 시도에 바로 DEAD가 된다."""
    _, dispatch_id = await complete_run(client)
    respx.post(VALIDATE).mock(return_value=httpx.Response(400))
    await (await make_dispatcher(worker_settings, engine)).dispatch_once()
    assert (await dispatch_row(db, dispatch_id))["status"] == "DEAD"


async def test_ack_before_mark_sent_keeps_acked(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                                worker_settings: Settings) -> None:
    """NiFi가 202 직후 /validation/start를 먼저 호출해도 ACKED가 SENT로 덮이지 않는다."""
    run, dispatch_id = await complete_run(client)
    # dispatcher가 lease를 잡고 NiFi에 보내는 중인 시점을 만든다.
    leased = await in_tx(engine, lambda c: dispatch.lease_due(c, batch=10, lease=timedelta(seconds=60)))
    assert [str(d.dispatch_id) for d in leased] == [dispatch_id]
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
    assert r.json()["started"] is True
    # 202 응답을 받은 dispatcher가 뒤늦게 SENT로 기록하려 하지만 이미 ACKED라 반영되지 않는다.
    assert await in_tx(engine, lambda c: dispatch.mark_sent(c, UUID(dispatch_id), 202)) is False
    assert (await dispatch_row(db, dispatch_id))["status"] == "ACKED"


@respx.mock
async def test_concurrent_dispatchers_send_once(client: httpx.AsyncClient, engine: AsyncEngine,
                                                worker_settings: Settings) -> None:
    """dispatcher 4개가 동시에 돌아도 dispatch 5건은 각각 정확히 한 번만 전송된다."""
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
    # lease만 잡고 보내기 전에 죽은 worker를 흉내 낸다(attempt_count가 1 늘어난다).
    await in_tx(engine, lambda c: dispatch.lease_due(c, batch=10, lease=timedelta(seconds=60)))
    route = respx.post(VALIDATE).mock(return_value=httpx.Response(202))
    d = await make_dispatcher(worker_settings, engine)
    assert await d.dispatch_once() == 0  # lease 유효
    # lease 만료를 흉내 낸다. lease 끝 시각은 next_attempt_at에 기록된다.
    await db.execute("UPDATE nifi_ops.load_dispatch SET next_attempt_at = clock_timestamp()")
    assert await d.dispatch_once() == 1 and route.call_count == 1
    assert (await dispatch_row(db, dispatch_id))["attempt_count"] == 2


@respx.mock
async def test_reissue_body(client: httpx.AsyncClient, engine: AsyncEngine,
                            worker_settings: Settings) -> None:
    """REISSUE_PARTITION dispatch는 NiFi가 파티션을 다시 추출하는 데 필요한 정보를 모두 담는다.

    run 경로, snapshot SCN, 파티션 경계·기대 행 수가 manifest 그대로 실린다.
    """
    run = await start_run(client, [3, 4])
    await claim(client, run, "0001")
    await in_tx(engine, lambda c: dispatch.enqueue_reissue(c, UUID(run.run_id), "0001"))
    route = respx.post(f"{NIFI}/reissue/ORACLE_INSP_DTL_DAILY").mock(return_value=httpx.Response(202))
    await (await make_dispatcher(worker_settings, engine)).dispatch_once()
    body = json.loads(route.calls.last.request.content)
    # dispatchId와 businessKey는 생성 값이라 비교에서 빼고 나머지 필드를 정확히 비교한다.
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
    """LISTEN 연결이 끊기면 다시 연결해 NOTIFY를 계속 받는다.

    재연결 뒤에도 LISTEN 연결은 하나뿐이어야 한다(연결 누수 없음).
    """
    route = respx.post(VALIDATE).mock(return_value=httpx.Response(202))
    d = await make_dispatcher(worker_settings, engine)
    stop = asyncio.Event()
    task = asyncio.create_task(d.run(stop))
    try:
        await asyncio.sleep(0.5)
        # DB 쪽에서 LISTEN 연결을 강제로 끊어 네트워크 단절을 흉내 낸다.
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
