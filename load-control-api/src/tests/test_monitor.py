"""모니터(TUI)용 조회 API를 검증한다: 요약·경보, 검증 지표·이벤트 목록, run 목록 진행률."""

import uuid

import httpx

from tests.conftest import Db
from tests.helpers import complete_run, create_run, to_staging_validated


async def test_summary_counts_and_alerts(client: httpx.AsyncClient, operator: httpx.AsyncClient,
                                         db: Db) -> None:
    """요약 API가 상태별 run 수, 최근 실패, dispatch 수, 경보를 모아 보여 준다.

    실패·PUBLISH_UNKNOWN·DEAD dispatch·heartbeat 정체를 SQL로 만들어 두고,
    경보 종류와 대상 run, ERROR 경보가 WARN보다 먼저 정렬되는지,
    DEAD 경보에만 dispatchId가 붙는지 확인한다. nifi 토큰으로도 조회할 수 있다.
    """
    active = await create_run(client)                       # CREATED
    staged = await to_staging_validated(client)             # STAGING_VALIDATED
    failed = await create_run(client)
    # 아래 SQL은 API로 만들기 번거로운 경보 대상 상태(실패, PUBLISH_UNKNOWN, DEAD, 정체)를 직접 만든다.
    await db.execute("UPDATE nifi_ops.load_run SET status = 'FAILED_EXTRACT', error_code = 'ORA-00942', "
                     "completed_at = clock_timestamp() WHERE run_id = :id", id=uuid.UUID(failed.run_id))
    unknown = await create_run(client)
    await db.execute("UPDATE nifi_ops.load_run SET status = 'PUBLISH_UNKNOWN' WHERE run_id = :id",
                     id=uuid.UUID(unknown.run_id))
    dead_run, _ = await complete_run(client)
    await db.execute("UPDATE nifi_ops.load_dispatch SET status = 'DEAD', last_error = 'HTTP 404' "
                     "WHERE run_id = :id", id=uuid.UUID(dead_run.run_id))
    stale = await create_run(client)
    await db.execute("UPDATE nifi_ops.load_run SET heartbeat_at = clock_timestamp() - interval '3 hours' "
                     "WHERE run_id = :id", id=uuid.UUID(stale.run_id))

    r = await operator.get("/v1/monitor/summary")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["activeRuns"] == {"CREATED": 2, "STAGING_VALIDATED": 1, "PUBLISH_UNKNOWN": 1,
                                  "EXTRACTED_VALIDATED": 1}
    assert body["recentRuns"] == {"FAILED_EXTRACT": 1} and body["recentWindowHours"] == 24
    assert body["dispatches"] == {"PENDING": 0, "SENT": 0, "DEAD": 1}
    # 정상 진행 중인 run(active, staged)은 경보에 나오지 않아야 한다.
    kinds = {(a["kind"], a["runId"]) for a in body["alerts"]}
    assert kinds == {("PUBLISH_UNKNOWN", unknown.run_id), ("DISPATCH_DEAD", dead_run.run_id),
                     ("RUN_FAILED", failed.run_id), ("RUN_STALE", stale.run_id)}
    assert [a["severity"] for a in body["alerts"]][-1] == "WARN"  # ERROR가 먼저
    dead = next(a for a in body["alerts"] if a["kind"] == "DISPATCH_DEAD")
    assert dead["dispatchId"] and all(a["dispatchId"] is None for a in body["alerts"] if a is not dead)
    failed_alert = next(a for a in body["alerts"] if a["kind"] == "RUN_FAILED")
    assert failed_alert["message"] == "ORA-00942"
    assert active.run_id not in {a["runId"] for a in body["alerts"]} and staged.run_id
    assert (await client.get("/v1/monitor/summary")).status_code == 200  # nifi 토큰도 조회 가능


async def test_validations_and_events(client: httpx.AsyncClient, db: Db) -> None:
    """검증 지표는 stage·지표명 순으로, 이벤트는 시간 순으로 돌려준다.

    limit을 주면 가장 최근 이벤트 N개를 돌려주고, 없는 run은 404다.
    """
    run = await to_staging_validated(client)
    r = await client.get(f"/v1/runs/{run.run_id}/validations")
    metrics = r.json()["metrics"]
    assert [(m["stage"], m["metricName"]) for m in metrics] == [
        ("SOURCE", "AMOUNT_SUM"), ("SOURCE", "SOURCE_COUNT"), ("STAGING", "DUP_PK_COUNT"),
        ("STAGING", "STAGE_COUNT")]
    events = (await client.get(f"/v1/runs/{run.run_id}/events")).json()["events"]
    names = [e["name"] for e in events]
    assert names[0] == "RUN_STARTED" and names[-1] == "STAGE_VALIDATED"
    assert [e["name"] for e in (await client.get(f"/v1/runs/{run.run_id}/events",
                                                  params={"limit": 2})).json()["events"]] == names[-2:]
    assert (await client.get(f"/v1/runs/{uuid.uuid4()}/events")).status_code == 404
    assert (await client.get(f"/v1/runs/{uuid.uuid4()}/validations")).status_code == 404


async def test_run_list_has_progress(client: httpx.AsyncClient) -> None:
    """run 목록 항목에 파티션 진행률(기대·성공·실패 수)과 heartbeat가 담긴다."""
    run, _ = await complete_run(client, [3, 0, 4])
    item = (await client.get("/v1/runs")).json()[0]
    assert item["runId"] == run.run_id
    progress = (item["expectedPartitionCount"], item["successPartitionCount"], item["failedPartitionCount"])
    assert progress == (3, 3, 0)
    assert item["stagingCount"] is None and item["heartbeatAt"]


async def test_cleanup_failed_alert_shows_latest_message(client: httpx.AsyncClient,
                                                         operator: httpx.AsyncClient, db: Db) -> None:
    """같은 run의 정리 실패가 여러 번이면 경보는 한 건이고 가장 최근 메시지를 보여 준다."""
    run, _ = await complete_run(client)
    for minutes, message in [(30, "old failure"), (1, "latest failure"), (20, "middle failure")]:
        await db.execute(
            "INSERT INTO nifi_ops.load_event (event_id, event_level, event_name, run_id, job_key,"
            " business_key, process_group, processor_name, message, event_time)"
            " SELECT gen_random_uuid(), 'WARN', 'CLEANUP_FAILED', run_id, job_key, business_key,"
            " 'TEST', 'test', :m, clock_timestamp() - make_interval(mins => :n)"
            " FROM nifi_ops.load_run WHERE run_id = :id",
            m=message, n=minutes, id=uuid.UUID(run.run_id))
    alerts = (await operator.get("/v1/monitor/summary")).json()["alerts"]
    cleanup = [a for a in alerts if a["kind"] == "CLEANUP_FAILED"]
    assert len(cleanup) == 1 and cleanup[0]["message"] == "latest failure"
