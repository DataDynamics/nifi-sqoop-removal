import uuid

import httpx

from tests.conftest import Db
from tests.helpers import complete_run, create_run, to_staging_validated


async def test_summary_counts_and_alerts(client: httpx.AsyncClient, operator: httpx.AsyncClient,
                                         db: Db) -> None:
    active = await create_run(client)                       # CREATED
    staged = await to_staging_validated(client)             # STAGING_VALIDATED
    failed = await create_run(client)
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
    kinds = {(a["kind"], a["runId"]) for a in body["alerts"]}
    assert kinds == {("PUBLISH_UNKNOWN", unknown.run_id), ("DISPATCH_DEAD", dead_run.run_id),
                     ("RUN_FAILED", failed.run_id), ("RUN_STALE", stale.run_id)}
    assert [a["severity"] for a in body["alerts"]][-1] == "WARN"  # ERROR가 먼저
    failed_alert = next(a for a in body["alerts"] if a["kind"] == "RUN_FAILED")
    assert failed_alert["message"] == "ORA-00942"
    assert active.run_id not in {a["runId"] for a in body["alerts"]} and staged.run_id
    assert (await client.get("/v1/monitor/summary")).status_code == 200  # nifi 토큰도 조회 가능


async def test_validations_and_events(client: httpx.AsyncClient, db: Db) -> None:
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
    run, _ = await complete_run(client, [3, 0, 4])
    item = (await client.get("/v1/runs")).json()[0]
    assert item["runId"] == run.run_id
    progress = (item["expectedPartitionCount"], item["successPartitionCount"], item["failedPartitionCount"])
    assert progress == (3, 3, 0)
    assert item["stagingCount"] is None and item["heartbeatAt"]
