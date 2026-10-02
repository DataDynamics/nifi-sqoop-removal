import asyncio
import uuid

import httpx

from tests.conftest import Db
from tests.helpers import complete_run, start_run


async def test_start_validation(client: httpx.AsyncClient, db: Db) -> None:
    run, dispatch_id = await complete_run(client, [3, 4])
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start",
                          json={"dispatchId": dispatch_id, "node": "nifi-01"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["started"] is True and body["runStatus"] == "STAGE_VALIDATING"
    assert body["jobKey"] == "ORACLE_INSP_DTL_DAILY"
    assert body["hdfsRunPath"] == run.hdfs_run_path
    assert body["snapshotScn"] == "1234567890"
    assert body["sourceCount"] == 7 and body["extractedCount"] == 7
    assert body["sourceMetrics"] == {"AMOUNT_SUM": "123.45", "SOURCE_COUNT": "7"}
    d = await db.one("SELECT status, acked_at FROM nifi_ops.load_dispatch "
                     "WHERE dispatch_id = CAST(:d AS uuid)", d=dispatch_id)
    assert d["status"] == "ACKED" and d["acked_at"] is not None

    again = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
    assert again.json() == {"started": False, "runStatus": "STAGE_VALIDATING", "jobKey": None,
                            "businessKey": None, "snapshotScn": None, "hdfsRunPath": None,
                            "stageTable": None, "sourceCount": None, "extractedCount": None,
                            "sourceMetrics": {}}


async def test_start_requires_matching_dispatch(client: httpx.AsyncClient) -> None:
    run, _ = await complete_run(client)
    _, other_dispatch = await complete_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": other_dispatch})
    assert r.status_code == 409 and r.json()["code"] == "DISPATCH_MISMATCH"
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": str(uuid.uuid4())})
    assert r.status_code == 409


async def test_start_before_extract_complete(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [3])
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": str(uuid.uuid4())})
    assert r.status_code == 409


async def test_concurrent_starts_single_winner(client: httpx.AsyncClient) -> None:
    run, dispatch_id = await complete_run(client)
    results = await asyncio.gather(*(
        client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
        for _ in range(8)))
    assert sum(r.json()["started"] for r in results) == 1
