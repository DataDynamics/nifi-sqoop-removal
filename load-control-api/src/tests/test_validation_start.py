"""NiFi 검증 flow의 시작 호출(/validation/start)을 검증한다.

시작 호출은 VALIDATE_RUN dispatch의 ACK 역할을 하고, 한 번만 started=True가 된다.
"""

import asyncio
import uuid

import httpx

from tests.conftest import Db
from tests.helpers import complete_run, start_run


async def test_start_validation(client: httpx.AsyncClient, db: Db) -> None:
    """검증 시작은 run을 STAGE_VALIDATING으로 바꾸고 검증에 필요한 run 정보를 돌려준다.

    dispatch는 ACKED가 되고, 두 번째 호출은 started=False와 빈 정보만 돌려준다
    (NiFi는 이를 보고 중복 실행을 멈춘다).
    """
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
    """다른 run의 dispatch나 없는 dispatch ID로는 시작할 수 없다(409)."""
    run, _ = await complete_run(client)
    _, other_dispatch = await complete_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": other_dispatch})
    assert r.status_code == 409 and r.json()["code"] == "DISPATCH_MISMATCH"
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": str(uuid.uuid4())})
    assert r.status_code == 409


async def test_start_before_extract_complete(client: httpx.AsyncClient) -> None:
    """추출이 끝나기 전(EXTRACTING)에는 검증을 시작할 수 없다(409)."""
    run = await start_run(client, [3])
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": str(uuid.uuid4())})
    assert r.status_code == 409


async def test_concurrent_starts_single_winner(client: httpx.AsyncClient) -> None:
    """검증 시작 8개가 동시에 와도 하나만 started=True다."""
    run, dispatch_id = await complete_run(client)
    results = await asyncio.gather(*(
        client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
        for _ in range(8)))
    assert sum(r.json()["started"] for r in results) == 1
