import uuid

import httpx

from tests.conftest import Db
from tests.helpers import complete_run, create_run, to_staging_validated


async def _publish_unknown(client: httpx.AsyncClient) -> str:
    run = await to_staging_validated(client)
    token = str(uuid.uuid4())
    await client.post(f"/v1/runs/{run.run_id}/publish/claim", json={"publishToken": token})
    await client.post(f"/v1/runs/{run.run_id}/publish/result",
                      json={"publishToken": token, "outcome": "PUBLISH_UNKNOWN"})
    return run.run_id


async def test_resolve_publish_unknown(client: httpx.AsyncClient, operator: httpx.AsyncClient,
                                       db: Db) -> None:
    run_id = await _publish_unknown(client)
    url = f"/v1/runs/{run_id}/publish-unknown/resolve"
    body = {"resolution": "PUBLISHED", "reason": "Hive query history에서 성공 확인, target count 일치"}
    assert (await client.post(url, json=body)).status_code == 403  # NiFi 토큰으로는 불가
    assert (await operator.post(url, json={"resolution": "PUBLISHED", "reason": "x"})).status_code == 422
    r = await operator.post(url, json=body)
    assert r.json() == {"runStatus": "PUBLISHED", "changed": True}
    assert (await operator.post(url, json=body)).json()["changed"] is False
    event = await db.one("SELECT message, details->>'resolution' AS resolution FROM nifi_ops.load_event "
                         "WHERE event_name = 'PUBLISH_UNKNOWN_RESOLVED'")
    assert event["resolution"] == "PUBLISHED" and "Hive" in event["message"]


async def test_resolve_requires_publish_unknown(client: httpx.AsyncClient,
                                                operator: httpx.AsyncClient) -> None:
    run = await to_staging_validated(client)
    r = await operator.post(f"/v1/runs/{run.run_id}/publish-unknown/resolve",
                            json={"resolution": "FAILED_PUBLISH", "reason": "not applicable"})
    assert r.status_code == 409


async def test_resend_dead_dispatch(client: httpx.AsyncClient, operator: httpx.AsyncClient, db: Db) -> None:
    run, dispatch_id = await complete_run(client)
    await db.execute("UPDATE nifi_ops.load_dispatch SET status = 'DEAD', attempt_count = 20")
    url = f"/v1/runs/{run.run_id}/dispatches/{dispatch_id}/resend"
    assert (await client.post(url)).status_code == 403
    r = await operator.post(url)
    assert r.json() == {"dispatchId": dispatch_id, "status": "PENDING"}
    row = await db.one("SELECT status, attempt_count FROM nifi_ops.load_dispatch")
    assert row == {"status": "PENDING", "attempt_count": 0}
    assert (await operator.post(url)).status_code == 409  # PENDING은 재전송 대상 아님
    other, _ = await complete_run(client)
    wrong_run = await operator.post(f"/v1/runs/{other.run_id}/dispatches/{dispatch_id}/resend")
    assert wrong_run.status_code == 404


async def test_list_runs(client: httpx.AsyncClient, operator: httpx.AsyncClient) -> None:
    a = await create_run(client, business_key="2026-10-01")
    await create_run(client, business_key="2026-10-02")
    r = await operator.get("/v1/runs", params={"businessKey": "2026-10-01"})
    assert [x["runId"] for x in r.json()] == [a.run_id]
    r = await client.get("/v1/runs", params={"status": "CREATED", "limit": 1})
    assert len(r.json()) == 1
    assert (await client.get("/v1/runs", params={"status": "NOPE"})).status_code == 422
