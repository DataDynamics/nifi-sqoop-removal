import httpx
from fastapi import FastAPI

from tests.conftest import OPERATOR_TOKEN
from tests.helpers import create_run


async def test_healthz_and_readyz(client: httpx.AsyncClient) -> None:
    assert (await client.get("/healthz")).json() == {"status": "ok"}
    assert (await client.get("/readyz")).json() == {"status": "ok"}


async def test_metrics_exposed(client: httpx.AsyncClient) -> None:
    await client.get("/healthz")
    r = await client.get("/metrics")
    assert r.status_code == 200
    assert "lca_requests_total" in r.text


async def test_request_id_is_propagated(client: httpx.AsyncClient) -> None:
    r = await client.get("/healthz", headers={"X-Request-Id": "nifi-req-1"})
    assert r.headers["X-Request-Id"] == "nifi-req-1"
    r = await client.get("/healthz", headers={"X-Request-Id": "bad id with spaces"})
    assert r.headers["X-Request-Id"] != "bad id with spaces"


async def test_missing_and_wrong_token(app: FastAPI) -> None:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        assert (await c.post("/v1/runs", json={})).status_code == 401
        r = await c.post("/v1/runs", json={}, headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 403


async def test_operator_can_read_but_not_write(app: FastAPI, client: httpx.AsyncClient) -> None:
    run = await create_run(client)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                                 headers={"Authorization": f"Bearer {OPERATOR_TOKEN}"}) as op:
        assert (await op.get(f"/v1/runs/{run.run_id}")).status_code == 200
        r = await op.post(f"/v1/runs/{run.run_id}/manifest", json={})
        assert r.status_code == 403


async def test_error_details_with_reserved_names(client: httpx.AsyncClient) -> None:
    """details에 status 같은 키가 있어도 오류 응답이 만들어진다(회귀 테스트)."""
    run = await create_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/fail", json={
        "expectedStatus": "EXTRACTING", "failStatus": "FAILED_EXTRACT", "errorStage": "X", "errorCode": "X"})
    assert r.status_code == 409
    assert r.json()["details"] == {"runStatus": "CREATED"}
