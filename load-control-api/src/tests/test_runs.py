"""run 생성, 업무키별 active run 중복 방지, 입력 검증, run 실패 보고를 검증한다."""

import uuid

import httpx

from tests.conftest import Db
from tests.helpers import create_run


async def test_create_run(client: httpx.AsyncClient, db: Db) -> None:
    """run을 만들면 CREATED 상태와 run별 HDFS 경로·stage 테이블 이름을 돌려준다.

    hdfsRoot 끝의 /는 정규화되고, NiFi가 문자열로 보낸 불린도 받아들인다.
    RUN_STARTED 이벤트가 하나 남는다.
    """
    r = await client.post("/v1/runs", json={
        "jobKey": "ORACLE_INSP_DTL_DAILY", "businessKey": "2026-09-28",
        "hdfsRoot": "/data/nifi/stage/", "stageTablePrefix": "TMP_INSP_DTL_",
        "allowEmptySource": "false", "parameters": {"trigger": "SCHEDULE"}})
    assert r.status_code == 200, r.text
    body = r.json()
    run_id = uuid.UUID(body["runId"])
    assert body["status"] == "CREATED"
    assert body["hdfsRunPath"] == f"/data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id={run_id}"
    assert body["stageTable"] == f"tmp_insp_dtl_{run_id.hex}"
    row = await db.one("SELECT status, parameters FROM nifi_ops.load_run WHERE run_id = :id", id=run_id)
    assert row["status"] == "CREATED"
    events = await db.all("SELECT event_name FROM nifi_ops.load_event WHERE run_id = :id", id=run_id)
    assert [e["event_name"] for e in events] == ["RUN_STARTED"]


async def test_duplicate_active_run_conflict(client: httpx.AsyncClient) -> None:
    """같은 업무키로 진행 중인 run이 있으면 새 run은 DUPLICATE_ACTIVE_RUN(409)이다."""
    await create_run(client, business_key="2026-09-28")
    r = await client.post("/v1/runs", json={
        "jobKey": "ORACLE_INSP_DTL_DAILY", "businessKey": "2026-09-28",
        "hdfsRoot": "/data/nifi/stage", "stageTablePrefix": "TMP_INSP_DTL_"})
    assert r.status_code == 409
    assert r.json()["code"] == "DUPLICATE_ACTIVE_RUN"


async def test_new_run_allowed_after_failure(client: httpx.AsyncClient) -> None:
    """run이 실패로 끝나면 같은 업무키로 새 run을 만들 수 있다."""
    run = await create_run(client, business_key="2026-09-29")
    r = await client.post(f"/v1/runs/{run.run_id}/fail", json={
        "expectedStatus": "CREATED", "failStatus": "FAILED_MANIFEST",
        "errorStage": "SNAPSHOT", "errorCode": "SCN_INVALID", "message": "bad scn"})
    assert r.status_code == 200 and r.json()["changed"] is True
    await create_run(client, business_key="2026-09-29")


async def test_invalid_inputs_rejected(client: httpx.AsyncClient) -> None:
    """잘못된 jobKey, 상대·탈출 경로, SQL 주입 가능한 prefix, 모르는 필드는 422다."""
    base = {"jobKey": "ORACLE_INSP_DTL_DAILY", "businessKey": "2026-09-28",
            "hdfsRoot": "/data/nifi/stage", "stageTablePrefix": "TMP_"}
    for patch in ({"jobKey": "lower-case"}, {"hdfsRoot": "relative/path"},
                  {"hdfsRoot": "/data/../etc"}, {"stageTablePrefix": "x; DROP"}, {"unknown": 1}):
        r = await client.post("/v1/runs", json={**base, **patch})
        assert r.status_code == 422, (patch, r.text)


async def test_fail_run_rules(client: httpx.AsyncClient) -> None:
    """run 실패 보고 규칙을 확인한다.

    실패 상태가 아닌 failStatus는 422, expectedStatus가 현재 상태와 다르면 409,
    같은 보고를 다시 보내면 changed=False(멱등)다.
    """
    run = await create_run(client)
    url = f"/v1/runs/{run.run_id}/fail"
    bad = await client.post(url, json={"expectedStatus": "CREATED", "failStatus": "SUCCESS",
                                       "errorStage": "X", "errorCode": "X"})
    assert bad.status_code == 422
    mismatch = await client.post(url, json={"expectedStatus": "EXTRACTING", "failStatus": "FAILED_EXTRACT",
                                            "errorStage": "X", "errorCode": "X"})
    assert mismatch.status_code == 409
    body = {"expectedStatus": "CREATED", "failStatus": "FAILED_MANIFEST",
            "errorStage": "SOURCE", "errorCode": "EMPTY"}
    assert (await client.post(url, json=body)).json()["changed"] is True
    again = await client.post(url, json=body)  # 멱등
    assert again.status_code == 200 and again.json()["changed"] is False


async def test_get_run_not_found(client: httpx.AsyncClient) -> None:
    """없는 run 조회는 RUN_NOT_FOUND(404)다."""
    r = await client.get(f"/v1/runs/{uuid.uuid4()}")
    assert r.status_code == 404
    assert r.json()["code"] == "RUN_NOT_FOUND"
