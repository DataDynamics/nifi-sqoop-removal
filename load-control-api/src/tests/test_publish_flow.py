"""STAGING 검증 → publish → TARGET 검증 → SUCCESS로 이어지는 후반 흐름을 검증한다.

publish는 publish token으로 한 번만 시작할 수 있고, 결과를 모르는 경우(PUBLISH_UNKNOWN)는
자동으로 확정하지 않는다.
"""

import asyncio
import uuid

import httpx

from tests.conftest import Db
from tests.helpers import complete_run, create_run, metrics, to_published, to_staging_validated


async def test_full_lifecycle_to_success(client: httpx.AsyncClient, db: Db) -> None:
    """PUBLISHED run에 TARGET 지표 PASS를 보고하면 SUCCESS로 끝난다.

    success 호출은 멱등이고, staging·target 건수, publish token, 완료 시각,
    이벤트 순서가 모두 남는다. 최종 상태라 같은 업무키로 새 run을 만들 수 있다.
    """
    run, token = await to_published(client)
    r = await client.post(f"/v1/runs/{run.run_id}/validations", json={
        "stage": "TARGET", "queryVersion": "v1", "metrics": metrics(("TARGET_COUNT", "7", "PASS"))})
    assert r.json() == {"recorded": 1, "failCount": 0, "runStatus": "PUBLISHED"}
    r = await client.post(f"/v1/runs/{run.run_id}/success", json={})
    assert r.json() == {"success": True, "runStatus": "SUCCESS", "reasons": []}
    again = await client.post(f"/v1/runs/{run.run_id}/success", json={})
    assert again.json()["success"] is True

    row = await db.one("SELECT status, staging_count, target_count, publish_token, published_at IS NOT NULL "
                       "AS published, completed_at IS NOT NULL AS completed FROM nifi_ops.load_run "
                       "WHERE run_id = CAST(:id AS uuid)", id=run.run_id)
    assert row == {"status": "SUCCESS", "staging_count": 7, "target_count": 7,
                   "publish_token": uuid.UUID(token), "published": True, "completed": True}
    names = [e["event_name"] for e in await db.all(
        "SELECT event_name FROM nifi_ops.load_event WHERE run_id = CAST(:id AS uuid) ORDER BY event_time",
        id=run.run_id)]
    tail = ["EXTRACT_VALIDATED", "STAGE_VALIDATION_STARTED", "STAGE_VALIDATED", "PUBLISH_STARTED",
            "PUBLISH_FINISHED", "RUN_SUCCESS"]
    assert names[-len(tail):] == tail
    # 최종 상태이므로 같은 업무키로 새 run을 만들 수 있다
    bk = await db.scalar("SELECT business_key FROM nifi_ops.load_run WHERE run_id = CAST(:id AS uuid)",
                         id=run.run_id)
    await create_run(client, business_key=bk)


async def test_stage_validated_requires_all_pass(client: httpx.AsyncClient, db: Db) -> None:
    """STAGING 지표가 없거나 하나라도 FAIL이면 stage-validated가 거부된다.

    거부 사유를 돌려주고, NiFi가 run을 실패로 보고한 뒤에는 publish claim도 안 된다.
    """
    run, dispatch_id = await complete_run(client)
    await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
    r = await client.post(f"/v1/runs/{run.run_id}/stage-validated")
    assert r.json() == {"stageValidated": False, "runStatus": "STAGE_VALIDATING", "reasons": ["NO_METRICS"]}
    await client.post(f"/v1/runs/{run.run_id}/validations", json={
        "stage": "STAGING", "metrics": metrics(("STAGE_COUNT", "7", "PASS"), ("MIN_TS", "a", "FAIL"))})
    r = await client.post(f"/v1/runs/{run.run_id}/stage-validated")
    assert r.json()["stageValidated"] is False and r.json()["reasons"] == ["FAIL MIN_TS"]
    fail = await client.post(f"/v1/runs/{run.run_id}/fail", json={
        "expectedStatus": "STAGE_VALIDATING", "failStatus": "FAILED_STAGE_VALIDATION",
        "errorStage": "STAGE_VALIDATE", "errorCode": "DQ_MISMATCH", "message": "MIN_TS"})
    assert fail.json()["runStatus"] == "FAILED_STAGE_VALIDATION"
    claim = await client.post(f"/v1/runs/{run.run_id}/publish/claim",
                              json={"publishToken": str(uuid.uuid4())})
    assert claim.json()["claimed"] is False


async def test_failed_metric_rerun_overrides(client: httpx.AsyncClient) -> None:
    """같은 metric/queryVersion 재보고는 UPSERT된다(NiFi 재시도)."""
    run, dispatch_id = await complete_run(client)
    await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
    url = f"/v1/runs/{run.run_id}/validations"
    await client.post(url, json={"stage": "STAGING", "metrics": metrics(("STAGE_COUNT", "7", "FAIL"))})
    await client.post(url, json={"stage": "STAGING", "metrics": metrics(("STAGE_COUNT", "7", "PASS"))})
    r = await client.post(f"/v1/runs/{run.run_id}/stage-validated")
    assert r.json()["stageValidated"] is True


async def test_validations_require_matching_status(client: httpx.AsyncClient) -> None:
    """지표 stage는 run 상태와 맞아야 한다.

    STAGING_VALIDATED에서 TARGET 지표는 409이고, SOURCE 지표는
    manifest로만 등록하므로 API로 보내면 422다.
    """
    run = await to_staging_validated(client)
    r = await client.post(f"/v1/runs/{run.run_id}/validations",
                          json={"stage": "TARGET", "metrics": metrics(("TARGET_COUNT", "7", "PASS"))})
    assert r.status_code == 409
    bad = await client.post(f"/v1/runs/{run.run_id}/validations",
                            json={"stage": "SOURCE", "metrics": metrics(("X", "1", "PASS"))})
    assert bad.status_code == 422


async def test_publish_claim_rules(client: httpx.AsyncClient) -> None:
    """publish claim은 첫 token만 성공하고, 같은 token 재시도는 성공, 다른 token은 실패한다."""
    run = await to_staging_validated(client)
    url = f"/v1/runs/{run.run_id}/publish/claim"
    token = str(uuid.uuid4())
    assert (await client.post(url, json={"publishToken": token})).json()["claimed"] is True
    assert (await client.post(url, json={"publishToken": token})).json()["claimed"] is True  # 재시도
    other = await client.post(url, json={"publishToken": str(uuid.uuid4())})
    assert other.json() == {"claimed": False, "runStatus": "PUBLISHING"}


async def test_concurrent_publish_claims(client: httpx.AsyncClient) -> None:
    """publish claim 10개가 동시에 와도 하나만 성공한다."""
    run = await to_staging_validated(client)
    results = await asyncio.gather(*(
        client.post(f"/v1/runs/{run.run_id}/publish/claim", json={"publishToken": str(uuid.uuid4())})
        for _ in range(10)))
    assert sum(r.json()["claimed"] for r in results) == 1


async def test_publish_result_rules(client: httpx.AsyncClient) -> None:
    """publish 결과는 claim한 token으로만 보고할 수 있고 멱등이다.

    PUBLISH_UNKNOWN이 된 뒤 늦게 온 PUBLISHED 보고는 409로 거부한다.
    """
    run = await to_staging_validated(client)
    token = str(uuid.uuid4())
    await client.post(f"/v1/runs/{run.run_id}/publish/claim", json={"publishToken": token})
    url = f"/v1/runs/{run.run_id}/publish/result"
    wrong = await client.post(url, json={"publishToken": str(uuid.uuid4()), "outcome": "PUBLISHED"})
    assert wrong.status_code == 409 and wrong.json()["code"] == "PUBLISH_TOKEN_MISMATCH"
    body = {"publishToken": token, "outcome": "PUBLISH_UNKNOWN", "errorCode": "TIMEOUT", "message": "hive"}
    assert (await client.post(url, json=body)).json() == {"runStatus": "PUBLISH_UNKNOWN", "changed": True}
    assert (await client.post(url, json=body)).json()["changed"] is False
    late = await client.post(url, json={"publishToken": token, "outcome": "PUBLISHED"})
    assert late.status_code == 409  # 자동 확정 금지: 운영자만 확정


async def test_failed_publish(client: httpx.AsyncClient, db: Db) -> None:
    """publish 실패 보고는 run을 FAILED_PUBLISH로 끝내고 오류 코드와 완료 시각을 남긴다."""
    run = await to_staging_validated(client)
    token = str(uuid.uuid4())
    await client.post(f"/v1/runs/{run.run_id}/publish/claim", json={"publishToken": token})
    r = await client.post(f"/v1/runs/{run.run_id}/publish/result", json={
        "publishToken": token, "outcome": "FAILED_PUBLISH", "errorCode": "42000", "message": "parse"})
    assert r.json()["runStatus"] == "FAILED_PUBLISH"
    row = await db.one("SELECT error_code, completed_at IS NOT NULL AS done FROM nifi_ops.load_run "
                       "WHERE run_id = CAST(:id AS uuid)", id=run.run_id)
    assert row == {"error_code": "42000", "done": True}


async def test_success_requires_target_pass(client: httpx.AsyncClient) -> None:
    """TARGET 지표가 없거나 FAIL이면 SUCCESS가 되지 않는다.

    이때 NiFi는 run을 FAILED_TARGET_VALIDATION으로 실패 보고한다.
    """
    run, _ = await to_published(client)
    r = await client.post(f"/v1/runs/{run.run_id}/success", json={"targetCount": "7"})
    assert r.json()["reasons"] == ["NO_METRICS"]
    await client.post(f"/v1/runs/{run.run_id}/validations", json={
        "stage": "TARGET", "metrics": metrics(("TARGET_COUNT", "7", "FAIL"))})
    assert (await client.post(f"/v1/runs/{run.run_id}/success", json={})).json()["success"] is False
    fail = await client.post(f"/v1/runs/{run.run_id}/fail", json={
        "expectedStatus": "PUBLISHED", "failStatus": "FAILED_TARGET_VALIDATION",
        "errorStage": "TARGET_VALIDATE", "errorCode": "COUNT_MISMATCH"})
    assert fail.json()["runStatus"] == "FAILED_TARGET_VALIDATION"


async def test_success_before_published(client: httpx.AsyncClient) -> None:
    """PUBLISHED 전에는 success가 거부되고 현재 상태가 사유로 돌아온다."""
    run = await to_staging_validated(client)
    r = await client.post(f"/v1/runs/{run.run_id}/success", json={})
    assert r.json() == {"success": False, "runStatus": "STAGING_VALIDATED",
                        "reasons": ["RUN_STATUS STAGING_VALIDATED"]}
