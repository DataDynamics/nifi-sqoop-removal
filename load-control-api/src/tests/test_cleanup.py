"""정리(cleanup) 대상 조회와 정리 완료 보고를 검증한다.

끝난 run은 결과별 보존 기간(성공 3일, 실패 14일)이 지나야 대상이 되고,
PUBLISH_UNKNOWN은 운영자가 확정하기 전까지 대상이 아니다.
"""

import uuid

import httpx

from tests.conftest import Db
from tests.helpers import create_run, to_published


async def _finish(db: Db, run_id: str, status: str, days_ago: float) -> None:
    """run을 끝난 상태로 만들고 끝난 시각을 과거로 옮긴다."""
    await db.execute("UPDATE nifi_ops.load_run SET status = :status, "
                     "completed_at = clock_timestamp() - make_interval(secs => :secs) WHERE run_id = :run_id",
                     status=status, secs=days_ago * 86400, run_id=uuid.UUID(run_id))


async def test_candidates_follow_retention(client: httpx.AsyncClient, db: Db) -> None:
    """보존 기간이 지난 끝난 run만 오래된 순으로 정리 대상에 나온다.

    보존 기간 안의 run, 진행 중 run, PUBLISH_UNKNOWN은 빠지고,
    jobKey·limit 필터와 잘못된 jobKey 형식(422)도 확인한다.
    """
    old_success = await create_run(client)
    new_success = await create_run(client)
    old_failed = await create_run(client)
    mid_failed = await create_run(client)
    active = await create_run(client)
    unknown = await create_run(client)
    await _finish(db, old_success.run_id, "SUCCESS", 4)      # 보존 3일 지남
    await _finish(db, new_success.run_id, "SUCCESS", 1)
    await _finish(db, old_failed.run_id, "FAILED_EXTRACT", 15)  # 보존 14일 지남
    await _finish(db, mid_failed.run_id, "TIMED_OUT", 5)
    await _finish(db, unknown.run_id, "PUBLISH_UNKNOWN", 30)  # 운영자 확정 전에는 대상 아님
    # 끝나지 않은 run은 시작한 지 오래돼도 정리 대상이 아니어야 한다.
    await db.execute("UPDATE nifi_ops.load_run SET started_at = started_at - interval '30 days' "
                     "WHERE run_id = :run_id", run_id=uuid.UUID(active.run_id))

    r = await client.get("/v1/cleanup/candidates", params={"jobKey": "ORACLE_INSP_DTL_DAILY"})
    assert r.status_code == 200, r.text
    runs = r.json()["runs"]
    assert [x["runId"] for x in runs] == [old_failed.run_id, old_success.run_id]  # 오래된 순
    assert runs[1]["hdfsRunPath"] == old_success.hdfs_run_path
    assert runs[1]["stageTable"] == f"tmp_insp_dtl_{uuid.UUID(old_success.run_id).hex}"
    assert runs[1]["status"] == "SUCCESS"

    assert (await client.get("/v1/cleanup/candidates", params={"jobKey": "OTHER_JOB"})).json() == {"runs": []}
    assert len((await client.get("/v1/cleanup/candidates", params={"limit": 1})).json()["runs"]) == 1
    assert (await client.get("/v1/cleanup/candidates", params={"jobKey": "bad-key"})).status_code == 422


async def test_report_cleanup(client: httpx.AsyncClient, operator: httpx.AsyncClient, db: Db) -> None:
    """정리 완료 보고는 cleaned_at과 RUN_CLEANED 이벤트를 남기고 멱등이다.

    NiFi 재시도와 운영자 수동 기록은 changed=False로 받아들이며,
    정리된 run은 다시 대상에 나오지 않는다.
    """
    run = await create_run(client)
    await _finish(db, run.run_id, "FAILED_STAGE_VALIDATION", 20)
    url = f"/v1/runs/{run.run_id}/cleanup"
    body = {"droppedTable": "stg.tmp_x", "deletedPath": run.hdfs_run_path}
    r = await client.post(url, json=body)
    assert r.json() == {"runStatus": "FAILED_STAGE_VALIDATION", "changed": True}
    assert (await client.post(url, json=body)).json()["changed"] is False  # NiFi 재시도
    assert (await operator.post(url, json=body)).json()["changed"] is False  # 운영자 수동 기록도 허용
    row = await db.one("SELECT cleaned_at IS NOT NULL AS cleaned FROM nifi_ops.load_run")
    assert row["cleaned"]
    event = await db.one("SELECT details->>'deletedPath' AS path FROM nifi_ops.load_event "
                         "WHERE event_name = 'RUN_CLEANED'")
    assert event["path"] == run.hdfs_run_path
    assert (await client.get("/v1/cleanup/candidates")).json() == {"runs": []}


async def test_report_cleanup_rejects_not_due(client: httpx.AsyncClient, db: Db) -> None:
    """정리 시점이 아닌 run(진행 중, 보존 기간 안)의 보고는 409로 거부한다.

    없는 run은 404, 경로 탈출(..)이 있는 deletedPath는 422다.
    """
    run, _ = await to_published(client)  # 진행 중(PUBLISHED)
    r = await client.post(f"/v1/runs/{run.run_id}/cleanup", json={})
    assert r.status_code == 409 and r.json()["code"] == "CLEANUP_NOT_DUE"
    await _finish(db, run.run_id, "SUCCESS", 1)  # 보존 기간 안
    assert (await client.post(f"/v1/runs/{run.run_id}/cleanup", json={})).status_code == 409
    assert (await client.post(f"/v1/runs/{uuid.uuid4()}/cleanup", json={})).status_code == 404
    bad = await client.post(f"/v1/runs/{run.run_id}/cleanup", json={"deletedPath": "../etc"})
    assert bad.status_code == 422
