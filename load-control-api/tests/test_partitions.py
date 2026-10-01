import uuid

import httpx

from tests.conftest import Db
from tests.helpers import chunk_body, claim, report, start_run


async def test_claim_rules(client: httpx.AsyncClient, db: Db) -> None:
    run = await start_run(client, [10, 20])
    url = f"/v1/runs/{run.run_id}/partitions/0000/claim"
    token = await claim(client, run, "0000")

    other = await client.post(url, json={"claimToken": str(uuid.uuid4()), "workerNode": "nifi-02"})
    assert other.json()["claimed"] is False

    retry = await client.post(url, json={"claimToken": token, "workerNode": "nifi-01"})  # 응답 유실 재시도
    assert retry.json() == {"claimed": True, "runStatus": "EXTRACTING",
                            "partitionStatus": "RUNNING", "attempt": 1}
    row = await db.one("SELECT attempt_count, worker_node FROM nifi_ops.load_partition "
                       "WHERE run_id = CAST(:id AS uuid) AND partition_id = '0000'", id=run.run_id)
    assert row == {"attempt_count": 1, "worker_node": "nifi-01"}


async def test_claim_unknown_partition_and_bad_path(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [10, 20])
    body = {"claimToken": str(uuid.uuid4()), "workerNode": "n"}
    assert (await client.post(f"/v1/runs/{run.run_id}/partitions/0009/claim", json=body)).status_code == 404
    assert (await client.post(f"/v1/runs/{run.run_id}/partitions/abc/claim", json=body)).status_code == 422


async def test_full_flow_completes_run_once(client: httpx.AsyncClient, db: Db) -> None:
    run = await start_run(client, [10, 7, 0])
    t0 = await claim(client, run, "0000")
    t1 = await claim(client, run, "0001")

    r = await report(client, run, "0000", t0, 0, 2, 5)
    assert r.json()["partitionStatus"] == "RUNNING" and r.json()["receivedChunks"] == 1
    r = await report(client, run, "0000", t0, 1, 2, 5)
    assert r.json()["partitionStatus"] == "SUCCESS"
    assert r.json()["runStatus"] == "EXTRACTING"

    r = await report(client, run, "0001", t1, 0, 1, 7)
    assert r.status_code == 200, r.text
    assert r.json() == {"recorded": True, "partitionStatus": "SUCCESS", "runStatus": "EXTRACTED_VALIDATED",
                        "receivedChunks": 1, "chunkCount": 1, "validationScheduled": True}

    run_row = await db.one("SELECT status, extracted_count, success_partition_count "
                           "FROM nifi_ops.load_run WHERE run_id = CAST(:id AS uuid)", id=run.run_id)
    assert run_row == {"status": "EXTRACTED_VALIDATED", "extracted_count": 17, "success_partition_count": 3}
    dispatches = await db.all("SELECT dispatch_type, status FROM nifi_ops.load_dispatch "
                              "WHERE run_id = CAST(:id AS uuid)", id=run.run_id)
    assert dispatches == [{"dispatch_type": "VALIDATE_RUN", "status": "PENDING"}]
    names = [e["event_name"] for e in await db.all(
        "SELECT event_name FROM nifi_ops.load_event WHERE run_id = CAST(:id AS uuid) ORDER BY event_time",
        id=run.run_id)]
    assert names.count("PARTITION_SUCCESS") == 2
    assert names[-1] == "EXTRACT_VALIDATED"

    # 마지막 chunk 재보고(응답 유실 재시도): 같은 결과, dispatch 추가 없음
    again = await report(client, run, "0001", t1, 0, 1, 7)
    assert again.status_code == 200 and again.json()["validationScheduled"] is False
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_dispatch") == 1

    detail = (await client.get(f"/v1/runs/{run.run_id}")).json()
    assert detail["status"] == "EXTRACTED_VALIDATED"
    assert detail["partitionCounts"] == {"SUCCESS": 3}
    assert detail["dispatches"][0]["status"] == "PENDING"


async def test_duplicate_chunk_is_idempotent(client: httpx.AsyncClient, db: Db) -> None:
    run = await start_run(client, [10, 10])
    t = await claim(client, run, "0000")
    for _ in range(3):
        r = await report(client, run, "0000", t, 0, 2, 5)
        assert r.status_code == 200 and r.json()["receivedChunks"] == 1
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_file") == 1


async def test_conflicting_chunk_after_success(client: httpx.AsyncClient, db: Db) -> None:
    run = await start_run(client, [10, 10])
    t = await claim(client, run, "0000")
    assert (await report(client, run, "0000", t, 0, 1, 10)).json()["partitionStatus"] == "SUCCESS"
    r = await report(client, run, "0000", t, 0, 1, 9)
    assert r.status_code == 409 and r.json()["code"] == "CHUNK_CONFLICT"
    assert await db.scalar("SELECT record_count FROM nifi_ops.load_file") == 10  # rollback 확인


async def test_claim_mismatch(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [10, 10])
    await claim(client, run, "0000")
    r = await report(client, run, "0000", str(uuid.uuid4()), 0, 1, 10)
    assert r.status_code == 409 and r.json()["code"] == "CLAIM_MISMATCH"


async def test_row_count_mismatch_fails_run(client: httpx.AsyncClient, db: Db) -> None:
    run = await start_run(client, [10, 10])
    t = await claim(client, run, "0000")
    r = await report(client, run, "0000", t, 0, 1, 9)
    assert r.json()["partitionStatus"] == "FAILED" and r.json()["runStatus"] == "FAILED_EXTRACT"
    row = await db.one("SELECT status, error_code FROM nifi_ops.load_run WHERE run_id = CAST(:id AS uuid)",
                       id=run.run_id)
    assert row == {"status": "FAILED_EXTRACT", "error_code": "ROW_COUNT_MISMATCH"}
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_dispatch") == 0


async def test_inconsistent_chunk_count_fails_partition(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [10, 10])
    t = await claim(client, run, "0000")
    await report(client, run, "0000", t, 0, 3, 4)
    r = await report(client, run, "0000", t, 1, 2, 6)  # chunkCount가 3 → 2로 바뀜
    assert r.json()["partitionStatus"] == "FAILED"


async def test_reports_after_run_failure_are_recorded_and_ignored(client: httpx.AsyncClient, db: Db) -> None:
    run = await start_run(client, [10, 10])
    t0 = await claim(client, run, "0000")
    t1 = await claim(client, run, "0001")
    fail = await client.post(f"/v1/runs/{run.run_id}/partitions/0001/fail", json={
        "claimToken": t1, "errorStage": "HDFS_WRITE", "errorClass": "TRANSIENT",
        "errorCode": "HDFS_RETRY_EXHAUSTED", "message": "retries exhausted", "attempt": "3"})
    assert fail.json() == {"partitionStatus": "FAILED", "runStatus": "FAILED_EXTRACT", "changed": True}

    r = await report(client, run, "0000", t0, 0, 1, 10)
    assert r.status_code == 200
    assert r.json()["runStatus"] == "FAILED_EXTRACT" and r.json()["validationScheduled"] is False
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_file") == 1
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_dispatch") == 0

    claim_after = await client.post(f"/v1/runs/{run.run_id}/partitions/0000/claim",
                                    json={"claimToken": str(uuid.uuid4()), "workerNode": "n"})
    assert claim_after.json()["claimed"] is False


async def test_snapshot_error_marks_snapshot_expired(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [10, 10])
    t = await claim(client, run, "0000")
    body = {"claimToken": t, "errorStage": "ORACLE_EXTRACT", "errorClass": "NON_RETRYABLE",
            "errorCode": "ORA-01555", "message": "snapshot too old"}
    r = await client.post(f"/v1/runs/{run.run_id}/partitions/0000/fail", json=body)
    assert r.json()["runStatus"] == "FAILED_SNAPSHOT_EXPIRED"
    again = await client.post(f"/v1/runs/{run.run_id}/partitions/0000/fail", json=body)
    assert again.json()["changed"] is False


async def test_chunk_validation(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [10, 10])
    t = await claim(client, run, "0000")
    url = f"/v1/runs/{run.run_id}/partitions/0000/chunks"
    outside = chunk_body(run, "0000", t, 0, 1, 10) | {"hdfsPath": "/data/other/part-0000.parquet"}
    assert (await client.post(url, json=outside)).json()["code"] == "HDFS_PATH_OUTSIDE_RUN"
    traversal = chunk_body(run, "0000", t, 0, 1, 10)
    traversal["hdfsPath"] = run.hdfs_run_path + "/../x.parquet"
    assert (await client.post(url, json=traversal)).status_code == 422
    out_of_range = chunk_body(run, "0000", t, 2, 2, 10)
    assert (await client.post(url, json=out_of_range)).json()["code"] == "CHUNK_INDEX_OUT_OF_RANGE"


async def test_same_hdfs_path_for_two_chunks_conflicts(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [10, 10])
    t = await claim(client, run, "0000")
    url = f"/v1/runs/{run.run_id}/partitions/0000/chunks"
    await client.post(url, json=chunk_body(run, "0000", t, 0, 2, 5))
    dup = chunk_body(run, "0000", t, 1, 2, 5) | {"hdfsPath": chunk_body(run, "0000", t, 0, 2, 5)["hdfsPath"]}
    r = await client.post(url, json=dup)
    assert r.status_code == 409 and r.json()["code"] == "UNIQUE_VIOLATION"
