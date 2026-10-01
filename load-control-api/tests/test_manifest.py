import httpx

from tests.conftest import Db
from tests.helpers import create_run, manifest_body


async def test_manifest_registers_partitions(client: httpx.AsyncClient, db: Db) -> None:
    run = await create_run(client)
    counts = [15000, 15000, 0, 15000, 15000, 15000, 15000, 15000]
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body(counts))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "EXTRACTING"
    assert body["emptyPartitionCount"] == 1
    assert [p["partitionId"] for p in body["dispatchPartitions"]] == [
        "0000", "0001", "0003", "0004", "0005", "0006", "0007"]
    assert body["validationScheduled"] is False

    run_row = await db.one("SELECT status, source_count, expected_partition_count, snapshot_scn "
                           "FROM nifi_ops.load_run WHERE run_id = CAST(:id AS uuid)", id=run.run_id)
    assert run_row["status"] == "EXTRACTING"
    assert run_row["source_count"] == 105000
    assert run_row["expected_partition_count"] == 8
    assert str(run_row["snapshot_scn"]) == "1234567890"
    empty = await db.one("SELECT status, actual_row_count FROM nifi_ops.load_partition "
                         "WHERE run_id = CAST(:id AS uuid) AND partition_id = '0002'", id=run.run_id)
    assert empty == {"status": "SUCCESS", "actual_row_count": 0}
    metrics = await db.all("SELECT metric_name, actual_value FROM nifi_ops.load_validation "
                           "WHERE run_id = CAST(:id AS uuid) AND stage = 'SOURCE' ORDER BY metric_name",
                           id=run.run_id)
    assert metrics == [{"metric_name": "AMOUNT_SUM", "actual_value": "123.45"},
                       {"metric_name": "SOURCE_COUNT", "actual_value": "105000"}]


async def test_manifest_retry_is_idempotent(client: httpx.AsyncClient) -> None:
    run = await create_run(client)
    body = manifest_body([10, 20])
    first = await client.post(f"/v1/runs/{run.run_id}/manifest", json=body)
    second = await client.post(f"/v1/runs/{run.run_id}/manifest", json=body)
    assert second.status_code == 200
    assert second.json()["dispatchPartitions"] == first.json()["dispatchPartitions"]


async def test_numeric_bounds_accepted(client: httpx.AsyncClient) -> None:
    run = await create_run(client)
    body = manifest_body([10, 20])
    for p in body["partitions"]:
        p["lowerBound"], p["upperBound"] = int(p["lowerBound"]), int(p["upperBound"])
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=body)
    assert r.status_code == 200, r.text


async def test_null_partition(client: httpx.AsyncClient) -> None:
    run = await create_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body([10, 20], null_count=5))
    assert r.status_code == 200, r.text
    assert "NULL" in [p["partitionId"] for p in r.json()["dispatchPartitions"]]


async def _assert_invalid(client: httpx.AsyncClient, db: Db, body: dict[str, object], reason: str) -> None:
    run = await create_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=body)
    assert r.status_code == 422, r.text
    assert r.json()["code"] == "MANIFEST_INVALID"
    assert reason in r.json()["message"]
    row = await db.one("SELECT status, error_code FROM nifi_ops.load_run WHERE run_id = CAST(:id AS uuid)",
                       id=run.run_id)
    assert row == {"status": "FAILED_MANIFEST", "error_code": "MANIFEST_INVALID"}  # 422여도 commit됨
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_partition WHERE run_id = CAST(:id AS uuid)",
                           id=run.run_id) == 0


async def test_sum_mismatch(client: httpx.AsyncClient, db: Db) -> None:
    await _assert_invalid(client, db, manifest_body([10, 20], sourceCount=31), "EXPECTED_SUM_MISMATCH")


async def test_partition_count_mismatch(client: httpx.AsyncClient, db: Db) -> None:
    await _assert_invalid(client, db, manifest_body([10, 20], plannedPartitionCount=3),
                          "PARTITION_COUNT_MISMATCH")


async def test_range_gap(client: httpx.AsyncClient, db: Db) -> None:
    body = manifest_body([10, 20, 30])
    body["partitions"][1]["lowerBound"] = "1002"
    await _assert_invalid(client, db, body, "RANGE_GAP_OR_OVERLAP")


async def test_last_partition_must_be_inclusive(client: httpx.AsyncClient, db: Db) -> None:
    body = manifest_body([10, 20])
    body["partitions"][1]["upperInclusive"] = False
    await _assert_invalid(client, db, body, "UPPER_INCLUSIVE_INVALID")


async def test_empty_source_blocked(client: httpx.AsyncClient, db: Db) -> None:
    await _assert_invalid(client, db, manifest_body([0, 0]), "EMPTY_SOURCE_BLOCKED")


async def test_empty_source_allowed_completes_immediately(client: httpx.AsyncClient, db: Db) -> None:
    run = await create_run(client, allow_empty=True)
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body([0, 0]))
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "EXTRACTED_VALIDATED"
    assert r.json()["validationScheduled"] is True
    assert r.json()["dispatchPartitions"] == []
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_dispatch WHERE run_id = CAST(:id AS uuid)",
                           id=run.run_id) == 1
