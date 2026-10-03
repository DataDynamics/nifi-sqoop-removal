"""manifest 등록(파티션 계획 검증과 등록)을 검증한다.

검증에 실패하면 422를 돌려주지만 run은 FAILED_MANIFEST로 commit되고 파티션은 하나도 남지 않는다.
"""

import uuid
from typing import Any

import httpx
import pytest

from tests.conftest import Db
from tests.helpers import create_run, manifest_body


async def test_manifest_registers_partitions(client: httpx.AsyncClient, db: Db) -> None:
    """정상 manifest는 run을 EXTRACTING으로 바꾸고 파티션·SOURCE 지표를 등록한다.

    기대 행 수가 0인 파티션은 바로 SUCCESS(0행)로 두고 추출 대상
    (dispatchPartitions)에서 뺀다. SOURCE 지표에는 sourceCount가 함께 남는다.
    """
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
    """같은 manifest를 다시 보내면(응답 유실 재시도) 같은 추출 대상을 돌려준다."""
    run = await create_run(client)
    body = manifest_body([10, 20])
    first = await client.post(f"/v1/runs/{run.run_id}/manifest", json=body)
    second = await client.post(f"/v1/runs/{run.run_id}/manifest", json=body)
    assert second.status_code == 200
    assert second.json()["dispatchPartitions"] == first.json()["dispatchPartitions"]


async def test_numeric_bounds_accepted(client: httpx.AsyncClient) -> None:
    """경계값을 문자열 대신 숫자로 보내도 받아들인다."""
    run = await create_run(client)
    body = manifest_body([10, 20])
    for p in body["partitions"]:
        p["lowerBound"], p["upperBound"] = int(p["lowerBound"]), int(p["upperBound"])
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=body)
    assert r.status_code == 200, r.text


async def test_null_partition(client: httpx.AsyncClient) -> None:
    """split 컬럼이 NULL인 행을 담는 NULL 파티션도 추출 대상에 포함된다."""
    run = await create_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body([10, 20], null_count=5))
    assert r.status_code == 200, r.text
    assert "NULL" in [p["partitionId"] for p in r.json()["dispatchPartitions"]]


async def _assert_invalid(client: httpx.AsyncClient, db: Db, body: dict[str, object], reason: str) -> None:
    """새 run에 manifest를 보내 422 MANIFEST_INVALID와 기대한 사유가 나오는지 확인한다.

    run은 FAILED_MANIFEST로 남고 파티션이 등록되지 않았는지도 확인한다.
    """
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
    """파티션 기대 행 수 합이 sourceCount와 다르면 거부한다."""
    await _assert_invalid(client, db, manifest_body([10, 20], sourceCount=31), "EXPECTED_SUM_MISMATCH")


async def test_partition_count_mismatch(client: httpx.AsyncClient, db: Db) -> None:
    """파티션 수가 plannedPartitionCount와 다르면 거부한다."""
    await _assert_invalid(client, db, manifest_body([10, 20], plannedPartitionCount=3),
                          "PARTITION_COUNT_MISMATCH")


async def test_range_gap(client: httpx.AsyncClient, db: Db) -> None:
    """인접 파티션 경계 사이에 빈틈이나 겹침이 있으면 거부한다."""
    body = manifest_body([10, 20, 30])
    # 두 번째 파티션 하한을 1001 → 1002로 바꿔 값 1001이 어느 파티션에도 속하지 않게 한다.
    body["partitions"][1]["lowerBound"] = "1002"
    await _assert_invalid(client, db, body, "RANGE_GAP_OR_OVERLAP")


async def test_last_partition_must_be_inclusive(client: httpx.AsyncClient, db: Db) -> None:
    """마지막 파티션이 상한을 포함하지 않으면 최댓값 행이 빠지므로 거부한다."""
    body = manifest_body([10, 20])
    body["partitions"][1]["upperInclusive"] = False
    await _assert_invalid(client, db, body, "UPPER_INCLUSIVE_INVALID")


async def test_empty_source_blocked(client: httpx.AsyncClient, db: Db) -> None:
    """allowEmptySource가 아닌데 원천이 0건이면 거부한다."""
    await _assert_invalid(client, db, manifest_body([0, 0]), "EMPTY_SOURCE_BLOCKED")


async def test_empty_source_allowed_completes_immediately(client: httpx.AsyncClient, db: Db) -> None:
    """allowEmptySource면 0건 원천은 추출 없이 바로 EXTRACTED_VALIDATED가 된다.

    추출 대상은 없고 검증 호출 dispatch 하나만 예약된다.
    """
    run = await create_run(client, allow_empty=True)
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body([0, 0]))
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "EXTRACTED_VALIDATED"
    assert r.json()["validationScheduled"] is True
    assert r.json()["dispatchPartitions"] == []
    assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_dispatch WHERE run_id = CAST(:id AS uuid)",
                           id=run.run_id) == 1


def _with(body: dict[str, object], index: int, **patch: object) -> dict[str, object]:
    """manifest 본문의 index번째 파티션에 patch를 덮어쓰고 본문을 돌려준다."""
    body["partitions"][index].update(patch)  # type: ignore[index]
    return body


@pytest.mark.parametrize(("make", "reason"), [
    (lambda: _with(manifest_body([10, 20]), 1, partitionId="0000"), "DUPLICATE_PARTITION_ID"),
    (lambda: _with(manifest_body([10, 20]), 0, lowerBound="5000"), "INVALID_BOUNDS"),
    (lambda: _with(manifest_body([10, 20]), 0, upperBound=None), "INVALID_BOUNDS"),
    (lambda: manifest_body([10, 20], sourceMinSplit="0"), "MIN_SPLIT_MISMATCH"),
    (lambda: manifest_body([10, 20], sourceMaxSplit="9999"), "MAX_SPLIT_MISMATCH"),
    (lambda: manifest_body([10, 20], null_count=5, sourceNullSplitCount=4, sourceCount=34),
     "NULL_COUNT_MISMATCH"),
    (lambda: manifest_body([10, 20], sourceNullSplitCount=3, sourceCount=33),
     "NULL_ROWS_WITHOUT_NULL_PARTITION"),
    (lambda: _with(manifest_body([10, 20], null_count=5), 2, lowerBound="1"), "INVALID_NULL_PARTITION"),
])
async def test_invalid_manifest_variants(client: httpx.AsyncClient, db: Db, make: Any, reason: str) -> None:
    """파티션 ID 중복, 경계 역전·누락, min/max split 불일치, NULL 파티션 규칙 위반을
    각각의 사유 코드로 거부한다.
    """
    await _assert_invalid(client, db, make(), reason)


async def test_multiple_null_partitions(client: httpx.AsyncClient, db: Db) -> None:
    """NULL 파티션이 둘 이상이면 거부한다."""
    body = manifest_body([10, 20], null_count=5)
    body["partitions"].append(dict(body["partitions"][-1], partitionId="NULL"))  # type: ignore[attr-defined]
    # ID만 다른 NULL 파티션을 하나 더 붙였다. 합계·파티션 수 검증보다 이 규칙이 먼저 걸려야 한다.
    await _assert_invalid(client, db, body, "MULTIPLE_NULL_PARTITIONS")


async def test_different_manifest_after_registration_conflicts(client: httpx.AsyncClient) -> None:
    """이미 등록된 run에 다른 manifest를 보내면 상태 불일치(409)다."""
    run = await create_run(client)
    await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body([10, 20]))
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body([10, 20, 30]))
    assert r.status_code == 409 and r.json()["code"] == "RUN_STATUS_MISMATCH"


async def test_manifest_unknown_run(client: httpx.AsyncClient) -> None:
    """없는 run에 manifest를 보내면 404다."""
    r = await client.post(f"/v1/runs/{uuid.uuid4()}/manifest", json=manifest_body([10]))
    assert r.status_code == 404
