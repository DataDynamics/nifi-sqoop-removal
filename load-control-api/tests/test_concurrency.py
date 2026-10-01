"""동시성 규칙 검증(API 설계 11.1). 요청마다 다른 DB 연결을 쓰므로 실제 잠금 경합이 일어난다."""

import asyncio
import uuid

import httpx
import pytest

from tests.conftest import Db
from tests.helpers import claim, report, start_run


@pytest.mark.parametrize("partitions,chunks", [(8, 1), (6, 3), (16, 2)])
async def test_concurrent_completion_schedules_validation_once(
        client: httpx.AsyncClient, db: Db, partitions: int, chunks: int) -> None:
    for _ in range(3):
        rows_per_chunk = 5
        run = await start_run(client, [rows_per_chunk * chunks] * partitions)
        tokens = {f"{i:04d}": await claim(client, run, f"{i:04d}") for i in range(partitions)}
        results = await asyncio.gather(*(
            report(client, run, pid, tok, idx, chunks, rows_per_chunk)
            for pid, tok in tokens.items() for idx in range(chunks)))
        assert all(r.status_code == 200 for r in results), [r.text for r in results if r.status_code != 200]
        scheduled = [r for r in results if r.json()["validationScheduled"]]
        assert len(scheduled) == 1
        row = await db.one("SELECT status, extracted_count FROM nifi_ops.load_run "
                           "WHERE run_id = CAST(:id AS uuid)", id=run.run_id)
        expected_rows = partitions * chunks * rows_per_chunk
        assert row == {"status": "EXTRACTED_VALIDATED", "extracted_count": expected_rows}
        assert await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_dispatch "
                               "WHERE run_id = CAST(:id AS uuid)", id=run.run_id) == 1


async def test_concurrent_claims_single_owner(client: httpx.AsyncClient) -> None:
    run = await start_run(client, [10, 10])
    results = await asyncio.gather(*(
        client.post(f"/v1/runs/{run.run_id}/partitions/0000/claim",
                    json={"claimToken": str(uuid.uuid4()), "workerNode": f"nifi-{i}"})
        for i in range(12)))
    assert sum(r.json()["claimed"] for r in results) == 1


async def test_concurrent_failure_and_completion(client: httpx.AsyncClient, db: Db) -> None:
    """한 파티션 완료와 다른 파티션 실패가 동시에 와도 run은 FAILED_EXTRACT이고 검증 호출은 없다."""
    for _ in range(5):
        run = await start_run(client, [10, 10])
        t0 = await claim(client, run, "0000")
        t1 = await claim(client, run, "0001")
        done, failed = await asyncio.gather(
            report(client, run, "0000", t0, 0, 1, 10),
            client.post(f"/v1/runs/{run.run_id}/partitions/0001/fail", json={
                "claimToken": t1, "errorStage": "X", "errorClass": "TRANSIENT", "errorCode": "E"}))
        status = await db.scalar("SELECT status FROM nifi_ops.load_run WHERE run_id = CAST(:id AS uuid)",
                                 id=run.run_id)
        dispatches = await db.scalar("SELECT COUNT(*) FROM nifi_ops.load_dispatch "
                                     "WHERE run_id = CAST(:id AS uuid)", id=run.run_id)
        assert status == "FAILED_EXTRACT" and dispatches == 0, (done.text, failed.text)


async def test_race_between_last_two_partitions(client: httpx.AsyncClient, db: Db) -> None:
    """API 설계 3.2: 마지막 두 파티션이 동시에 끝나도 run은 정확히 한 번 완료된다."""
    for _ in range(10):
        run = await start_run(client, [3, 3])
        t0 = await claim(client, run, "0000")
        t1 = await claim(client, run, "0001")
        a, b = await asyncio.gather(report(client, run, "0000", t0, 0, 1, 3),
                                    report(client, run, "0001", t1, 0, 1, 3))
        assert [a.json()["validationScheduled"], b.json()["validationScheduled"]].count(True) == 1
        assert await db.scalar("SELECT status FROM nifi_ops.load_run WHERE run_id = CAST(:id AS uuid)",
                               id=run.run_id) == "EXTRACTED_VALIDATED"
