"""NiFi가 보내는 요청을 흉내 내는 헬퍼."""

import uuid
from dataclasses import dataclass
from typing import Any

import httpx

WIDTH = 1000


@dataclass
class Run:
    run_id: str
    hdfs_run_path: str


def partitions_for(counts: list[int], *, null_count: int | None = None) -> list[dict[str, Any]]:
    """연속 경계 [1 + i*WIDTH, 1 + (i+1)*WIDTH), 마지막만 상한 포함."""
    parts: list[dict[str, Any]] = []
    n = len(counts)
    for i, c in enumerate(counts):
        parts.append({"partitionId": f"{i:04d}", "lowerBound": str(1 + i * WIDTH),
                      "upperBound": str(1 + (i + 1) * WIDTH), "upperInclusive": i == n - 1,
                      "isNullPartition": False, "expectedRowCount": c})
    if null_count is not None:
        parts.append({"partitionId": "NULL", "lowerBound": None, "upperBound": None,
                      "upperInclusive": False, "isNullPartition": True, "expectedRowCount": null_count})
    return parts


def manifest_body(counts: list[int], *, null_count: int | None = None, **overrides: Any) -> dict[str, Any]:
    parts = partitions_for(counts, null_count=null_count)
    body: dict[str, Any] = {
        "snapshotScn": "1234567890",
        "sourceCount": sum(counts) + (null_count or 0),
        "sourceNullSplitCount": null_count or 0,
        "sourceMinSplit": "1",
        "sourceMaxSplit": str(1 + len(counts) * WIDTH),
        "plannedPartitionCount": len(parts),
        "sourceMetrics": {"AMOUNT_SUM": "123.45"},
        "partitions": parts,
    }
    body.update(overrides)
    return body


async def create_run(client: httpx.AsyncClient, business_key: str | None = None,
                     allow_empty: bool = False) -> Run:
    r = await client.post("/v1/runs", json={
        "jobKey": "ORACLE_INSP_DTL_DAILY",
        "businessKey": business_key or f"2026-09-28-{uuid.uuid4().hex[:8]}",
        "hdfsRoot": "/data/nifi/stage",
        "stageTablePrefix": "TMP_INSP_DTL_",
        "allowEmptySource": allow_empty,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    return Run(body["runId"], body["hdfsRunPath"])


async def start_run(client: httpx.AsyncClient, counts: list[int], **kwargs: Any) -> Run:
    run = await create_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body(counts, **kwargs))
    assert r.status_code == 200, r.text
    return run


async def claim(client: httpx.AsyncClient, run: Run, pid: str, token: str | None = None) -> str:
    token = token or str(uuid.uuid4())
    r = await client.post(f"/v1/runs/{run.run_id}/partitions/{pid}/claim",
                          json={"claimToken": token, "workerNode": "nifi-01"})
    assert r.status_code == 200, r.text
    assert r.json()["claimed"] is True, r.text
    return token


def chunk_body(run: Run, pid: str, token: str, index: int, count: int, rows: int) -> dict[str, Any]:
    # NiFi AttributesToJSON처럼 모든 값을 문자열로 보낸다.
    return {"claimToken": token, "chunkIndex": str(index), "chunkCount": str(count),
            "fragmentIdentifier": f"frag-{pid}",
            "hdfsPath": f"{run.hdfs_run_path}/part-{pid}-{index:06d}.parquet",
            "recordCount": str(rows), "byteCount": str(rows * 10)}


async def report(client: httpx.AsyncClient, run: Run, pid: str, token: str, index: int, count: int,
                 rows: int) -> httpx.Response:
    return await client.post(f"/v1/runs/{run.run_id}/partitions/{pid}/chunks",
                             json=chunk_body(run, pid, token, index, count, rows))
