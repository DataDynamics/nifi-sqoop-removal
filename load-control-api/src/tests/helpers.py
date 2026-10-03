"""NiFi가 보내는 요청을 흉내 내는 헬퍼."""

import uuid
from dataclasses import dataclass
from typing import Any

import httpx

WIDTH = 1000


@dataclass
class Run:
    """생성된 run의 식별자와 HDFS 적재 경로."""
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
    """partitions_for로 만든 파티션 목록과 일관된 manifest 요청 본문을 만든다.

    sourceCount·min/max split·파티션 수가 파티션 목록과 맞게 계산되므로 그대로 보내면
    검증을 통과한다. overrides로 특정 필드를 바꿔 실패 사례를 만든다.
    """
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
    """POST /v1/runs로 CREATED 상태 run을 만든다.

    business_key를 주지 않으면 매번 다른 값을 써서 active run 중복 충돌을 피한다.
    """
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
    """run을 만들고 manifest를 등록해 EXTRACTING 상태로 만든다."""
    run = await create_run(client)
    r = await client.post(f"/v1/runs/{run.run_id}/manifest", json=manifest_body(counts, **kwargs))
    assert r.status_code == 200, r.text
    return run


async def claim(client: httpx.AsyncClient, run: Run, pid: str, token: str | None = None) -> str:
    """파티션을 claim하고 성공했는지 확인한 뒤 claim token을 돌려준다."""
    token = token or str(uuid.uuid4())
    r = await client.post(f"/v1/runs/{run.run_id}/partitions/{pid}/claim",
                          json={"claimToken": token, "workerNode": "nifi-01"})
    assert r.status_code == 200, r.text
    assert r.json()["claimed"] is True, r.text
    return token


def chunk_body(run: Run, pid: str, token: str, index: int, count: int, rows: int) -> dict[str, Any]:
    # NiFi AttributesToJSON처럼 모든 값을 문자열로 보낸다.
    """chunk 보고 본문을 만든다. HDFS 경로는 run 경로 아래 파티션·chunk별로 고유하다."""
    return {"claimToken": token, "chunkIndex": str(index), "chunkCount": str(count),
            "fragmentIdentifier": f"frag-{pid}",
            "hdfsPath": f"{run.hdfs_run_path}/part-{pid}-{index:06d}.parquet",
            "recordCount": str(rows), "byteCount": str(rows * 10)}


async def report(client: httpx.AsyncClient, run: Run, pid: str, token: str, index: int, count: int,
                 rows: int) -> httpx.Response:
    """chunk 하나를 보고하고 응답을 그대로 돌려준다(상태 코드 확인은 호출자가 한다)."""
    return await client.post(f"/v1/runs/{run.run_id}/partitions/{pid}/chunks",
                             json=chunk_body(run, pid, token, index, count, rows))


async def complete_run(client: httpx.AsyncClient, counts: list[int] | None = None) -> tuple[Run, str]:
    """모든 파티션을 보고해 run을 EXTRACTED_VALIDATED로 만들고 (run, VALIDATE_RUN dispatch id)를 돌려준다."""
    counts = counts or [3, 4]
    run = await start_run(client, counts)
    for i, c in enumerate(counts):
        if c == 0:
            continue
        pid = f"{i:04d}"
        token = await claim(client, run, pid)
        r = await report(client, run, pid, token, 0, 1, c)
        assert r.status_code == 200, r.text
    detail = (await client.get(f"/v1/runs/{run.run_id}")).json()
    assert detail["status"] == "EXTRACTED_VALIDATED", detail
    return run, detail["dispatches"][0]["dispatchId"]


def metrics(*items: tuple[str, str, str]) -> list[dict[str, str]]:
    """(지표명, 기대값, PASS|FAIL) 튜플로 검증 지표 목록을 만든다.

    PASS면 실제값을 기대값과 같게, FAIL이면 "x"로 둔다.
    """
    return [{"metricName": n, "expectedValue": e, "actualValue": e if r == "PASS" else "x", "result": r}
            for n, e, r in items]


async def to_staging_validated(client: httpx.AsyncClient) -> Run:
    """추출 완료 → 검증 시작 → STAGING 지표 PASS 보고 → stage-validated까지 진행해
    STAGING_VALIDATED 상태 run을 돌려준다.
    """
    run, dispatch_id = await complete_run(client, [3, 4])
    r = await client.post(f"/v1/runs/{run.run_id}/validation/start", json={"dispatchId": dispatch_id})
    assert r.json()["started"] is True, r.text
    r = await client.post(f"/v1/runs/{run.run_id}/validations", json={
        "stage": "STAGING", "metrics": metrics(("STAGE_COUNT", "7", "PASS"), ("DUP_PK_COUNT", "0", "PASS"))})
    assert r.status_code == 200, r.text
    r = await client.post(f"/v1/runs/{run.run_id}/stage-validated")
    assert r.json()["stageValidated"] is True, r.text
    return run


async def to_published(client: httpx.AsyncClient) -> tuple[Run, str]:
    """STAGING_VALIDATED run을 publish claim 후 PUBLISHED로 보고해 (run, publish token)을 돌려준다."""
    run = await to_staging_validated(client)
    token = str(uuid.uuid4())
    r = await client.post(f"/v1/runs/{run.run_id}/publish/claim", json={"publishToken": token})
    assert r.json()["claimed"] is True, r.text
    r = await client.post(f"/v1/runs/{run.run_id}/publish/result",
                          json={"publishToken": token, "outcome": "PUBLISHED"})
    assert r.json()["runStatus"] == "PUBLISHED", r.text
    return run, token
