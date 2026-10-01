"""manifest 등록과 불변식 검증(가이드 7.4, API 설계 3.4)."""

from dataclasses import dataclass, field
from decimal import Decimal
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncConnection

from load_control.domain import RunStatus
from load_control.errors import Conflict, NotFound
from load_control.repositories import dispatch, events, partitions, runs, validations
from load_control.schemas.runs import ManifestPartition, ManifestRequest, ManifestResponse

log = structlog.get_logger(__name__)


@dataclass
class ManifestOutcome:
    """불변식 위반은 FAILED_MANIFEST를 commit한 뒤 422로 응답해야 하므로 예외 대신 결과로 돌려준다."""

    response: ManifestResponse | None = None
    violations: list[str] = field(default_factory=list)


def _dec(v: str | None) -> Decimal | None:
    return Decimal(v) if v is not None else None


def check_invariants(req: ManifestRequest, allow_empty_source: bool) -> list[str]:
    """manifest 불변식(가이드 7.4, API 설계 3.4). 위반 사유 목록을 돌려준다."""
    v: list[str] = []
    parts = req.partitions
    ids = [p.partition_id for p in parts]
    if len(set(ids)) != len(ids):
        v.append("DUPLICATE_PARTITION_ID")
    if len(parts) != req.planned_partition_count:
        v.append(f"PARTITION_COUNT_MISMATCH planned={req.planned_partition_count} actual={len(parts)}")
    total = sum(p.expected_row_count for p in parts)
    if total != req.source_count:
        v.append(f"EXPECTED_SUM_MISMATCH sum={total} source={req.source_count}")
    if req.source_count == 0 and not allow_empty_source:
        v.append("EMPTY_SOURCE_BLOCKED")

    null_parts = [p for p in parts if p.is_null_partition]
    ranged = [p for p in parts if not p.is_null_partition]
    if len(null_parts) > 1:
        v.append("MULTIPLE_NULL_PARTITIONS")
    for p in null_parts:
        if p.partition_id != "NULL" or p.lower_bound is not None or p.upper_bound is not None:
            v.append(f"INVALID_NULL_PARTITION {p.partition_id}")
        if p.expected_row_count != req.source_null_split_count:
            v.append(f"NULL_COUNT_MISMATCH expected={p.expected_row_count} "
                     f"source={req.source_null_split_count}")
    if not null_parts and req.source_null_split_count > 0:
        v.append("NULL_ROWS_WITHOUT_NULL_PARTITION")

    bounded: list[tuple[Decimal, Decimal, ManifestPartition]] = []
    for p in ranged:
        lo, hi = _dec(p.lower_bound), _dec(p.upper_bound)
        if p.partition_id == "NULL" or lo is None or hi is None or lo > hi:
            v.append(f"INVALID_BOUNDS {p.partition_id}")
            continue
        bounded.append((lo, hi, p))
    bounded.sort(key=lambda t: (t[0], t[2].partition_id))
    for i, (_lo, hi, p) in enumerate(bounded):
        last = i == len(bounded) - 1
        if p.upper_inclusive != last:
            v.append(f"UPPER_INCLUSIVE_INVALID {p.partition_id}")
        if not last and bounded[i + 1][0] != hi:
            v.append(f"RANGE_GAP_OR_OVERLAP {p.partition_id}->{bounded[i + 1][2].partition_id}")
    if bounded:
        if req.source_min_split is not None and bounded[0][0] != Decimal(req.source_min_split):
            v.append("MIN_SPLIT_MISMATCH")
        if req.source_max_split is not None and bounded[-1][1] != Decimal(req.source_max_split):
            v.append("MAX_SPLIT_MISMATCH")
    return v


def _dispatchable(rows: list[partitions.PartitionRow]) -> list[ManifestPartition]:
    return [ManifestPartition(
        partition_id=p.partition_id,
        lower_bound=str(p.lower_bound) if p.lower_bound is not None else None,
        upper_bound=str(p.upper_bound) if p.upper_bound is not None else None,
        upper_inclusive=p.upper_inclusive, is_null_partition=p.is_null_partition,
        expected_row_count=p.expected_row_count) for p in rows if p.expected_row_count > 0]


async def register_manifest(conn: AsyncConnection, run_id: UUID, req: ManifestRequest) -> ManifestOutcome:
    """SCN·source 지표 저장, 불변식 검증, 파티션 일괄 등록, CREATED → EXTRACTING을 한 트랜잭션으로.

    0건 파티션은 바로 SUCCESS로 넣고 Worker 대상(dispatchPartitions)에서 뺀다. 모든 파티션이
    0건이면(allowEmptySource) 이 자리에서 run 완료까지 판정하고 검증 호출을 예약한다.
    """
    run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")

    if run.status != RunStatus.CREATED:
        # 응답 유실 후 재요청: 이미 같은 manifest가 등록됐으면 같은 응답을 돌려준다.
        existing = await partitions.list_for_run(conn, run_id)
        same = (existing and sorted(p.partition_id for p in existing)
                == sorted(p.partition_id for p in req.partitions)
                and run.source_count == req.source_count)
        if not same:
            log.warning("manifest_conflict", runId=str(run_id), runStatus=run.status)
            raise Conflict("RUN_STATUS_MISMATCH", runStatus=run.status)
        log.info("manifest_replayed", runId=str(run_id), runStatus=run.status)
        return ManifestOutcome(response=ManifestResponse(
            run_id=str(run_id), status=RunStatus(run.status),
            dispatch_partitions=_dispatchable(existing),
            empty_partition_count=sum(1 for p in existing if p.expected_row_count == 0),
            validation_scheduled=False))

    allow_empty = bool(run.parameters.get("allowEmptySource", False))
    violations = check_invariants(req, allow_empty)
    if violations:
        message = "; ".join(violations)
        await runs.fail(conn, run_id, expected=RunStatus.CREATED, to=RunStatus.FAILED_MANIFEST,
                        stage="MANIFEST", code="MANIFEST_INVALID", message=message)
        await events.record(conn, "MANIFEST_INVALID", run, level="ERROR",
                            error_code="MANIFEST_INVALID", message=message)
        log.error("manifest_invalid", runId=str(run_id), jobKey=run.job_key,
                  businessKey=run.business_key, violations=violations)
        return ManifestOutcome(violations=violations)

    await partitions.insert_many(conn, [{
        "run_id": run_id, "partition_id": p.partition_id,
        "lower_bound": _dec(p.lower_bound), "upper_bound": _dec(p.upper_bound),
        "upper_inclusive": p.upper_inclusive, "is_null_partition": p.is_null_partition,
        "expected_row_count": p.expected_row_count} for p in req.partitions])
    empty = sum(1 for p in req.partitions if p.expected_row_count == 0)
    await runs.start_extracting(
        conn, run_id, snapshot_scn=_dec(req.snapshot_scn), source_count=req.source_count,
        source_null_split_count=req.source_null_split_count,
        source_min_split=_dec(req.source_min_split), source_max_split=_dec(req.source_max_split),
        expected_partition_count=len(req.partitions), empty_partition_count=empty)
    await validations.upsert_many(
        conn, run_id, "SOURCE", req.source_metrics_version,
        [{"metric_name": "SOURCE_COUNT", "actual_value": str(req.source_count), "result": "PASS"}]
        + [{"metric_name": k, "actual_value": val, "result": "PASS"}
           for k, val in req.source_metrics.items()])
    await events.record(conn, "MANIFEST_CREATED", run, row_count=req.source_count,
                        details={"partitions": len(req.partitions), "empty": empty,
                                 "snapshotScn": req.snapshot_scn})
    log.info("manifest_registered", runId=str(run_id), partitions=len(req.partitions),
             emptyPartitions=empty, sourceCount=req.source_count, snapshotScn=req.snapshot_scn)

    scheduled = False
    status = RunStatus.EXTRACTING
    if empty == len(req.partitions) and await runs.try_complete_extract(conn, run_id):
        await dispatch.enqueue_validation(conn, run_id)
        await events.record(conn, "EXTRACT_VALIDATED", run, row_count=0)
        scheduled, status = True, RunStatus.EXTRACTED_VALIDATED
        log.info("extract_validated", runId=str(run_id), extractedCount=0, reason="all partitions empty")

    return ManifestOutcome(response=ManifestResponse(
        run_id=str(run_id), status=status,
        dispatch_partitions=[p for p in req.partitions if p.expected_row_count > 0],
        empty_partition_count=empty, validation_scheduled=scheduled))
