"""파티션 claim, chunk 보고 판정, 파티션 실패(API 설계 3장, 9.6)."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection

from load_control import metrics
from load_control.domain import SNAPSHOT_ERROR_CODES, PartitionStatus, RunStatus
from load_control.errors import Conflict, NotFound, Unprocessable
from load_control.repositories import dispatch, events, files, partitions, runs
from load_control.repositories.partitions import PartitionRow
from load_control.repositories.runs import RunRow
from load_control.schemas.partitions import (
    ChunkReport,
    ChunkResult,
    ClaimRequest,
    ClaimResponse,
    PartitionFailRequest,
    PartitionFailResponse,
)


async def _lock_run_and_partition(conn: AsyncConnection, run_id: UUID,
                                  partition_id: str) -> tuple[RunRow, PartitionRow]:
    # 잠금 순서 고정: load_run → load_partition (API 설계 9.5)
    with metrics.RUN_LOCK_WAIT.time():
        run = await runs.lock(conn, run_id)
    if run is None:
        raise NotFound("RUN_NOT_FOUND")
    part = await partitions.lock(conn, run_id, partition_id)
    if part is None:
        raise NotFound("PARTITION_NOT_FOUND")
    return run, part


async def claim(conn: AsyncConnection, run_id: UUID, partition_id: str,
                req: ClaimRequest) -> ClaimResponse:
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)

    def response(claimed: bool, status: str, attempt: int) -> ClaimResponse:
        return ClaimResponse(claimed=claimed, run_status=RunStatus(run.status),
                             partition_status=PartitionStatus(status), attempt=attempt)

    if run.status != RunStatus.EXTRACTING:
        return response(False, part.status, part.attempt_count)
    if part.status == PartitionStatus.RUNNING and part.claim_token == req.claim_token:
        # 응답 유실 후 같은 token 재요청: 소유권 유지(가이드 4.1 "Claim과 상태 전이")
        await partitions.touch(conn, run_id, partition_id)
        return response(True, part.status, part.attempt_count)
    if part.status not in (PartitionStatus.PENDING, PartitionStatus.RETRY):
        return response(False, part.status, part.attempt_count)

    attempt = await partitions.claim(conn, run_id, partition_id, claim_token=req.claim_token,
                                     worker_node=req.worker_node)
    if part.status == PartitionStatus.RETRY:
        await dispatch.ack_reissue(conn, run_id, partition_id)  # 재발행 수신 확인(가이드 13.2)
    await runs.touch(conn, run_id)
    await events.record(conn, "PARTITION_STARTED", run, partition_id=partition_id,
                        details={"workerNode": req.worker_node, "attempt": attempt})
    return response(True, PartitionStatus.RUNNING, attempt)


async def report_chunk(conn: AsyncConnection, run_id: UUID, partition_id: str,
                       req: ChunkReport) -> ChunkResult:
    if req.chunk_index >= req.chunk_count:
        raise Unprocessable("CHUNK_INDEX_OUT_OF_RANGE")
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)
    if part.claim_token != req.claim_token:
        raise Conflict("CLAIM_MISMATCH")
    if (run.hdfs_run_path is None or not req.hdfs_path.startswith(run.hdfs_run_path + "/")
            or ".." in req.hdfs_path.split("/")):
        raise Unprocessable("HDFS_PATH_OUTSIDE_RUN")

    changed = await files.upsert(conn, run_id, partition_id, chunk_index=req.chunk_index,
                                 chunk_count=req.chunk_count,
                                 fragment_identifier=req.fragment_identifier,
                                 hdfs_path=req.hdfs_path, record_count=req.record_count,
                                 byte_count=req.byte_count)
    if part.status == PartitionStatus.SUCCESS and changed:
        raise Conflict("CHUNK_CONFLICT")  # 예외로 rollback되어 기록도 취소된다
    await partitions.touch(conn, run_id, partition_id)
    await runs.touch(conn, run_id)

    agg = await files.aggregate(conn, run_id, partition_id)
    result = ChunkResult(recorded=True, partition_status=PartitionStatus(part.status),
                         run_status=RunStatus(run.status), received_chunks=agg.files,
                         chunk_count=req.chunk_count, validation_scheduled=False)
    if (run.status != RunStatus.EXTRACTING or part.status != PartitionStatus.RUNNING
            or agg.files < req.chunk_count):
        metrics.CHUNK_REPORTS.labels("progress" if run.status == RunStatus.EXTRACTING
                                     else "ignored").inc()
        return result

    if agg.is_complete(req.chunk_count) and agg.rows == part.expected_row_count:
        await partitions.mark_success(conn, run_id, partition_id, claim_token=req.claim_token,
                                      rows=agg.rows, files=agg.files, bytes_=agg.bytes)
        await runs.increment_success(conn, run_id)
        await events.record(conn, "PARTITION_SUCCESS", run, partition_id=partition_id,
                            row_count=agg.rows, details={"files": agg.files, "bytes": agg.bytes})
        result.partition_status = PartitionStatus.SUCCESS
        if await runs.try_complete_extract(conn, run_id):
            await dispatch.enqueue_validation(conn, run_id)
            await events.record(conn, "EXTRACT_VALIDATED", run, row_count=run.source_count)
            result.run_status = RunStatus.EXTRACTED_VALIDATED
            result.validation_scheduled = True
            metrics.CHUNK_REPORTS.labels("run_complete").inc()
        else:
            metrics.CHUNK_REPORTS.labels("partition_success").inc()
        return result

    message = (f"files={agg.files} chunkCount={req.chunk_count} distinctCounts={agg.counts} "
               f"rows={agg.rows} expected={part.expected_row_count}")
    await partitions.mark_failed(conn, run_id, partition_id, code="ROW_COUNT_MISMATCH",
                                 message=message)
    await runs.increment_failed(conn, run_id)
    await runs.fail(conn, run_id, expected=RunStatus.EXTRACTING, to=RunStatus.FAILED_EXTRACT,
                    stage="PARTITION_GATE", code="ROW_COUNT_MISMATCH", message=message)
    await events.record(conn, "PARTITION_FAILED", run, level="ERROR", partition_id=partition_id,
                        error_code="ROW_COUNT_MISMATCH", message=message)
    await events.record(conn, "RUN_FAILED", run, level="ERROR", error_code="ROW_COUNT_MISMATCH",
                        message=message)
    result.partition_status, result.run_status = PartitionStatus.FAILED, RunStatus.FAILED_EXTRACT
    metrics.CHUNK_REPORTS.labels("failed").inc()
    return result


async def fail_partition(conn: AsyncConnection, run_id: UUID, partition_id: str,
                         req: PartitionFailRequest) -> PartitionFailResponse:
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)
    if part.claim_token != req.claim_token:
        raise Conflict("CLAIM_MISMATCH")
    if part.status == PartitionStatus.FAILED:  # 멱등 재요청
        return PartitionFailResponse(partition_status=PartitionStatus.FAILED,
                                     run_status=RunStatus(run.status), changed=False)
    if part.status != PartitionStatus.RUNNING:
        raise Conflict("PARTITION_STATUS_MISMATCH", partitionStatus=part.status)

    snapshot = req.error_code.upper() in SNAPSHOT_ERROR_CODES or req.error_class.upper() == "SNAPSHOT"
    run_to = RunStatus.FAILED_SNAPSHOT_EXPIRED if snapshot else RunStatus.FAILED_EXTRACT
    await partitions.mark_failed(conn, run_id, partition_id, code=req.error_code, message=req.message)
    await runs.increment_failed(conn, run_id)
    await events.record(conn, "PARTITION_FAILED", run, level="ERROR", partition_id=partition_id,
                        error_class=req.error_class, error_code=req.error_code, message=req.message,
                        details={"stage": req.error_stage, "attempt": req.attempt})
    run_status = RunStatus(run.status)
    if run.status == RunStatus.EXTRACTING:
        await runs.fail(conn, run_id, expected=RunStatus.EXTRACTING, to=run_to,
                        stage=req.error_stage, code=req.error_code, message=req.message)
        await events.record(conn, "RUN_FAILED", run, level="ERROR", error_code=req.error_code,
                            message=f"partition {partition_id}: {req.message}")
        run_status = run_to
    return PartitionFailResponse(partition_status=PartitionStatus.FAILED, run_status=run_status,
                                 changed=True)
