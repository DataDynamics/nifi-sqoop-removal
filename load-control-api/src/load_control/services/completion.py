"""파티션 claim, chunk 보고 판정, 파티션 실패(API 설계 3장, 9.6).

이 모듈이 "모든 파티션이 끝났는가"를 판정하는 핵심이다. 모든 함수는 run 행을 먼저 잠가
같은 run의 판정을 직렬화한다. 그래서 마지막 파티션들이 동시에 끝나도 run 완료와 검증 호출 예약은
정확히 한 번 일어난다.
"""

from uuid import UUID

import structlog
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

log = structlog.get_logger(__name__)


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
    """파티션 처리 소유권을 준다. claimed=false면 NiFi Worker는 Oracle 조회를 시작하지 않는다.

    - run이 EXTRACTING이 아니면(실패·종료) 거절
    - 같은 token 재요청(응답 유실 후 재시도)은 소유권 유지
    - PENDING/RETRY만 새로 claim. RETRY의 claim은 재발행 요청의 수신 확인(ACK)이기도 하다
    """
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)
    ctx = {"runId": str(run_id), "partitionId": partition_id, "workerNode": req.worker_node}

    def response(claimed: bool, status: str, attempt: int) -> ClaimResponse:
        return ClaimResponse(claimed=claimed, run_status=RunStatus(run.status),
                             partition_status=PartitionStatus(status), attempt=attempt)

    if run.status != RunStatus.EXTRACTING:
        log.info("partition_claim_refused", reason="run_not_extracting", runStatus=run.status, **ctx)
        return response(False, part.status, part.attempt_count)
    if part.status == PartitionStatus.RUNNING and part.claim_token == req.claim_token:
        # 응답 유실 후 같은 token 재요청: 소유권 유지(가이드 4.1 "Claim과 상태 전이")
        await partitions.touch(conn, run_id, partition_id)
        log.debug("partition_claim_replayed", attempt=part.attempt_count, **ctx)
        return response(True, part.status, part.attempt_count)
    if part.status not in (PartitionStatus.PENDING, PartitionStatus.RETRY):
        # 다른 Worker가 이미 처리 중이거나 끝난 파티션(중복 FlowFile). 정상 경합이다.
        log.info("partition_claim_refused", reason="not_claimable", partitionStatus=part.status, **ctx)
        return response(False, part.status, part.attempt_count)

    attempt = await partitions.claim(conn, run_id, partition_id, claim_token=req.claim_token,
                                     worker_node=req.worker_node)
    if part.status == PartitionStatus.RETRY:
        await dispatch.ack_reissue(conn, run_id, partition_id)  # 재발행 수신 확인(가이드 13.2)
    await runs.touch(conn, run_id)
    await events.record(conn, "PARTITION_STARTED", run, partition_id=partition_id,
                        details={"workerNode": req.worker_node, "attempt": attempt})
    log.info("partition_claimed", attempt=attempt, reissued=part.status == PartitionStatus.RETRY, **ctx)
    return response(True, PartitionStatus.RUNNING, attempt)


async def report_chunk(conn: AsyncConnection, run_id: UUID, partition_id: str,
                       req: ChunkReport) -> ChunkResult:
    """chunk 보고를 기록하고 파티션·run 완료를 판정한다(API 설계 3.2).

    1. run·파티션 잠금, claim token과 HDFS 경로 확인
    2. load_file UPSERT(같은 chunk 재보고는 멱등), heartbeat 갱신
    3. chunk가 다 모이면 파티션 판정: row 합계 일치 → SUCCESS, 불일치 → 파티션과 run 실패
    4. 파티션이 SUCCESS가 되면 run 판정: 모두 SUCCESS이고 합계 일치 → EXTRACTED_VALIDATED + 검증 호출 예약
    """
    ctx = {"runId": str(run_id), "partitionId": partition_id, "chunkIndex": req.chunk_index}
    if req.chunk_index >= req.chunk_count:
        raise Unprocessable("CHUNK_INDEX_OUT_OF_RANGE")
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)
    if part.claim_token != req.claim_token:
        # 재발행 후 늦게 살아난 이전 Worker이거나 잘못된 FlowFile이다.
        log.warning("chunk_claim_mismatch", partitionStatus=part.status, **ctx)
        raise Conflict("CLAIM_MISMATCH")
    if (run.hdfs_run_path is None or not req.hdfs_path.startswith(run.hdfs_run_path + "/")
            or ".." in req.hdfs_path.split("/")):
        log.warning("chunk_path_outside_run", hdfsPath=req.hdfs_path, hdfsRunPath=run.hdfs_run_path, **ctx)
        raise Unprocessable("HDFS_PATH_OUTSIDE_RUN")

    changed = await files.upsert(conn, run_id, partition_id, chunk_index=req.chunk_index,
                                 chunk_count=req.chunk_count,
                                 fragment_identifier=req.fragment_identifier,
                                 hdfs_path=req.hdfs_path, record_count=req.record_count,
                                 byte_count=req.byte_count)
    if part.status == PartitionStatus.SUCCESS and changed:
        log.warning("chunk_conflict_after_success", recordCount=req.record_count, **ctx)
        raise Conflict("CHUNK_CONFLICT")  # 예외로 rollback되어 기록도 취소된다
    await partitions.touch(conn, run_id, partition_id)
    await runs.touch(conn, run_id)

    agg = await files.aggregate(conn, run_id, partition_id)
    result = ChunkResult(recorded=True, partition_status=PartitionStatus(part.status),
                         run_status=RunStatus(run.status), received_chunks=agg.files,
                         chunk_count=req.chunk_count, validation_scheduled=False)
    if (run.status != RunStatus.EXTRACTING or part.status != PartitionStatus.RUNNING
            or agg.files < req.chunk_count):
        if run.status == RunStatus.EXTRACTING:
            metrics.CHUNK_REPORTS.labels("progress").inc()
            log.debug("chunk_recorded", receivedChunks=agg.files, chunkCount=req.chunk_count,
                      partitionStatus=part.status, **ctx)
        else:
            # run이 이미 실패·종료됐다. 파일은 정리용으로 기록만 하고 판정하지 않는다.
            metrics.CHUNK_REPORTS.labels("ignored").inc()
            log.info("chunk_after_run_end", runStatus=run.status, **ctx)
        return result

    if agg.is_complete(req.chunk_count) and agg.rows == part.expected_row_count:
        await partitions.mark_success(conn, run_id, partition_id, claim_token=req.claim_token,
                                      rows=agg.rows, files=agg.files, bytes_=agg.bytes)
        await runs.increment_success(conn, run_id)
        await events.record(conn, "PARTITION_SUCCESS", run, partition_id=partition_id,
                            row_count=agg.rows, details={"files": agg.files, "bytes": agg.bytes})
        result.partition_status = PartitionStatus.SUCCESS
        log.info("partition_success", rows=agg.rows, files=agg.files, bytes=agg.bytes, **ctx)
        if await runs.try_complete_extract(conn, run_id):
            await dispatch.enqueue_validation(conn, run_id)
            await events.record(conn, "EXTRACT_VALIDATED", run, row_count=run.source_count)
            result.run_status = RunStatus.EXTRACTED_VALIDATED
            result.validation_scheduled = True
            metrics.CHUNK_REPORTS.labels("run_complete").inc()
            log.info("extract_validated", runId=str(run_id), jobKey=run.job_key,
                     businessKey=run.business_key, extractedCount=run.source_count,
                     detail="all partitions succeeded; validation dispatch scheduled")
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
    log.error("partition_row_mismatch", files=agg.files, chunkCount=req.chunk_count,
              distinctChunkCounts=agg.counts, rows=agg.rows, expectedRows=part.expected_row_count, **ctx)
    return result


async def fail_partition(conn: AsyncConnection, run_id: UUID, partition_id: str,
                         req: PartitionFailRequest) -> PartitionFailResponse:
    """Worker의 최종 실패 보고. 파티션 하나가 실패하면 run 전체가 실패하고 게시는 차단된다."""
    ctx = {"runId": str(run_id), "partitionId": partition_id}
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)
    if part.claim_token != req.claim_token:
        log.warning("partition_fail_claim_mismatch", partitionStatus=part.status, **ctx)
        raise Conflict("CLAIM_MISMATCH")
    if part.status == PartitionStatus.FAILED:  # 멱등 재요청
        log.debug("partition_fail_replayed", **ctx)
        return PartitionFailResponse(partition_status=PartitionStatus.FAILED,
                                     run_status=RunStatus(run.status), changed=False)
    if part.status != PartitionStatus.RUNNING:
        log.warning("partition_fail_rejected", partitionStatus=part.status, **ctx)
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
    log.error("partition_failed", errorStage=req.error_stage, errorClass=req.error_class,
              errorCode=req.error_code, attempt=req.attempt, runStatus=run_status,
              message=req.message[:300], **ctx)
    return PartitionFailResponse(partition_status=PartitionStatus.FAILED, run_status=run_status,
                                 changed=True)
