"""파티션 소유권, chunk 보고, 완료 판정 및 실패 처리를 담당한다.

이 모듈은 모든 파티션의 완료 여부를 판정하는 핵심 경로다. 항상 run 행을 먼저 잠가 같은 run의
판정을 직렬화한다. 따라서 여러 파티션이 동시에 끝나더라도 run 완료 처리와 검증 호출 예약은 한 번만
실행된다.
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
    """run 행과 파티션 행을 이 순서로 잠가 돌려준다.

    run 잠금 대기 시간은 RUN_LOCK_WAIT 메트릭으로 잰다(같은 run에 보고가 몰리면 길어진다).

    Raises:
        NotFound: RUN_NOT_FOUND 또는 PARTITION_NOT_FOUND.
    """
    # 잠금 순서 고정: load_run → load_partition
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
    """NiFi worker에 파티션 처리 소유권을 부여한다.

    - run이 `EXTRACTING` 상태가 아니면 거절한다.
    - 같은 token으로 다시 요청하면 기존 소유권을 유지한다.
    - `PENDING`과 `RETRY` 상태만 새로 claim할 수 있다.
    - `RETRY` 파티션의 claim은 재발행 요청에 대한 ACK 역할도 한다.

    새 claim에는 token과 worker 노드를 저장하고 시도 횟수를 올린 뒤 `PARTITION_STARTED` 이벤트를
    남긴다. 중복 FlowFile이나 늦게 도착한 재발행은 정상적인 경합이므로 예외 대신 `claimed=false`를
    반환한다. 이 응답을 받은 worker는 Oracle 조회를 시작하지 않는다.
    """
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)
    ctx = {"runId": str(run_id), "partitionId": partition_id, "workerNode": req.worker_node}

    def response(claimed: bool, status: str, attempt: int) -> ClaimResponse:
        """잠금 시점의 run 상태를 담은 claim 응답을 만든다."""
        return ClaimResponse(claimed=claimed, run_status=RunStatus(run.status),
                             partition_status=PartitionStatus(status), attempt=attempt)

    if run.status != RunStatus.EXTRACTING:
        log.info("partition_claim_refused", reason="run_not_extracting", runStatus=run.status, **ctx)
        return response(False, part.status, part.attempt_count)
    if part.status == PartitionStatus.RUNNING and part.claim_token == req.claim_token:
        # 응답 유실 후 같은 token 재요청: 소유권 유지
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
        await dispatch.ack_reissue(conn, run_id, partition_id)  # 재발행 수신 확인
    await runs.touch(conn, run_id)
    await events.record(conn, "PARTITION_STARTED", run, partition_id=partition_id,
                        details={"workerNode": req.worker_node, "attempt": attempt})
    log.info("partition_claimed", attempt=attempt, reissued=part.status == PartitionStatus.RETRY, **ctx)
    return response(True, PartitionStatus.RUNNING, attempt)


async def report_chunk(conn: AsyncConnection, run_id: UUID, partition_id: str,
                       req: ChunkReport) -> ChunkResult:
    """chunk 보고를 기록하고 파티션과 run의 완료 여부를 판정한다.

    처리 순서:

    1. run과 파티션을 잠그고 claim token 및 HDFS 경로를 검증한다.
    2. `load_file`에 UPSERT하고 heartbeat를 갱신한다. 같은 chunk의 재보고는 멱등이다.
    3. 모든 chunk가 모이면 건수 합계를 비교해 파티션의 성공 또는 실패를 판정한다.
    4. 모든 파티션이 성공하면 run을 `EXTRACTED_VALIDATED`로 바꾸고 검증 호출을 예약한다.

    완료 판정은 run이 `EXTRACTING`, 파티션이 `RUNNING`이고 받은 chunk 수가 `chunkCount` 이상일 때만
    수행한다. 그 밖의 경우에는 파일만 기록하고 현재 상태를 반환한다. run 종료 후 도착한 보고도
    기록하여 Cleanup이 삭제해야 할 파일을 원장에 남긴다.

    상태 전이와 기록:
    - 성공: 파티션 RUNNING → SUCCESS, success_partition_count +1, PARTITION_SUCCESS 이벤트.
      마지막 파티션이면 run EXTRACTING → EXTRACTED_VALIDATED, VALIDATE_RUN dispatch 예약,
      EXTRACT_VALIDATED 이벤트.
    - 불일치(chunk 누락·중복 chunkCount·row 합계 차이): 파티션 → FAILED, run EXTRACTING → FAILED_EXTRACT
      (ROW_COUNT_MISMATCH), PARTITION_FAILED·RUN_FAILED 이벤트. 이 경우도 예외가 아니라 결과로 돌려준다.

    run 행 잠금으로 같은 run의 보고를 직렬화하므로 완료 처리와 검증 예약은 정확히 한 번만 일어난다.

    Raises:
        Unprocessable: CHUNK_INDEX_OUT_OF_RANGE(chunkIndex >= chunkCount),
            HDFS_PATH_OUTSIDE_RUN(run 경로 밖이거나 '..' 포함).
        Conflict: CLAIM_MISMATCH(소유권 없음), CHUNK_CONFLICT(이미 SUCCESS인 파티션에 다른 내용의 보고).
        NotFound: RUN_NOT_FOUND, PARTITION_NOT_FOUND.
    """
    ctx = {"runId": str(run_id), "partitionId": partition_id, "chunkIndex": req.chunk_index}
    # 잠금을 잡기 전에 입력만으로 판단할 수 있는 오류를 먼저 거른다.
    if req.chunk_index >= req.chunk_count:
        raise Unprocessable("CHUNK_INDEX_OUT_OF_RANGE")
    run, part = await _lock_run_and_partition(conn, run_id, partition_id)
    if part.claim_token != req.claim_token:
        # 재발행 후 늦게 살아난 이전 Worker이거나 잘못된 FlowFile이다.
        log.warning("chunk_claim_mismatch", partitionStatus=part.status, **ctx)
        raise Conflict("CLAIM_MISMATCH")
    # Cleanup은 run 경로 전체를 지우므로 파일이 반드시 그 아래에 있어야 한다. 비교할 접두어 끝에 `/`를
    # 붙여 이름만 비슷한 다른 디렉터리를 제외하고, `..`로 상위 경로를 가리키는 입력도 거부한다.
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

    # 이번 요청의 값만 보지 않고 원장에 기록된 모든 `WRITTEN` chunk를 집계해 판정한다.
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

    # chunk가 chunkCount 이상 모였다. 0..n-1이 빠짐없이 같은 chunkCount로 보고됐고 row 합계가 manifest의
    # 예상 건수와 같아야 SUCCESS다. 그렇지 않으면 데이터 누락·중복이므로 재시도하지 않고 run을 실패시킨다.
    if agg.is_complete(req.chunk_count) and agg.rows == part.expected_row_count:
        await partitions.mark_success(conn, run_id, partition_id, claim_token=req.claim_token,
                                      rows=agg.rows, files=agg.files, bytes_=agg.bytes)
        await runs.increment_success(conn, run_id)
        await events.record(conn, "PARTITION_SUCCESS", run, partition_id=partition_id,
                            row_count=agg.rows, details={"files": agg.files, "bytes": agg.bytes})
        result.partition_status = PartitionStatus.SUCCESS
        log.info("partition_success", rows=agg.rows, files=agg.files, bytes=agg.bytes, **ctx)
        # 파티션 수·row 합계를 DB에서 다시 세어 판정한다. True를 받은 요청 하나만 검증을 예약한다.
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
    """Worker의 최종 실패 보고. 파티션 하나가 실패하면 run 전체가 실패하고 게시는 차단된다.

    파티션 RUNNING → FAILED, failed_partition_count +1, PARTITION_FAILED 이벤트를 남긴다. run이 EXTRACTING이면
    run도 실패로 바꾸고 RUN_FAILED 이벤트를 남긴다. 오류 코드가 SNAPSHOT_ERROR_CODES(ORA-01555 등)이거나
    errorClass가 SNAPSHOT이면 같은 SCN으로 다시 읽을 수 없으므로 FAILED_SNAPSHOT_EXPIRED, 그 밖은
    FAILED_EXTRACT다. run이 이미 끝난 상태면 파티션만 FAILED로 기록한다.
    이미 FAILED인 파티션의 같은 token 재요청은 changed=False로 성공 응답한다(멱등).

    Raises:
        Conflict: CLAIM_MISMATCH(소유권 없음),
            PARTITION_STATUS_MISMATCH(RUNNING이 아님. 예: SUCCESS, TIMED_OUT).
        NotFound: RUN_NOT_FOUND, PARTITION_NOT_FOUND.
    """
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
