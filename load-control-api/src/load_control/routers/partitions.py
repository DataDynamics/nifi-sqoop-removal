"""파티션 엔드포인트(NiFi PG-20 Worker가 호출, 실패 보고는 PG-90).

모든 호출은 run 행 → 파티션 행 순서로 잠근 뒤 판정하므로 같은 run의 보고는 직렬화된다.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Request

from load_control.routers.deps import PartitionIdPath, run_tx
from load_control.schemas.partitions import (
    ChunkReport,
    ChunkResult,
    ClaimRequest,
    ClaimResponse,
    PartitionFailRequest,
    PartitionFailResponse,
)
from load_control.security import require_role
from load_control.services import completion

router = APIRouter(prefix="/v1/runs/{run_id}/partitions/{partition_id}", tags=["partitions"],
                   dependencies=[Depends(require_role("nifi"))])


@router.post("/claim", response_model=ClaimResponse)
async def claim(run_id: UUID, partition_id: PartitionIdPath, body: ClaimRequest,
                request: Request) -> ClaimResponse:
    """파티션 처리 소유권 요청.

    claimed=false면 Oracle 조회를 시작하지 않는다. 같은 token 재요청은 claimed=true.

    PENDING·RETRY 파티션만 새로 claim해 RUNNING으로 바꾸고 시도 횟수(attempt)를 올린다. RETRY 파티션의
    claim은 재발행 dispatch의 ACK로도 기록한다. run이 EXTRACTING이 아니거나(실패·종료) 다른 Worker가
    처리 중·완료한 파티션이면 claimed=false로 200을 돌려준다(정상 경합). run·파티션이 없으면 404.
    """
    return await run_tx(request, lambda conn: completion.claim(conn, run_id, partition_id, body))


@router.post("/chunks", response_model=ChunkResult)
async def report_chunk(run_id: UUID, partition_id: PartitionIdPath, body: ChunkReport,
                       request: Request) -> ChunkResult:
    """PutHDFS가 성공한 chunk 하나를 보고한다.

    API가 파티션·run 완료를 판정하고, 마지막 보고에서 검증 호출을 예약한다.

    - 같은 chunk 재보고는 기록 1행으로 멱등 처리한다.
    - chunk가 다 모였고 건수 합계가 예상 건수와 같으면 파티션 SUCCESS, 다르면 파티션 FAILED와
      run FAILED_EXTRACT.
    - 모든 파티션이 SUCCESS가 되면 run을 EXTRACTED_VALIDATED로 바꾸고 같은 트랜잭션에서 검증 dispatch를
      예약한다(validationScheduled=true). 이 전이는 run당 정확히 한 번 일어난다.
    - run이 이미 실패·종료됐으면 파일만 기록하고 현재 상태를 200으로 돌려준다.

    오류: claim token 불일치 409 CLAIM_MISMATCH, 이미 성공한 파티션에 다른 내용 409 CHUNK_CONFLICT,
    hdfsPath가 run 경로 밖이면 422 HDFS_PATH_OUTSIDE_RUN, chunkIndex >= chunkCount면 422.
    """
    return await run_tx(request, lambda conn: completion.report_chunk(conn, run_id, partition_id, body))


@router.post("/fail", response_model=PartitionFailResponse)
async def fail_partition(run_id: UUID, partition_id: PartitionIdPath, body: PartitionFailRequest,
                         request: Request) -> PartitionFailResponse:
    """재시도를 다 쓴 최종 실패를 보고한다.

    run 전체가 FAILED_EXTRACT(스냅샷 오류는 FAILED_SNAPSHOT_EXPIRED)가 된다.

    스냅샷 오류: errorCode가 ORA-01555·ORA-08180이거나 errorClass가 SNAPSHOT. 같은 SCN으로 다시 읽을 수
    없으므로 새 run이 필요하다는 뜻이다. run이 이미 EXTRACTING이 아니면 파티션만 FAILED로 바꾼다.
    claim한 Worker만 보고할 수 있다(token 불일치 409 CLAIM_MISMATCH). 이미 FAILED면 changed=false(멱등),
    RUNNING이 아닌 다른 상태면 409 PARTITION_STATUS_MISMATCH.
    """
    return await run_tx(request,
                        lambda conn: completion.fail_partition(conn, run_id, partition_id, body))
