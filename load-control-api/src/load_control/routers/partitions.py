"""파티션 엔드포인트(NiFi PG-20 Worker가 호출)."""

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
    """
    return await run_tx(request, lambda conn: completion.claim(conn, run_id, partition_id, body))


@router.post("/chunks", response_model=ChunkResult)
async def report_chunk(run_id: UUID, partition_id: PartitionIdPath, body: ChunkReport,
                       request: Request) -> ChunkResult:
    """PutHDFS가 성공한 chunk 하나를 보고한다.

    API가 파티션·run 완료를 판정하고, 마지막 보고에서 검증 호출을 예약한다.
    """
    return await run_tx(request, lambda conn: completion.report_chunk(conn, run_id, partition_id, body))


@router.post("/fail", response_model=PartitionFailResponse)
async def fail_partition(run_id: UUID, partition_id: PartitionIdPath, body: PartitionFailRequest,
                         request: Request) -> PartitionFailResponse:
    """재시도를 다 쓴 최종 실패를 보고한다.

    run 전체가 FAILED_EXTRACT(스냅샷 오류는 FAILED_SNAPSHOT_EXPIRED)가 된다.
    """
    return await run_tx(request,
                        lambda conn: completion.fail_partition(conn, run_id, partition_id, body))
