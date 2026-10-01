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
    return await run_tx(request, lambda conn: completion.claim(conn, run_id, partition_id, body))


@router.post("/chunks", response_model=ChunkResult)
async def report_chunk(run_id: UUID, partition_id: PartitionIdPath, body: ChunkReport,
                       request: Request) -> ChunkResult:
    return await run_tx(request, lambda conn: completion.report_chunk(conn, run_id, partition_id, body))


@router.post("/fail", response_model=PartitionFailResponse)
async def fail_partition(run_id: UUID, partition_id: PartitionIdPath, body: PartitionFailRequest,
                         request: Request) -> PartitionFailResponse:
    return await run_tx(request,
                        lambda conn: completion.fail_partition(conn, run_id, partition_id, body))
