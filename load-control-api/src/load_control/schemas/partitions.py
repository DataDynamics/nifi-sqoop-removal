"""파티션 엔드포인트 모델."""

from uuid import UUID

from pydantic import Field

from load_control.domain import PartitionStatus, RunStatus
from load_control.schemas.common import ApiModel, Message, ShortText


class ClaimRequest(ApiModel):
    """claim 요청. claimToken은 Worker가 한 번 만들어 재시도에도 그대로 쓴다."""

    claim_token: UUID  # 이 시도의 소유권 증표. 이후 chunk·실패 보고에 같은 값을 보낸다
    worker_node: str = Field(min_length=1, max_length=200)  # 처리하는 NiFi 노드 이름(조회·추적용)


class ClaimResponse(ApiModel):
    """claim 결과. claimed=false면 처리하지 않는다."""

    claimed: bool  # true일 때만 Oracle 조회를 시작한다
    run_status: RunStatus
    partition_status: PartitionStatus
    attempt: int  # 이 파티션의 claim 횟수(재발행되면 늘어난다)


class ChunkReport(ApiModel):
    """chunk 하나의 보고. NiFi AttributesToJSON의 문자열 값도 받는다."""

    claim_token: UUID  # claim 때 보낸 token. 다르면 409 CLAIM_MISMATCH(이전 시도의 늦은 보고)
    chunk_index: int = Field(ge=0)  # 0부터 시작하는 chunk 번호. chunk_count보다 작아야 한다
    chunk_count: int = Field(gt=0, le=1_000_000)  # 이 파티션의 전체 chunk 수(모든 보고에서 같아야 한다)
    fragment_identifier: str | None = Field(default=None, max_length=100)  # NiFi fragment.identifier(추적용)
    hdfs_path: str = Field(min_length=1, max_length=1500)  # PutHDFS가 쓴 파일. run 경로 아래여야 한다
    record_count: int = Field(ge=0)  # 이 chunk의 행 수. 파티션 합계를 예상 건수와 비교한다
    byte_count: int | None = Field(default=None, ge=0)  # 파일 크기(바이트, 기록용)


class ChunkResult(ApiModel):
    """chunk 판정 결과. NiFi는 로그 수준만 정하고 흐름을 바꾸지 않는다."""

    recorded: bool  # 파일 기록 여부(오류가 아니면 항상 true)
    partition_status: PartitionStatus  # 이 보고 반영 후 파티션 상태
    run_status: RunStatus  # 이 보고 반영 후 run 상태
    received_chunks: int  # 지금까지 기록된 이 파티션의 chunk 수
    chunk_count: int  # 요청의 chunkCount
    validation_scheduled: bool  # 이 보고로 run이 완료되어 검증 호출을 예약했으면 true


class PartitionFailRequest(ApiModel):
    """파티션 최종 실패 보고."""

    claim_token: UUID  # claim 때 보낸 token. 소유자만 실패를 보고할 수 있다
    error_stage: ShortText  # 실패한 NiFi 단계. run을 실패로 바꿀 때 run error_stage로 기록한다
    error_class: ShortText  # 오류 분류. SNAPSHOT이면 run을 FAILED_SNAPSHOT_EXPIRED로 바꾼다
    error_code: ShortText  # 예: ORA-01555(스냅샷 오류), 그 밖의 NiFi·DB 오류 코드
    message: Message = ""
    attempt: int | None = Field(default=None, ge=0)  # NiFi 쪽 재시도 횟수(이벤트 기록용)


class PartitionFailResponse(ApiModel):
    """파티션 실패 처리 결과."""

    partition_status: PartitionStatus  # FAILED
    run_status: RunStatus  # 처리 후 run 상태(보통 FAILED_EXTRACT 또는 FAILED_SNAPSHOT_EXPIRED)
    changed: bool  # 이미 FAILED였으면(재요청) false
