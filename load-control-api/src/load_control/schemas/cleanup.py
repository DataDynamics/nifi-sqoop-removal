"""정리(PG-70 Cleanup) 엔드포인트 모델."""

from datetime import datetime

from pydantic import Field

from load_control.domain import RunStatus
from load_control.schemas.common import ApiModel, HdfsPath


class CleanupCandidate(ApiModel):
    """보존 기간이 지나 정리할 run. NiFi는 이 경로와 테이블만 지운다."""

    run_id: str
    job_key: str
    business_key: str
    status: RunStatus
    hdfs_run_path: str | None
    stage_table: str | None
    completed_at: datetime


class CleanupCandidatesResponse(ApiModel):
    """정리 후보 목록(끝난 시각이 오래된 순)."""

    runs: list[CleanupCandidate]


class CleanupRequest(ApiModel):
    """NiFi가 지운 대상. 기록용이며 API는 이 값으로 아무것도 지우지 않는다."""

    dropped_table: str | None = Field(default=None, max_length=300)
    deleted_path: HdfsPath | None = None


class CleanupResponse(ApiModel):
    """정리 기록 결과. 이미 기록된 run이면 changed=false."""

    run_status: RunStatus
    changed: bool
