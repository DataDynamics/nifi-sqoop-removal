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
    status: RunStatus  # SUCCESS 또는 실패·TIMED_OUT 상태
    hdfs_run_path: str | None  # 지울 HDFS run 경로(run 생성 때 API가 만든 값)
    stage_table: str | None  # DROP할 staging table 이름
    completed_at: datetime  # 끝난 시각. completed_at이 없으면 마지막 heartbeat 시각. 보존 기간 계산 기준


class CleanupCandidatesResponse(ApiModel):
    """정리 후보 목록(끝난 시각이 오래된 순)."""

    runs: list[CleanupCandidate]  # 최대 min(limit, cleanup.max_batch)개


class CleanupRequest(ApiModel):
    """NiFi가 지운 대상. 기록용이며 API는 이 값으로 아무것도 지우지 않는다."""

    dropped_table: str | None = Field(default=None, max_length=300)  # DROP한 staging table 이름
    deleted_path: HdfsPath | None = None  # 삭제한 HDFS 경로


class CleanupResponse(ApiModel):
    """정리 기록 결과. 이미 기록된 run이면 changed=false."""

    run_status: RunStatus  # 정리 기록은 상태를 바꾸지 않으므로 run의 현재(끝난) 상태
    changed: bool  # 이번 호출로 cleaned_at을 기록했으면 true
