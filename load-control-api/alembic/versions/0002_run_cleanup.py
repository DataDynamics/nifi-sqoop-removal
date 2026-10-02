"""load_run.cleaned_at: staging table·run 경로 정리 기록

NiFi PG-70 Cleanup이 보존 기간이 지난 run의 staging external table과 HDFS run 경로를 지운 뒤
POST /v1/runs/{id}/cleanup으로 보고하면 API가 이 컬럼을 채운다. 가이드 4.1 DDL과 함께 고친다.

Revision ID: 0002_run_cleanup
Revises: 0001_nifi_ops_baseline
Create Date: 2026-10-03
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0002_run_cleanup"
down_revision: str | None = "0001_nifi_ops_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE nifi_ops.load_run ADD COLUMN cleaned_at timestamptz")
    # 정리 후보 조회(job별, 끝난 시각 순)용. 정리된 run은 인덱스에서 빠진다.
    op.execute("""
CREATE INDEX ix_load_run_cleanup
    ON nifi_ops.load_run (job_key, completed_at)
 WHERE cleaned_at IS NULL
""")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS nifi_ops.ix_load_run_cleanup")
    op.execute("ALTER TABLE nifi_ops.load_run DROP COLUMN IF EXISTS cleaned_at")
