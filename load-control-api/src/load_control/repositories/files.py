"""load_file(chunk 보고 원장) SQL.

NiFi Worker가 HDFS에 chunk 파일 하나를 쓸 때마다 보고하는 행을 (run_id, partition_id, chunk_index)
기본키로 쌓는다. 파티션 완료 판정은 이 원장의 WRITTEN 행 집계로 한다. 재발행 시 이전 시도의 행은
FAILED로 바꿔 집계에서 뺀다. hdfs_path에는 uq_load_file_path 유니크 제약이 있다.
"""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


@dataclass(frozen=True, slots=True)
class FileAggregate:
    """파티션 하나의 chunk 집계 결과."""

    files: int
    lo: int | None
    hi: int | None
    rows: int
    bytes: int
    counts: int  # 서로 다른 fragment_count(=chunkCount) 값의 수

    def is_complete(self, chunk_count: int) -> bool:
        """chunk 0..n-1이 모두 있고 모든 보고의 chunkCount가 같다.

        chunk_index는 기본키라 중복이 없으므로, 개수가 n이고 최소 0·최대 n-1이면 빈 번호 없이 다 모인 것이다.
        counts == 1은 모든 chunk가 같은 chunkCount를 보고했다는 뜻이다(서로 다른 분할 결과가 섞이지 않음).
        """
        return (self.files == chunk_count and self.lo == 0 and self.hi == chunk_count - 1
                and self.counts == 1)


async def upsert(conn: AsyncConnection, run_id: UUID, partition_id: str, *, chunk_index: int,
                 chunk_count: int, fragment_identifier: str | None, hdfs_path: str,
                 record_count: int, byte_count: int | None) -> bool:
    """chunk 보고를 기록한다. 새 행이거나 기존 값과 달라졌으면 True(호출 전 partition 행 잠금 필수).

    같은 chunk의 재보고(NiFi 재시도)는 같은 값으로 덮어써 멱등이다. 반환값 changed는 호출자가
    "이미 SUCCESS인 파티션에 다른 내용이 들어왔는가(CHUNK_CONFLICT)"를 판단하는 데 쓴다.
    비교 대상은 fragment_count·hdfs_path·record_count·byte_count이며 fragment_identifier와 status는
    비교하지 않는다. 그래서 FAILED로 무효화된 행에 같은 값이 다시 보고되면 changed=False이면서
    status는 WRITTEN으로 돌아온다.

    SELECT 후 UPSERT 사이의 경합은 호출자가 잡은 partition 행 잠금으로 막는다(같은 파티션의 보고는 직렬화).
    """
    existing = (await conn.execute(text("""
        SELECT fragment_count, hdfs_path, record_count, byte_count
          FROM nifi_ops.load_file
         WHERE run_id = :run_id AND partition_id = :partition_id AND chunk_index = :chunk_index
    """), {"run_id": run_id, "partition_id": partition_id, "chunk_index": chunk_index})).first()
    changed = existing is None or tuple(existing) != (chunk_count, hdfs_path, record_count, byte_count)
    # ON CONFLICT (기본키) DO UPDATE: 같은 chunk 재보고는 새 행을 만들지 않고 최신 값으로 덮어쓰며,
    # 무효화(FAILED)된 행도 WRITTEN으로 되살린다.
    await conn.execute(text("""
        INSERT INTO nifi_ops.load_file (
            run_id, partition_id, chunk_index, fragment_identifier, fragment_count,
            hdfs_path, record_count, byte_count, status)
        VALUES (:run_id, :partition_id, :chunk_index, :fragment_identifier, :chunk_count,
                :hdfs_path, :record_count, :byte_count, 'WRITTEN')
        ON CONFLICT (run_id, partition_id, chunk_index)
        DO UPDATE SET
            fragment_identifier = EXCLUDED.fragment_identifier,
            fragment_count      = EXCLUDED.fragment_count,
            hdfs_path           = EXCLUDED.hdfs_path,
            record_count        = EXCLUDED.record_count,
            byte_count          = EXCLUDED.byte_count,
            status              = 'WRITTEN',
            updated_at          = clock_timestamp()
    """), {"run_id": run_id, "partition_id": partition_id, "chunk_index": chunk_index,
           "fragment_identifier": fragment_identifier, "chunk_count": chunk_count,
           "hdfs_path": hdfs_path, "record_count": record_count, "byte_count": byte_count})
    return changed


async def aggregate(conn: AsyncConnection, run_id: UUID, partition_id: str) -> FileAggregate:
    """파티션의 WRITTEN chunk를 집계한다.

    FAILED로 무효화된 이전 시도의 chunk는 빠진다. 행이 없으면 files=0, lo/hi=None, rows·bytes=0이다.
    byte_count가 NULL인 chunk는 SUM에서 무시된다.
    """
    m = (await conn.execute(text("""
        SELECT COUNT(*)                       AS files,
               MIN(chunk_index)               AS lo,
               MAX(chunk_index)               AS hi,
               COALESCE(SUM(record_count), 0) AS rows,
               COALESCE(SUM(byte_count), 0)   AS bytes,
               COUNT(DISTINCT fragment_count) AS counts
          FROM nifi_ops.load_file
         WHERE run_id = :run_id AND partition_id = :partition_id AND status = 'WRITTEN'
    """), {"run_id": run_id, "partition_id": partition_id})).mappings().one()
    return FileAggregate(files=int(m["files"]), lo=m["lo"], hi=m["hi"], rows=int(m["rows"]),
                         bytes=int(m["bytes"]), counts=int(m["counts"]))


async def invalidate(conn: AsyncConnection, run_id: UUID, partition_id: str) -> int:
    """재발행 전에 이전 시도의 chunk 기록을 집계에서 뺀다. 같은 chunk를 다시 보고하면 WRITTEN으로 돌아온다.

    WRITTEN → FAILED로 바꾼 행 수를 돌려준다(RECOVERY_REISSUED 이벤트의 invalidatedFiles). 행은 지우지 않고
    상태만 바꾸므로 이전 시도의 보고 기록은 원장에 남는다. 호출자(sweeper)는 run·파티션 행을 이미 잠근 상태다.
    """
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_file SET status = 'FAILED', updated_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id AND status = 'WRITTEN'
    """), {"run_id": run_id, "partition_id": partition_id})
    return result.rowcount
