from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


@dataclass(frozen=True, slots=True)
class FileAggregate:
    files: int
    lo: int | None
    hi: int | None
    rows: int
    bytes: int
    counts: int  # 서로 다른 fragment_count(=chunkCount) 값의 수

    def is_complete(self, chunk_count: int) -> bool:
        """chunk 0..n-1이 모두 있고 모든 보고의 chunkCount가 같다(API 설계 3.2의 4단계)."""
        return (self.files == chunk_count and self.lo == 0 and self.hi == chunk_count - 1
                and self.counts == 1)


async def upsert(conn: AsyncConnection, run_id: UUID, partition_id: str, *, chunk_index: int,
                 chunk_count: int, fragment_identifier: str | None, hdfs_path: str,
                 record_count: int, byte_count: int | None) -> bool:
    """chunk 보고를 기록한다. 새 행이거나 기존 값과 달라졌으면 True(호출 전 partition 행 잠금 필수)."""
    existing = (await conn.execute(text("""
        SELECT fragment_count, hdfs_path, record_count, byte_count
          FROM nifi_ops.load_file
         WHERE run_id = :run_id AND partition_id = :partition_id AND chunk_index = :chunk_index
    """), {"run_id": run_id, "partition_id": partition_id, "chunk_index": chunk_index})).first()
    changed = existing is None or tuple(existing) != (chunk_count, hdfs_path, record_count, byte_count)
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
    """재발행 전에 이전 시도의 chunk 기록을 집계에서 뺀다. 같은 chunk를 다시 보고하면 WRITTEN으로 돌아온다."""
    result = await conn.execute(text("""
        UPDATE nifi_ops.load_file SET status = 'FAILED', updated_at = clock_timestamp()
         WHERE run_id = :run_id AND partition_id = :partition_id AND status = 'WRITTEN'
    """), {"run_id": run_id, "partition_id": partition_id})
    return result.rowcount
