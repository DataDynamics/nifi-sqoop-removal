"""bin/oracle.sh의 Oracle 연결과 실행(python-oracledb thin 모드).

읽기 전용(기본)에서는 문장마다 `SET TRANSACTION READ ONLY`로 트랜잭션을 열고 결과를 읽은 뒤 ROLLBACK한다.
statements.write_reason 검사를 통과한 문장도 DB가 DML을 한 번 더 막는다(ORA-01456). 원천 DB의 undo
보존 시간 안이면 `SELECT ... AS OF SCN <run의 snapshot_scn>`으로 NiFi가 추출한 시점의 원천을 다시 볼 수 있다.

쓰기 모드(--write)는 autocommit을 끈다. COMMIT을 직접 입력해야 반영되고, 종료하면 ROLLBACK한다.
"""

import re
from typing import Any

import oracledb

from load_control.config import OracleClientSettings
from load_control.query.output import ResultSet
from load_control.query.sqlshell import ConnectError, MetaHandler, QueryError, SqlBackend

# LOB을 LOB 객체가 아닌 str·bytes로 받는다(출력할 때 따로 읽지 않아도 된다).
oracledb.defaults.fetch_lobs = False

_DESC = re.compile(r"^\s*DESC(RIBE)?\s+(\S+)\s*$", re.IGNORECASE)
_NAME = re.compile(r'^("[^"]+"|[A-Za-z0-9_$#*%]+)(\.("[^"]+"|[A-Za-z0-9_$#*%]+))?$')

_TABLES_SQL = """
SELECT o.owner, o.object_name AS name, o.object_type AS type, t.num_rows,
       TO_CHAR(t.last_analyzed, 'YYYY-MM-DD HH24:MI:SS') AS last_analyzed
  FROM all_objects o
  LEFT JOIN all_tables t ON t.owner = o.owner AND t.table_name = o.object_name
 WHERE o.object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SYNONYM')
   AND o.owner LIKE :owner AND o.object_name LIKE :name
   AND o.owner NOT IN (SELECT username FROM all_users WHERE oracle_maintained = 'Y')
   AND o.owner <> 'PUBLIC'
 ORDER BY o.owner, o.object_name
"""
_COLUMNS_SQL = """
SELECT c.owner, c.column_id, c.column_name, c.data_type, c.data_precision, c.data_scale,
       c.char_length, c.char_used, c.nullable, c.data_default, m.comments
  FROM all_tab_columns c
  LEFT JOIN all_col_comments m
    ON m.owner = c.owner AND m.table_name = c.table_name AND m.column_name = c.column_name
 WHERE c.table_name = :name AND (:owner IS NULL OR c.owner = :owner)
 ORDER BY c.owner, c.column_id
"""
_SCHEMAS_SQL = """
SELECT username AS schema, TO_CHAR(created, 'YYYY-MM-DD') AS created
  FROM all_users
 WHERE oracle_maintained = 'N' AND username LIKE :name
 ORDER BY username
"""


def _ident(part: str) -> str:
    """식별자 한 조각. 큰따옴표로 감싸면 대소문자를 그대로, 아니면 대문자로(Oracle 규칙)."""
    return part[1:-1] if part.startswith('"') else part.upper()


def split_name(name: str) -> tuple[str | None, str]:
    """`스키마.이름` 또는 `이름`을 나눈다. 형식이 틀리면 QueryError."""
    match = _NAME.match(name)
    if not match:
        raise QueryError(f"이름 형식이 아닙니다: {name}")
    if match.group(3):
        return _ident(match.group(1)), _ident(match.group(3))
    return None, _ident(match.group(1))


def _like(part: str | None) -> str:
    """패턴 조각을 LIKE 값으로. `*`도 `%`로 받는다. 없으면 전체."""
    return "%" if part is None else part.replace("*", "%")


def column_type(row: dict[str, Any]) -> str:
    """all_tab_columns 한 행의 타입 표기(NUMBER(18,2), VARCHAR2(100 CHAR) 등)."""
    dtype, prec, scale = row["DATA_TYPE"], row["DATA_PRECISION"], row["DATA_SCALE"]
    if dtype == "NUMBER" and prec is not None:
        return f"NUMBER({prec},{scale})" if scale else f"NUMBER({prec})"
    if dtype == "NUMBER" and scale == 0:
        return "INTEGER"
    if dtype in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW") and row["CHAR_LENGTH"]:
        unit = {"C": " CHAR", "B": " BYTE"}.get(row["CHAR_USED"] or "", "")
        return f"{dtype}({row['CHAR_LENGTH']}{unit if dtype in ('VARCHAR2', 'CHAR') else ''})"
    return str(dtype)


def error_message(exc: BaseException) -> str:
    """oracledb 오류에서 `Help: https://...` 줄을 뺀 메시지."""
    lines = [ln for ln in str(exc).splitlines() if ln.strip() and not ln.startswith("Help: ")]
    return "\n".join(lines) or exc.__class__.__name__


class OracleBackend(SqlBackend):
    """oracledb 연결 하나로 문장을 차례로 실행한다."""

    dialect = "oracle"
    tool = "oracle"

    def __init__(self, cfg: OracleClientSettings, allow_write: bool = False) -> None:
        self.cfg = cfg
        self.allow_write = allow_write
        self._conn: oracledb.Connection | None = None

    def _connection(self) -> oracledb.Connection:
        """연결을 돌려준다. 없으면 맺고 call_timeout·current_schema를 적용한다."""
        if self._conn is None:
            try:
                conn = oracledb.connect(user=self.cfg.user, password=self.cfg.password, dsn=self.cfg.dsn)
            except oracledb.Error as exc:
                raise ConnectError(f"Oracle 연결 실패({self.cfg.dsn}): {error_message(exc)}") from None
            conn.call_timeout = int(self.cfg.call_timeout.total_seconds() * 1000)
            conn.autocommit = False
            if self.cfg.current_schema:
                conn.current_schema = self.cfg.current_schema
            self._conn = conn
        return self._conn

    def connect(self) -> None:
        """연결이 없으면 맺는다."""
        self._connection()

    def _run(self, sql: str, params: dict[str, Any] | None, max_rows: int) -> ResultSet:
        """실행하고 행을 읽는다. 읽기 전용이면 READ ONLY 트랜잭션 안에서 실행하고 끝에 ROLLBACK."""
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.arraysize = self.cfg.arraysize
                if not self.allow_write:
                    cur.execute("SET TRANSACTION READ ONLY")
                try:
                    cur.execute(sql, params or {})
                    if cur.description is None:
                        return ResultSet(rowcount=cur.rowcount)
                    columns = [d[0] for d in cur.description]
                    rows = cur.fetchmany(max_rows + 1) if max_rows else cur.fetchall()
                    truncated = bool(max_rows) and len(rows) > max_rows
                    return ResultSet(columns, [tuple(r) for r in rows[:max_rows or None]], truncated)
                finally:
                    if not self.allow_write:
                        conn.rollback()
        except oracledb.Error as exc:
            raise QueryError(error_message(exc)) from None

    def execute(self, sql: str, max_rows: int) -> ResultSet:
        """SQL 문장 하나. `DESC 이름`은 SQL*Plus처럼 \\d로 처리한다."""
        if match := _DESC.match(sql):
            return self.describe(match.group(2), False)
        return self._run(sql, None, max_rows)

    def cancel(self) -> None:
        """진행 중인 호출을 끊고 연결을 버린다. 다음 문장에서 다시 연결한다."""
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.cancel()
                conn.close()
            except Exception:  # 취소된 연결은 close에서 ORA-01013·StopIteration 등을 낸다. 버리면 된다
                pass

    def close(self) -> None:
        """연결을 닫는다. 쓰기 모드의 커밋되지 않은 변경은 버린다."""
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.rollback()
                conn.close()
            except Exception:  # 이미 끊긴 연결
                pass

    def list_tables(self, pattern: str | None) -> ResultSet:
        """테이블·뷰·materialized view·synonym 목록. 스키마를 생략하면 Oracle 관리 계정 밖 전체."""
        owner, name = split_name(pattern) if pattern else (None, None)
        return self._run(_TABLES_SQL, {"owner": _like(owner), "name": _like(name)}, 0)

    def describe(self, name: str, verbose: bool) -> ResultSet:
        """열 목록. 스키마를 생략했는데 여러 스키마에 같은 이름이 있으면 스키마를 붙이라고 알린다."""
        owner, table = split_name(name)
        if owner is None and self.cfg.current_schema:
            owner = self.cfg.current_schema.upper()
        raw = self._run(_COLUMNS_SQL, {"owner": owner, "name": table}, 0)
        rows = [dict(zip(raw.columns, r, strict=True)) for r in raw.rows]
        owners = sorted({r["OWNER"] for r in rows})
        if not owners:
            raise QueryError(f"테이블·뷰를 찾을 수 없습니다: {name}")
        if len(owners) > 1:
            raise QueryError(f"여러 스키마에 있습니다({', '.join(owners)}). 스키마.{table} 형식으로 지정한다")
        columns = ["#", "column", "type", "null"]
        if verbose:
            columns += ["default", "comment"]
        out = []
        for r in rows:
            row: list[Any] = [r["COLUMN_ID"], r["COLUMN_NAME"], column_type(r),
                              "" if r["NULLABLE"] == "Y" else "NOT NULL"]
            if verbose:
                row += [(r["DATA_DEFAULT"] or "").strip() or None, r["COMMENTS"]]
            out.append(tuple(row))
        return ResultSet(columns, out)

    def list_schemas(self, pattern: str | None) -> ResultSet:
        """Oracle이 관리하지 않는 사용자(스키마) 목록."""
        return self._run(_SCHEMAS_SQL, {"name": _like(pattern.upper() if pattern else None)}, 0)

    def current_scn(self, args: list[str]) -> ResultSet:
        """\\scn: 현재 SCN. v$database 권한이 없으면 TIMESTAMP_TO_SCN(SYSTIMESTAMP)로 근사한다."""
        try:
            return self._run("SELECT current_scn FROM v$database", None, 0)
        except QueryError:
            return self._run("SELECT TIMESTAMP_TO_SCN(SYSTIMESTAMP) AS current_scn FROM dual", None, 0)

    def extra_meta(self) -> dict[str, tuple[str, MetaHandler]]:
        """Oracle 전용 \\scn."""
        return {"scn": ("\\scn                현재 SCN(AS OF SCN 조회와 비교할 때)", self.current_scn)}

    def conninfo(self) -> str:
        """접속 대상과 모드. 연결 전이면 서버 버전을 빼고 보여 준다."""
        version = f", Oracle {self._conn.version}" if self._conn is not None else ""
        mode = "쓰기(autocommit 끔, COMMIT 필요)" if self.allow_write else "읽기 전용"
        return f"oracle {self.cfg.user}@{self.cfg.dsn}{version}, {mode}"

    def prompt(self) -> str:
        """oracle:<사용자>."""
        return f"oracle:{self.cfg.user}"
