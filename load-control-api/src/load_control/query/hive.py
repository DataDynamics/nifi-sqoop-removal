"""bin/hive.sh의 HiveServer2 연결과 실행(impyla, Thrift).

beeline(JVM, Hive 클라이언트 설치) 없이 HS2 Thrift 포트로 접속한다. 인증은 이 환경 기준인 NONE(SASL PLAIN,
user만 보내고 비밀번호는 검사하지 않음)과 NOSASL만 지원한다.

Hive에는 읽기 전용 트랜잭션이 없으므로 읽기 전용 모드는 statements.write_reason 검사로만 지킨다.
문장마다 hive.query.timeout.seconds를 confOverlay로 보내 오래 걸리는 조회를 서버가 끊게 한다.
"""

import fnmatch
import re
from typing import Any

from impala.dbapi import connect
from impala.error import Error as ImpalaError
from thrift.Thrift import TException

from load_control.config import HiveClientSettings
from load_control.query.output import ResultSet
from load_control.query.sqlshell import QueryError, SqlBackend

_USE = re.compile(r"^\s*USE\s+`?([A-Za-z0-9_]+)`?\s*$", re.IGNORECASE)
_NAME = re.compile(r"^`?[A-Za-z0-9_*?]+`?(\.`?[A-Za-z0-9_*?]+`?)?$")
_DRIVER_ERRORS = (ImpalaError, TException, OSError, EOFError)


def _match(name: str, pattern: str | None) -> bool:
    """대소문자를 무시한 glob 일치. pattern이 없으면 모두."""
    return pattern is None or fnmatch.fnmatch(name.lower(), pattern.replace("`", "").lower())


def error_message(exc: BaseException) -> str:
    """HS2 오류에서 Java 스택을 빼고 핵심 메시지만 남긴다(첫 줄, 최대 1000자)."""
    text = str(exc).strip() or exc.__class__.__name__
    first = text.splitlines()[0]
    return first[:1000]


class HiveBackend(SqlBackend):
    """impyla 연결 하나로 문장을 차례로 실행한다. 현재 database는 USE 결과로 따라간다."""

    dialect = "hive"
    tool = "hive"

    def __init__(self, cfg: HiveClientSettings, allow_write: bool = False) -> None:
        self.cfg = cfg
        self.allow_write = allow_write
        self.database = cfg.database
        self._conn: Any = None
        self._cur: Any = None

    def connect(self) -> None:
        """연결이 없으면 맺는다. 다시 연결할 때는 마지막 USE한 database로 들어간다."""
        if self._conn is not None:
            return
        try:
            conn = connect(host=self.cfg.host, port=self.cfg.port, database=self.database,
                           auth_mechanism="PLAIN" if self.cfg.auth == "NONE" else "NOSASL",
                           user=self.cfg.user, password=self.cfg.password or "unused",
                           use_http_transport=self.cfg.transport == "http", http_path=self.cfg.http_path,
                           timeout=self.cfg.connect_timeout)
            cur = conn.cursor()
        except _DRIVER_ERRORS as exc:
            raise QueryError(f"HiveServer2 연결 실패({self.cfg.host}:{self.cfg.port}): "
                             f"{error_message(exc)}") from None
        self._conn, self._cur = conn, cur

    def _run(self, sql: str, max_rows: int) -> ResultSet:
        """실행하고 행을 읽는다. 결과가 없는 문장은 rowcount만."""
        self.connect()
        cur = self._cur
        overlay = {"hive.query.timeout.seconds": str(int(self.cfg.query_timeout.total_seconds()))}
        try:
            cur.execute(sql, configuration=overlay)
            if not cur.description:
                return ResultSet(rowcount=-1)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(max_rows + 1) if max_rows else cur.fetchall()
            truncated = bool(max_rows) and len(rows) > max_rows
            if truncated:
                cur.close_operation()  # 나머지 결과를 서버에서 버린다
            return ResultSet(columns, [tuple(r) for r in rows[:max_rows or None]], truncated)
        except _DRIVER_ERRORS as exc:
            raise QueryError(error_message(exc)) from None

    def execute(self, sql: str, max_rows: int) -> ResultSet:
        """문장 하나. USE가 성공하면 프롬프트·재연결에 쓸 현재 database를 바꾼다."""
        result = self._run(sql, max_rows)
        if match := _USE.match(sql):
            self.database = match.group(1).lower()
        return result

    def cancel(self) -> None:
        """실행 중인 operation을 취소하고 연결을 버린다. 다음 문장에서 다시 연결한다."""
        conn, cur, self._conn, self._cur = self._conn, self._cur, None, None
        if conn is None:
            return
        try:
            cur.cancel_operation()
        except Exception:  # 끊긴 연결 정리 중 오류는 무시한다
            pass
        try:
            conn.close()
        except Exception:
            pass

    def close(self) -> None:
        """연결을 닫는다."""
        conn, self._conn, self._cur = self._conn, None, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _check_name(self, name: str) -> str:
        """메타 명령 인자로 받은 이름을 SQL에 넣기 전에 형식을 확인한다."""
        if not _NAME.match(name):
            raise QueryError(f"이름 형식이 아닙니다: {name}")
        return name

    def list_tables(self, pattern: str | None) -> ResultSet:
        """SHOW TABLES [IN db] 뒤 이름을 glob(`*`, `?`)으로 거른다.

        LIKE 패턴 문법이 Hive 3(`*`, `|`)과 Hive 4(SQL `%`, `_`)에서 달라 서버에 넘기지 않는다.
        """
        db, name = None, pattern
        if pattern and "." in pattern:
            db, name = self._check_name(pattern).replace("`", "").split(".", 1)
        result = self._run("SHOW TABLES" + (f" IN {db}" if db else ""), 0)
        target = db or self.database
        rows = [(target, r[0]) for r in result.rows if _match(r[0], name)]
        return ResultSet(["database", "table"], rows)

    def describe(self, name: str, verbose: bool) -> ResultSet:
        """DESCRIBE [FORMATTED] 이름. FORMATTED는 위치·형식·파티션·통계까지 보여 준다."""
        return self._run(f"DESCRIBE {'FORMATTED ' if verbose else ''}{self._check_name(name)}", 0)

    def list_schemas(self, pattern: str | None) -> ResultSet:
        """SHOW DATABASES 뒤 glob으로 거른다(list_tables와 같은 이유)."""
        result = self._run("SHOW DATABASES", 0)
        return ResultSet(result.columns, [r for r in result.rows if _match(r[0], pattern)])

    def conninfo(self) -> str:
        """접속 대상과 모드."""
        mode = "쓰기 허용" if self.allow_write else "읽기 전용"
        return (f"hive {self.cfg.user}@{self.cfg.host}:{self.cfg.port}/{self.database} "
                f"({self.cfg.transport}, auth {self.cfg.auth}), {mode}")

    def prompt(self) -> str:
        """hive:<현재 database>."""
        return f"hive:{self.database}"
