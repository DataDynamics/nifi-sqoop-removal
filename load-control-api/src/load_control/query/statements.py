"""SQL 입력을 문장 단위로 나누고, 읽기 전용 모드에서 실행해도 되는 문장인지 판정한다.

나누기: psql처럼 따옴표·주석 밖의 `;`에서 문장이 끝난다. Oracle은 SQL*Plus처럼 PL/SQL 블록
(BEGIN, DECLARE, CREATE PROCEDURE 등) 안의 `;`로는 끝나지 않고 `/`만 있는 줄에서 끝난다.
`/`만 있는 줄은 Oracle에서 일반 문장도 끝낸다. 돌려주는 문장에는 끝의 `;`를 붙이지 않는다
(oracledb는 SQL 끝의 `;`를 오류로 본다). PL/SQL 블록은 `END;`까지 그대로 둔다.

읽기 전용 판정은 첫 키워드와 금지 키워드로 한다. 문자열·주석·따옴표 식별자는 공백으로 가린 뒤(mask_code)
단어를 보므로 `WHERE note = 'delete'` 같은 값에는 걸리지 않는다. Oracle은 이 검사와 별도로
`SET TRANSACTION READ ONLY` 트랜잭션 안에서 실행해 DB가 다시 한 번 막는다(oracle.py).
"""

import re
from typing import Literal

Dialect = Literal["oracle", "hive"]

# 읽기 전용 모드에서 허용하는 첫 키워드. Oracle의 DESC는 SQL이 아니라 도구가 \d로 바꿔 처리한다.
_READ_FIRST: dict[str, frozenset[str]] = {
    "oracle": frozenset({"SELECT", "WITH", "DESC", "DESCRIBE"}),
    # Hive의 SET·RESET·USE는 세션에만 영향을 준다. FROM은 `FROM t SELECT ...` 형식 때문에 허용하되
    # `FROM t INSERT ...`(multi-insert)는 아래 금지 키워드로 막는다.
    "hive": frozenset({"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "USE", "SET", "RESET",
                       "FROM", "VALUES"}),
}
# 첫 키워드가 허용이어도 문장 어디에든 있으면 거부하는 단어. WITH ... INSERT, FROM ... INSERT,
# EXPLAIN ANALYZE INSERT(Hive는 EXPLAIN ANALYZE가 실제로 실행한다) 같은 경우를 막는다.
_WRITE_WORDS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "TRUNCATE", "DROP", "CREATE",
                          "ALTER", "GRANT", "REVOKE", "LOAD", "MSCK", "EXPORT", "IMPORT", "LOCK"})
# 금지 키워드를 검사할 첫 키워드. SHOW CREATE TABLE, DESCRIBE, SET 값 등은 단어에 CREATE가 있어도 읽기다.
_QUERY_FIRST = frozenset({"SELECT", "WITH", "FROM", "EXPLAIN", "VALUES"})
# PL/SQL 블록 시작. 이 문장들은 `;`가 아닌 `/` 줄에서 끝난다.
_PLSQL_START = re.compile(
    r"^\s*(BEGIN|DECLARE|CREATE\s+(OR\s+REPLACE\s+)?"
    r"((NON)?EDITIONABLE\s+)?(PROCEDURE|FUNCTION|PACKAGE|TRIGGER|TYPE)\b)", re.IGNORECASE)
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$#]*")


def mask_code(sql: str, backslash: bool = False) -> str:
    """문자열 상수·따옴표 식별자·주석을 공백으로 바꾼 같은 길이의 SQL. 키워드·`;` 위치 검사용이다.

    줄바꿈은 남겨 위치가 원문과 같다. 끝나지 않은 따옴표·주석은 끝까지 지운다(그 안의 `;`는 문장 끝이
    아니다). backslash가 참이면 따옴표 안의 역슬래시를 이스케이프로 본다(Hive). Oracle은 역슬래시가
    일반 문자이고 따옴표를 두 번 써서(`''`) 이스케이프한다. 두 방언 모두 `''` 이스케이프를 처리한다.
    """
    out = list(sql)
    i, n = 0, len(sql)

    def blank(a: int, b: int) -> None:
        for k in range(a, min(b, n)):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        two = sql[i:i + 2]
        ch = sql[i]
        if two == "--":
            end = sql.find("\n", i)
            end = n if end < 0 else end
            blank(i, end)
            i = end
        elif two == "/*":
            end = sql.find("*/", i + 2)
            end = n if end < 0 else end + 2
            blank(i, end)
            i = end
        elif ch in "'\"`":
            j = i + 1
            while j < n:
                if sql[j] == ch:
                    if sql[j + 1:j + 2] == ch:  # '' 또는 "" 이스케이프
                        j += 2
                        continue
                    break
                if backslash and sql[j] == "\\":
                    j += 1
                j += 1
            blank(i, j + 1)
            i = j + 1
        else:
            i += 1
    return "".join(out)


def write_reason(sql: str, dialect: Dialect) -> str | None:
    """읽기 전용 모드에서 거부할 이유. 실행해도 되면 None.

    첫 키워드가 허용 목록에 없거나, 문장 안에 쓰기 키워드가 있거나, Oracle `SELECT ... FOR UPDATE`
    (행 잠금)이면 거부한다. 판정은 보수적이라 열 이름이 정확히 `DELETE` 같은 드문 경우도 거부한다.
    """
    code = mask_code(sql, backslash=dialect == "hive")
    words = [w.upper() for w in _WORD.findall(code)]
    if not words:
        return None
    if words[0] not in _READ_FIRST[dialect]:
        return f"{words[0]} 문장"
    found = sorted(set(words[1:]) & _WRITE_WORDS)
    if found and words[0] in _QUERY_FIRST:
        return f"쓰기 키워드 {', '.join(found)}"
    if dialect == "oracle" and re.search(r"\bFOR\s+UPDATE\b", code, re.IGNORECASE):
        return "SELECT ... FOR UPDATE(행 잠금)"
    return None


class StatementBuffer:
    """줄 단위로 입력을 받아 완성된 문장을 돌려준다. 대화형 입력과 -c·-f·stdin이 함께 쓴다.

    따옴표·블록 주석이 줄을 넘어가도 상태를 이어서 본다. pending이 있으면 대화형 프롬프트가
    `->`(이어 쓰는 중)로 바뀐다.
    """

    def __init__(self, dialect: Dialect) -> None:
        self.dialect = dialect
        self._backslash = dialect == "hive"
        self._buf = ""

    @property
    def pending(self) -> bool:
        """아직 끝나지 않은 문장이 있는지(공백·주석만 있으면 없다고 본다)."""
        return bool(mask_code(self._buf, self._backslash).strip())

    def reset(self) -> None:
        """입력 중인 문장을 버린다(대화형에서 Ctrl-C)."""
        self._buf = ""

    def _plsql(self) -> bool:
        """현재 버퍼가 `/` 줄로만 끝나는 Oracle PL/SQL 블록인지."""
        return self.dialect == "oracle" and bool(_PLSQL_START.match(mask_code(self._buf, self._backslash)))

    def add_line(self, line: str) -> list[str]:
        """한 줄을 더하고 이 줄로 완성된 문장 목록을 돌려준다(없으면 빈 목록)."""
        if self.dialect == "oracle" and line.strip() == "/":
            stmt = self._take(len(self._buf))
            return [stmt] if stmt else []
        self._buf += line if line.endswith("\n") else line + "\n"
        if self._plsql():
            return []
        done: list[str] = []
        while (cut := self._find_terminator()) is not None:
            stmt = self._take(cut, skip=1)
            if stmt:
                done.append(stmt)
            if self._plsql():  # 남은 부분이 PL/SQL 블록으로 시작하면 `/`를 기다린다
                break
        return done

    def flush(self) -> str | None:
        """입력이 끝났을 때 `;` 없이 남은 문장(-c 'SELECT 1'처럼). 없으면 None."""
        stmt = self._take(len(self._buf))
        return stmt or None

    def _take(self, cut: int, skip: int = 0) -> str:
        """버퍼 앞 cut 글자를 문장으로 떼어 내고 skip 글자(;)를 버린다. 주석만 있으면 빈 문자열."""
        stmt, self._buf = self._buf[:cut], self._buf[cut + skip:]
        if not mask_code(stmt, self._backslash).strip():
            return ""
        stmt = stmt.strip()
        if not self._is_plsql_text(stmt):
            stmt = stmt.rstrip(";").rstrip()
        if not self._buf.strip():
            self._buf = ""
        return stmt

    def _is_plsql_text(self, stmt: str) -> bool:
        """PL/SQL 블록이면 끝의 END; 를 지우지 않는다."""
        return self.dialect == "oracle" and bool(_PLSQL_START.match(mask_code(stmt, self._backslash)))

    def _find_terminator(self) -> int | None:
        """따옴표·주석 밖의 첫 `;` 위치. 닫히지 않은 따옴표 안의 `;`는 세지 않는다."""
        cut = mask_code(self._buf, self._backslash).find(";")
        return None if cut < 0 else cut
