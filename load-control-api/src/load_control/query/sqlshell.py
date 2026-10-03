"""psql과 비슷한 SQL 셸. Oracle·Hive 공통이고 DB별 차이는 SqlBackend 구현(oracle.py, hive.py)에 둔다.

입력 경로는 세 가지이며 모두 StatementBuffer로 문장을 나눈다.
- 대화형: 터미널에서 인자 없이 실행. 줄 편집·이력, 문장이 끝나지 않았으면 `->` 프롬프트
- -c SQL(여러 번 가능), -f 파일: 차례로 실행하고 첫 오류에서 멈춘다
- 표준입력 파이프: -f -와 같다

백슬래시 메타 명령(\\dt, \\d, \\x 등)은 문장 입력 중이 아닐 때 줄 첫머리에서만 받는다.
읽기 전용(기본)에서는 write_reason이 거부한 문장을 실행하지 않고 종료 코드 3으로 표시한다.

종료 코드: 0 성공, 1 실행 오류, 2 사용법·접속 오류, 3 읽기 전용 거부, 130 Ctrl-C로 취소.
"""

import shlex
import sys
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import TextIO

from load_control.query.console import read_line, setup_history
from load_control.query.output import FORMATS, ResultSet, render
from load_control.query.statements import Dialect, StatementBuffer, write_reason

EXIT_OK, EXIT_ERROR, EXIT_USAGE, EXIT_REFUSED, EXIT_INTERRUPTED = 0, 1, 2, 3, 130
_HUMAN = ("table", "vertical")  # 행 수·시간 같은 안내를 결과와 같은 출력에 붙이는 형식


class QueryError(Exception):
    """사용자에게 그대로 보여 줄 오류(드라이버 오류를 정리한 메시지, 찾을 수 없는 테이블 등)."""


MetaHandler = Callable[[list[str]], ResultSet | str | None]


class SqlBackend(ABC):
    """DB별 연결과 실행. 연결은 처음 실행할 때 맺고, 끊기면(Ctrl-C 취소 등) 다음 실행에서 다시 맺는다."""

    dialect: Dialect
    tool: str  # 이력 파일 이름 등에 쓰는 도구 이름(oracle, hive)

    @abstractmethod
    def connect(self) -> None:
        """연결이 없으면 맺는다. 실패하면 QueryError. 대화형 시작 때 접속 정보를 미리 확인하는 데 쓴다."""

    @abstractmethod
    def execute(self, sql: str, max_rows: int) -> ResultSet:
        """문장 하나를 실행한다. 행은 max_rows(0이면 제한 없음)까지만 읽고 truncated를 표시한다.

        드라이버 오류는 QueryError로 바꿔 낸다.
        """

    @abstractmethod
    def cancel(self) -> None:
        """실행 중인 문장을 취소한다(Ctrl-C). 연결 상태를 알 수 없으므로 닫고 다음 실행에서 다시 맺는다."""

    @abstractmethod
    def close(self) -> None:
        """연결을 닫는다. 쓰기 모드의 커밋되지 않은 변경은 버린다(rollback)."""

    @abstractmethod
    def list_tables(self, pattern: str | None) -> ResultSet:
        """\\dt: 테이블·뷰 목록. pattern은 `스키마.이름`, `이름` 형식이며 `*`를 와일드카드로 쓴다."""

    @abstractmethod
    def describe(self, name: str, verbose: bool) -> ResultSet:
        """\\d 이름: 열 목록. verbose(\\d+)면 더 자세히."""

    @abstractmethod
    def list_schemas(self, pattern: str | None) -> ResultSet:
        """\\dn: 스키마(Oracle 사용자, Hive database) 목록."""

    @abstractmethod
    def conninfo(self) -> str:
        """\\conninfo: 접속 대상·사용자·모드 한 줄."""

    @abstractmethod
    def prompt(self) -> str:
        """대화형 프롬프트 앞부분(예: oracle:NIFI_READER, hive:stg)."""

    def extra_meta(self) -> dict[str, tuple[str, MetaHandler]]:
        """DB 전용 메타 명령 {이름: (도움말, 처리 함수)}. 예: Oracle \\scn."""
        return {}


class SqlShell:
    """입력을 받아 SqlBackend로 실행하고 결과를 출력한다. status에 종료 코드를 모은다."""

    def __init__(self, backend: SqlBackend, *, out: TextIO | None = None, err: TextIO | None = None,
                 fmt: str = "table", header: bool = True, max_rows: int = 1000, allow_write: bool = False,
                 timing: bool = False, null: str = "", echo: bool = False) -> None:
        self.backend = backend
        self.out = out or sys.stdout
        self.err = err or sys.stderr
        self._stdout = self.out  # \o 로 바꾼 출력을 되돌릴 대상
        self.fmt = fmt
        self.header = header
        self.max_rows = max_rows
        self.allow_write = allow_write
        self.timing = timing
        self.null = null
        self.echo = echo
        self.status = EXIT_OK
        self._depth = 0  # \i 중첩 깊이
        self._errors = 0  # 지금까지 낸 오류 수. 메타 명령 안에서 오류가 났는지 판단한다
        self._meta: dict[str, tuple[str, MetaHandler]] = {
            "dt": ("\\dt [패턴]          테이블·뷰 목록(예: \\dt APP.INSP*)", self._m_dt),
            "d": ("\\d 이름             열 목록(DESC와 같다)", lambda a: self._m_d(a, False)),
            "d+": ("\\d+ 이름            열 목록 자세히", lambda a: self._m_d(a, True)),
            "dn": ("\\dn [패턴]          스키마·database 목록", self._m_dn),
            "conninfo": ("\\conninfo           접속 정보", lambda a: self.backend.conninfo()),
            "x": ("\\x [on|off]         확장 출력(행 하나를 세로로) 전환", self._m_x),
            "format": (f"\\format 형식        출력 형식: {', '.join(FORMATS)}", self._m_format),
            "t": ("\\t [on|off]         열 이름 머리글 끄기·켜기", self._m_t),
            "timing": ("\\timing [on|off]    실행 시간 표시", self._m_timing),
            "maxrows": ("\\maxrows N         최대 출력 행 수(0이면 제한 없음)", self._m_maxrows),
            "o": ("\\o [파일]           결과를 파일로 쓴다. 인자 없으면 화면으로 되돌린다", self._m_o),
            "i": ("\\i 파일             파일의 SQL을 실행한다", self._m_i),
            "?": ("\\?                  이 도움말", self._m_help),
            "q": ("\\q                  종료(Ctrl-D도 같다)", lambda a: None),
        }
        self._meta.update(backend.extra_meta())

    # ---- 실행 -------------------------------------------------------------------------------------

    def execute(self, sql: str) -> bool:
        """문장 하나를 실행하고 출력한다. 계속 진행해도 되면 True(일괄 실행은 False에서 멈춘다)."""
        if self.echo:
            print(sql + ";", file=self.out)
        if not self.allow_write and (reason := write_reason(sql, self.backend.dialect)):
            self._error(f"읽기 전용 모드라 실행하지 않습니다({reason}). 쓰기가 필요하면 --write로 실행한다",
                        EXIT_REFUSED)
            return False
        started = time.monotonic()
        try:
            result = self.backend.execute(sql, self.max_rows)
        except QueryError as exc:
            self._error(str(exc), EXIT_ERROR)
            return False
        except KeyboardInterrupt:
            self.backend.cancel()
            self._error("취소했습니다(연결은 다음 문장에서 다시 맺습니다)", EXIT_INTERRUPTED)
            return False
        self._show(result, time.monotonic() - started)
        return True

    def _show(self, result: ResultSet, elapsed: float) -> None:
        """결과와 안내(행 수, 잘림, 시간)를 출력한다. csv·tsv·json은 안내를 표준오류로 보낸다."""
        human = self.fmt in _HUMAN
        if result.columns:
            text = render(result, self.fmt, self.header, self.null)
            if text:
                print(text, file=self.out)
            n = len(result.rows)
            cut = f"처음 {n}행만 출력했습니다(--max-rows 또는 \\maxrows로 조정, 0이면 제한 없음)"
            if self.fmt == "table" or (human and n == 0):
                print(f"({n}행)", file=self.out)
            if result.truncated:
                self._note(cut, to_err=not human)
        else:
            self._note("완료" + (f": {result.rowcount}행 처리" if result.rowcount >= 0 else ""))
        if self.timing:
            self._note(f"시간: {elapsed * 1000:.1f} ms")

    def _note(self, msg: str, to_err: bool = False) -> None:
        """안내 한 줄. 사람용 형식이면 결과와 같은 출력, 아니면 표준오류(파이프 결과를 깨끗하게 둔다)."""
        stream = self.err if to_err or self.fmt not in _HUMAN else self.out
        if stream is self.err:
            self.out.flush()  # 결과보다 안내가 먼저 보이지 않게
        print(msg, file=stream)

    def _error(self, msg: str, code: int) -> None:
        """오류를 표준오류에 쓰고 종료 코드를 기록한다. 먼저 난 더 심각한 코드를 덮지 않는다."""
        self.out.flush()
        print(f"오류: {msg}", file=self.err)
        self._errors += 1
        if self.status in (EXIT_OK, EXIT_REFUSED) or code == EXIT_INTERRUPTED:
            self.status = code

    # ---- 입력 -------------------------------------------------------------------------------------

    def run_text(self, text: str) -> bool:
        """여러 줄 텍스트(-c, -f, 표준입력)를 실행한다. 오류가 나면 그 자리에서 멈추고 False."""
        buf = StatementBuffer(self.backend.dialect)
        for line in text.splitlines():
            if not buf.pending and line.lstrip().startswith("\\"):
                keep = self.meta(line.strip())
                if keep is None:
                    return False  # 오류
                if not keep:
                    return True  # \q
                continue
            for stmt in buf.add_line(line):
                if not self.execute(stmt):
                    return False
        tail = buf.flush()
        return self.execute(tail) if tail else True

    def run_file(self, path: str) -> bool:
        """-f 파일 또는 \\i. `-`는 표준입력."""
        try:
            text = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            self._error(f"파일을 읽을 수 없습니다: {exc}", EXIT_USAGE)
            return False
        return self.run_text(text)

    def interactive(self) -> None:
        """대화형 루프. Ctrl-C는 입력 중인 문장을 버리고, Ctrl-D·\\q는 끝낸다. 오류가 나도 계속한다."""
        setup_history(self.backend.tool)
        print(f"{self.backend.conninfo()}\n도움말 \\?, 종료 \\q", file=self.out)
        buf = StatementBuffer(self.backend.dialect)
        mode = "[write]" if self.allow_write else ""
        while True:
            prompt = f"{self.backend.prompt()}{mode}{'->' if buf.pending else '=>'} "
            try:
                line = read_line(prompt)
            except KeyboardInterrupt:
                print(file=self.out)
                buf.reset()
                continue
            if line is None:
                return
            if not buf.pending and line.lstrip().startswith("\\"):
                if self.meta(line.strip()) is False:
                    return
                continue
            for stmt in buf.add_line(line):
                self.execute(stmt)
            self.status = EXIT_OK  # 대화형은 지난 오류로 종료 코드를 바꾸지 않는다

    # ---- 메타 명령 ---------------------------------------------------------------------------------

    def meta(self, line: str) -> bool | None:
        """백슬래시 명령을 처리한다. True 계속, False 종료(\\q), None 오류."""
        try:
            parts = shlex.split(line[1:])
        except ValueError as exc:
            self._error(f"명령을 해석할 수 없습니다: {exc}", EXIT_USAGE)
            return None
        if not parts:
            return True
        name, args = parts[0], parts[1:]
        if name == "q":
            return False
        entry = self._meta.get(name)
        if entry is None:
            self._error(f"알 수 없는 명령 \\{name} (\\? 로 목록을 본다)", EXIT_USAGE)
            return None
        started = time.monotonic()
        errors = self._errors
        try:
            result = entry[1](args)
        except QueryError as exc:
            self._error(str(exc), EXIT_ERROR)
            return None
        except KeyboardInterrupt:
            self.backend.cancel()
            self._error("취소했습니다", EXIT_INTERRUPTED)
            return None
        if isinstance(result, ResultSet):
            self._show(result, time.monotonic() - started)
        elif isinstance(result, str):
            print(result, file=self.out)
        return None if self._errors > errors else True  # \i 안의 문장이 실패해도 오류로 본다

    def _m_dt(self, args: list[str]) -> ResultSet:
        return self.backend.list_tables(args[0] if args else None)

    def _m_d(self, args: list[str], verbose: bool) -> ResultSet | str:
        if not args:
            return self.backend.list_tables(None)
        return self.backend.describe(args[0], verbose)

    def _m_dn(self, args: list[str]) -> ResultSet:
        return self.backend.list_schemas(args[0] if args else None)

    def _m_x(self, args: list[str]) -> str:
        on = _toggle(args, self.fmt == "vertical")
        self.fmt = "vertical" if on else ("table" if self.fmt == "vertical" else self.fmt)
        return f"확장 출력 {'켬' if on else '끔'}"

    def _m_format(self, args: list[str]) -> str:
        if not args or args[0] not in FORMATS:
            raise QueryError(f"형식: {', '.join(FORMATS)} (현재 {self.fmt})")
        self.fmt = args[0]
        return f"출력 형식 {self.fmt}"

    def _m_t(self, args: list[str]) -> str:
        self.header = not _toggle(args, not self.header)
        return f"머리글 {'켬' if self.header else '끔'}"

    def _m_timing(self, args: list[str]) -> str:
        self.timing = _toggle(args, self.timing)
        return f"시간 표시 {'켬' if self.timing else '끔'}"

    def _m_maxrows(self, args: list[str]) -> str:
        try:
            value = int(args[0])
            if value < 0:
                raise ValueError
        except (IndexError, ValueError):
            raise QueryError(f"\\maxrows N (0 이상, 현재 {self.max_rows})") from None
        self.max_rows = value
        return f"최대 출력 행 수 {value or '제한 없음'}"

    def _m_o(self, args: list[str]) -> str | None:
        if self.out is not self._stdout:
            self.out.close()
        if not args:
            self.out = self._stdout
            return None
        try:
            self.out = open(args[0], "w", encoding="utf-8")
        except OSError as exc:
            self.out = self._stdout
            raise QueryError(f"파일을 열 수 없습니다: {exc}") from None
        return None

    def _m_i(self, args: list[str]) -> None:
        if not args:
            raise QueryError("\\i 파일")
        if self._depth >= 10:
            raise QueryError("\\i 중첩이 너무 깊습니다")
        self._depth += 1
        try:
            self.run_file(args[0])  # 실패하면 run_file이 오류를 출력하고 그 자리에서 멈춘다
        finally:
            self._depth -= 1

    def _m_help(self, args: list[str]) -> str:
        lines = [help_text for help_text, _ in self._meta.values()]
        plsql = " (PL/SQL 블록은 / 줄로 끝낸다)" if self.backend.dialect == "oracle" else ""
        lines.append("SQL은 ;로 끝낸다" + plsql)
        return "\n".join(lines)


def _toggle(args: list[str], current: bool) -> bool:
    """on/off 인자가 있으면 그 값, 없으면 현재 값을 뒤집는다."""
    if args and args[0].lower() in ("on", "off"):
        return args[0].lower() == "on"
    if args:
        raise QueryError("on 또는 off")
    return not current
