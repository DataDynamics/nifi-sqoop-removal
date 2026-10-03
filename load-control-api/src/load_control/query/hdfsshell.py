"""bin/hdfs.sh 명령 셸. `hdfs dfs`와 비슷한 명령을 WebHDFS로 실행한다.

실행 방법은 SQL 셸과 같다: 인자로 명령 하나(`bin/hdfs.sh ls -h /data`), -c(여러 번), -f 파일,
표준입력 파이프, 인자 없이 터미널이면 대화형(cd로 옮겨 다니는 현재 디렉터리가 있다).

경로는 현재 디렉터리(처음은 clients.hdfs.home) 기준 상대 경로도 받고, 마지막이 아닌 조각에도
`*`, `?`, `[...]` glob을 쓸 수 있다(예: du -s /data/nifi/stage/*/run=*).

읽기 전용(기본)에서는 mkdir·rm·mv·put·chmod를 실행하지 않고 종료 코드 3을 낸다. --write를 줘도 `/`나
`/data`처럼 깊이 2 미만인 경로는 지우거나 옮기지 않는다. Parquet 같은 바이너리 파일은 터미널에
그대로 쏟지 않는다(cat -f로 강제하거나 get으로 받는다).

종료 코드: 0 성공, 1 실행 오류, 2 사용법 오류, 3 읽기 전용 거부, 130 Ctrl-C.
"""

import fnmatch
import shlex
import sys
from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, TextIO

from load_control.query.console import read_line, setup_history
from load_control.query.output import FORMATS, ResultSet, render
from load_control.query.sqlshell import (
    EXIT_ERROR,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_REFUSED,
    EXIT_USAGE,
)
from load_control.query.webhdfs import FileStatus, HdfsError, WebHdfs, join, normalize

WRITE_COMMANDS = frozenset({"mkdir", "rm", "mv", "put", "chmod"})
_GLOB_CHARS = set("*?[")
_TYPE = {"DIRECTORY": "d", "SYMLINK": "l"}


class UsageError(Exception):
    """명령 인자가 잘못됐다(종료 코드 2)."""


class Refused(Exception):
    """읽기 전용이거나 보호 경로라 실행하지 않았다(종료 코드 3)."""


def human(n: int) -> str:
    """1024 단위 크기(hdfs dfs -h와 같은 표기: 1.5 K, 12.0 M)."""
    value = float(n)
    for unit in ("", " K", " M", " G", " T", " P"):
        if abs(value) < 1024 or unit == " P":
            return f"{int(value)}" if not unit else f"{value:.1f}{unit}"
        value /= 1024
    return str(n)  # pragma: no cover - 위 루프가 항상 돌려준다


def mode_string(st: FileStatus) -> str:
    """8진수 권한을 drwxr-x--- 형식으로."""
    bits = int(st.permission or "0", 8)
    chars = "".join(ch if bits & (1 << (8 - i)) else "-" for i, ch in enumerate("rwxrwxrwx"))
    return _TYPE.get(st.type, "-") + chars


def mtime(ms: int) -> str:
    """epoch milliseconds를 현지 시각 YYYY-MM-DD HH:MM으로."""
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M") if ms else ""


def parse_opts(args: list[str], flags: str = "", valued: tuple[str, ...] = ()) -> tuple[
        set[str], dict[str, str], list[str]]:
    """명령 인자를 (켜진 플래그, 값 옵션, 위치 인자)로 나눈다. `-lh`처럼 붙인 플래그도 받는다.

    옵션은 위치 인자 뒤에도 올 수 있다(find 경로 -name x). `-`로 시작하는 경로는 `--` 뒤에 둔다.
    valued는 값을 받는 옵션 이름(예: "c", "name", "type")이다. 모르는 옵션이면 UsageError.
    """
    on: set[str] = set()
    values: dict[str, str] = {}
    rest: list[str] = []
    it = iter(args)
    for arg in it:
        if arg == "--":
            rest.extend(it)
            break
        if arg.startswith("-") and len(arg) > 1:
            name = arg.lstrip("-")
            if name in valued:
                try:
                    values[name] = next(it)
                except StopIteration:
                    raise UsageError(f"-{name}에 값이 필요합니다") from None
            elif all(ch in flags for ch in name):
                on.update(name)
            else:
                raise UsageError(f"모르는 옵션: {arg}")
        else:
            rest.append(arg)
    return on, values, rest


class HdfsShell:
    """WebHDFS 명령 셸. status에 종료 코드를 모은다."""

    def __init__(self, fs: WebHdfs, *, home: str = "/", allow_write: bool = False, fmt: str = "table",
                 header: bool = True, out: TextIO | None = None, err: TextIO | None = None,
                 binary_out: BinaryIO | None = None) -> None:
        self.fs = fs
        self.home = normalize(home)
        self.cwd = self.home
        self.allow_write = allow_write
        self.fmt = fmt
        self.header = header
        self.out = out or sys.stdout
        self.err = err or sys.stderr
        self._binary_out = binary_out
        self.status = EXIT_OK
        self._commands: dict[str, tuple[str, Callable[[list[str]], None]]] = {
            "ls": ("ls [-R] [-h] [-d] [경로...]      목록(-d 디렉터리 자체, -R 하위 전체)", self.cmd_ls),
            "cd": ("cd [경로]                        현재 디렉터리 이동(인자 없으면 home)", self.cmd_cd),
            "pwd": ("pwd                              현재 디렉터리", self.cmd_pwd),
            "stat": ("stat 경로...                     파일·디렉터리 상세", self.cmd_stat),
            "du": ("du [-s] [-h] 경로...              크기(-s 합계만)", self.cmd_du),
            "count": ("count [-h] 경로...               디렉터리·파일 수와 크기", self.cmd_count),
            "find": ("find [경로] [-name 패턴] [-type f|d]  하위 검색", self.cmd_find),
            "cat": ("cat [-f] 경로...                 내용 출력(-f 바이너리도 출력)", self.cmd_cat),
            "head": ("head [-c 바이트] [-f] 경로        앞부분(기본 1024바이트)", self.cmd_head),
            "tail": ("tail [-c 바이트] [-f] 경로        끝부분(기본 1024바이트)", self.cmd_tail),
            "get": ("get [-f] 경로 [로컬경로]          로컬로 내려받기(-f 덮어쓰기)", self.cmd_get),
            "mkdir": ("mkdir 경로...                    [쓰기] 디렉터리 생성(상위 포함)", self.cmd_mkdir),
            "rm": ("rm [-r] [-f] 경로...             [쓰기] 삭제(-r 디렉터리, -f 없어도 성공)", self.cmd_rm),
            "mv": ("mv 원본 대상                     [쓰기] 이름 변경·이동", self.cmd_mv),
            "put": ("put [-f] 로컬경로 경로            [쓰기] 올리기(-f 덮어쓰기)", self.cmd_put),
            "chmod": ("chmod 8진수권한 경로...          [쓰기] 권한 변경(예: 750)", self.cmd_chmod),
            "format": (f"format 형식                      출력 형식: {', '.join(FORMATS)}", self.cmd_format),
            "help": ("help                             이 도움말", self.cmd_help),
        }

    # ---- 실행 -------------------------------------------------------------------------------------

    def run(self, argv: list[str]) -> bool:
        """명령 하나(이미 나눈 인자). 성공하면 True. 오류를 출력하고 종료 코드를 기록한다."""
        if not argv:
            return True
        name, args = argv[0], argv[1:]
        entry = self._commands.get(name)
        try:
            if entry is None:
                raise UsageError(f"모르는 명령: {name} (help로 목록을 본다)")
            if name in WRITE_COMMANDS and not self.allow_write:
                raise Refused(f"읽기 전용 모드라 {name}을(를) 실행하지 않습니다. 필요하면 --write로 실행한다")
            entry[1](args)
            return True
        except UsageError as exc:
            self._error(str(exc), EXIT_USAGE)
        except Refused as exc:
            self._error(str(exc), EXIT_REFUSED)
        except HdfsError as exc:
            kind = f"{exc.exception}: " if exc.exception else ""
            # NameNode 접속 실패(exception 없음, status 0)는 접속 오류 2로 구분한다
            self._error(f"{kind}{exc}", EXIT_USAGE if not exc.exception and not exc.status else EXIT_ERROR)
        except OSError as exc:
            self._error(str(exc), EXIT_ERROR)
        except KeyboardInterrupt:
            self._error("취소했습니다", EXIT_INTERRUPTED)
        return False

    def run_line(self, line: str) -> bool | None:
        """한 줄을 명령으로 실행한다. 빈 줄·# 주석은 건너뛴다. exit·quit·\\q면 None(종료)."""
        try:
            argv = shlex.split(line, comments=True)
        except ValueError as exc:
            self._error(f"명령을 해석할 수 없습니다: {exc}", EXIT_USAGE)
            return False
        if argv and argv[0] in ("exit", "quit", "\\q"):
            return None
        return self.run(argv)

    def run_text(self, text: str) -> bool:
        """여러 줄(-c, -f, 표준입력)을 차례로 실행한다. 오류가 나면 멈추고 False."""
        for line in text.splitlines():
            result = self.run_line(line)
            if result is None:
                return True
            if not result:
                return False
        return True

    def interactive(self) -> None:
        """대화형 루프. 오류가 나도 계속한다. Ctrl-D·exit로 끝낸다."""
        setup_history("hdfs")
        mode = "쓰기 허용" if self.allow_write else "읽기 전용"
        print(f"hdfs {self.fs.user}@{self.fs.active_url}, {mode}\n도움말 help, 종료 exit", file=self.out)
        while True:
            mark = "[write]" if self.allow_write else ""
            try:
                line = read_line(f"hdfs:{self.cwd}{mark}> ")
            except KeyboardInterrupt:
                print(file=self.out)
                continue
            if line is None or self.run_line(line) is None:
                return
            self.status = EXIT_OK

    def _error(self, msg: str, code: int) -> None:
        """오류를 표준오류에 쓰고 종료 코드를 기록한다(먼저 난 오류 코드를 유지)."""
        self.out.flush()
        print(f"오류: {msg}", file=self.err)
        if self.status in (EXIT_OK, EXIT_REFUSED) or code == EXIT_INTERRUPTED:
            self.status = code

    def _info(self, msg: str) -> None:
        """진행 안내는 표준오류로(결과 출력과 섞이지 않게). 앞선 결과가 먼저 보이도록 flush한다."""
        self.out.flush()
        print(msg, file=self.err)

    def _print(self, result: ResultSet) -> None:
        """표 출력. 행이 없으면 table 형식에서는 아무것도 쓰지 않는다."""
        if result.rows or self.fmt == "json":
            print(render(result, self.fmt, self.header), file=self.out)

    # ---- 경로 -------------------------------------------------------------------------------------

    def resolve(self, path: str) -> str:
        """현재 디렉터리 기준 절대 경로."""
        return join(self.cwd, path)

    def expand(self, path: str) -> list[str]:
        """glob을 펼친 절대 경로 목록. glob이 없으면 그대로 하나(존재 여부는 보지 않는다).

        glob이 아무것도 고르지 못하면 UsageError(hdfs dfs의 `No such file or directory`와 같다).
        """
        absolute = self.resolve(path)
        if not _GLOB_CHARS & set(absolute):
            return [absolute]
        found = ["/"]
        for part in [p for p in absolute.split("/") if p]:
            nxt: list[str] = []
            for base in found:
                if _GLOB_CHARS & set(part):
                    try:
                        children = self.fs.listdir(base)
                    except HdfsError:
                        continue
                    nxt.extend(c.path for c in children
                               if c.path != base and fnmatch.fnmatchcase(c.path.rsplit("/", 1)[-1], part))
                else:
                    nxt.append(join(base, part))
            found = nxt
        if not found:
            raise UsageError(f"일치하는 경로가 없습니다: {path}")
        return sorted(found)

    def _paths(self, args: list[str], default_cwd: bool = False) -> list[str]:
        """위치 인자들을 펼친다. 없으면 default_cwd일 때 현재 디렉터리, 아니면 UsageError."""
        if not args:
            if default_cwd:
                return [self.cwd]
            raise UsageError("경로가 필요합니다")
        return [p for a in args for p in self.expand(a)]

    def _protect(self, path: str) -> None:
        """깊이 2 미만 경로(/, /data)는 지우거나 옮기지 않는다."""
        if len([p for p in path.split("/") if p]) < 2:
            raise Refused(f"보호 경로라 변경하지 않습니다: {path}")

    # ---- 조회 명령 ---------------------------------------------------------------------------------

    def _ls_row(self, st: FileStatus, hum: bool) -> tuple[object, ...]:
        size: object = human(st.length) if hum else st.length
        repl = "-" if st.is_dir else st.replication
        return (mode_string(st), repl, st.owner, st.group, size, mtime(st.modification_time), st.path)

    def cmd_ls(self, args: list[str]) -> None:
        """ls: 디렉터리면 내용, 파일이면 그 파일. -d는 디렉터리 자체, -R은 하위 전체.

        -l은 hdfs dfs 습관을 위해 받기만 한다(항상 자세히 보여 준다).
        """
        on, _, rest = parse_opts(args, "Rhdl")
        rows: list[tuple[object, ...]] = []
        for path in self._paths(rest, default_cwd=True):
            if "d" in on:
                items = [self.fs.status(path)]
            elif "R" in on:
                items = list(self.fs.walk(path))
            else:
                items = self.fs.listdir(path)
            rows.extend(self._ls_row(s, "h" in on) for s in items)
        self._print(ResultSet(["permission", "repl", "owner", "group", "size", "modified", "path"], rows))

    def cmd_cd(self, args: list[str]) -> None:
        """cd: 디렉터리인지 확인하고 옮긴다."""
        target = self._paths(args)[0] if args else self.home
        if not self.fs.status(target).is_dir:
            raise UsageError(f"디렉터리가 아닙니다: {target}")
        self.cwd = target

    def cmd_pwd(self, args: list[str]) -> None:
        """pwd."""
        print(self.cwd, file=self.out)

    def cmd_stat(self, args: list[str]) -> None:
        """stat: 경로마다 세로 표."""
        rows = []
        for path in self._paths(args):
            st = self.fs.status(path)
            rows.append((st.path, st.type, st.length, mode_string(st), st.owner, st.group, st.replication,
                         st.block_size, mtime(st.modification_time), mtime(st.access_time)))
        columns = ["path", "type", "length", "permission", "owner", "group", "replication", "block_size",
                   "modified", "accessed"]
        result = ResultSet(columns, rows)
        print(render(result, "vertical" if self.fmt == "table" else self.fmt, self.header), file=self.out)

    def cmd_du(self, args: list[str]) -> None:
        """du: 디렉터리면 바로 아래 항목마다 크기. -s면 인자 경로 합계 하나."""
        on, _, rest = parse_opts(args, "sh")
        rows = []
        for path in self._paths(rest, default_cwd=True):
            targets = [path] if "s" in on else [s.path for s in self.fs.listdir(path)]
            for target in targets:
                cs = self.fs.content_summary(target)
                size: object = human(cs.length) if "h" in on else cs.length
                used: object = human(cs.space_consumed) if "h" in on else cs.space_consumed
                rows.append((size, used, target))
        self._print(ResultSet(["size", "disk_space_consumed_with_all_replicas", "path"], rows))

    def cmd_count(self, args: list[str]) -> None:
        """count: 디렉터리 수(자신 포함), 파일 수, 크기."""
        on, _, rest = parse_opts(args, "h")
        rows = []
        for path in self._paths(rest, default_cwd=True):
            cs = self.fs.content_summary(path)
            size: object = human(cs.length) if "h" in on else cs.length
            rows.append((cs.directory_count, cs.file_count, size, path))
        self._print(ResultSet(["dirs", "files", "size", "path"], rows))

    def cmd_find(self, args: list[str]) -> None:
        """find: 하위 전체에서 이름(glob)·종류로 고른다. 경로 목록만 출력한다."""
        _, values, rest = parse_opts(args, "", ("name", "type"))
        kind = values.get("type")
        if kind not in (None, "f", "d"):
            raise UsageError("-type은 f 또는 d")
        for path in self._paths(rest, default_cwd=True):
            for st in self.fs.walk(path):
                if kind and (st.is_dir != (kind == "d")):
                    continue
                if "name" in values and not fnmatch.fnmatchcase(st.path.rsplit("/", 1)[-1], values["name"]):
                    continue
                print(st.path, file=self.out)

    def _write_bytes(self, chunks: Iterator[bytes], force: bool) -> None:
        """파일 내용을 표준출력(바이너리)으로. 터미널에 바이너리를 쏟으려 하면 첫 chunk에서 멈춘다."""
        out = self._binary_out or sys.stdout.buffer
        tty = self._binary_out is None and sys.stdout.isatty()
        self.out.flush()
        first = True
        for chunk in chunks:
            if first and tty and not force and (chunk.startswith(b"PAR1") or b"\x00" in chunk[:4096]):
                raise Refused("바이너리 파일(Parquet 등)이라 터미널에 출력하지 않습니다. "
                              "get으로 받거나 Hive staging 테이블로 조회한다(강제 출력은 -f)")
            first = False
            out.write(chunk)
        out.flush()

    def cmd_cat(self, args: list[str]) -> None:
        """cat: 파일 내용 전체."""
        on, _, rest = parse_opts(args, "f")
        for path in self._paths(rest):
            self._write_bytes(self.fs.read(path), "f" in on)

    def _count_arg(self, values: dict[str, str]) -> int:
        try:
            n = int(values.get("c", "1024"))
        except ValueError:
            raise UsageError("-c는 바이트 수") from None
        if n <= 0:
            raise UsageError("-c는 1 이상")
        return n

    def cmd_head(self, args: list[str]) -> None:
        """head: 앞 N바이트."""
        on, values, rest = parse_opts(args, "f", ("c",))
        n = self._count_arg(values)
        for path in self._paths(rest):
            self._write_bytes(self.fs.read(path, 0, n), "f" in on)

    def cmd_tail(self, args: list[str]) -> None:
        """tail: 끝 N바이트(파일 크기에서 offset을 계산한다)."""
        on, values, rest = parse_opts(args, "f", ("c",))
        n = self._count_arg(values)
        for path in self._paths(rest):
            size = self.fs.status(path).length
            self._write_bytes(self.fs.read(path, max(size - n, 0), n), "f" in on)

    def cmd_get(self, args: list[str]) -> None:
        """get: 파일 하나를 로컬로. 로컬 경로를 생략하면 현재 작업 디렉터리에 같은 이름으로."""
        on, _, rest = parse_opts(args, "f")
        if not 1 <= len(rest) <= 2:
            raise UsageError("get [-f] 경로 [로컬경로]")
        src = self._paths(rest[:1])[0]
        if self.fs.status(src).is_dir:
            raise UsageError(f"디렉터리는 받지 않습니다: {src}")
        dst = Path(rest[1]) if len(rest) == 2 else Path(src.rsplit("/", 1)[-1])
        if dst.is_dir():
            dst = dst / src.rsplit("/", 1)[-1]
        if dst.exists() and "f" not in on:
            raise UsageError(f"로컬 파일이 이미 있습니다: {dst} (-f로 덮어쓴다)")
        size = 0
        with dst.open("wb") as fh:
            for chunk in self.fs.read(src):
                fh.write(chunk)
                size += len(chunk)
        self._info(f"{src} -> {dst} ({size} bytes)")

    # ---- 쓰기 명령(--write) ------------------------------------------------------------------------

    def cmd_mkdir(self, args: list[str]) -> None:
        """mkdir: 상위 디렉터리까지 만든다(hdfs dfs -mkdir -p와 같다)."""
        _, _, rest = parse_opts(args, "p")
        for path in self._paths(rest):
            self.fs.mkdirs(path)

    def cmd_rm(self, args: list[str]) -> None:
        """rm: -r 없이 비어 있지 않은 디렉터리는 HDFS가 거부한다. -f면 없는 경로도 성공으로 본다."""
        on, _, rest = parse_opts(args, "rf")
        if not rest:
            raise UsageError("rm [-r] [-f] 경로...")
        paths: list[str] = []
        for arg in rest:
            try:
                paths.extend(self.expand(arg))
            except UsageError:
                if "f" not in on:
                    raise
        for path in paths:
            self._protect(path)
        for path in paths:
            if not self.fs.delete(path, recursive="r" in on) and "f" not in on:
                raise HdfsError(f"없는 경로입니다: {path}", "FileNotFoundException")
            self._info(f"삭제: {path}")

    def cmd_mv(self, args: list[str]) -> None:
        """mv: HDFS RENAME. 대상이 있는 디렉터리면 그 안으로 옮긴다."""
        _, _, rest = parse_opts(args)
        if len(rest) != 2:
            raise UsageError("mv 원본 대상")
        src, dst = self._paths(rest[:1])[0], self.resolve(rest[1])
        self._protect(src)
        if not self.fs.rename(src, dst):
            raise HdfsError(f"이동하지 못했습니다: {src} -> {dst}")

    def cmd_put(self, args: list[str]) -> None:
        """put: 로컬 파일 하나를 올린다. 대상이 디렉터리면 그 안에 같은 이름으로."""
        on, _, rest = parse_opts(args, "f")
        if len(rest) != 2:
            raise UsageError("put [-f] 로컬경로 경로")
        local = Path(rest[0])
        if not local.is_file():
            raise UsageError(f"로컬 파일이 없습니다: {local}")
        dst = self.resolve(rest[1])
        if self.fs.exists(dst) and self.fs.status(dst).is_dir:
            dst = join(dst, local.name)
        self.fs.create(dst, local.read_bytes(), overwrite="f" in on)
        self._info(f"{local} -> {dst}")

    def cmd_chmod(self, args: list[str]) -> None:
        """chmod: 8진수 권한만 받는다(u+x 같은 기호 형식은 받지 않는다)."""
        _, _, rest = parse_opts(args)
        if len(rest) < 2 or not all(ch in "01234567" for ch in rest[0]) or len(rest[0]) not in (3, 4):
            raise UsageError("chmod 8진수권한 경로... (예: chmod 750 /data/x)")
        for path in self._paths(rest[1:]):
            self.fs.set_permission(path, rest[0])

    # ---- 기타 -------------------------------------------------------------------------------------

    def cmd_format(self, args: list[str]) -> None:
        """format: 표 출력 형식(table, vertical, csv, tsv, json)."""
        if len(args) != 1 or args[0] not in FORMATS:
            raise UsageError(f"format {'|'.join(FORMATS)} (현재 {self.fmt})")
        self.fmt = args[0]

    def cmd_help(self, args: list[str]) -> None:
        """help: 명령 목록."""
        lines = [h for h, _ in self._commands.values()]
        lines.append("경로에 glob(*, ?, [..])을 쓸 수 있다. [쓰기] 명령은 --write로 실행했을 때만 동작한다")
        print("\n".join(lines), file=self.out)
