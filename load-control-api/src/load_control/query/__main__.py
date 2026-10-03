"""운영 조회 도구 진입점: python -m load_control.query {oracle|hive|hdfs} [옵션] [hdfs 명령...]

bin/oracle.sh, bin/hive.sh, bin/hdfs.sh가 이 모듈을 부른다. 접속 정보는 config.yaml clients 섹션에서
읽고, 명령행 옵션(--dsn, --host, --url 등)이 같은 항목을 덮어쓴다. 환경변수 LCA_CLIENTS__ORACLE__PASSWORD
같은 덮어쓰기는 Settings.load가 처리한다.

실행 방식(앞의 것이 우선):
1. -c 명령, -f 파일: 준 순서대로 실행하고 첫 오류에서 멈춘다(-f -는 표준입력)
2. hdfs의 위치 인자: 명령 하나(bin/hdfs.sh ls -h /data)
3. 표준입력이 파이프: 전체를 읽어 실행
4. 그 밖(터미널): 대화형

종료 코드: 0 성공, 1 실행 오류, 2 사용법·설정·접속 오류, 3 읽기 전용 거부, 130 Ctrl-C.
"""

import argparse
import getpass
import os
import sys
from typing import Any

from pydantic import BaseModel, ValidationError

from load_control.config import (
    CONFIG_ENV,
    HdfsClientSettings,
    HiveClientSettings,
    OracleClientSettings,
    Settings,
)
from load_control.query.output import FORMATS
from load_control.query.sqlshell import EXIT_OK, EXIT_USAGE, QueryError, SqlBackend, SqlShell


class _Script(argparse.Action):
    """-c와 -f를 입력 순서대로 한 목록(('c', 값) 또는 ('f', 값))에 모은다."""

    def __call__(self, parser: argparse.ArgumentParser, namespace: argparse.Namespace,
                 values: Any, option_string: str | None = None) -> None:
        items = list(getattr(namespace, "script", None) or [])
        items.append(("c" if option_string in ("-c", "--command") else "f", values))
        namespace.script = items


def build_parser() -> argparse.ArgumentParser:
    """도구별 하위 명령과 옵션."""
    parser = argparse.ArgumentParser(prog="python -m load_control.query",
                                     description="Oracle·Hive·HDFS 운영 조회 도구(기본 읽기 전용)")
    sub = parser.add_subparsers(dest="tool", required=True)

    def common(p: argparse.ArgumentParser, sql: bool) -> None:
        p.add_argument("--config", help=f"config.yaml 경로(기본: ${CONFIG_ENV} 또는 ./config/config.yaml)")
        p.add_argument("-c", "--command", action=_Script, dest="script", metavar="명령",
                       help="실행할 SQL·명령(여러 번 줄 수 있다)")
        p.add_argument("-f", "--file", action=_Script, dest="script", metavar="파일",
                       help="실행할 파일(-는 표준입력)")
        p.add_argument("--write", action="store_true",
                       help="쓰기 허용(DML·DDL, HDFS mkdir·rm·mv·put·chmod). 기본은 읽기 전용")
        p.add_argument("-F", "--format", choices=FORMATS, default="table", help="출력 형식(기본 table)")
        p.add_argument("-t", "--no-header", action="store_true", help="열 이름 머리글 생략")
        if sql:
            p.add_argument("--max-rows", type=int, default=1000, metavar="N",
                           help="문장마다 최대 출력 행 수(기본 1000, 0이면 제한 없음)")
            p.add_argument("--timing", action="store_true", help="실행 시간 표시")
            p.add_argument("--echo", action="store_true", help="실행 전에 문장을 출력")
            p.add_argument("--null", default="", metavar="문자열",
                           help="table·vertical에서 NULL 표시(기본 빈칸)")

    ora = sub.add_parser("oracle", help="Oracle SQL(clients.oracle)")
    common(ora, True)
    ora.add_argument("--dsn", help="host:port/service_name")
    ora.add_argument("--user")
    ora.add_argument("-W", "--password-prompt", action="store_true", help="비밀번호를 물어본다")

    hive = sub.add_parser("hive", help="HiveServer2 SQL(clients.hive)")
    common(hive, True)
    hive.add_argument("--host")
    hive.add_argument("--port", type=int)
    hive.add_argument("-d", "--database")
    hive.add_argument("--user")

    hdfs = sub.add_parser("hdfs", help="WebHDFS 명령(clients.hdfs)")
    common(hdfs, False)
    hdfs.add_argument("--url", action="append", metavar="URL", help="NameNode WebHDFS 주소(여러 번 가능)")
    hdfs.add_argument("--user")
    hdfs.add_argument("command", nargs=argparse.REMAINDER, help="명령 하나(예: ls -h /data). 없으면 대화형")
    return parser


def client_settings[T: BaseModel](cls: type[T], base: BaseModel | None, overrides: dict[str, Any],
                                  section: str) -> T:
    """config의 clients.<section>에 명령행 값을 덮어써 검증한다. 필수 값이 없으면 SystemExit(2)."""
    data = base.model_dump() if base is not None else {}
    data.update({k: v for k, v in overrides.items() if v is not None})
    try:
        return cls.model_validate(data)
    except ValidationError as exc:
        missing = ", ".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
        print(f"오류: config.yaml clients.{section} 설정이 없거나 잘못됐습니다({missing})", file=sys.stderr)
        raise SystemExit(EXIT_USAGE) from None


def make_sql_backend(args: argparse.Namespace, settings: Settings) -> SqlBackend:
    """oracle·hive 백엔드를 만든다. 드라이버 import는 여기서 해서 hdfs만 쓸 때 영향이 없게 한다."""
    if args.tool == "oracle":
        from load_control.query.oracle import OracleBackend

        password = getpass.getpass("Oracle 비밀번호: ") if args.password_prompt else None
        ora = client_settings(OracleClientSettings, settings.clients.oracle,
                              {"dsn": args.dsn, "user": args.user, "password": password}, "oracle")
        return OracleBackend(ora, allow_write=args.write)
    from load_control.query.hive import HiveBackend

    hive = client_settings(HiveClientSettings, settings.clients.hive,
                           {"host": args.host, "port": args.port, "database": args.database,
                            "user": args.user}, "hive")
    return HiveBackend(hive, allow_write=args.write)


def run_sql(args: argparse.Namespace, settings: Settings) -> int:
    """oracle·hive 실행. 대화형이면 먼저 연결해 접속 정보 오류를 바로 알린다."""
    backend = make_sql_backend(args, settings)
    shell = SqlShell(backend, fmt=args.format, header=not args.no_header, max_rows=args.max_rows,
                     allow_write=args.write, timing=args.timing, null=args.null, echo=args.echo)
    try:
        if args.script:
            for kind, value in args.script:
                ok = shell.run_text(value) if kind == "c" else shell.run_file(value)
                if not ok:
                    break
        elif not sys.stdin.isatty():
            shell.run_text(sys.stdin.read())
        else:
            try:
                backend.connect()
            except QueryError as exc:
                print(f"오류: {exc}", file=sys.stderr)
                return EXIT_USAGE
            shell.interactive()
    finally:
        backend.close()
    return shell.status


def run_hdfs(args: argparse.Namespace, settings: Settings) -> int:
    """hdfs 실행."""
    from load_control.query.hdfsshell import HdfsShell
    from load_control.query.webhdfs import WebHdfs

    cfg = client_settings(HdfsClientSettings, settings.clients.hdfs,
                          {"namenode_urls": args.url, "user": args.user}, "hdfs")
    fs = WebHdfs([str(u) for u in cfg.namenode_urls], cfg.user, cfg.timeout_seconds,
                 datanode_hosts=cfg.datanode_hosts)
    shell = HdfsShell(fs, home=cfg.home, allow_write=args.write, fmt=args.format, header=not args.no_header)
    try:
        if args.script:
            for kind, value in args.script:
                if kind == "c":
                    ok = shell.run_text(value)
                else:
                    try:
                        text = sys.stdin.read() if value == "-" else open(value, encoding="utf-8").read()
                    except OSError as exc:
                        print(f"오류: 파일을 읽을 수 없습니다: {exc}", file=sys.stderr)
                        return EXIT_USAGE
                    ok = shell.run_text(text)
                if not ok:
                    break
        elif args.command:
            shell.run(args.command)
        elif not sys.stdin.isatty():
            shell.run_text(sys.stdin.read())
        else:
            shell.interactive()
    finally:
        fs.close()
    return shell.status


def main(argv: list[str] | None = None) -> int:
    """인자를 읽고 도구를 실행해 종료 코드를 돌려준다."""
    args = build_parser().parse_args(argv)
    try:
        settings = Settings.load(args.config)
    except (FileNotFoundError, ValidationError) as exc:
        print(f"오류: 설정을 읽을 수 없습니다: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        code = run_hdfs(args, settings) if args.tool == "hdfs" else run_sql(args, settings)
        sys.stdout.flush()  # 파이프가 닫혔으면 여기서 BrokenPipeError가 난다
        return code
    except BrokenPipeError:
        # `| head`처럼 읽는 쪽이 먼저 닫았다. 남은 출력을 버려 종료 시 flush 오류 메시지를 막는다.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
