"""운영 조회 도구 SQL 셸(SqlShell)의 일괄 실행·메타 명령·종료 코드를 가짜 백엔드로 검증한다."""

import io
from pathlib import Path

from load_control.query.output import ResultSet
from load_control.query.sqlshell import (
    EXIT_ERROR,
    EXIT_OK,
    EXIT_REFUSED,
    MetaHandler,
    QueryError,
    SqlBackend,
    SqlShell,
)


class FakeBackend(SqlBackend):
    """실행한 문장을 기록하고, 'fail'이 들어간 문장은 오류, 'many'는 5행을 돌려준다."""

    dialect = "oracle"
    tool = "fake"

    def __init__(self) -> None:
        self.executed: list[str] = []

    def connect(self) -> None:
        pass

    def execute(self, sql: str, max_rows: int) -> ResultSet:
        self.executed.append(sql)
        if "fail" in sql:
            raise QueryError("ORA-00942: table or view does not exist")
        if "many" in sql:
            rows = [(i,) for i in range(5)]
            return ResultSet(["n"], rows[:max_rows or None], truncated=bool(max_rows) and max_rows < 5)
        return ResultSet(["x"], [(1,)])

    def cancel(self) -> None:
        pass

    def close(self) -> None:
        pass

    def list_tables(self, pattern: str | None) -> ResultSet:
        return ResultSet(["name"], [(pattern or "ALL",)])

    def describe(self, name: str, verbose: bool) -> ResultSet:
        return ResultSet(["column"], [(f"{name}:{verbose}",)])

    def list_schemas(self, pattern: str | None) -> ResultSet:
        return ResultSet(["schema"], [("APP",)])

    def conninfo(self) -> str:
        return "fake"

    def prompt(self) -> str:
        return "fake"

    def extra_meta(self) -> dict[str, tuple[str, MetaHandler]]:
        return {"scn": ("\\scn", lambda a: ResultSet(["scn"], [(42,)]))}


def shell(**kw: object) -> tuple[SqlShell, FakeBackend, io.StringIO, io.StringIO]:
    backend = FakeBackend()
    out, err = io.StringIO(), io.StringIO()
    return SqlShell(backend, out=out, err=err, **kw), backend, out, err  # type: ignore[arg-type]


def test_runs_statements_and_meta_in_order() -> None:
    """-c 텍스트의 문장과 메타 명령을 순서대로 실행하고 결과·행 수를 출력한다."""
    sh, backend, out, _ = shell()
    assert sh.run_text("select 1 from dual;\n\\dt APP.*\n\\d+ t\n\\scn\nselect 2 from dual")
    assert backend.executed == ["select 1 from dual", "select 2 from dual"]
    text = out.getvalue()
    assert "APP.*" in text and "t:True" in text and "42" in text
    assert text.count("(1행)") == 5
    assert sh.status == EXIT_OK


def test_write_statement_is_refused_with_exit_3() -> None:
    """읽기 전용이면 DML을 백엔드로 보내지 않고 멈추며 종료 코드 3."""
    sh, backend, _, err = shell()
    assert not sh.run_text("delete from t; select 1 from dual;")
    assert backend.executed == []
    assert "읽기 전용" in err.getvalue()
    assert sh.status == EXIT_REFUSED


def test_write_mode_executes_dml() -> None:
    """--write면 DML도 실행한다."""
    sh, backend, _, _ = shell(allow_write=True)
    assert sh.run_text("update t set a = 1;")
    assert backend.executed == ["update t set a = 1"]


def test_stops_at_first_error() -> None:
    """일괄 실행은 첫 오류에서 멈추고 종료 코드 1. 오류 메시지는 표준오류로."""
    sh, backend, _, err = shell()
    assert not sh.run_text("select fail from x;\nselect 2 from dual;")
    assert backend.executed == ["select fail from x"]
    assert "ORA-00942" in err.getvalue()
    assert sh.status == EXIT_ERROR


def test_truncation_note_goes_to_stderr_for_machine_formats() -> None:
    """csv에서는 결과만 표준출력으로, 잘림 안내는 표준오류로 보낸다."""
    sh, _, out, err = shell(fmt="csv", max_rows=2)
    sh.run_text("select many from dual")
    assert out.getvalue().splitlines() == ["n", "0", "1"]
    assert "처음 2행" in err.getvalue()


def test_format_switch_output_file_and_include(tmp_path: Path) -> None:
    """\\format, \\o 파일, \\i 파일이 동작한다."""
    script = tmp_path / "q.sql"
    script.write_text("select 3 from dual;\n", encoding="utf-8")
    target = tmp_path / "out.json"
    sh, backend, _, _ = shell()
    assert sh.run_text(f"\\format json\n\\o {target}\n\\i {script}\n\\o")
    assert backend.executed == ["select 3 from dual"]
    assert target.read_text(encoding="utf-8").strip().startswith("[")


def test_unknown_meta_is_error() -> None:
    """모르는 메타 명령은 오류로 멈춘다."""
    sh, _, _, err = shell()
    assert not sh.run_text("\\nope")
    assert "알 수 없는 명령" in err.getvalue()
