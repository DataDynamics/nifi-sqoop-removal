"""운영 조회 도구의 문장 나누기(StatementBuffer)와 읽기 전용 판정(write_reason)을 검증한다."""

import pytest

from load_control.query.statements import StatementBuffer, write_reason


def feed(dialect: str, text: str) -> list[str]:
    """여러 줄을 넣고 끝에 flush까지 한 문장 목록."""
    buf = StatementBuffer(dialect)  # type: ignore[arg-type]
    out = [s for line in text.splitlines() for s in buf.add_line(line)]
    tail = buf.flush()
    return out + ([tail] if tail else [])


def test_splits_on_semicolon_outside_quotes_and_comments() -> None:
    """따옴표·주석 안의 ;는 문장 끝이 아니고, 끝의 ;는 떼어 낸다. ; 없는 마지막 문장도 돌려준다."""
    text = "select ';' a from dual; -- c;\nselect 1 /* ; */\n from dual;\nselect 2"
    assert feed("oracle", text) == ["select ';' a from dual", "-- c;\nselect 1 /* ; */\n from dual",
                                    "select 2"]


def test_statement_continues_across_lines_until_semicolon() -> None:
    """;가 나올 때까지 pending이 참이라 대화형 프롬프트가 `->`가 된다."""
    buf = StatementBuffer("hive")
    assert buf.add_line("select 'a") == []
    assert buf.pending
    assert buf.add_line("b;' as x;") == ["select 'a\nb;' as x"]
    assert not buf.pending


def test_oracle_plsql_block_ends_only_with_slash() -> None:
    """BEGIN 블록은 안의 ;로 끝나지 않고 / 줄에서 END;까지 한 문장이 된다."""
    buf = StatementBuffer("oracle")
    assert [buf.add_line(x) for x in ("begin", "  null;", "end;")] == [[], [], []]
    assert buf.add_line("/") == ["begin\n  null;\nend;"]


def test_backslash_escape_is_hive_only() -> None:
    """Hive는 '\\''를 이스케이프로 보고, Oracle은 역슬래시를 일반 문자로 본다."""
    assert feed("hive", "select 'it\\'s;';") == ["select 'it\\'s;'"]
    assert feed("oracle", "select 'C:\\' from dual;") == ["select 'C:\\' from dual"]


@pytest.mark.parametrize(("sql", "dialect", "refused"), [
    ("select * from t", "oracle", False),
    ("/* x */ (select 1 from dual)", "oracle", False),
    ("with a as (select 1 from dual) select * from a", "oracle", False),
    ("select note from t where note = 'delete me'", "oracle", False),
    ("desc app.insp_dtl", "oracle", False),
    ("delete from t", "oracle", True),
    ("select * from t for update", "oracle", True),
    ("begin null; end;", "oracle", True),
    ("show create table dw.insp_dtl", "hive", False),
    ("describe formatted dw.insp_dtl", "hive", False),
    ("use stg", "hive", False),
    ("set hive.exec.dynamic.partition.mode=nonstrict", "hive", False),
    ("from t select a", "hive", False),
    ("from t insert overwrite table u select a", "hive", True),
    ("with a as (select 1) insert into t select * from a", "hive", True),
    ("explain analyze insert into t values (1)", "hive", True),
    ("drop table stg.x", "hive", True),
    ("msck repair table dw.insp_dtl", "hive", True),
])
def test_write_reason(sql: str, dialect: str, refused: bool) -> None:
    """읽기 전용에서 허용·거부할 문장."""
    assert (write_reason(sql, dialect) is not None) is refused  # type: ignore[arg-type]
