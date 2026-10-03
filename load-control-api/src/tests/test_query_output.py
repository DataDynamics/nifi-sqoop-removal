"""운영 조회 도구의 결과 출력 형식을 검증한다."""

import json
from datetime import datetime
from decimal import Decimal

from load_control.query.output import ResultSet, display_width, render

RESULT = ResultSet(["id", "이름", "amount"], [(1, "가나", Decimal("1.50")), (22, None, Decimal("-3"))])


def test_table_aligns_wide_chars_and_right_aligns_numbers() -> None:
    """한글은 2칸으로 세고 숫자 열은 오른쪽 정렬한다. 구분선은 열 폭+2, 줄 끝 공백은 지운다."""
    lines = render(RESULT, "table").splitlines()
    assert lines == [
        " id | 이름 | amount",
        "----+------+--------",
        "  1 | 가나 |   1.50",
        " 22 |      |     -3",
    ]
    assert display_width(lines[2]) == display_width(lines[0])  # 한글 값도 머리글과 같은 폭


def test_vertical_and_escapes() -> None:
    """확장 출력은 행마다 RECORD 머리와 `열 | 값`. 줄바꿈은 \\n으로 보여 준다."""
    text = render(ResultSet(["a", "bb"], [("x\ny", datetime(2026, 1, 2, 3, 4, 5))]), "vertical", null="NULL")
    assert text.splitlines() == ["-[ RECORD 1 ]--", "a  | x\\ny", "bb | 2026-01-02 03:04:05"]


def test_csv_tsv_json() -> None:
    """csv는 RFC 4180, tsv는 탭 이스케이프, json은 Decimal을 문자열로 보존한다."""
    rs = ResultSet(["a", "b"], [("x,y", "t\tz"), (None, Decimal("10.10"))])
    assert render(rs, "csv").splitlines() == ["a,b", '"x,y",t\tz', ",10.10"]
    assert render(rs, "tsv", header=False).splitlines() == ["x,y\tt\\tz", "\t10.10"]
    assert json.loads(render(rs, "json")) == [{"a": "x,y", "b": "t\tz"}, {"a": None, "b": "10.10"}]
