"""조회 결과 출력 형식: table(psql식 정렬 표), vertical(psql \\x 확장 출력), csv, tsv, json.

드라이버가 돌려준 값(None, Decimal, datetime, bytes 등)을 형식별로 문자열 또는 JSON 값으로 바꾼다.
table·vertical은 사람이 읽는 용도라 한글(동아시아 전각 문자)을 2칸으로 계산해 열을 맞추고,
값 안의 줄바꿈·탭은 \\n, \\t로 보여 표가 깨지지 않게 한다. csv·tsv·json은 다른 도구로 넘기는 용도라
값을 바꾸지 않는다(tsv만 구분자 충돌을 피하려고 탭·줄바꿈을 이스케이프한다).
"""

import csv
import io
import json
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Literal, get_args

Format = Literal["table", "vertical", "csv", "tsv", "json"]
FORMATS: tuple[str, ...] = get_args(Format)

_ESCAPES = str.maketrans({"\n": "\\n", "\r": "\\r", "\t": "\\t"})


@dataclass
class ResultSet:
    """한 문장의 결과. 행이 없는 문장(DDL·DML·USE 등)은 columns가 비고 rowcount만 의미가 있다.

    truncated는 max_rows에서 잘라 나머지 행을 읽지 않았다는 표시다. rowcount는 DML이 바꾼 행 수이며
    드라이버가 모르면 -1이다.
    """

    columns: list[str] = field(default_factory=list)
    rows: list[tuple[Any, ...]] = field(default_factory=list)
    truncated: bool = False
    rowcount: int = -1


def to_text(value: Any, null: str = "") -> str:
    """값 하나를 사람이 읽는 문자열로 바꾼다. None은 null 인자, bytes는 16진수, 날짜·시각은 ISO 형식."""
    if value is None:
        return null
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date | time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).hex()
    return str(value)


def to_json_value(value: Any) -> Any:
    """JSON에 넣을 값. Decimal은 정밀도를 잃지 않도록 문자열로, int·float·bool·None은 그대로 둔다."""
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, timedelta):
        return value.total_seconds()
    return to_text(value)


def display_width(text: str) -> int:
    """터미널에 표시되는 칸 수. 전각(W, F) 문자는 2칸, 결합 문자는 0칸으로 센다."""
    width = 0
    for ch in text:
        if unicodedata.combining(ch):
            continue
        width += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return width


def _pad(text: str, width: int, right: bool) -> str:
    """표시 폭 기준으로 공백을 채운다. right이면 오른쪽 정렬(숫자 열)."""
    gap = " " * max(width - display_width(text), 0)
    return gap + text if right else text + gap


def _is_number(value: Any) -> bool:
    """오른쪽 정렬할 숫자 값인지. bool은 int의 하위 타입이지만 숫자로 보지 않는다."""
    return isinstance(value, int | float | Decimal) and not isinstance(value, bool)


def render_table(result: ResultSet, header: bool = True, null: str = "") -> str:
    """psql 기본 출력과 같은 정렬 표. 숫자 열 값은 오른쪽 정렬한다. 행 수 footer는 붙이지 않는다."""
    cells = [[to_text(v, null).translate(_ESCAPES) for v in row] for row in result.rows]
    widths = [display_width(c) if header else 0 for c in result.columns]
    for row in cells:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], display_width(cell))
    lines = []
    if header:
        lines.append(" " + " | ".join(_pad(c, widths[i], False)
                                      for i, c in enumerate(result.columns)).rstrip())
        lines.append("+".join("-" * (w + 2) for w in widths))  # 칸마다 양옆 공백 1칸씩
    for raw, row in zip(result.rows, cells, strict=True):
        lines.append(" " + " | ".join(_pad(cell, widths[i], _is_number(raw[i]))
                                      for i, cell in enumerate(row)).rstrip())
    return "\n".join(lines)


def render_vertical(result: ResultSet, null: str = "") -> str:
    """psql \\x 확장 출력. 열이 많거나 값이 길 때 행 하나를 `열 | 값` 여러 줄로 보여 준다."""
    width = max((display_width(c) for c in result.columns), default=0)
    lines = []
    for n, row in enumerate(result.rows, start=1):
        lines.append(f"-[ RECORD {n} ]" + "-" * max(width - 8, 2))
        for col, value in zip(result.columns, row, strict=True):
            lines.append(f"{_pad(col, width, False)} | {to_text(value, null).translate(_ESCAPES)}".rstrip())
    return "\n".join(lines)


def render_csv(result: ResultSet, header: bool = True, delimiter: str = ",") -> str:
    """RFC 4180 CSV. 따옴표·구분자·줄바꿈이 있는 값만 따옴표로 감싼다. None은 빈 값."""
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=delimiter, lineterminator="\n")
    if header:
        writer.writerow(result.columns)
    writer.writerows([to_text(v) for v in row] for row in result.rows)
    return buf.getvalue().rstrip("\n")


def render_tsv(result: ResultSet, header: bool = True) -> str:
    """탭 구분. 값 안의 탭·줄바꿈은 \\t, \\n으로 이스케이프한다(Hive·sort·awk로 넘기기 쉽게)."""
    lines = ["\t".join(result.columns)] if header else []
    lines.extend("\t".join(to_text(v).translate(_ESCAPES) for v in row) for row in result.rows)
    return "\n".join(lines)


def render_json(result: ResultSet) -> str:
    """행마다 {열: 값} 객체인 JSON 배열. 같은 이름의 열이 여러 개면 뒤 열이 앞 값을 덮는다."""
    rows = [{c: to_json_value(v) for c, v in zip(result.columns, row, strict=True)}
            for row in result.rows]
    return json.dumps(rows, ensure_ascii=False, indent=2)


def render(result: ResultSet, fmt: str, header: bool = True, null: str = "") -> str:
    """형식 이름으로 출력 문자열을 만든다. 알 수 없는 형식이면 ValueError."""
    match fmt:
        case "table":
            return render_table(result, header, null)
        case "vertical":
            return render_vertical(result, null)
        case "csv":
            return render_csv(result, header)
        case "tsv":
            return render_tsv(result, header)
        case "json":
            return render_json(result)
    raise ValueError(f"unknown format: {fmt} ({', '.join(FORMATS)})")
