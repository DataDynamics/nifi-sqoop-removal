"""공통 타입과 기반 모델."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints
from pydantic.alias_generators import to_camel

PARTITION_ID_PATTERN = r"^([0-9]{4}|NULL)$"

JobKey = Annotated[str, StringConstraints(pattern=r"^[A-Z0-9_]{1,200}$")]
BusinessKey = Annotated[str, StringConstraints(pattern=r"^[0-9A-Za-z_.:\-]{1,200}$")]
PartitionId = Annotated[str, StringConstraints(pattern=PARTITION_ID_PATTERN)]
DecimalStr = Annotated[str, StringConstraints(pattern=r"^-?[0-9]{1,38}$")]
TablePrefix = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,60}$")]
HdfsPath = Annotated[str, StringConstraints(pattern=r"^/[A-Za-z0-9_.=/\-]{1,1499}$")]
ShortText = Annotated[str, StringConstraints(max_length=100)]
Message = Annotated[str, StringConstraints(max_length=2000)]


class ApiModel(BaseModel):
    """JSON은 camelCase, Python 속성은 snake_case(API 설계 9.4).

    - lax 모드: NiFi AttributesToJSON의 "5000", "false" 같은 문자열을 int/bool로 받는다.
    - coerce_numbers_to_str: 경계값이 JSON 숫자로 와도 DecimalStr로 받는다.
    - extra="forbid": NiFi attribute 목록 실수를 422로 드러낸다.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
        coerce_numbers_to_str=True,
    )


class ErrorResponse(ApiModel):
    """오류 응답 형식."""

    code: str
    message: str
    request_id: str | None = None
