"""공통 타입과 기반 모델."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints
from pydantic.alias_generators import to_camel

# 파티션 ID: 4자리 번호(0000~9999) 또는 split 컬럼이 NULL인 행을 모은 "NULL" 파티션
PARTITION_ID_PATTERN = r"^([0-9]{4}|NULL)$"

# 적재 Job 식별자(대문자·숫자·밑줄). HDFS 경로와 NiFi 호출 URL에 들어가므로 형식을 좁힌다.
JobKey = Annotated[str, StringConstraints(pattern=r"^[A-Z0-9_]{1,200}$")]
# 업무 키(보통 업무일자, 예: 2026-09-28). 같은 jobKey + businessKey에는 활성 run이 하나만 있다.
BusinessKey = Annotated[str, StringConstraints(pattern=r"^[0-9A-Za-z_.:\-]{1,200}$")]
PartitionId = Annotated[str, StringConstraints(pattern=PARTITION_ID_PATTERN)]
# 정수 문자열(최대 38자리, Oracle NUMBER 범위). SCN·split 경계처럼 JSON 숫자로는 정밀도를 잃는 값
DecimalStr = Annotated[str, StringConstraints(pattern=r"^-?[0-9]{1,38}$")]
# staging table 이름 접두사. 뒤에 runId hex를 붙여 Hive 식별자로 쓰므로 영문·숫자·밑줄만 허용한다.
TablePrefix = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,60}$")]
# HDFS 절대 경로. 영문·숫자와 `_ . = / -`만 허용한다(hdfsRoot의 `..` 구간은 run 생성 서비스가 거부한다).
HdfsPath = Annotated[str, StringConstraints(pattern=r"^/[A-Za-z0-9_.=/\-]{1,1499}$")]
# 코드성 짧은 문자열(오류 코드, 단계 이름, 쿼리 버전 등)
ShortText = Annotated[str, StringConstraints(max_length=100)]
# 사람이 읽는 메시지·지표 값. DB의 message·error_message 컬럼(varchar(2000))과 같은 길이다.
Message = Annotated[str, StringConstraints(max_length=2000)]


class ApiModel(BaseModel):
    """JSON은 camelCase, Python 속성은 snake_case.

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
    """오류 응답 형식. main.py의 예외 처리기가 같은 모양(+ details)으로 응답한다."""

    code: str  # 고정 오류 코드(예: CLAIM_MISMATCH, RUN_NOT_FOUND). NiFi 분기용
    message: str  # 사람이 읽을 설명
    request_id: str | None = None  # X-Request-Id. 로그 추적 키
