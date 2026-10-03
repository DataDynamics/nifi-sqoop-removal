"""API 오류 계층. main.py의 예외 처리기가 {code, message, requestId, details} JSON으로 바꾼다."""

from typing import Any


class ApiError(Exception):
    """API가 의도적으로 반환하는 오류. 트랜잭션 안에서 발생하면 rollback된다.

    하위 클래스가 HTTP 상태(status)를 정한다. code는 NiFi·운영자가 분기에 쓰는 고정 문자열이다
    (예: CLAIM_MISMATCH). 직접 쓰지 말고 하위 클래스를 쓴다.
    """

    status = 500  # 하위 클래스가 덮어쓴다

    def __init__(self, code: str, message: str | None = None, **details: Any) -> None:
        """code: 오류 코드, message: 사람이 읽을 설명(없으면 code), details: 응답 details에 넣을 값."""
        super().__init__(code)
        self.code = code
        self.message = message or code
        self.details = details


class NotFound(ApiError):
    """run, 파티션, dispatch가 없음. NiFi는 No Retry로 분기한다."""

    status = 404


class Conflict(ApiError):
    """소유권·상태 충돌(CLAIM_MISMATCH 등). 정상 경합이 많으며 NiFi는 재시도하지 않는다."""

    status = 409


class Unprocessable(ApiError):
    """입력·불변식 위반. 설정이나 NiFi flow 오류이므로 재시도해도 같다."""

    status = 422
