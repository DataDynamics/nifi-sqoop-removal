"""API 오류 계층. main.py의 예외 처리기가 {code, message, requestId, details} JSON으로 바꾼다."""

from typing import Any


class ApiError(Exception):
    """API가 의도적으로 반환하는 오류. 트랜잭션 안에서 발생하면 rollback된다."""

    status = 500

    def __init__(self, code: str, message: str | None = None, **details: Any) -> None:
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
