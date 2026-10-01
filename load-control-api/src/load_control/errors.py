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
    status = 404


class Conflict(ApiError):
    status = 409


class Unprocessable(ApiError):
    status = 422
