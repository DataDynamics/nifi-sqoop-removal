import hashlib
import hmac
import sys
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer = HTTPBearer(auto_error=False)


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def require_role(*roles: str) -> Callable[..., Awaitable[str]]:
    """Bearer 토큰의 SHA-256 digest가 설정의 role별 목록에 있는지 확인한다(API 설계 9.7).

    설정에는 토큰 원문이 아니라 digest만 둔다. 토큰 교체 기간에는 이전·신규 digest를 함께 둔다.
    """

    async def dependency(
        request: Request,
        cred: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        if cred is None:
            raise HTTPException(status_code=401, detail="UNAUTHENTICATED",
                                headers={"WWW-Authenticate": "Bearer"})
        digest = token_digest(cred.credentials)
        configured: dict[str, list[str]] = request.app.state.settings.auth.token_digests
        for role in roles:
            if any(hmac.compare_digest(digest, d.lower()) for d in configured.get(role, ())):
                request.state.role = role
                return role
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    return dependency


if __name__ == "__main__":  # python -m load_control.security <token>
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m load_control.security <token>")
    print(token_digest(sys.argv[1]))
