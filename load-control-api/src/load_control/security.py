"""Bearer 토큰 인증. role: nifi(NiFi 서비스 계정), operator(운영자).

설정(auth.token_digests)에는 토큰 원문 대신 SHA-256 hex digest만 둔다. 설정 파일이 새어도 토큰을
되살릴 수 없게 하기 위해서다. digest 생성: `python -m load_control.security <token>`.
"""

import hashlib
import hmac
import sys
from collections.abc import Awaitable, Callable
from typing import Annotated

import structlog
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

log = structlog.get_logger(__name__)

# auto_error=False: 헤더가 없을 때 FastAPI 기본 오류 대신 직접 401과 로그를 남기기 위해서다.
bearer = HTTPBearer(auto_error=False)


def token_digest(token: str) -> str:
    """설정 파일에 넣을 토큰 digest(SHA-256 hex)."""
    return hashlib.sha256(token.encode()).hexdigest()


def require_role(*roles: str) -> Callable[..., Awaitable[str]]:
    """Bearer 토큰의 SHA-256 digest가 설정의 role별 목록에 있는지 확인한다.

    설정에는 토큰 원문이 아니라 digest만 둔다. 토큰 교체 기간에는 이전·신규 digest를 함께 둔다.
    roles 중 하나라도 맞으면 통과한다(앞에 적은 role이 우선). 라우터의 `dependencies=[Depends(...)]`로 쓴다.

    - 토큰 없음: 401 UNAUTHENTICATED(WWW-Authenticate: Bearer)
    - 토큰이 허용 role 어디에도 없음: 403 FORBIDDEN
    비교는 hmac.compare_digest로 해 응답 시간으로 digest를 추측하지 못하게 한다. 설정의 digest는
    소문자로 바꿔 비교하므로 대문자 hex로 적어도 된다.
    """

    async def dependency(
        request: Request,
        cred: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> str:
        """허용된 role이면 그 이름을 돌려주고 request.state.role에 남긴다."""
        if cred is None:
            log.info("auth_missing_token", path=request.url.path)
            raise HTTPException(status_code=401, detail="UNAUTHENTICATED",
                                headers={"WWW-Authenticate": "Bearer"})
        digest = token_digest(cred.credentials)
        configured: dict[str, list[str]] = request.app.state.settings.auth.token_digests
        for role in roles:
            if any(hmac.compare_digest(digest, d.lower()) for d in configured.get(role, ())):
                request.state.role = role
                return role
        # 토큰 자체는 남기지 않는다. digest 앞 8자리만 남겨 어느 토큰인지 추적할 수 있게 한다.
        log.warning("auth_forbidden", path=request.url.path, requiredRoles=list(roles),
                    tokenDigestPrefix=digest[:8])
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    return dependency


if __name__ == "__main__":  # python -m load_control.security <token>
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m load_control.security <token>")
    print(token_digest(sys.argv[1]))
