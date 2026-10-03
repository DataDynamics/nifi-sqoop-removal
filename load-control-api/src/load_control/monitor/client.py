"""모니터가 쓰는 조회 API 클라이언트.

조회(GET)는 nifi 또는 operator role 토큰으로, 운영 작업(POST)은 operator 토큰으로 부른다.
실패는 모두 ApiError 하나로 바꿔 화면이 메시지만 보여 주고 다음 새로고침에서 다시 시도하게 한다.
"""

from typing import Any

import httpx


class ApiError(Exception):
    """API 호출 실패(연결, 인증, 4xx·5xx).

    메시지는 사람이 읽을 한국어 문장이며 화면에 그대로 보여 준다.
    """


class MonitorClient:
    """Load Control API 클라이언트: 조회와 운영 작업(재전송, PUBLISH_UNKNOWN 확정).

    httpx.AsyncClient 하나를 앱 수명 동안 재사용한다(연결 풀 유지). 화면들이 같은 인스턴스를
    동시에 써도 되며, 각 호출은 독립된 요청이다. 끝날 때 close()로 연결을 닫는다.
    """

    def __init__(self, base_url: str, token: str | None, *, operator_token: str | None = None,
                 timeout: float = 5.0, transport: httpx.AsyncBaseTransport | None = None) -> None:
        """기본 헤더에 조회 토큰을 넣고, 운영 작업용 헤더는 따로 만들어 둔다.

        operator_token이 없으면 조회 토큰을 운영 작업에도 쓴다(그 토큰이 operator role이면 그대로 된다).
        timeout은 요청당 초 단위이며, 새로고침이 API 지연에 오래 묶이지 않도록 짧게 둔다.
        transport는 테스트에서 httpx.MockTransport·ASGITransport를 끼우는 데 쓴다.
        """
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.base_url = base_url.rstrip("/")
        op = operator_token or token
        self._operator_headers = {"Authorization": f"Bearer {op}"} if op else {}
        self._client = httpx.AsyncClient(base_url=self.base_url, headers=headers, timeout=timeout,
                                         transport=transport)

    async def close(self) -> None:
        """연결 풀을 닫는다. 앱이 내려갈 때(on_unmount) 한 번 부른다."""
        await self._client.aclose()

    async def _get(self, path: str, **params: Any) -> Any:
        """조회 토큰으로 GET하고 JSON 본문을 돌려준다.

        값이 None인 쿼리 인자는 보내지 않는다(API 기본값을 쓰게 한다). 연결 실패·401/403·그 밖의
        4xx/5xx는 ApiError로 바꾼다. 상태를 바꾸지 않으므로 몇 번을 다시 불러도 된다.
        """
        try:
            r = await self._client.get(path, params={k: v for k, v in params.items() if v is not None})
        except httpx.HTTPError as e:
            raise ApiError(f"API 연결 실패: {e.__class__.__name__}") from e
        if r.status_code in (401, 403):
            raise ApiError(f"API 인증 실패({r.status_code}): monitor.token을 확인하세요")
        if r.status_code >= 400:
            raise ApiError(f"API 오류 {r.status_code}: {r.text[:200]}")
        return r.json()

    async def ready(self) -> bool:
        """/readyz가 200이면 True(API 프로세스가 살아 있고 DB에 SELECT 1이 된다).

        인증 없는 엔드포인트라 토큰 문제와 무관하게 API·DB 상태만 본다. 실패는 예외 없이 False다.
        """
        try:
            r = await self._client.get("/readyz")
        except httpx.HTTPError:
            return False
        return r.status_code == 200

    async def summary(self) -> dict[str, Any]:
        """대시보드 요약: 진행 중·최근 run 수, dispatch 현황, 정리 대상 수, 경보(alerts)."""
        return dict(await self._get("/v1/monitor/summary"))

    async def runs(self, *, limit: int = 100, status: str | None = None,
                   job_key: str | None = None) -> list[dict[str, Any]]:
        """run 목록(최근 순). status·job_key를 주면 API가 그 조건으로 거른다. limit은 API에서 최대 500이다."""
        return list(await self._get("/v1/runs", limit=limit, status=status, jobKey=job_key))

    async def run(self, run_id: str) -> dict[str, Any]:
        """run 하나의 상세(파티션, dispatch 포함). 없는 run이면 404가 ApiError로 온다."""
        return dict(await self._get(f"/v1/runs/{run_id}"))

    async def validations(self, run_id: str) -> list[dict[str, Any]]:
        """run의 SOURCE·STAGING·TARGET 검증 지표 목록."""
        return list((await self._get(f"/v1/runs/{run_id}/validations"))["metrics"])

    async def events(self, run_id: str, limit: int = 300) -> list[dict[str, Any]]:
        """run의 이벤트 중 최근 limit개를 오래된 순으로 돌려준다(API 상태 변화와 NiFi 오류)."""
        return list((await self._get(f"/v1/runs/{run_id}/events", limit=limit))["events"])

    # 운영 작업(operator 토큰). API가 이벤트와 로그에 남긴다.
    async def _post(self, path: str, body: dict[str, Any] | None = None) -> Any:
        """operator 토큰으로 POST하고 JSON 본문을 돌려준다.

        요청 단위로 Authorization 헤더를 operator 토큰으로 바꿔 보낸다(기본 헤더를 덮어쓴다).
        API가 거부하면 오류 본문의 code·message(예: DISPATCH_STATUS_MISMATCH)를 ApiError 메시지에 담아
        운영자가 이유를 바로 보게 한다. 타임아웃 등 연결 실패는 API가 처리했는지 알 수 없으므로,
        다시 하기 전에 새로고침으로 상태를 확인하는 것이 안전하다.
        """
        try:
            r = await self._client.post(path, json=body, headers=self._operator_headers)
        except httpx.HTTPError as e:
            raise ApiError(f"API 연결 실패: {e.__class__.__name__}") from e
        if r.status_code in (401, 403):
            raise ApiError(f"권한 없음({r.status_code}): operator 토큰이 필요합니다(monitor.operator_token)")
        if r.status_code >= 400:
            try:
                # API 오류 본문: {"code": ..., "message": ...}. JSON이 아니면(프록시 오류 등) 원문 앞부분
                detail = r.json()
                msg = f"{detail.get('code')}: {detail.get('message')}"
            except ValueError:
                msg = r.text[:200]
            raise ApiError(f"API 거부 {r.status_code} {msg}")
        return r.json()

    async def resend_dispatch(self, run_id: str, dispatch_id: str) -> dict[str, Any]:
        """DEAD 또는 ACK 없는 SENT dispatch를 PENDING으로 되돌려 다시 보내게 한다.

        API가 run을 잠그고 status가 DEAD·SENT일 때만 PENDING, attempt_count 0으로 바꾸며
        DISPATCH_RESENT 이벤트를 남긴다. 실제 전송은 worker의 dispatcher가 한다.
        이미 PENDING·ACKED이면 409(DISPATCH_STATUS_MISMATCH)가 ApiError로 온다.
        돌려주는 값은 {"dispatchId", "status": "PENDING"}이다.
        """
        return dict(await self._post(f"/v1/runs/{run_id}/dispatches/{dispatch_id}/resend"))

    async def resolve_publish_unknown(self, run_id: str, resolution: str, reason: str) -> dict[str, Any]:
        """PUBLISH_UNKNOWN을 PUBLISHED 또는 FAILED_PUBLISH로 확정한다.

        resolution은 "PUBLISHED" 또는 "FAILED_PUBLISH", reason은 5자 이상(이벤트에 남는다).
        이미 같은 상태로 확정된 run이면 API가 changed=False로 성공을 돌려주므로 다시 불러도 안전하다.
        PUBLISH_UNKNOWN이 아닌 다른 상태면 409(RUN_STATUS_MISMATCH)가 ApiError로 온다.
        돌려주는 값은 {"runStatus", "changed"}이다.
        """
        return dict(await self._post(f"/v1/runs/{run_id}/publish-unknown/resolve",
                                     {"resolution": resolution, "reason": reason}))
