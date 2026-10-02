"""모니터가 쓰는 조회 API 클라이언트."""

from typing import Any

import httpx


class ApiError(Exception):
    """API 호출 실패(연결, 인증, 5xx)."""


class MonitorClient:
    """Load Control API 조회 전용 클라이언트."""

    def __init__(self, base_url: str, token: str | None, *, operator_token: str | None = None,
                 timeout: float = 5.0, transport: httpx.AsyncBaseTransport | None = None) -> None:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.base_url = base_url.rstrip("/")
        op = operator_token or token
        self._operator_headers = {"Authorization": f"Bearer {op}"} if op else {}
        self._client = httpx.AsyncClient(base_url=self.base_url, headers=headers, timeout=timeout,
                                         transport=transport)

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, **params: Any) -> Any:
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
        try:
            r = await self._client.get("/readyz")
        except httpx.HTTPError:
            return False
        return r.status_code == 200

    async def summary(self) -> dict[str, Any]:
        return dict(await self._get("/v1/monitor/summary"))

    async def runs(self, *, limit: int = 100, status: str | None = None,
                   job_key: str | None = None) -> list[dict[str, Any]]:
        return list(await self._get("/v1/runs", limit=limit, status=status, jobKey=job_key))

    async def run(self, run_id: str) -> dict[str, Any]:
        return dict(await self._get(f"/v1/runs/{run_id}"))

    async def validations(self, run_id: str) -> list[dict[str, Any]]:
        return list((await self._get(f"/v1/runs/{run_id}/validations"))["metrics"])

    async def events(self, run_id: str, limit: int = 300) -> list[dict[str, Any]]:
        return list((await self._get(f"/v1/runs/{run_id}/events", limit=limit))["events"])

    # 운영 작업(operator 토큰). API가 이벤트와 로그에 남긴다.
    async def _post(self, path: str, body: dict[str, Any] | None = None) -> Any:
        try:
            r = await self._client.post(path, json=body, headers=self._operator_headers)
        except httpx.HTTPError as e:
            raise ApiError(f"API 연결 실패: {e.__class__.__name__}") from e
        if r.status_code in (401, 403):
            raise ApiError(f"권한 없음({r.status_code}): operator 토큰이 필요합니다(monitor.operator_token)")
        if r.status_code >= 400:
            try:
                detail = r.json()
                msg = f"{detail.get('code')}: {detail.get('message')}"
            except ValueError:
                msg = r.text[:200]
            raise ApiError(f"API 거부 {r.status_code} {msg}")
        return r.json()

    async def resend_dispatch(self, run_id: str, dispatch_id: str) -> dict[str, Any]:
        """DEAD 또는 ACK 없는 SENT dispatch를 PENDING으로 되돌려 다시 보내게 한다."""
        return dict(await self._post(f"/v1/runs/{run_id}/dispatches/{dispatch_id}/resend"))

    async def resolve_publish_unknown(self, run_id: str, resolution: str, reason: str) -> dict[str, Any]:
        """PUBLISH_UNKNOWN을 PUBLISHED 또는 FAILED_PUBLISH로 확정한다."""
        return dict(await self._post(f"/v1/runs/{run_id}/publish-unknown/resolve",
                                     {"resolution": resolution, "reason": reason}))
