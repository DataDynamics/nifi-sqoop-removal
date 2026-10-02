"""모니터가 쓰는 조회 API 클라이언트."""

from typing import Any

import httpx


class ApiError(Exception):
    """API 호출 실패(연결, 인증, 5xx)."""


class MonitorClient:
    """Load Control API 조회 전용 클라이언트."""

    def __init__(self, base_url: str, token: str | None, *, timeout: float = 5.0,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.base_url = base_url.rstrip("/")
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
