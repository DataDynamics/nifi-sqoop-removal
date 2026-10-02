import json
import logging
import re
import ssl
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import structlog
from pydantic import ValidationError

from load_control.config import LoggingSettings, Settings
from load_control.logging import configure_logging
from load_control.main import create_app
from load_control.server import uvicorn_options
from tests.conftest import NIFI_TOKEN, override


def read_json_lines(path: Path) -> list[dict[str, Any]]:
    for h in logging.getLogger().handlers:
        h.flush()
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    yield
    configure_logging(LoggingSettings(level="WARNING", format="console"))


def test_file_output_and_logger_levels(tmp_path: Path) -> None:
    log_file = tmp_path / "logs" / "lca.log"
    levels = {"sqlalchemy.engine": "WARNING", "load_control.noisy": "ERROR"}
    configure_logging(LoggingSettings(level="INFO", stdout=False, file={"path": log_file, "format": "json"},  # type: ignore[arg-type]
                                      loggers=levels))  # type: ignore[arg-type]
    structlog.get_logger("load_control.test").info("hello", runId="r-1", 한글="값")
    structlog.get_logger("load_control.noisy").warning("suppressed")
    logging.getLogger("sqlalchemy.engine").info("SELECT 1")        # WARNING 미만이라 버려짐
    logging.getLogger("sqlalchemy.engine").warning("slow query")   # stdlib 로그도 같은 json 형식
    lines = read_json_lines(log_file)
    events = [line["event"] for line in lines]
    assert events == ["hello", "slow query"]
    hello = lines[0]
    assert hello["runId"] == "r-1" and hello["한글"] == "값"
    assert hello["level"] == "info" and hello["logger"] == "load_control.test" and "timestamp" in hello
    assert lines[1]["logger"] == "sqlalchemy.engine"


def test_reconfigure_does_not_duplicate_handlers(tmp_path: Path) -> None:
    cfg = LoggingSettings(stdout=True, file={"path": tmp_path / "a.log"})  # type: ignore[arg-type]
    configure_logging(cfg)
    configure_logging(cfg)
    assert len(logging.getLogger().handlers) == 2


def test_exception_is_logged_as_structured_traceback(tmp_path: Path) -> None:
    log_file = tmp_path / "e.log"
    configure_logging(LoggingSettings(stdout=False, file={"path": log_file, "format": "json"}))  # type: ignore[arg-type]
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        structlog.get_logger("load_control.test").exception("failed")
    line = read_json_lines(log_file)[0]
    assert line["event"] == "failed" and line["exception"][0]["exc_type"] == "RuntimeError"


@pytest.mark.parametrize("access_log", [True, False])
async def test_access_log(tmp_path: Path, settings: Settings, engine: Any, access_log: bool) -> None:
    log_file = tmp_path / "access.log"
    s = override(settings, logging={"level": "INFO", "stdout": False, "access_log": access_log,
                                    "file": {"path": log_file, "format": "json"}})
    app = create_app(s)
    async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test",
            headers={"Authorization": f"Bearer {NIFI_TOKEN}"}) as c:
        await c.get("/healthz")  # 조용한 경로
        r = await c.get("/v1/runs/00000000-0000-0000-0000-000000000000", headers={"X-Request-Id": "req-42"})
        assert r.status_code == 404
    lines = read_json_lines(log_file)
    access = [line for line in lines if line.get("logger") == "load_control.access"]
    assert any(line["event"] == "api_started" for line in lines)
    if not access_log:
        assert access == []
        return
    assert [a["event"] for a in access] == ["api_request", "api_response"]  # /healthz는 남기지 않는다
    req, entry = access
    run_id = "00000000-0000-0000-0000-000000000000"
    assert req["path"] == f"/v1/runs/{run_id}" and req["runId"] == run_id
    assert req["message"].startswith("API 요청 수신")
    assert entry["endpoint"] == "/v1/runs/{run_id}" and entry["httpStatus"] == 404
    assert entry["requestId"] == "req-42" and entry["level"] == "warning" and entry["role"] == "nifi"
    assert "RUN_NOT_FOUND" in entry["responseBody"] and entry["runId"] == run_id
    api_error = next(line for line in lines if line["event"] == "api_error")
    assert api_error["requestId"] == "req-42" and api_error["code"] == "RUN_NOT_FOUND"
    assert api_error["runId"] == run_id  # 서비스 로그에도 경로의 runId가 붙는다


async def test_secrets_not_logged_on_startup(tmp_path: Path, settings: Settings) -> None:
    log_file = tmp_path / "s.log"
    s = override(settings, logging={"level": "INFO", "stdout": False,
                                    "file": {"path": log_file, "format": "json"}},
                 database={"url": "postgresql+asyncpg://u:SuperSecret@db.example:5432/lca"})
    app = create_app(s)
    async with app.router.lifespan_context(app):  # 엔진은 지연 연결이라 실제로 접속하지 않는다
        pass
    text = log_file.read_text(encoding="utf-8")
    started = next(line for line in read_json_lines(log_file) if line["event"] == "api_started")
    assert started["dbHost"] == "db.example" and started["dbName"] == "lca"
    assert "SuperSecret" not in text


def test_text_format_timestamp_and_korean_message(tmp_path: Path) -> None:
    log_file = tmp_path / "t.log"
    configure_logging(LoggingSettings(level="INFO", stdout=False,
                                      file={"path": log_file}))  # type: ignore[arg-type]
    structlog.get_logger("load_control.services.runs").info(
        "run_created", runId="r-1", jobKey="JOB_A", businessKey="2026-09-28", note="공백 있는 값")
    structlog.get_logger("load_control.x").info("unmapped_event", n=1)
    for h in logging.getLogger().handlers:
        h.flush()
    first, second = log_file.read_text(encoding="utf-8").splitlines()
    assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} INFO  "
                    r"\[load_control\.services\.runs\] ", first)
    assert "run 생성: JOB_A 업무일자 2026-09-28 (run_created)" in first
    assert "runId=r-1" in first and 'note="공백 있는 값"' in first
    assert second.endswith("[load_control.x] unmapped_event n=1")


def test_file_path_service_placeholder(tmp_path: Path) -> None:
    cfg = LoggingSettings(stdout=False, file={"path": tmp_path / "{service}.log"})  # type: ignore[arg-type]
    configure_logging(cfg, service="worker")
    structlog.get_logger("load_control.worker").info("worker_started")
    for h in logging.getLogger().handlers:
        h.flush()
    assert "worker 시작" in (tmp_path / "worker.log").read_text(encoding="utf-8")
    assert not (tmp_path / "server.log").exists()


async def test_access_log_request_body(tmp_path: Path, settings: Settings, engine: Any) -> None:
    log_file = tmp_path / "b.log"
    s = override(settings, logging={"level": "INFO", "stdout": False, "access_body_max": 40,
                                    "file": {"path": log_file, "format": "json"}})
    app = create_app(s)
    async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test",
            headers={"Authorization": f"Bearer {NIFI_TOKEN}"}) as c:
        r = await c.post("/v1/runs", json={"jobKey": "ORACLE_INSP_DTL_DAILY", "businessKey": "2026-09-30",
                                           "hdfsRoot": "/data/nifi/stage", "stageTablePrefix": "TMP_"})
        assert r.status_code == 200 and r.json()["runId"]  # 응답 본문을 읽고 다시 만들어도 그대로다
    lines = read_json_lines(log_file)
    req = next(line for line in lines if line["event"] == "api_request")
    res = next(line for line in lines if line["event"] == "api_response")
    assert req["requestBody"].startswith('{"jobKey":"ORACLE_INSP_DTL_DAILY"')
    assert "...(+" in req["requestBody"]  # access_body_max에서 잘림
    assert res["httpStatus"] == 200 and res["responseBody"].startswith('{"runId"')
    created = next(line for line in lines if line["event"] == "run_created")
    assert created["requestId"] == req["requestId"] and created["message"].startswith("run 생성")
    assert "Bearer" not in log_file.read_text(encoding="utf-8")


def test_uvicorn_options(settings: Settings) -> None:
    s = override(settings, server={"host": "127.0.0.1", "port": 9000, "workers": 3, "root_path": "/lca"})
    opts = uvicorn_options(s)
    assert opts["host"] == "127.0.0.1" and opts["port"] == 9000 and opts["workers"] == 3
    assert opts["root_path"] == "/lca" and opts["log_config"] is None and opts["access_log"] is False
    assert "ssl_certfile" not in opts
    tls = override(settings, server={"ssl_certfile": "c.pem", "ssl_keyfile": "k.pem",
                                     "ssl_ca_certs": "ca.pem", "ssl_client_cert_required": True})
    opts = uvicorn_options(tls)
    assert opts["ssl_certfile"] == "c.pem" and opts["ssl_cert_reqs"] == ssl.CERT_REQUIRED


@pytest.mark.parametrize("section", [
    {"server": {"ssl_certfile": "c.pem"}},                                    # key 없음
    {"server": {"ssl_certfile": "c", "ssl_keyfile": "k", "ssl_client_cert_required": True}},  # CA 없음
    {"server": {"port": 70000}},
    {"logging": {"stdout": False}},                                           # 출력 없음
    {"logging": {"loggers": {"x": "VERBOSE"}}},
])
def test_invalid_server_logging_settings(settings: Settings, section: dict[str, dict[str, Any]]) -> None:
    with pytest.raises(ValidationError):
        override(settings, **section)
