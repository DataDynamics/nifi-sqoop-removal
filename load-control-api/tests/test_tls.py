"""worker → NiFi TLS on/off, 인증서 검증 skip, CA 검증, mTLS를 실제 HTTPS 서버로 확인한다."""

import http.server
import json
import ssl
import subprocess
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from load_control.config import Settings
from load_control.worker.dispatcher import Dispatcher, make_client, nifi_ssl_context
from tests.conftest import Db, override
from tests.helpers import complete_run


@dataclass
class Certs:
    ca: Path
    server_cert: Path
    server_key: Path
    client_cert: Path
    client_key: Path
    self_signed_cert: Path
    self_signed_key: Path


def _openssl(*args: str) -> None:
    subprocess.run(["openssl", *args], check=True, capture_output=True)


@pytest.fixture(scope="session")
def certs(tmp_path_factory: pytest.TempPathFactory) -> Certs:
    """테스트용 CA, CA가 서명한 서버·클라이언트 인증서, 자체 서명 서버 인증서."""
    d = tmp_path_factory.mktemp("certs")
    ext = d / "san.ext"
    ext.write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=test-ca",
             "-keyout", str(d / "ca.key"), "-out", str(d / "ca.pem"))
    for name in ("server", "client"):
        _openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}",
                 "-keyout", str(d / f"{name}.key"), "-out", str(d / f"{name}.csr"))
        _openssl("x509", "-req", "-in", str(d / f"{name}.csr"), "-CA", str(d / "ca.pem"),
                 "-CAkey", str(d / "ca.key"), "-CAcreateserial", "-days", "1", "-extfile", str(ext),
                 "-out", str(d / f"{name}.pem"))
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost",
             "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
             "-keyout", str(d / "self.key"), "-out", str(d / "self.pem"))
    return Certs(d / "ca.pem", d / "server.pem", d / "server.key", d / "client.pem", d / "client.key",
                 d / "self.pem", d / "self.key")


class _Handler(http.server.BaseHTTPRequestHandler):
    received: ClassVar[list[dict[str, Any]]] = []

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        peer = self.connection.getpeercert() if isinstance(self.connection, ssl.SSLSocket) else None
        _Handler.received.append({"path": self.path, "body": json.loads(body), "peer": peer})
        self.send_response(202)
        self.end_headers()

    def log_message(self, *args: Any) -> None:
        pass


@dataclass
class Receiver:
    url: str
    received: list[dict[str, Any]]


def _serve(cert: Path, key: Path, ca: Path | None = None, require_client: bool = False) -> Iterator[Receiver]:
    """NiFi PG-05를 흉내 내는 HTTPS 서버(202 응답)."""
    _Handler.received = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    if require_client:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(ca)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield Receiver(f"https://localhost:{server.server_address[1]}", _Handler.received)
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def self_signed_receiver(certs: Certs) -> Iterator[Receiver]:
    yield from _serve(certs.self_signed_cert, certs.self_signed_key)


@pytest.fixture
def ca_receiver(certs: Certs) -> Iterator[Receiver]:
    yield from _serve(certs.server_cert, certs.server_key)


@pytest.fixture
def mtls_receiver(certs: Certs) -> Iterator[Receiver]:
    yield from _serve(certs.server_cert, certs.server_key, certs.ca, require_client=True)


def tls_settings(settings: Settings, url: str, **tls: Any) -> Settings:
    return override(settings, nifi={"receiver_url": url, "tls": {"enabled": True, **tls}})


async def dispatch_status(db: Db) -> dict[str, Any]:
    return await db.one("SELECT status, last_http_status, last_error FROM nifi_ops.load_dispatch")


# ---------------------------------------------------------------- 설정 검증

@pytest.mark.parametrize(("nifi", "message"), [
    ({"receiver_url": "https://n:1", "tls": {"enabled": False}}, "must use http://"),
    ({"receiver_url": "http://n:1", "tls": {"enabled": True}}, "must use https://"),
    ({"tls": {"enabled": True, "client_cert": "c.pem"}}, "must be set together"),
    ({"tls": {"enabled": False, "ca_bundle": "ca.pem"}}, "nifi.tls.enabled is false"),
])
def test_invalid_nifi_tls(settings: Settings, nifi: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        override(settings, nifi=nifi)


def test_ssl_context_modes(settings: Settings, certs: Certs) -> None:
    assert nifi_ssl_context(override(settings, nifi={"receiver_url": "http://n:1"})) is None
    skip = nifi_ssl_context(tls_settings(settings, "https://n:1", verify=False))
    assert skip is not None and skip.verify_mode == ssl.CERT_NONE and skip.check_hostname is False
    strict = nifi_ssl_context(tls_settings(settings, "https://n:1", ca_bundle=str(certs.ca)))
    assert strict is not None and strict.verify_mode == ssl.CERT_REQUIRED and strict.check_hostname is True


# ---------------------------------------------------------------- 실제 HTTPS 전송

async def test_tls_verify_skipped_with_self_signed(
        client: httpx.AsyncClient, db: Db, engine: AsyncEngine, settings: Settings,
        self_signed_receiver: Receiver) -> None:
    """verify: false면 자체 서명 인증서의 NiFi에도 보낸다."""
    run, dispatch_id = await complete_run(client)
    s = tls_settings(settings, self_signed_receiver.url, verify=False)
    async with make_client(s) as http_client:
        assert await Dispatcher(s, engine, http_client).dispatch_once() == 1
    assert (await dispatch_status(db))["status"] == "SENT"
    assert self_signed_receiver.received[0]["body"] == {"runId": run.run_id, "dispatchId": dispatch_id}
    assert self_signed_receiver.received[0]["path"] == "/validate/ORACLE_INSP_DTL_DAILY"


async def test_tls_verify_rejects_self_signed(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                              settings: Settings, self_signed_receiver: Receiver) -> None:
    """verify: true(기본)면 신뢰할 수 없는 인증서를 거부하고 재시도로 남긴다."""
    await complete_run(client)
    s = tls_settings(settings, self_signed_receiver.url)
    async with make_client(s) as http_client:
        await Dispatcher(s, engine, http_client).dispatch_once()
    row = await dispatch_status(db)
    assert row["status"] == "PENDING" and "CERTIFICATE_VERIFY_FAILED" in row["last_error"]
    assert self_signed_receiver.received == []


async def test_tls_verify_with_ca_bundle(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                         settings: Settings, certs: Certs, ca_receiver: Receiver) -> None:
    await complete_run(client)
    s = tls_settings(settings, ca_receiver.url, ca_bundle=str(certs.ca))
    async with make_client(s) as http_client:
        assert await Dispatcher(s, engine, http_client).dispatch_once() == 1
    assert (await dispatch_status(db))["status"] == "SENT"


async def test_mtls_client_certificate(client: httpx.AsyncClient, db: Db, engine: AsyncEngine,
                                       settings: Settings, certs: Certs, mtls_receiver: Receiver) -> None:
    await complete_run(client)
    without = tls_settings(settings, mtls_receiver.url, ca_bundle=str(certs.ca))
    async with make_client(without) as http_client:
        await Dispatcher(without, engine, http_client).dispatch_once()
    assert (await dispatch_status(db))["status"] == "PENDING"  # 클라이언트 인증서가 없어 handshake 실패

    await db.execute("UPDATE nifi_ops.load_dispatch SET next_attempt_at = clock_timestamp()")
    with_cert = tls_settings(settings, mtls_receiver.url, ca_bundle=str(certs.ca),
                             client_cert=str(certs.client_cert), client_key=str(certs.client_key))
    async with make_client(with_cert) as http_client:
        assert await Dispatcher(with_cert, engine, http_client).dispatch_once() == 1
    assert (await dispatch_status(db))["status"] == "SENT"
    subject = dict(x[0] for x in mtls_receiver.received[0]["peer"]["subject"])
    assert subject["commonName"] == "client"
