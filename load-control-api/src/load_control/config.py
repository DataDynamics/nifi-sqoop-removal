"""설정: config.yaml(API 설계 9.4).

로드 순서(앞이 우선): 생성자 인자 > 환경변수 > config.yaml > 기본값.
- 파일 위치: LCA_CONFIG 환경변수, 없으면 현재 디렉터리의 config.yaml.
- 환경변수 덮어쓰기는 비밀값 주입용이다. 섹션 구분자는 '__'이다. 예: LCA_DATABASE__URL
- 모르는 키는 거부한다(오타 방지).
"""

import os
from contextvars import ContextVar
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

CONFIG_ENV = "LCA_CONFIG"
DEFAULT_CONFIG_FILE = "config.yaml"

_config_file: ContextVar[Path | None] = ContextVar("lca_config_file", default=None)


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Section(BaseModel):
    """설정 섹션 공통 기반. 모르는 키(오타)가 있으면 검증 오류로 시작을 거부한다."""

    model_config = ConfigDict(extra="forbid")


class ServerTlsSettings(Section):
    """API 서버가 TLS를 직접 종료할지. 보통은 LB/ingress에서 종료하므로 기본은 off(API 설계 9.7)."""

    enabled: bool = False
    certfile: str | None = None  # 서버 인증서(PEM)
    keyfile: str | None = None  # 서버 개인키(PEM)
    ca_certs: str | None = None  # 클라이언트 인증서를 검증할 CA(mTLS)
    client_cert_required: bool = False  # true면 mTLS(클라이언트 인증서 필수)

    @model_validator(mode="after")
    def _check(self) -> "ServerTlsSettings":
        if not self.enabled:
            if self.client_cert_required:
                raise ValueError("server.tls.client_cert_required needs server.tls.enabled: true")
            return self
        if not (self.certfile and self.keyfile):
            raise ValueError("server.tls.enabled needs server.tls.certfile and server.tls.keyfile")
        if self.client_cert_required and not self.ca_certs:
            raise ValueError("server.tls.client_cert_required needs server.tls.ca_certs")
        return self


class ServerSettings(Section):
    """API HTTP 서버(uvicorn) 설정. `python -m load_control.server`가 읽는다."""

    host: str = "0.0.0.0"  # bind address. 같은 호스트의 프록시 뒤에만 둔다면 127.0.0.1
    port: int = Field(default=8080, ge=1, le=65535)
    workers: int = Field(default=1, ge=1)  # 프로세스 수. 1보다 크면 메트릭은 프로세스별로 나뉜다
    root_path: str = ""  # 리버스 프록시가 경로 접두사를 붙일 때(예: /load-control)
    proxy_headers: bool = True  # X-Forwarded-For/Proto를 신뢰할지
    forwarded_allow_ips: str = "127.0.0.1"  # proxy_headers를 신뢰할 프록시 IP 목록(쉼표 구분, "*" 가능)
    timeout_keep_alive: int = Field(default=5, ge=1)  # 초
    timeout_graceful_shutdown: int = Field(default=30, ge=1)  # SIGTERM 후 진행 중 요청을 기다리는 초
    limit_concurrency: int | None = Field(default=None, ge=1)  # 프로세스당 동시 연결 상한, 초과 시 503
    tls: ServerTlsSettings = Field(default_factory=ServerTlsSettings)


class DatabaseSettings(Section):
    """관리 DB(PostgreSQL nifi_ops) 연결."""

    url: SecretStr  # postgresql+asyncpg://load_control_api@meta:5432/nifiops (API 런타임 계정)
    migration_url: SecretStr | None = None  # alembic 전용 DDL 계정. 없으면 url 사용
    listen_dsn: SecretStr | None = None  # worker LISTEN 전용: postgresql://... (asyncpg 직접 연결)
    pool_size: int = Field(default=10, ge=1)
    max_overflow: int = Field(default=5, ge=0)
    tx_attempts: int = Field(default=3, ge=1)


class AuthSettings(Section):
    """Bearer 토큰 인증. 토큰 원문은 두지 않고 SHA-256 digest만 둔다."""

    # role → Bearer 토큰 SHA-256 hex digest 목록. 생성: python -m load_control.security <token>
    token_digests: dict[Literal["nifi", "operator"], list[str]] = Field(default_factory=dict)


class NifiTlsSettings(Section):
    """worker → NiFi 호출의 TLS. NiFi PG-05가 TLS를 쓰지 않으면 enabled: false(기본)."""

    enabled: bool = False
    # false면 NiFi 서버 인증서 검증(체인·호스트 이름)을 건너뛴다. 자체 서명 인증서를 쓰는 개발·PoC용이며,
    # 중간자 공격을 막지 못하므로 운영에서는 ca_bundle로 검증한다.
    verify: bool = True
    ca_bundle: str | None = None  # NiFi 서버 인증서를 검증할 CA(PEM). 없으면 시스템 기본 CA
    client_cert: str | None = None  # NiFi가 클라이언트 인증서(mTLS)를 요구할 때(PEM)
    client_key: str | None = None

    @model_validator(mode="after")
    def _check(self) -> "NifiTlsSettings":
        if bool(self.client_cert) != bool(self.client_key):
            raise ValueError("nifi.tls.client_cert and nifi.tls.client_key must be set together")
        if not self.enabled and (self.ca_bundle or self.client_cert):
            raise ValueError("nifi.tls.ca_bundle/client_cert are set but nifi.tls.enabled is false")
        return self


class NifiSettings(Section):
    """worker가 NiFi PG-05 Control Receiver를 호출할 때 쓰는 설정."""

    receiver_url: AnyHttpUrl | None = None  # worker 전용: NiFi LB의 PG-05 주소. TLS off면 http, on이면 https
    timeout_seconds: float = Field(default=10.0, gt=0)
    tls: NifiTlsSettings = Field(default_factory=NifiTlsSettings)

    @model_validator(mode="after")
    def _check_scheme(self) -> "NifiSettings":
        # 설정 실수(https 주소인데 TLS off 등)를 시작 단계에서 막는다.
        if self.receiver_url is not None:
            expected = "https" if self.tls.enabled else "http"
            if self.receiver_url.scheme != expected:
                raise ValueError(f"nifi.receiver_url must use {expected}:// when nifi.tls.enabled is "
                                 f"{str(self.tls.enabled).lower()}")
        return self


class RecoverySettings(Section):
    """sweeper의 stale·timeout 기준(가이드 13.1, API 설계 7장)."""

    run_timeout: timedelta = timedelta(hours=6)
    extract_query_timeout: timedelta = timedelta(minutes=60)  # NiFi EXTRACT.QUERY.TIMEOUT과 같은 값
    stale: timedelta = timedelta(minutes=90)
    mode: Literal["FAIL", "REISSUE"] = "FAIL"
    max_attempts: int = Field(default=3, ge=1)  # REISSUE 모드에서 이 횟수에 도달하면 run TIMED_OUT
    validation_stale: timedelta = timedelta(hours=2)
    publish_stale: timedelta = timedelta(hours=2)
    sweeper_interval: timedelta = timedelta(minutes=1)

    @model_validator(mode="after")
    def _check_stale(self) -> "RecoverySettings":
        # heartbeat는 쿼리 실행 중 갱신되지 않으므로 stale 기준은 query timeout보다 커야 한다(가이드 13.1).
        if self.stale <= self.extract_query_timeout:
            raise ValueError("recovery.stale must be greater than recovery.extract_query_timeout")
        return self


class DispatchSettings(Section):
    """outbox(load_dispatch) 전달 정책(API 설계 4장)."""

    max_attempts: int = Field(default=20, ge=1)
    backoff_min: timedelta = timedelta(seconds=5)
    backoff_max: timedelta = timedelta(minutes=5)
    ack_timeout: timedelta = timedelta(minutes=10)
    lease: timedelta = timedelta(seconds=60)
    poll_interval: timedelta = timedelta(seconds=5)
    batch: int = Field(default=20, ge=1)


class WorkerSettings(Section):
    """worker 프로세스(dispatcher, sweeper) 설정."""

    metrics_host: str = "0.0.0.0"  # /metrics bind address
    metrics_port: int | None = Field(default=9100, ge=1, le=65535)  # null이면 /metrics를 열지 않는다


class LogFileSettings(Section):
    """파일 로그. 크기 기준으로 회전한다."""

    path: Path
    max_bytes: int = Field(default=100 * 1024 * 1024, ge=1024)
    backup_count: int = Field(default=10, ge=0)


def _default_logger_levels() -> dict[str, LogLevel]:
    return {"sqlalchemy.engine": "WARNING", "asyncpg": "WARNING", "httpx": "WARNING",
            "httpcore": "WARNING", "uvicorn.error": "INFO", "alembic": "INFO"}


class LoggingSettings(Section):
    """로그 설정. structlog 이벤트와 stdlib 로그(uvicorn, SQLAlchemy 등)를 같은 형식으로 낸다."""

    level: LogLevel = "INFO"  # root 수준
    format: Literal["json", "console"] = "json"  # 운영은 json, 개발은 console
    stdout: bool = True  # 표준출력으로 낼지(컨테이너 수집용)
    file: LogFileSettings | None = None  # 파일로도 낼 때. 파일은 항상 json
    access_log: bool = True  # API 요청마다 access 로그(구조화)를 남길지
    # logger별 수준. 예: SQL을 보려면 sqlalchemy.engine: INFO
    loggers: dict[str, LogLevel] = Field(default_factory=_default_logger_levels)

    @model_validator(mode="after")
    def _check_outputs(self) -> "LoggingSettings":
        if not self.stdout and self.file is None:
            raise ValueError("logging needs at least one output: stdout or file")
        return self


class Settings(BaseSettings):
    """전체 설정. `Settings.load()`로 config.yaml을 읽는다. 테스트는 생성자 인자로 직접 만든다."""

    model_config = SettingsConfigDict(env_prefix="LCA_", env_nested_delimiter="__", extra="forbid")

    server: ServerSettings = Field(default_factory=ServerSettings)
    database: DatabaseSettings
    auth: AuthSettings = Field(default_factory=AuthSettings)
    nifi: NifiSettings = Field(default_factory=NifiSettings)
    recovery: RecoverySettings = Field(default_factory=RecoverySettings)
    dispatch: DispatchSettings = Field(default_factory=DispatchSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """설정 소스 순서(앞이 우선): 생성자 인자 > 환경변수 > config.yaml.

        config.yaml은 `load()`가 경로를 지정했을 때만 읽는다. 그래서 `Settings(...)`를 직접 만드는
        테스트는 개발자 PC의 config.yaml에 영향을 받지 않는다.
        """
        sources: list[PydanticBaseSettingsSource] = [init_settings, env_settings]
        path = _config_file.get()
        if path is not None:
            sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=path,
                                                    yaml_file_encoding="utf-8"))
        return tuple(sources)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Settings":
        """config.yaml을 읽는다. path가 없으면 LCA_CONFIG, 그것도 없으면 ./config.yaml."""
        config_path = Path(path or os.environ.get(CONFIG_ENV) or DEFAULT_CONFIG_FILE)
        if not config_path.is_file():
            raise FileNotFoundError(
                f"config file not found: {config_path} (set {CONFIG_ENV} or create {DEFAULT_CONFIG_FILE})")
        token = _config_file.set(config_path)
        try:
            return cls()
        finally:
            _config_file.reset(token)


@lru_cache
def get_settings() -> Settings:
    """프로세스당 한 번 config.yaml을 읽어 캐시한다."""
    return Settings.load()
