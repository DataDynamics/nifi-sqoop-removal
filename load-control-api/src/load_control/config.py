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


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DatabaseSettings(Section):
    url: SecretStr  # postgresql+asyncpg://load_control_api@meta:5432/nifiops (API 런타임 계정)
    migration_url: SecretStr | None = None  # alembic 전용 DDL 계정. 없으면 url 사용
    listen_dsn: SecretStr | None = None  # worker LISTEN 전용: postgresql://... (asyncpg 직접 연결)
    pool_size: int = Field(default=10, ge=1)
    max_overflow: int = Field(default=5, ge=0)
    tx_attempts: int = Field(default=3, ge=1)


class AuthSettings(Section):
    # role → Bearer 토큰 SHA-256 hex digest 목록. 생성: python -m load_control.security <token>
    token_digests: dict[Literal["nifi", "operator"], list[str]] = Field(default_factory=dict)


class NifiSettings(Section):
    receiver_url: AnyHttpUrl | None = None  # worker 전용: NiFi LB의 PG-05 수신 주소
    client_cert: str | None = None
    client_key: str | None = None
    ca_bundle: str | None = None
    timeout_seconds: float = Field(default=10.0, gt=0)


class RecoverySettings(Section):
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
    max_attempts: int = Field(default=20, ge=1)
    backoff_min: timedelta = timedelta(seconds=5)
    backoff_max: timedelta = timedelta(minutes=5)
    ack_timeout: timedelta = timedelta(minutes=10)
    lease: timedelta = timedelta(seconds=60)
    poll_interval: timedelta = timedelta(seconds=5)
    batch: int = Field(default=20, ge=1)


class WorkerSettings(Section):
    metrics_port: int | None = 9100  # worker 프로세스 /metrics 포트, null이면 끔


class LoggingSettings(Section):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "json"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LCA_", env_nested_delimiter="__", extra="forbid")

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
    return Settings.load()
