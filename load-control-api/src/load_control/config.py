from datetime import timedelta
from functools import lru_cache
from typing import Literal

from pydantic import AnyHttpUrl, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """환경변수 `LCA_*`로 주입하는 설정. API 설계 9.4."""

    model_config = SettingsConfigDict(env_prefix="LCA_", env_file=".env", extra="ignore")

    database_url: SecretStr  # postgresql+asyncpg://load_control_api@meta:5432/nifiops
    listen_dsn: SecretStr | None = None  # worker 전용: postgresql://... (asyncpg LISTEN)
    db_pool_size: int = 10
    db_max_overflow: int = 5
    db_tx_attempts: int = 3

    # role → SHA-256 hex digest 목록. 예: {"nifi": ["..."], "operator": ["..."]}
    token_digests: dict[str, list[str]] = {}

    nifi_receiver_url: AnyHttpUrl | None = None  # worker 전용: NiFi LB의 PG-05 수신 주소
    nifi_client_cert: str | None = None
    nifi_client_key: str | None = None
    nifi_ca_bundle: str | None = None
    nifi_timeout_seconds: float = 10.0

    run_timeout: timedelta = timedelta(hours=6)
    extract_query_timeout: timedelta = timedelta(minutes=60)
    recovery_stale: timedelta = timedelta(minutes=90)
    recovery_mode: Literal["FAIL", "REISSUE"] = "FAIL"
    recovery_max_attempts: int = 3  # REISSUE 모드에서 이 횟수를 넘으면 run TIMED_OUT
    validation_stale: timedelta = timedelta(hours=2)
    publish_stale: timedelta = timedelta(hours=2)

    dispatch_max_attempts: int = 20
    dispatch_backoff_min: timedelta = timedelta(seconds=5)
    dispatch_backoff_max: timedelta = timedelta(minutes=5)
    dispatch_ack_timeout: timedelta = timedelta(minutes=10)
    dispatch_lease: timedelta = timedelta(seconds=60)
    dispatch_poll_interval: timedelta = timedelta(seconds=5)
    dispatch_batch: int = 20
    sweeper_interval: timedelta = timedelta(minutes=1)

    worker_metrics_port: int | None = 9100  # worker 프로세스 /metrics 포트, None이면 끔

    log_level: str = "INFO"
    log_json: bool = True


    @model_validator(mode="after")
    def _check_timeouts(self) -> "Settings":
        # heartbeat는 쿼리 실행 중 갱신되지 않으므로 stale 기준은 query timeout보다 커야 한다(가이드 13.1).
        if self.recovery_stale <= self.extract_query_timeout:
            raise ValueError("LCA_RECOVERY_STALE must be greater than LCA_EXTRACT_QUERY_TIMEOUT")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
