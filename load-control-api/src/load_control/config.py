"""`config.yaml` 기반 애플리케이션 설정.

설정값은 `생성자 인자 > 환경 변수 > config.yaml > 기본값` 순서로 우선한다.

- 설정 파일은 `LCA_CONFIG`가 가리키는 경로에서 읽는다. 기본값은 `config/config.yaml`이다.
- 환경 변수의 중첩 구분자는 `__`이다. 예: `LCA_DATABASE__URL`.
- 알 수 없는 키는 오타로 보고 거부한다.
- 기간은 ISO 8601 형식(예: `PT90M`, `PT6H`) 또는 초 단위 숫자로 입력한다.

server와 worker는 같은 파일을 읽으며, 각 설정 클래스에 사용하는 프로세스를 명시한다.
"""

import os
from contextvars import ContextVar
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

CONFIG_ENV = "LCA_CONFIG"
DEFAULT_CONFIG_FILE = "config/config.yaml"  # 설치 디렉터리(bin 스크립트의 작업 디렉터리) 기준

# `Settings.load()`가 선택한 파일 경로를 `settings_customise_sources()`에 전달한다. 클래스 변수 대신
# `ContextVar`를 사용해 현재 호출에만 경로가 보이게 하고 병렬 테스트 사이의 간섭을 막는다.
_config_file: ContextVar[Path | None] = ContextVar("lca_config_file", default=None)


# 로그 수준 이름. stdlib logging.setLevel()이 그대로 받는 문자열이다.
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Section(BaseModel):
    """설정 섹션 공통 기반. 모르는 키(오타)가 있으면 검증 오류로 시작을 거부한다."""

    model_config = ConfigDict(extra="forbid")


class ServerSettings(Section):
    """API HTTP 서버(uvicorn) 설정. `python -m load_control.server`가 읽는다."""

    host: str = "0.0.0.0"  # 바인딩 주소. 같은 호스트의 프록시만 접근하면 127.0.0.1 사용
    port: int = Field(default=8080, ge=1, le=65535)  # 수신 포트. 모니터의 기본 API 주소에도 사용
    workers: int = Field(default=1, ge=1)  # 프로세스 수. 1보다 크면 메트릭은 프로세스별로 나뉜다
    root_path: str = ""  # 리버스 프록시가 경로 접두사를 붙일 때(예: /load-control)
    proxy_headers: bool = True  # `X-Forwarded-For`와 `X-Forwarded-Proto` 헤더 신뢰 여부
    forwarded_allow_ips: str = "127.0.0.1"  # 신뢰할 프록시 IP. 쉼표로 구분하며 `*`도 허용
    timeout_keep_alive: int = Field(default=5, ge=1)  # 초
    timeout_graceful_shutdown: int = Field(default=30, ge=1)  # SIGTERM 후 진행 중 요청을 기다리는 초
    limit_concurrency: int | None = Field(default=None, ge=1)  # 프로세스당 동시 연결 상한, 초과 시 503
    # TLS를 앱에서 직접 종료할 때만 설정한다. 보통은 LB/ingress에서 mTLS를 종료한다.
    ssl_certfile: str | None = None  # 서버 인증서(PEM) 경로
    ssl_keyfile: str | None = None  # 서버 개인키(PEM) 경로. ssl_certfile과 함께 설정해야 한다
    ssl_ca_certs: str | None = None  # 클라이언트 인증서 검증용 CA
    ssl_client_cert_required: bool = False  # true면 mTLS(클라이언트 인증서 필수)

    @model_validator(mode="after")
    def _check_tls(self) -> "ServerSettings":
        """TLS 설정 조합을 검사한다.

        인증서와 개인키는 둘 다 있거나 둘 다 없어야 한다. mTLS(ssl_client_cert_required)는
        서버 인증서와 클라이언트 검증용 CA가 모두 있어야 한다. 어긋나면 시작을 거부한다.
        """
        if bool(self.ssl_certfile) != bool(self.ssl_keyfile):
            raise ValueError("server.ssl_certfile and server.ssl_keyfile must be set together")
        if self.ssl_client_cert_required and not (self.ssl_certfile and self.ssl_ca_certs):
            raise ValueError(
                "server.ssl_client_cert_required needs ssl_certfile, ssl_keyfile and ssl_ca_certs")
        return self


class DatabaseSettings(Section):
    """server, worker 및 Alembic이 공유하는 관리 DB 연결 설정."""

    # URL에 비밀번호가 포함되므로 `config.yaml`은 권한 600으로 관리한다. 로그에는 호스트와 DB 이름만 남긴다.
    url: str  # postgresql+asyncpg://load_control_api:<pw>@meta:5432/nifiops (API 런타임 계정)
    migration_url: str | None = None  # alembic 전용 DDL 계정. 없으면 url 사용
    # dispatcher가 PostgreSQL `LISTEN`을 유지할 때 쓰는 asyncpg 전용 DSN이다. 설정하지 않으면
    # `pg_notify` 알림 없이 `dispatch.poll_interval` 주기로만 조회한다.
    listen_dsn: str | None = None
    pool_size: int = Field(default=10, ge=1)  # 프로세스당 SQLAlchemy 연결 pool 상시 크기
    max_overflow: int = Field(default=5, ge=0)  # pool_size를 넘어 잠시 더 열 수 있는 연결 수
    # deadlock·serialization 실패 시 트랜잭션 전체를 다시 실행하는 최대 횟수(첫 시도 포함).
    # API 요청(routers.deps.run_tx)에 적용된다.
    tx_attempts: int = Field(default=3, ge=1)


class AuthSettings(Section):
    """Bearer 토큰 인증(server). 토큰 원문은 두지 않고 SHA-256 digest만 둔다."""

    # role → Bearer 토큰 SHA-256 hex digest 목록. 생성: python -m load_control.security <token>
    # 토큰 교체 기간에는 이전·신규 digest를 함께 둔다. 비어 있으면 모든 API 호출이 401/403이다.
    token_digests: dict[Literal["nifi", "operator"], list[str]] = Field(default_factory=dict)


class NifiSettings(Section):
    """worker가 NiFi PG-05 Control Receiver를 호출할 때 쓰는 설정."""

    # worker 전용: NiFi LB의 PG-05 수신 주소. 호출 URL은 {receiver_url}/validate|reissue/{jobKey}.
    # 없으면 worker가 시작하지 않는다(server는 쓰지 않는다).
    receiver_url: AnyHttpUrl | None = None
    client_cert: str | None = None  # mTLS 클라이언트 인증서(PEM). client_key와 둘 다 있을 때만 쓴다
    client_key: str | None = None  # mTLS 클라이언트 개인키(PEM)
    ca_bundle: str | None = None  # NiFi 서버 인증서 검증용 CA. 없으면 시스템 기본 CA로 검증한다
    timeout_seconds: float = Field(default=10.0, gt=0)  # NiFi 호출 한 번의 HTTP timeout(초)


class RecoverySettings(Section):
    """worker의 sweeper가 정체와 시간 초과를 판단하는 기준."""

    # CREATED·EXTRACTING run이 시작(started_at) 후 이 시간을 넘기면 TIMED_OUT으로 끝낸다.
    run_timeout: timedelta = timedelta(hours=6)
    extract_query_timeout: timedelta = timedelta(minutes=60)  # NiFi EXTRACT.QUERY.TIMEOUT과 같은 값
    # RUNNING 파티션의 heartbeat(claim·chunk 보고 때 갱신)가 이보다 오래되면 멈춘 것으로 본다.
    stale: timedelta = timedelta(minutes=90)
    # `FAIL`은 run과 미완료 파티션을 `TIMED_OUT`으로 끝낸다. `REISSUE`는 정체된 파티션을 `RETRY`로
    # 되돌리고 재발행한다. 같은 SCN을 다시 읽어야 하므로 Oracle undo 보존 시간이 충분할 때만 사용한다.
    mode: Literal["FAIL", "REISSUE"] = "FAIL"
    max_attempts: int = Field(default=3, ge=1)  # REISSUE 모드에서 이 횟수에 도달하면 run TIMED_OUT
    # STAGE_VALIDATING·PUBLISHED run이 이 시간 동안 변화가 없으면 ERROR 이벤트만 남긴다(자동 전이 없음).
    validation_stale: timedelta = timedelta(hours=2)
    # PUBLISHING run이 게시 시작 후 이 시간 안에 결과를 보고하지 않으면 PUBLISH_UNKNOWN으로 바꾼다.
    publish_stale: timedelta = timedelta(hours=2)
    sweeper_interval: timedelta = timedelta(minutes=1)  # sweeper 실행 주기

    @model_validator(mode="after")
    def _check_stale(self) -> "RecoverySettings":
        """stale이 extract_query_timeout 이하이면 시작을 거부한다.

        그렇지 않으면 정상적으로 긴 쿼리를 실행 중인 파티션을 sweeper가 멈춘 것으로 오판한다.
        """
        # heartbeat는 쿼리 실행 중 갱신되지 않으므로 stale 기준은 query timeout보다 커야 한다.
        if self.stale <= self.extract_query_timeout:
            raise ValueError("recovery.stale must be greater than recovery.extract_query_timeout")
        return self


class DispatchSettings(Section):
    """worker dispatcher의 outbox(`load_dispatch`) 전달 정책."""

    # 전송 시도 상한. 시도 횟수는 lease할 때마다 1씩 늘고, 이 횟수째 시도까지 실패하면 DEAD로 바꾼다.
    # 4xx 응답은 설정 오류로 보고 횟수와 관계없이 바로 DEAD.
    max_attempts: int = Field(default=20, ge=1)
    # 5xx·연결 실패 뒤 재시도 간격: backoff_min × 2^(시도-1), 최대 backoff_max(지수 backoff).
    backoff_min: timedelta = timedelta(seconds=5)
    backoff_max: timedelta = timedelta(minutes=5)
    # SENT(NiFi가 2xx로 받음) 후 이 시간 안에 /validation/start(ACK)가 없으면 sweeper가 다시 PENDING으로.
    ack_timeout: timedelta = timedelta(minutes=10)
    # 전송 전 행을 선점하는 기간이다. worker가 전송 중 종료되면 이 시간이 지난 뒤 다른 worker가 다시
    # 가져간다. 중복 선점을 막으려면 `nifi.timeout_seconds`보다 길어야 한다.
    lease: timedelta = timedelta(seconds=60)
    poll_interval: timedelta = timedelta(seconds=5)  # pg_notify를 놓쳤을 때를 대비한 폴링 주기
    batch: int = Field(default=20, ge=1)  # 한 번에 lease해서 동시에 보내는 최대 dispatch 수


class CleanupSettings(Section):
    """정리 대상 판정 기준(NiFi PG-70 Cleanup이 조회). 끝난 시각(completed_at)부터 센다."""

    success_retention: timedelta = timedelta(days=3)   # SUCCESS run의 staging·run 경로 보존 기간
    failed_retention: timedelta = timedelta(days=14)   # 실패·TIMED_OUT run(원인 확인용)
    max_batch: int = Field(default=200, ge=1)          # 한 번에 돌려줄 최대 run 수


class MonitorSettings(Section):
    """TUI 모니터(bin/monitor.sh).

    조회 API로 상태를 보고, operator 토큰으로 dispatch 재전송·PUBLISH_UNKNOWN 확정을 한다.
    """

    api_url: AnyHttpUrl | None = None  # 기본: http://127.0.0.1:<server.port>
    token: str | None = None           # 조회용: nifi 또는 operator role 토큰 원문(digest가 아니다)
    operator_token: str | None = None  # 운영 작업(재전송, PUBLISH_UNKNOWN 확정)용 operator 토큰. 없으면 token
    refresh_seconds: float = Field(default=5.0, ge=1.0)  # 화면 자동 새로고침 주기(초)
    log_dir: Path = Path("logs")       # server.log, worker.log, *.pid 위치(설치 디렉터리 기준)


class WorkerSettings(Section):
    """worker 프로세스(dispatcher, sweeper) 설정."""

    metrics_host: str = "0.0.0.0"  # /metrics bind address
    metrics_port: int | None = Field(default=9100, ge=1, le=65535)  # null이면 /metrics를 열지 않는다


class LogFileSettings(Section):
    """파일 로그. 크기 기준으로 회전한다."""

    # 로그 파일 경로. `{service}`는 server 또는 worker로 바뀐다(예: logs/{service}.log).
    # 상위 디렉터리는 없으면 만든다.
    path: Path
    format: Literal["text", "json"] = "text"  # text: 사람이 읽는 한 줄 형식, json: 수집기용
    max_bytes: int = Field(default=100 * 1024 * 1024, ge=1024)  # 이 크기(바이트)를 넘으면 회전한다
    backup_count: int = Field(default=10, ge=0)  # 남겨 둘 회전 파일 수(.1 ~ .N)


def _default_logger_levels() -> dict[str, LogLevel]:
    """외부 라이브러리 logger의 기본 수준.

    빈번한 SQL·HTTP 클라이언트 로그는 `WARNING` 이상만 남긴다. Uvicorn의 시작·종료와 Alembic의
    migration 진행 상황은 `INFO` 이상을 남긴다.
    """
    return {"sqlalchemy.engine": "WARNING", "asyncpg": "WARNING", "httpx": "WARNING",
            "httpcore": "WARNING", "uvicorn.error": "INFO", "alembic": "INFO"}


class LoggingSettings(Section):
    """로그 설정. structlog 이벤트와 stdlib 로그(uvicorn, SQLAlchemy 등)를 같은 형식으로 낸다."""

    level: LogLevel = "INFO"  # root 수준
    format: Literal["text", "json", "console"] = "text"  # 표준출력 형식. console은 개발용(색상)
    stdout: bool = True  # 표준출력으로 낼지(컨테이너·systemd journal 수집용)
    file: LogFileSettings | None = None  # 파일로도 낼 때
    access_log: bool = True  # API 요청마다 수신·응답 로그를 남길지
    access_body: bool = True  # 수신·응답 로그에 요청·응답 본문(JSON)을 넣을지
    access_body_max: int = Field(default=2000, ge=0)  # 본문을 이 글자 수에서 자른다
    # logger별 수준. 예: SQL을 보려면 sqlalchemy.engine: INFO
    loggers: dict[str, LogLevel] = Field(default_factory=_default_logger_levels)

    @model_validator(mode="after")
    def _check_outputs(self) -> "LoggingSettings":
        """출력(stdout, file)이 하나도 없으면 로그가 사라지므로 시작을 거부한다."""
        if not self.stdout and self.file is None:
            raise ValueError("logging needs at least one output: stdout or file")
        return self


class Settings(BaseSettings):
    """전체 설정. `Settings.load()`로 config.yaml을 읽는다. 테스트는 생성자 인자로 직접 만든다."""

    model_config = SettingsConfigDict(env_prefix="LCA_", env_nested_delimiter="__", extra="forbid")

    server: ServerSettings = Field(default_factory=ServerSettings)  # API 서버(uvicorn)
    database: DatabaseSettings  # 필수: 관리 DB 연결
    auth: AuthSettings = Field(default_factory=AuthSettings)  # API 인증 토큰 digest
    nifi: NifiSettings = Field(default_factory=NifiSettings)  # worker → NiFi PG-05 호출
    recovery: RecoverySettings = Field(default_factory=RecoverySettings)  # sweeper 기준
    dispatch: DispatchSettings = Field(default_factory=DispatchSettings)  # outbox 전달 정책
    cleanup: CleanupSettings = Field(default_factory=CleanupSettings)  # 정리 대상 보존 기간
    monitor: MonitorSettings = Field(default_factory=MonitorSettings)  # TUI 모니터
    worker: WorkerSettings = Field(default_factory=WorkerSettings)  # worker 프로세스(/metrics)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)  # 로그 형식·출력

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
        테스트는 개발자 PC의 config.yaml에 영향을 받지 않는다. .env 파일과 secrets 디렉터리는 쓰지 않는다.
        """
        sources: list[PydanticBaseSettingsSource] = [init_settings, env_settings]
        path = _config_file.get()
        if path is not None:
            sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=path,
                                                    yaml_file_encoding="utf-8"))
        return tuple(sources)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Settings":
        """config.yaml을 읽는다. path가 없으면 LCA_CONFIG, 그것도 없으면 ./config/config.yaml.

        파일이 없으면 FileNotFoundError, 모르는 키·잘못된 값이면 pydantic ValidationError를 낸다.
        경로는 이 호출 동안에만 ContextVar로 settings_customise_sources에 전달한다.
        """
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
