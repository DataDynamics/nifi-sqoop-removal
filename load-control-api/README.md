# Load Control API

Sqoop 대체 적재(NiFi)의 상태 원장 기록과 완료 판정을 담당하는 FastAPI 서비스다. 설계는 [Load Control API 설계](../load-control-api-design.md), NiFi 연동은 [가이드](../nifi-sqoop-removal-guide.md) 9장을 따른다.

## 구현 범위

API 설계 12장 전환 순서 중 API 쪽 작업을 모두 구현했다. NiFi Flow 전환(3단계)과 권한 회수(7단계)는 NiFi·DBA 작업이다.

| 구분 | 엔드포인트·기능 |
|---|---|
| 추출 | `POST /v1/runs`, `/manifest`, `/fail`, `/partitions/{pid}/claim`, `/chunks`, `/partitions/{pid}/fail` |
| 검증·게시 | `POST /validation/start`, `/validations`, `/stage-validated`, `/publish/claim`, `/publish/result`, `/success` |
| 조회 | `GET /v1/runs`, `GET /v1/runs/{id}` (role `nifi`, `operator`) |
| 운영자 | `POST /dispatches/{id}/resend`, `/publish-unknown/resolve` (role `operator`만) |
| 정리 | `GET /v1/cleanup/candidates`, `POST /v1/runs/{id}/cleanup` (NiFi PG-70, 운영자 수동 기록) |
| worker | outbox dispatcher(LISTEN/NOTIFY, lease, backoff, DEAD), sweeper(stale 파티션, run timeout, ACK timeout 재전송, 검증 정체 경보, 게시 결과 불명) |

## 구조

```text
src/load_control/
├── main.py           # create_app() factory, 예외 처리기, router 등록
├── server.py         # API 진입점(python -m load_control.server)
├── config.py         # Settings (config.yaml 로드)
├── db.py             # engine, in_tx (deadlock 재시도), SQLSTATE 헬퍼
├── security.py       # Bearer 토큰 role 인증 (digest 비교)
├── errors.py         # ApiError → JSON 오류 응답
├── logging.py        # structlog JSON, X-Request-Id, access log, 요청 메트릭
├── metrics.py        # Prometheus 메트릭
├── domain.py         # RunStatus, PartitionStatus, 허용 실패 전이
├── schemas/          # Pydantic 요청·응답 모델 (camelCase JSON)
├── repositories/     # SQL만 (판단 없음)
├── services/         # 트랜잭션 단위 업무 규칙 (manifest 불변식, claim, chunk 판정, 검증, 게시)
├── routers/          # 인증, 입력 검증, 트랜잭션 시작
└── worker/           # python -m load_control.worker: dispatcher + sweeper
alembic/versions/0001_nifi_ops_baseline.py   # 가이드 4.1 DDL
alembic/versions/0002_run_cleanup.py         # load_run.cleaned_at(정리 기록)
tests/                                       # 실제 PostgreSQL 대상 통합·동시성 테스트
```

## 설정

설정은 `config.yaml` 하나로 관리한다. 항목 설명은 [`config.example.yaml`](./config.example.yaml)에 있다.

- 파일 위치: `LCA_CONFIG` 환경변수, 없으면 현재 디렉터리의 `config.yaml`. 파일이 없으면 시작하지 않는다.
- 우선순위: 환경변수 > `config.yaml` > 기본값. 비밀값은 `LCA_DATABASE__URL`처럼 환경변수로 덮어쓸 수 있다(섹션 구분자 `__`).
- 모르는 키(오타)나 잘못된 값이 있으면 시작하지 않는다.
- 기간 값은 ISO 8601(`PT90M`) 또는 초 단위 숫자.

| 섹션 | 내용 |
|---|---|
| `server` | API bind address(`host`), `port`, 프로세스 수(`workers`), 프록시 헤더, graceful shutdown, 선택적 TLS/mTLS |
| `database` | DB URL(런타임, migration, LISTEN), pool |
| `auth` | role별 토큰 digest |
| `nifi` | worker가 NiFi PG-05를 호출할 주소(HTTP) |
| `recovery`, `dispatch` | sweeper·outbox 기준 |
| `cleanup` | 정리 대상 보존 기간(`success_retention` 3일, `failed_retention` 14일), `max_batch` |
| `worker` | worker `/metrics` bind address와 port |
| `logging` | 수준, 형식(json/console), 표준출력, 회전 파일, access 로그 on/off, logger별 수준 |

## 개발 환경

```bash
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python -e ".[dev]"
cp config.example.yaml config.yaml   # 값 채우기 (config.yaml은 git에 올리지 않음)
```

토큰 digest 생성:

```bash
.venv/bin/python -m load_control.security '<token>'
```

## Migration

```bash
# config.yaml의 database.migration_url(없으면 database.url)을 쓴다
.venv/bin/alembic upgrade head
```

- 버전 테이블은 `nifi_ops.alembic_version`이다.
- 가이드 4.1 DDL로 이미 수동 생성한 DB는 `alembic stamp 0001_nifi_ops_baseline`으로 기준점만 맞춘다.
- `load_control_api`, `nifi_runtime` 역할이 있으면 권한도 함께 부여한다. 역할 생성은 DBA가 한다.

## 실행

```bash
# API: config.yaml의 server 섹션(host, port, workers, TLS 등)으로 uvicorn 실행. Dockerfile 기본 명령과 같다
python -m load_control.server --config config.yaml      # 또는 설치 후 load-control-api

# worker (dispatcher + sweeper). 같은 이미지에서 명령만 바꿔 2개 띄운다
python -m load_control.worker --config config.yaml      # 또는 load-control-worker

# 개발 중 자동 재시작
LCA_CONFIG=config.yaml .venv/bin/uvicorn --factory load_control.main:create_app --reload --port 8080
```

`--config`를 주지 않으면 `LCA_CONFIG`, 그것도 없으면 현재 디렉터리의 `config.yaml`을 읽는다.

worker는 `nifi.receiver_url`(NiFi LB의 PG-05 주소)이 없으면 시작하지 않는다. `database.listen_dsn`이 없으면 NOTIFY 없이 `dispatch.poll_interval`마다 폴링만 한다. SIGTERM을 받으면 진행 중인 작업을 끝내고 종료한다. 여러 개를 띄워도 lease와 advisory lock 때문에 같은 dispatch를 두 번 보내거나 같은 정리를 두 번 하지 않는다.

`recovery.stale`은 `recovery.extract_query_timeout`보다 커야 하며, 그렇지 않으면 API와 worker 모두 시작하지 않는다.

- `GET /healthz`: 프로세스 생존(DB 미확인)
- `GET /readyz`: DB `SELECT 1`
- `GET /metrics`: Prometheus(API). `server.workers`가 1보다 크면 요청을 받은 프로세스의 값만 나온다. 합계가 필요하면 `PROMETHEUS_MULTIPROC_DIR`을 설정하고 multiprocess collector로 바꾼다.
- worker `/metrics`: `worker.metrics_host`:`worker.metrics_port`(기본 0.0.0.0:9100). dispatch backlog, 활성 run 수, sweeper 처리 건수.
- OpenAPI 문서: `/docs`, `/openapi.json`

## 테스트

테스트는 실제 PostgreSQL이 필요하다. 동시성 규칙(run 행 잠금, CAS, partial unique index)은 mock으로 검증할 수 없기 때문이다.

```bash
# 이미 떠 있는 PostgreSQL 사용 (DB는 비어 있어야 함, 테스트마다 nifi_ops 테이블을 TRUNCATE)
LCA_TEST_DATABASE_URL=postgresql+asyncpg://postgres@127.0.0.1:5432/lca_test .venv/bin/pytest

# Docker가 있으면 testcontainers가 postgres:16-alpine을 띄운다
.venv/bin/pytest

.venv/bin/ruff check .
.venv/bin/mypy src

# 커버리지(greenlet 추적 설정은 pyproject.toml에 있음)
.venv/bin/pytest --cov
```

주요 동시성 테스트:

- `test_concurrency.py`: 마지막 파티션 동시 완료 시 검증 예약 1회, 동시 claim 1명
- `test_dispatcher.py`: 동시 dispatcher가 같은 행을 한 번만 전송, ACK가 `mark_sent`보다 먼저 와도 `ACKED` 유지, lease 만료 후 재전송, LISTEN 재연결
- `test_validation_start.py`, `test_publish_flow.py`: 동시 검증 시작·동시 publish claim에서 승자 1명
- `test_sweeper.py`: 여러 sweeper가 동시에 돌아도 같은 run을 한 번만 정리

`tests/test_concurrency.py`는 마지막 파티션들의 chunk를 `asyncio.gather`로 동시에 보고해 검증 호출 예약이 정확히 1회인지 확인한다. run 행 잠금과 run 행 UPDATE를 모두 제거하면 "아무도 run을 완료하지 못하는" 경합이 재현되어 이 테스트가 실패한다.
