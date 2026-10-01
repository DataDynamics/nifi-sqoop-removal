# Load Control API

Sqoop 대체 적재(NiFi)의 상태 원장 기록과 완료 판정을 담당하는 FastAPI 서비스다. 설계는 [Load Control API 설계](../load-control-api-design.md), NiFi 연동은 [가이드](../nifi-sqoop-removal-guide.md) 9장을 따른다.

## 구현 범위

API 설계 12장 전환 순서 중 1~2단계를 구현했다.

| 단계 | 내용 | 상태 |
|---|---|---|
| 1. 프로젝트 골격 | 설정, Alembic baseline(가이드 4.1 DDL), 인증, 오류 처리, health, 메트릭, 테스트 환경 | 완료 |
| 2. API 1차 | `POST /v1/runs`, `/manifest`, `/fail`, `/partitions/{pid}/claim`, `/chunks`, `/fail`, `GET /v1/runs/{id}`, 판정 트랜잭션, outbox 예약 | 완료 |
| 4. outbox 전달 | worker 프로세스 dispatcher, `/validation/start` | 미구현 |
| 5. 검증·게시 | `/validations`, `/stage-validated`, `/publish/*`, `/success` | 미구현 |
| 6. sweeper | stale·timeout 정리 | 미구현 |

chunk 판정에서 run 완료가 확정되면 `load_dispatch`에 `VALIDATE_RUN` 행을 넣고 `pg_notify('load_dispatch')`까지 실행한다. 이 행을 NiFi로 전달하는 dispatcher는 4단계에서 구현한다.

## 구조

```text
src/load_control/
├── main.py           # create_app() factory, 예외 처리기, router 등록
├── config.py         # Settings (LCA_* 환경변수)
├── db.py             # engine, in_tx (deadlock 재시도), SQLSTATE 헬퍼
├── security.py       # Bearer 토큰 role 인증 (digest 비교)
├── errors.py         # ApiError → JSON 오류 응답
├── logging.py        # structlog JSON, X-Request-Id, access log, 요청 메트릭
├── metrics.py        # Prometheus 메트릭
├── domain.py         # RunStatus, PartitionStatus, 허용 실패 전이
├── schemas/          # Pydantic 요청·응답 모델 (camelCase JSON)
├── repositories/     # SQL만 (판단 없음)
├── services/         # 트랜잭션 단위 업무 규칙 (manifest 불변식, claim, chunk 판정)
└── routers/          # 인증, 입력 검증, 트랜잭션 시작
alembic/versions/0001_nifi_ops_baseline.py   # 가이드 4.1 DDL
tests/                                       # 실제 PostgreSQL 대상 통합·동시성 테스트
```

## 개발 환경

```bash
uv venv -p 3.12 .venv
uv pip install -p .venv/bin/python -e ".[dev]"
cp .env.example .env   # 값 채우기
```

토큰 digest 생성:

```bash
.venv/bin/python -m load_control.security '<token>'
```

## Migration

```bash
export LCA_MIGRATION_DATABASE_URL=postgresql+asyncpg://<ddl-user>@<host>:5432/<db>
.venv/bin/alembic upgrade head
```

- 버전 테이블은 `nifi_ops.alembic_version`이다.
- 가이드 4.1 DDL로 이미 수동 생성한 DB는 `alembic stamp 0001_nifi_ops_baseline`으로 기준점만 맞춘다.
- `load_control_api`, `nifi_runtime` 역할이 있으면 권한도 함께 부여한다. 역할 생성은 DBA가 한다.

## 실행

```bash
# 개발
.venv/bin/uvicorn --factory load_control.main:create_app --reload --port 8080

# 운영 (Dockerfile 기본 명령과 같음)
gunicorn 'load_control.main:create_app()' -k uvicorn.workers.UvicornWorker -w 4 -b 0.0.0.0:8080
```

- `GET /healthz`: 프로세스 생존(DB 미확인)
- `GET /readyz`: DB `SELECT 1`
- `GET /metrics`: Prometheus. Gunicorn 멀티 프로세스에서 프로세스 합계가 필요하면 `PROMETHEUS_MULTIPROC_DIR`을 설정하고 multiprocess collector로 바꾼다.
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
```

`tests/test_concurrency.py`는 마지막 파티션들의 chunk를 `asyncio.gather`로 동시에 보고해 검증 호출 예약이 정확히 1회인지 확인한다. run 행 잠금과 run 행 UPDATE를 모두 제거하면 "아무도 run을 완료하지 못하는" 경합이 재현되어 이 테스트가 실패한다.
