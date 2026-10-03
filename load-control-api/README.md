# Load Control API

Sqoop 대체 적재(NiFi)의 상태 원장 기록과 완료 판정을 담당하는 FastAPI 서비스다. 전체 설계는
[아키텍처와 전환 설계](../docs/01-architecture.md), API 요청·상태 전이·복구 동작은
[Load Control API 상호작용](../docs/05-api-interactions.md)에 있다.

처음 설치한다면 "설치" → "설정" → "운영 스크립트" 순서로 보면 된다.

프로세스는 두 개다. **server**는 NiFi·운영자의 HTTP 요청을 받아 상태를 판정·기록하고, **worker**는 요청과 관계없이 돌며 NiFi로 다음 단계 호출을 보내고(dispatcher) 멈춘 작업을 찾는다(sweeper). 나눈 이유와 NiFi·server·worker 사이의 시퀀스 다이어그램은 [아키텍처](../docs/01-architecture.md#2-데이터-plane과-control-plane)와 [API 상호작용](../docs/05-api-interactions.md#7-outbox와-pg-05-ack)에 있다.

## 기능

| 구분 | 엔드포인트·기능 |
|---|---|
| 추출 | `POST /v1/runs`, `/manifest`, `/fail`, `/partitions/{pid}/claim`, `/chunks`, `/partitions/{pid}/fail` |
| 검증·게시 | `POST /validation/start`, `/validations`, `/stage-validated`, `/publish/claim`, `/publish/result`, `/success` |
| 조회 | `GET /v1/runs`, `GET /v1/runs/{id}` (role `nifi`, `operator`) |
| 운영자 | `POST /dispatches/{id}/resend`, `/publish-unknown/resolve` (role `operator`만) |
| 정리 | `GET /v1/cleanup/candidates`, `POST /v1/runs/{id}/cleanup` (NiFi PG-70, 운영자 수동 기록) |
| 모니터 | `GET /v1/monitor/summary`, `/v1/runs/{id}/validations`, `/v1/runs/{id}/events`, TUI `bin/monitor.sh`(조회와 운영 작업) |
| worker | outbox dispatcher(LISTEN/NOTIFY, lease, backoff, DEAD), sweeper(stale 파티션, run timeout, ACK timeout 재전송, 검증 정체 경보, 게시 결과 불명) |

## 디렉터리 구조

```text
load-control-api/
├── bin/          운영 스크립트(아래 "운영 스크립트"), monitor.sh(TUI), systemd/(서비스 파일)
├── config/       config.example.yaml, alembic.ini, config.yaml(실제 설정, git 제외)
├── logs/         server.log, worker.log(로그), server.out, worker.out(표준출력), *.pid (git 제외)
├── packages/     airgap 설치용 wheel(git 제외)과 requirements.txt(고정 버전 목록)
├── src/
│   ├── load_control/   소스 코드
│   ├── migrations/     Alembic migration(nifi_ops 테이블 정의의 원본)
│   └── tests/          실제 PostgreSQL 대상 통합·동시성 테스트
├── pyproject.toml, Dockerfile, README.md
└── .venv/        실행 환경(bin/install.sh가 만든다, git 제외)
```

```text
src/load_control/
├── main.py           # create_app() factory, 예외 처리기, router 등록
├── server.py         # API 진입점(python -m load_control.server)
├── config.py         # Settings (config/config.yaml 로드)
├── db.py             # engine, in_tx (deadlock 재시도), SQLSTATE 헬퍼
├── security.py       # Bearer 토큰 role 인증 (digest 비교)
├── errors.py         # ApiError → JSON 오류 응답
├── logging.py        # structlog JSON, X-Request-Id, access log, 요청 메트릭
├── metrics.py        # Prometheus 메트릭
├── domain.py         # RunStatus, PartitionStatus, 허용 실패 전이
├── schemas/          # Pydantic 요청·응답 모델 (camelCase JSON)
├── repositories/     # SQL만 (판단 없음)
├── services/         # 트랜잭션 단위 업무 규칙 (manifest 불변식, claim, chunk 판정, 검증, 게시, 정리)
├── routers/          # 인증, 입력 검증, 트랜잭션 시작
├── worker/           # python -m load_control.worker: dispatcher + sweeper
├── monitor/          # python -m load_control.monitor: TUI 모니터(textual)
└── query/            # python -m load_control.query: bin/oracle.sh·hive.sh·hdfs.sh 조회 도구
src/migrations/versions/0001_nifi_ops_baseline.py   # nifi_ops 테이블·인덱스·권한
src/migrations/versions/0002_run_cleanup.py         # load_run.cleaned_at(정리 기록)
```

## 설치

프로젝트를 패키지로 설치하지 않는다. `.venv`에는 의존 패키지만 두고, bin 스크립트가 `PYTHONPATH=src`로 소스를 실행한다. 그래서 설치 장비에 빌드 도구가 필요 없다. Python 3.12 이상이 필요하다(RHEL 9는 `dnf install python3.12`).

```bash
# 1) 인터넷이 되는 장비: wheel을 packages/에 받는다(대상 Python 3.12, manylinux x86_64)
bin/download-packages.sh            # requirements.txt를 다시 만들려면 --lock (uv 필요)

# 2) 디렉터리 전체(packages/ 포함)를 airgap 장비로 옮긴 뒤
bin/install.sh                      # packages/만 써서 .venv 생성(pip --no-index). 인터넷이 되면 --online
cp config/config.example.yaml config/config.yaml   # 값 채우기, 권한 600
bin/migrate.sh                      # alembic upgrade head
bin/start.sh
```

- `LCA_INSTALL_PYTHON`(기본 `python3.12`)으로 venv를 만들 python을 지정한다
- `LCA_PKG_PYTHON`, `LCA_PKG_PLATFORMS`로 받을 wheel의 Python 버전과 플랫폼을 바꾼다
- 의존성을 바꿨으면 `bin/download-packages.sh --lock`으로 `packages/requirements.txt`를 다시 만들어 커밋한다

## 운영 스크립트

모든 스크립트는 설치 디렉터리(`bin/`의 상위)에서 실행되고, 설정은 `config/config.yaml`(`LCA_CONFIG`로 변경 가능)을 쓴다. 서비스는 `server`(API), `worker`(dispatcher + sweeper)이고, 생략하면 둘 다다.

| 스크립트 | 동작 |
|---|---|
| `bin/start.sh [server\|worker\|all]` | 백그라운드로 시작. PID는 `logs/<서비스>.pid`, 표준출력은 `logs/<서비스>.out`. server는 `/readyz`가 ok가 될 때까지(최대 30초) 기다린다. 이미 실행 중이면 건너뛴다 |
| `bin/stop.sh [server\|worker\|all]` | SIGTERM 후 graceful shutdown을 기다린다. `LCA_STOP_TIMEOUT`초(기본 45) 안에 끝나지 않으면 프로세스 세션 전체를 SIGKILL |
| `bin/restart.sh [server\|worker\|all]` | stop 후 start |
| `bin/status.sh [server\|worker\|all] [--wait 초]` | 실행 여부와 server `/readyz`. 모두 정상이면 0, 아니면 3 |
| `bin/migrate.sh [alembic 인자]` | 기본 `upgrade head`. 예: `bin/migrate.sh current` |
| `bin/install.sh [--online]` | `.venv` 생성과 의존 패키지 설치 |
| `bin/download-packages.sh [--lock]` | airgap용 wheel 받기 |
| `bin/monitor.sh [--url URL] [--token TOKEN]` | TUI 모니터(아래 "모니터") |
| `bin/oracle.sh`, `bin/hive.sh`, `bin/hdfs.sh` | psql과 비슷한 Oracle·Hive·HDFS 조회 도구(아래 "조회 도구"). 기본 읽기 전용 |

- PID 파일이 남아 있어도 그 PID가 이 서비스의 python 프로세스가 아니면 중지된 것으로 본다
- bin 스크립트와 systemd 중 하나만 쓴다

## systemd

`bin/systemd/`에 서비스 파일 두 개와 설치 스크립트가 있다.

| 파일 | 내용 |
|---|---|
| `load-control-api.service` | API 서버. `TimeoutStopSec=45`, 실패 시 5초 뒤 재시작 |
| `load-control-worker.service` | dispatcher + sweeper |
| `install.sh` | `@LCA_HOME@`(설치 디렉터리), `@LCA_USER@`(실행 사용자)를 채워 `/etc/systemd/system`에 설치하고 enable |

```bash
bin/stop.sh                                   # bin/start.sh로 띄운 프로세스가 있으면 먼저 멈춘다
sudo bin/systemd/install.sh [실행 사용자]     # 기본: 설치 디렉터리 소유자
sudo systemctl start load-control-api load-control-worker
systemctl status load-control-api load-control-worker
sudo bin/systemd/install.sh --uninstall       # 제거
```

서비스는 bin 스크립트와 같은 명령(`PYTHONPATH=src .venv/bin/python -m load_control.server|worker --config config/config.yaml`)을 설치 디렉터리에서 실행한다. 표준출력은 journal(`journalctl -u load-control-api`)로 가고, 로그 파일은 아래와 같다.

## 모니터

`bin/monitor.sh`는 터미널 화면에서 상태를 보고 자주 하는 운영 작업을 하는 도구다.

```bash
bin/monitor.sh                         # config/config.yaml의 monitor 섹션 사용
bin/monitor.sh --url http://api-host:8080 --token <token> --operator-token <operator token>
bin/monitor.sh --mouse                 # 호환성이 확인된 터미널에서만 마우스 활성화
```

| 화면 | 내용 | 키 |
|---|---|---|
| 대시보드 | API 준비 여부, server·worker PID, 진행 중·최근 24시간 run 수, dispatch(PENDING·SENT·DEAD), 정리 대상 수, 경보, run 목록(진행률·단계별 건수·소요) | `Enter` 상세, `x` 선택한 경보 조치, `s` 서비스 관리, `l` 로그, `a` 진행 중만, `r` 새로고침, `q` 종료 |
| run 상세 | run 정보, 탭: 파티션(상태·건수·시도·노드), 검증 지표(SOURCE·STAGING·TARGET, PASS/FAIL), dispatch, 이벤트 타임라인 | `s` dispatch 재전송, `p` `PUBLISH_UNKNOWN` 확정, `l` 이 run의 로그, `Esc` 뒤로 |
| 로그 | `logs/server.log`, `worker.log` 실시간. 입력란 글자(run ID, requestId 등)가 들어간 줄만 표시 | `F2` WARN·ERROR만, `F3` server/worker 전환, `Esc` 뒤로 |

경보 종류:

| 종류 | 뜻 |
|---|---|
| `PUBLISH_UNKNOWN` | 게시 결과 불명. 운영자 확정 필요 |
| `DISPATCH_DEAD` | NiFi 호출을 포기함. PG-05 수신 확인 후 재전송 |
| `RUN_FAILED` | 최근 24시간 안에 실패·`TIMED_OUT`으로 끝난 run |
| `RUN_STALE` | 오래 멈춘 run(`recovery.stale`, `recovery.validation_stale` 기준) |
| `CLEANUP_FAILED` | 정리 실패가 남아 있는 run |

운영 작업(모두 확인 창을 거치고, 확인 창의 기본 선택은 취소다):

| 작업 | 키 | 동작 |
|---|---|---|
| dispatch 재전송 | 대시보드 `DISPATCH_DEAD` 경보에서 `x`, run 상세에서 `s`(dispatch 탭에서 고른 행, 없으면 첫 DEAD·SENT) | `POST /v1/runs/{id}/dispatches/{id}/resend`. 먼저 PG-05 수신(포트, 등록된 Job)을 확인한다 |
| `PUBLISH_UNKNOWN` 확정 | 대시보드 `PUBLISH_UNKNOWN` 경보에서 `x`, run 상세에서 `p` | 결과(`FAILED_PUBLISH` 기본, `PUBLISHED`)와 확인 근거(5자 이상)를 입력한다. `PUBLISHED`면 PG-60이 돌지 않으므로 target 검증을 직접 한다 |
| 서비스 시작·중지·재시작 | 대시보드 `s` | 이 호스트의 `bin/start.sh`, `stop.sh`, `restart.sh`를 실행하고 출력을 보여 준다. systemd로 관리 중이면 막고 `systemctl`을 안내한다. server를 멈추면 모니터도 잠시 API에 붙지 못한다 |

- 설정: `monitor.api_url`(기본 `http://127.0.0.1:<server.port>`), `monitor.token`(조회용 nifi 또는 operator 토큰 원문), `monitor.operator_token`(운영 작업용 operator 토큰, 없으면 `token`), `monitor.refresh_seconds`(기본 5초), `monitor.log_dir`
- 운영 작업은 operator role이 필요하다. 토큰이 nifi role이면 "권한 없음"이 뜨고 아무것도 바뀌지 않는다
- 서비스 PID와 로그는 이 디렉터리의 `logs/`에서 읽으므로 API 서버 호스트에서 실행한다. 다른 호스트에서 `--url`로 붙으면 API 정보만 보인다. systemd로 띄웠으면 PID는 "PID 파일 없음"으로 나오고 API 준비 여부로 판단한다
- 터미널이 UTF-8이어야 한글이 깨지지 않는다
- 마우스 입력은 기본적으로 비활성화된다. 목록 이동에는 방향키, `j`/`k`, `PageUp`/`PageDown`, `Home`/`End`를 사용하고 `Enter`로 상세 화면을 연다
- Textual 8.2.8의 Linux 입력 드라이버는 일부 터미널·SSH·tmux 환경에서 레거시 X10 마우스 이벤트를 UTF-8 문자로 잘못 해석하여 `UnicodeDecodeError`를 일으킬 수 있다. 터미널의 SGR 마우스 모드 호환성을 확인한 경우에만 `--mouse`를 사용하고, 오류가 재발하면 옵션 없이 다시 실행한다

## 조회 도구

적재 결과를 원천·HDFS·Hive에서 직접 확인하는 명령행 도구다. sqlplus, beeline, hadoop 클라이언트 없이
`config.yaml`의 `clients` 섹션 접속 정보로 실행한다. 전체 사용법은
[부록 A. 운영 조회 도구 사용법](../docs/appendix-a-query-tools.md)에 있다.

```bash
bin/oracle.sh                                   # 대화형(\? 도움말, \dt, \d 이름, \scn, \x, \q)
bin/oracle.sh -c "SELECT COUNT(*) FROM APP.INSP_DTL AS OF SCN 2390399 WHERE BASE_DT = DATE '2026-09-28'"
bin/hive.sh -F csv --max-rows 0 -f check.sql > out.csv
bin/hdfs.sh ls -h /data/nifi/stage/ORACLE_INSP_DTL_DAILY
bin/hdfs.sh du -s -h '/data/nifi/stage/*/*'     # glob은 따옴표로 감싼다
```

- 기본은 읽기 전용이다. DML·DDL과 HDFS `mkdir`·`rm`·`mv`·`put`·`chmod`는 `--write`를 줘야 실행한다.
  Oracle은 문장마다 `SET TRANSACTION READ ONLY`로 실행한다.
- 종료 코드: 0 성공, 1 실행 오류, 2 사용법·설정·접속 오류, 3 읽기 전용 거부, 130 Ctrl-C.

## 로그

| 파일 | 내용 |
|---|---|
| `logs/server.log` | API 로그. 요청 수신·응답, 상태 전이, 오류 |
| `logs/worker.log` | worker 로그. NiFi 호출(dispatch), sweeper 조치 |
| `logs/server.out`, `logs/worker.out` | bin 스크립트로 띄웠을 때의 표준출력(시작 실패 등). `logging.stdout: true`면 같은 로그가 중복된다 |

한 줄 형식(`logging.file.format: text`):

```text
2026-10-03 04:12:40.557 INFO  [load_control.services.runs] run 생성: ORACLE_INSP_DTL_DAILY 업무일자 2026-09-28 (run_created) runId=ca803dd4-... jobKey=ORACLE_INSP_DTL_DAILY requestId=3f06e0d2-...
2026-10-03 04:13:35.734 WARN  [load_control.access] API 응답: POST /v1/runs/.../fail → 404 (31.6ms) (api_response) httpStatus=404 runId=... requestId=manual-trace-1 responseBody={"code":"RUN_NOT_FOUND",...}
```

- 시각은 서버 현지 시각 `YYYY-MM-DD HH:MM:SS.SSS`
- 메시지는 한글이고 괄호 안이 이벤트 코드다. 이벤트 코드와 메시지는 `src/load_control/log_messages.py`에 있다
- API 호출마다 `api_request`(수신: 경로, 요청 본문)와 `api_response`(응답: 상태, 소요 ms, 응답 본문) 두 줄을 남긴다. 본문은 `logging.access_body_max`(기본 2000자)에서 자른다. Authorization 헤더는 남기지 않는다
- 같은 요청의 모든 로그에 `requestId`(NiFi가 보낸 `X-Request-Id`)와 경로의 `runId`, `partitionId`가 붙는다. 추적은 `grep <runId> logs/*.log` 또는 `grep <requestId> logs/server.log`
- worker는 NiFi 호출 전(`dispatch_sending`: URL, 본문)과 후(`dispatch_sent`/`dispatch_retry`/`dispatch_dead`: 상태, 소요 ms)를 남긴다
- 수집기로 보낼 때는 `logging.file.format: json`으로 바꾼다(같은 필드, 한 줄 JSON)
- 파일은 `logging.file.max_bytes`마다 회전한다. `server.workers`가 2 이상이면 여러 프로세스가 같은 파일을 회전하므로, 회전은 logrotate(copytruncate)에 맡기고 `max_bytes`를 크게 둔다. `logs/*.out`도 logrotate로 회전한다

## 설정

설정은 `config/config.yaml` 하나로 관리한다. 항목 설명은 [`config/config.example.yaml`](./config/config.example.yaml)에 있다. DB 비밀번호도 URL에 평문으로 적는다. 파일 권한을 `600`으로 두고 git에 올리지 않는다(`.gitignore`). 로그에는 DB host와 이름만 남는다.

- 파일 위치: `--config`, 없으면 `LCA_CONFIG` 환경변수, 그것도 없으면 현재 디렉터리의 `config/config.yaml`. 파일이 없으면 시작하지 않는다.
- 우선순위: 환경변수 > `config.yaml` > 기본값. `LCA_DATABASE__URL`처럼 환경변수로 덮어쓸 수 있다(섹션 구분자 `__`).
- 모르는 키(오타)나 잘못된 값이 있으면 시작하지 않는다.
- 기간 값은 ISO 8601(`PT90M`) 또는 초 단위 숫자.
- 상대 경로(`logging.file.path` 등)는 설치 디렉터리 기준이다(bin 스크립트가 그 디렉터리에서 실행한다).

| 섹션 | 내용 |
|---|---|
| `server` | API bind address(`host`), `port`, 프로세스 수(`workers`), 프록시 헤더, graceful shutdown, 선택적 TLS |
| `database` | DB URL(런타임, migration, LISTEN), pool |
| `auth` | role별 토큰 digest |
| `nifi` | worker가 NiFi PG-05를 호출할 주소(HTTP) |
| `recovery`, `dispatch` | sweeper·outbox 기준 |
| `cleanup` | 정리 대상 보존 기간(`success_retention` 3일, `failed_retention` 14일), `max_batch` |
| `worker` | worker `/metrics` bind address와 port |
| `clients` | 조회 도구(`bin/oracle.sh`, `bin/hive.sh`, `bin/hdfs.sh`)의 접속 정보. server·worker는 읽지 않는다 |
| `logging` | 수준, 형식(text/json/console), 표준출력, 회전 파일(`{service}` → server·worker), API 수신·응답 로그와 본문, logger별 수준 |

토큰 digest 생성:

```bash
PYTHONPATH=src .venv/bin/python -m load_control.security '<token>'
```

## Migration

```bash
bin/migrate.sh            # config의 database.migration_url(없으면 database.url)로 upgrade head
```

- 설정 파일은 `config/alembic.ini`, migration 스크립트는 `src/migrations/`다.
- 버전 테이블은 `nifi_ops.alembic_version`이다.
- 테이블을 이미 수동으로 만든 DB는 `bin/migrate.sh stamp 0001_nifi_ops_baseline`으로 기준점만 맞춘다.
- `load_control_api`, `nifi_runtime` 역할이 있으면 권한도 함께 부여한다. 역할 생성은 DBA가 한다.

## 실행 상세

운영은 bin 스크립트로 한다. 스크립트가 실행하는 명령은 다음과 같다.

```bash
PYTHONPATH=src .venv/bin/python -m load_control.server --config config/config.yaml   # API
PYTHONPATH=src .venv/bin/python -m load_control.worker --config config/config.yaml   # worker
```

worker는 `nifi.receiver_url`(NiFi LB의 PG-05 주소)이 없으면 시작하지 않는다. `database.listen_dsn`이 없으면 NOTIFY 없이 `dispatch.poll_interval`마다 폴링만 한다. SIGTERM을 받으면 진행 중인 작업을 끝내고 종료한다. 여러 개를 띄워도 lease와 advisory lock 때문에 같은 dispatch를 두 번 보내거나 같은 정리를 두 번 하지 않는다.

`recovery.stale`은 `recovery.extract_query_timeout`보다 커야 하며, 그렇지 않으면 API와 worker 모두 시작하지 않는다.

- `GET /healthz`: 프로세스 생존(DB 미확인)
- `GET /readyz`: DB `SELECT 1`
- `GET /metrics`: Prometheus(API). `server.workers`가 1보다 크면 요청을 받은 프로세스의 값만 나온다. 합계가 필요하면 `PROMETHEUS_MULTIPROC_DIR`을 설정하고 multiprocess collector로 바꾼다.
- worker `/metrics`: `worker.metrics_host`:`worker.metrics_port`(기본 0.0.0.0:9100). dispatch backlog, 활성 run 수, sweeper 처리 건수.
- OpenAPI 문서: `/docs`, `/openapi.json`

## 개발 환경과 테스트

개발 도구(pytest, ruff, mypy)는 운영 `.venv`와 분리한 `.venv-dev`에 둔다.

```bash
uv venv -p 3.12 .venv-dev
uv pip install -p .venv-dev/bin/python -e ".[dev]"

# 개발 중 자동 재시작
LCA_CONFIG=config/config.yaml .venv-dev/bin/python -m uvicorn --factory load_control.main:create_app --reload --port 8080
```

테스트는 실제 PostgreSQL이 필요하다. 동시성 규칙(run 행 잠금, CAS, partial unique index)은 mock으로 검증할 수 없기 때문이다.

```bash
# 이미 떠 있는 PostgreSQL 사용 (DB는 비어 있어야 함, 테스트마다 nifi_ops 테이블을 TRUNCATE)
LCA_TEST_DATABASE_URL=postgresql+asyncpg://postgres@127.0.0.1:5432/lca_test .venv-dev/bin/python -m pytest

# Docker가 있으면 testcontainers가 postgres:16-alpine을 띄운다
.venv-dev/bin/python -m pytest

.venv-dev/bin/python -m ruff check src
.venv-dev/bin/python -m mypy src/load_control

# 커버리지(greenlet 추적 설정은 pyproject.toml에 있음)
.venv-dev/bin/python -m pytest --cov
```

`test_dispatcher.py::test_listen_reconnects_after_connection_loss`는 서버 전체의 `LISTEN` 연결 수를 센다. 같은 PostgreSQL에 다른 worker가 붙어 있으면 실패하고, 그 worker의 LISTEN 연결도 끊는다(worker는 다시 연결한다).

주요 동시성 테스트:

- `test_concurrency.py`: 마지막 파티션 동시 완료 시 검증 예약 1회, 동시 claim 1명
- `test_dispatcher.py`: 동시 dispatcher가 같은 행을 한 번만 전송, ACK가 `mark_sent`보다 먼저 와도 `ACKED` 유지, lease 만료 후 재전송, LISTEN 재연결
- `test_validation_start.py`, `test_publish_flow.py`: 동시 검증 시작·동시 publish claim에서 승자 1명
- `test_sweeper.py`: 여러 sweeper가 동시에 돌아도 같은 run을 한 번만 정리

`src/tests/test_concurrency.py`는 마지막 파티션들의 chunk를 `asyncio.gather`로 동시에 보고해 검증 호출 예약이 정확히 1회인지 확인한다. run 행 잠금과 run 행 UPDATE를 모두 제거하면 "아무도 run을 완료하지 못하는" 경합이 재현되어 이 테스트가 실패한다.
