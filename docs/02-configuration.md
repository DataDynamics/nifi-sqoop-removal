# 설정 레퍼런스

설정은 두 파일로 나뉜다.

- NiFi Flow 생성 설정: `poc/config.v4.example.json`
- Load Control API 설정: `load-control-api/config/config.example.yaml`

두 설정은 독립적이지 않다. 특히 timeout, URL, token, Job key, HDFS root는 양쪽 관계를 함께 확인해야 한다.

## 1. 설정 파일 관리 원칙

- 실제 설정 파일은 저장소 밖에 두고 권한을 `600`으로 설정한다.
- 비밀번호와 token은 예시 파일에 직접 쓰지 않는다.
- `CONTROL.API.AUTHORIZATION`에는 `Bearer ` 접두사까지 포함한다.
- `JOB.KEY`는 `[A-Z0-9_]{1,200}` 형식이어야 한다.
- 공통 Parameter Context는 모든 Job이 공유한다. 새 Job 빌드 시 `common_params`가 기존 값을 갱신하므로
  Job마다 다른 값을 넣지 않는다.
- Job Parameter Context는 빌드 때 삭제 후 다시 만든다. 같은 Job PG가 있으면 빌더가 중단된다.
- Load Control API는 알 수 없는 설정 키를 거부하므로 오타가 있으면 시작되지 않는다.

## 2. NiFi 빌더 최상위 구조

```json
{
  "names": {},
  "common_params": {},
  "job_params": {}
}
```

### 2.1 `names`

대부분 생략한다. 여러 Job이 같은 `common_context`와 `control_receiver`를 사용해야 한다.

| 키 | 기본값 | 의미 |
|---|---|---|
| `process_group` | `JOB_<JOB.KEY>` | root에 생성할 Job PG 이름 |
| `job_context` | `PC_JOB_<JOB.KEY>` | Job Parameter Context |
| `common_context` | `PC_SQOOP_REPLACEMENT_COMMON` | 공유 Parameter Context |
| `control_receiver` | `PG-05 Control Receiver` | 공유 수신 PG 이름 |

## 3. NiFi `common_params`

### 3.1 Load Control 연결

| Parameter | 예 | 의미와 결정 기준 |
|---|---|---|
| `CONTROL.API.URL` | `http://api-lb:8080/v1` | API server 또는 LB의 `/v1`까지 포함한 주소 |
| `CONTROL.API.AUTHORIZATION` | `Bearer <nifi-token>` | `nifi` role token의 원문. 전체 값이 sensitive parameter |
| `CONTROL.API.TIMEOUT` | `30 secs` | NiFi가 API 응답을 기다리는 시간. 일반 판정 요청 기준 |
| `CONTROL.LISTEN.PORT` | `9443` | PG-05 `HandleHttpRequest`가 모든 NiFi 노드에서 여는 포트 |
| `CLEANUP.BATCH` | `50` | PG-70이 한 주기에 요청하는 cleanup 후보 수 |

API worker의 `nifi.receiver_url`은 `http://<NiFi LB>:<CONTROL.LISTEN.PORT>`여야 한다. NiFi→API와
API worker→NiFi 방향의 방화벽을 각각 확인한다.

### 3.2 관리 PostgreSQL

NiFi는 `load_event` INSERT만 수행한다. 상태 테이블은 API만 갱신한다.

| Parameter | 의미 |
|---|---|
| `META.JDBC.URL` | `jdbc:postgresql://<host>:5432/<db>` |
| `META.JDBC.USER` | `nifi_runtime` 권장 |
| `META.JDBC.PASSWORD` | NiFi runtime 계정 비밀번호 |
| `META.JDBC.DRIVER.PATH` | 모든 NiFi 노드의 PostgreSQL JDBC jar 경로 |

### 3.3 Oracle

| Parameter | 예 | 의미와 결정 기준 |
|---|---|---|
| `ORACLE.JDBC.URL` | `jdbc:oracle:thin:@//host:1521/SERVICE` | 원천 DB JDBC URL |
| `ORACLE.JDBC.USER` | `NIFI_READER` | SELECT와 flashback 권한을 가진 계정 |
| `ORACLE.JDBC.PASSWORD` | secret | 원천 계정 비밀번호 |
| `ORACLE.JDBC.DRIVER.PATH` | `/opt/nifi/jdbc/ojdbc11.jar` | 모든 NiFi 노드에서 같은 경로 |
| `ORACLE.POOL.MAX` | `8` | Job 하나의 Oracle Hikari pool 상한 |
| `ORACLE.NUMBER.DEFAULT.PRECISION` | `38` | precision 없는 `NUMBER`의 Parquet 기본 precision |
| `ORACLE.NUMBER.DEFAULT.SCALE` | `10` | precision 없는 `NUMBER`의 Parquet 기본 scale |

`ORACLE.POOL.MAX`는 최소한 PG-10의 SCN/manifest 조회와 PG-20 병렬 조회를 감당해야 하지만 DB 승인 세션을
넘으면 안 된다. 여러 Job의 최악 동시 실행까지 합산한다.

정밀도 없는 `NUMBER`는 `SRC.COLUMNS`에서 `CAST(... AS NUMBER(p,s))`로 명시하는 것이 안전하다. 기본
scale보다 소수 자릿수가 많으면 오류 없이 반올림될 수 있다.

### 3.4 HDFS와 추출

| Parameter | 예 | 의미와 결정 기준 |
|---|---|---|
| `HADOOP.CONF.FILES` | `/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml` | 모든 NiFi 노드의 설정 파일 |
| `HDFS.STAGE.ROOT` | `/data/nifi/stage` | API가 run별 경로를 만드는 상위 경로 |
| `HDFS.PERMISSIONS.UMASK` | `027` | PutHDFS 파일 umask |
| `EXTRACT.FETCH.SIZE` | `5000` | Oracle JDBC fetch size. 메모리와 round trip 절충 |
| `EXTRACT.ROWS.PER.FILE` | `500000` | Parquet FlowFile/chunk당 최대 행 수 |
| `EXTRACT.QUERY.TIMEOUT` | `60 min` | 파티션 SQL 최대 실행 시간 |

`EXTRACT.ROWS.PER.FILE`이 너무 작으면 HDFS small file과 API chunk 보고가 많아지고, 너무 크면 NiFi content
claim과 메모리 부담이 커진다. 실제 row 크기로 파일 크기를 측정해 결정한다.

API의 `recovery.extract_query_timeout`을 같은 값으로 두고 `recovery.stale`은 반드시 더 크게 둔다.

### 3.5 Hive

| Parameter | 예 | 의미와 결정 기준 |
|---|---|---|
| `HIVE.JDBC.URL` | `jdbc:hive2://hs2:10000/default?hive.resultset.use.unique.column.names=false` | HiveServer2 URL. query option 필수 |
| `HIVE.JDBC.USER` | `nifi` | Hive 접속 사용자 |
| `HIVE.JDBC.PASSWORD` | secret/빈 값 | 환경 인증 방식에 맞춤 |
| `HIVE.POOL.MAX` | `4` | Job별 Hive 연결 pool 상한 |
| `HIVE.QUERY.TIMEOUT` | `1800` | Hive statement timeout(초) |

NiFi JVM 시간대와 Hive의 `hive.local.time.zone`은 같아야 한다.

## 4. NiFi `job_params`

### 4.1 Job과 원천 범위

| Parameter | 예 | 의미·주의사항 |
|---|---|---|
| `JOB.KEY` | `ORACLE_INSP_DTL_DAILY` | PG, Context, API route, HDFS 경로의 고정 키 |
| `BUSINESS.KEY` | `2026-09-28` | 기본 구현은 `YYYY-MM-DD` 형식만 허용 |
| `SRC.OWNER` | `APP` | Oracle owner. 신뢰된 설정값만 사용 |
| `SRC.TABLE` | `INSP_DTL` | Oracle table |
| `SRC.COLUMNS` | `ID, BASE_DT, CAST(AMOUNT AS NUMBER(18,2)) AS AMOUNT` | 추출 select list. Hive DDL 순서와 맞춤 |
| `SRC.SPLIT.COLUMN` | `INSP_DTL_SEQ` | 숫자형, 범위 내 NULL 없음, 가능하면 고른 분포와 인덱스 |
| `SRC.BASE.WHERE` | `BASE_DT = TO_DATE('${load.business.key}','YYYY-MM-DD')` | 모든 원천 지표와 파티션 SQL에 공통 적용 |
| `PARTITION.COUNT` | `8` | manifest 범위 수. 병렬도와 동일하지 않을 수 있음 |
| `ALLOW.EMPTY.SOURCE` | `false` | `false`면 원천 0건 manifest를 실패 처리 |

`SRC.OWNER`, `SRC.TABLE`, column 및 SQL 조각은 Parameter Context를 변경할 수 있는 관리자만 수정해야 한다.
FlowFile에서 받은 자유 입력을 SQL 식별자나 조건으로 사용하면 안 된다.

### 4.2 데이터 품질

| Parameter | 의미 |
|---|---|
| `DQ.AMOUNT.COLUMN` | 원천/staging/target의 합계를 비교할 숫자 컬럼 |
| `DQ.TIMESTAMP.COLUMN` | 최솟값·최댓값을 비교할 시각 컬럼 |
| `DQ.PK.COLUMN` | staging/target에서 중복 수를 검사할 key 컬럼 |

현재 Flow는 `SOURCE_COUNT`, `NULL_SPLIT_COUNT`, `DUP_PK_COUNT`, `AMOUNT_SUM`, `MIN_TS`, `MAX_TS`를
기본 지표로 사용한다. 업무상 허용 오차가 필요하면 PG-40/60의 지표 SQL과 API에 기록하는 result를 함께
변경한다.

### 4.3 staging과 target

| Parameter | 예 | 의미·주의사항 |
|---|---|---|
| `HIVE.STAGE.DB` | `stg` | run별 external table을 만들 DB |
| `HIVE.STAGE.TABLE.PREFIX` | `TMP_INSP_DTL_` | API가 run ID를 붙이는 접두사 |
| `HIVE.STAGE.DDL.COLUMNS` | `ID DECIMAL(19,0), BASE_DT TIMESTAMP, ...` | `SRC.COLUMNS`의 결과 이름·순서·타입과 일치 |
| `HIVE.TARGET.DB` | `dw` | 게시 대상 DB |
| `HIVE.TARGET.TABLE` | `insp_dtl` | 게시 대상 table |
| `TARGET.PARTITION.CLAUSE` | `PARTITION (base_dt='${load.business.key}')` | 교체 범위. 빈 값이면 table 전체 overwrite |
| `HIVE.INSERT.COLUMNS` | `ID, ITEM_CD, AMOUNT, REG_TS` | target insert select list. 정적 partition column 제외 |
| `TARGET.BUSINESS.WHERE` | `base_dt = '${load.business.key}'` | target 검증 범위. partition clause와 동일 범위 |

`TARGET.PARTITION.CLAUSE`와 `TARGET.BUSINESS.WHERE`가 다르면 게시 성공 뒤 target 검증이 실패하거나 잘못된
범위를 성공으로 오인할 수 있다. 변경 검토 시 두 값을 한 쌍으로 본다.

## 5. Parameter 상호 제약

| 관계 | 조건 |
|---|---|
| Oracle 동시성 | 전체 PG-20 동시 query 수 ≤ Oracle 승인 세션과 pool 합계 |
| 추출 timeout | API `extract_query_timeout` = NiFi `EXTRACT.QUERY.TIMEOUT` |
| stale 판정 | API `recovery.stale` > `extract_query_timeout` |
| PG-05 주소 | API `nifi.receiver_url` port = `CONTROL.LISTEN.PORT` |
| API 인증 | NiFi token 원문 ↔ API `auth.token_digests.nifi` digest |
| HDFS 경로 | API가 받은 `hdfsRoot` = `HDFS.STAGE.ROOT` |
| staging schema | `SRC.COLUMNS` 결과 = `HIVE.STAGE.DDL.COLUMNS` |
| target 범위 | `TARGET.PARTITION.CLAUSE` = `TARGET.BUSINESS.WHERE`의 업무 범위 |
| 시간대 | NiFi JVM timezone = Hive `hive.local.time.zone` |
| cleanup batch | NiFi `CLEANUP.BATCH` ≤ API `cleanup.max_batch` 권장 |

## 6. Load Control API 설정

설정 우선순위는 환경 변수 > `config.yaml` > 기본값이다. 환경 변수의 중첩 구분자는 `__`다.
예: `LCA_DATABASE__URL`.

### 6.1 `server`

| 키 | 기본/예 | 의미 |
|---|---|---|
| `host` | `0.0.0.0` | bind address |
| `port` | `8080` | HTTP port |
| `workers` | 예시 `4` | Uvicorn worker 프로세스 수 |
| `root_path` | `""` | reverse proxy path prefix |
| `proxy_headers` | `true` | forwarded header 신뢰 여부 |
| `forwarded_allow_ips` | `127.0.0.1` | 신뢰할 proxy IP 목록 |
| `timeout_keep_alive` | `5` | HTTP keep-alive 초 |
| `timeout_graceful_shutdown` | `30` | 종료 시 진행 요청 대기 초 |
| `limit_concurrency` | `null` | 프로세스당 동시 연결 상한 |
| `ssl_certfile`, `ssl_keyfile` | `null` | 앱에서 TLS를 종료할 때 사용 |
| `ssl_ca_certs`, `ssl_client_cert_required` | `null`, `false` | 앱 수준 mTLS 설정 |

현재 프로젝트 전제는 HTTP지만 API 자체 TLS 옵션은 구현되어 있다. LB에서 TLS/mTLS를 종료한다면 앱의 TLS
필드는 비우고 proxy 신뢰 범위를 제한한다.

### 6.2 `database`

| 키 | 의미 |
|---|---|
| `url` | API/worker 런타임 async SQLAlchemy URL |
| `migration_url` | Alembic DDL 계정 URL. 없으면 `url` 사용 |
| `listen_dsn` | worker의 asyncpg `LISTEN` 전용 DSN. 없으면 polling만 사용 |
| `pool_size` | 프로세스당 상시 pool 크기 |
| `max_overflow` | pool 초과 임시 연결 수 |
| `tx_attempts` | deadlock/serialization 트랜잭션 재시도 횟수 |

최대 DB 연결 수를 계산할 때 `server workers × (pool_size + max_overflow)`와 worker 인스턴스의 pool,
LISTEN 전용 연결, migration/운영 연결을 합산한다.

### 6.3 `auth`

```yaml
auth:
  token_digests:
    nifi: ["<sha256-hex>"]
    operator: ["<sha256-hex>"]
```

- `nifi`: Flow 호출과 읽기 API
- `operator`: dispatch 재전송, `PUBLISH_UNKNOWN` 확정 등 운영 작업
- 교체 기간에는 이전·신규 digest를 함께 둔다.
- 원문 token은 API 설정의 `monitor` 또는 NiFi sensitive parameter에만 둔다.

digest 생성:

```bash
cd load-control-api
PYTHONPATH=src .venv/bin/python -m load_control.security '<token>'
```

### 6.4 `nifi`

| 키 | 의미 |
|---|---|
| `receiver_url` | API worker가 PG-05로 호출할 base URL |
| `timeout_seconds` | 한 번의 NiFi HTTP 호출 timeout |
| `client_cert`, `client_key`, `ca_bundle` | 선택적 mTLS client 설정 |

worker는 `receiver_url`이 없으면 시작하지 않는다.

### 6.5 `recovery`

| 키 | 기본 | 의미 |
|---|---:|---|
| `run_timeout` | `PT6H` | `CREATED`/`EXTRACTING` 전체 실행 상한 |
| `extract_query_timeout` | `PT60M` | NiFi query timeout과 같은 값 |
| `stale` | `PT90M` | RUNNING 파티션 heartbeat 정체 기준 |
| `mode` | `FAIL` | `FAIL` 또는 `REISSUE` |
| `max_attempts` | `3` | REISSUE 시 파티션 최대 claim 횟수 |
| `validation_stale` | `PT2H` | 검증 정체 경보 기준 |
| `publish_stale` | `PT2H` | PUBLISHING→PUBLISH_UNKNOWN 기준 |
| `sweeper_interval` | `PT1M` | 복구 점검 주기 |

`REISSUE`는 같은 SCN을 다시 읽으므로 Oracle undo가 충분하다는 증거가 있을 때만 사용한다.

### 6.6 `dispatch`

| 키 | 기본 | 의미 |
|---|---:|---|
| `max_attempts` | `20` | 전송과 ACK 재전송을 포함한 최대 시도 |
| `backoff_min` | `PT5S` | 첫 backoff |
| `backoff_max` | `PT5M` | 최대 backoff |
| `ack_timeout` | `PT10M` | 202 이후 실제 처리 ACK 대기 |
| `lease` | `PT60S` | dispatch 선점 시간. `nifi.timeout_seconds`보다 길게 |
| `poll_interval` | `PT5S` | NOTIFY 유실 대비 polling |
| `batch` | `20` | 한 번에 lease하고 병렬 전송할 수 |

### 6.7 `cleanup`, `worker`, `monitor`, `logging`

| 섹션.키 | 기본/예 | 의미 |
|---|---|---|
| `cleanup.success_retention` | `P3D` | 성공 run 산출물 보존 |
| `cleanup.failed_retention` | `P14D` | 실패/TIMED_OUT 산출물 보존 |
| `cleanup.max_batch` | `200` | API가 한 번에 반환할 최대 후보 수 |
| `worker.metrics_host` | `0.0.0.0` | worker metric bind address |
| `worker.metrics_port` | `9100` | `null`이면 metric server 비활성 |
| `monitor.api_url` | `null` | TUI가 접속할 API, null이면 localhost |
| `monitor.token` | `null` | 조회용 token 원문 |
| `monitor.operator_token` | `null` | 운영 작업용 token 원문 |
| `monitor.refresh_seconds` | `5` | TUI 새로고침 주기 |
| `monitor.log_dir` | `logs` | TUI가 읽을 로그/PID 경로 |
| `logging.level` | `INFO` | root 로그 수준 |
| `logging.format` | `text` | stdout: `text`, `json`, `console` |
| `logging.stdout` | 예시 `false` | journal/container 수집이면 true |
| `logging.access_log` | `true` | API 수신·응답 로그 |
| `logging.access_body` | `true` | JSON 본문 기록 여부 |
| `logging.access_body_max` | `2000` | 본문 최대 글자 수 |
| `logging.file.path` | `logs/{service}.log` | `{service}`는 server/worker |
| `logging.file.max_bytes` | `104857600` | 회전 기준 크기 |
| `logging.file.backup_count` | `10` | 회전 파일 수 |
| `logging.loggers` | logger별 | SQL/HTTP 등 개별 수준 |

## 7. 변경 영향도

| 변경 | 영향 |
|---|---|
| 공통 Parameter | PG-05와 모든 Job에 영향. NiFi update-request가 참조 구성요소를 잠시 정지시킬 수 있음 |
| Job Parameter | 해당 Job 재생성 또는 Context 변경 필요 |
| `HDFS.STAGE.ROOT`/stage prefix | 기존 cleanup 대상이 PG-70 안전 검사에서 거부될 수 있음 |
| `JOB.KEY` | 새 Job으로 간주. PG-05 route와 HDFS 경로 변경 |
| target partition clause | 데이터 교체 범위 변경. 별도 리뷰와 비운영 검증 필수 |
| recovery timeout | sweeper의 자동 조치 시점 변경 |
| token | API digest와 NiFi/TUI 원문을 겹치는 기간에 순차 교체 |

설정을 확정했으면 [설치 및 적용 절차](./03-installation-and-apply.md)로 진행한다.
