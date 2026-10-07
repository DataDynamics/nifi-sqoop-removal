# 설정 레퍼런스

설정은 두 파일로 나뉩니다.

- NiFi Flow 배포 설정: `nifi-flow/job-config.example.json`
- Load Control API 설정: `load-control-api/config/config.example.yaml`

두 설정은 독립적이지 않습니다. 특히 timeout, URL, token, Job key, HDFS root는 양쪽 관계를 함께 확인해야 합니다.

## 1. 설정 파일 관리 원칙

- 실제 설정 파일은 저장소 밖에 두고 권한을 `600`으로 설정합니다.
- 비밀번호와 token은 예시 파일에 직접 쓰지 않습니다.
- `nifi` token은 NiFi 로그인 token이 아니라 Load Control API가 NiFi Flow를 식별하는 별도 공유 비밀값입니다.
  NiFi 자체 인증을 사용하지 않는 환경에서도 생성해야 합니다.
- `CONTROL.API.AUTHORIZATION`에는 `Bearer ` 접두사까지 포함합니다.
- `JOB.KEY`는 `[A-Z0-9_]{1,200}` 형식이어야 합니다.
- 공통 Parameter Context는 모든 Job이 공유합니다. 새 Job 빌드 시 `common_params`가 기존 값을 갱신하므로
  Job마다 다른 값을 넣지 않습니다.
- Job Parameter Context는 빌드 때 삭제 후 다시 만듭니다. 같은 Job PG가 있으면 빌더가 중단됩니다.
- Load Control API는 알 수 없는 설정 키를 거부하므로 오타가 있으면 시작되지 않습니다.

## 2. 검증에 사용한 예시

아래 값은 [검증 결과](./08-verification-results.md)에 기록된 실제 검증 시나리오와
[`nifi-flow/job-config.example.json`](../nifi-flow/job-config.example.json)을 한곳에서 볼 수 있도록 정리한 것입니다.
비밀번호·token·호스트명은 저장소에 남기지 않았으므로 placeholder를 실제 환경 값으로 바꿔야 합니다.

### 2.1 시험 환경과 원천 데이터 기준값

| 항목 | 검증값 |
|---|---|
| NiFi | CFM 4.12 / NiFi 2.6.0, 2노드, 비보안 |
| Oracle | Oracle Database 23ai Free, ojdbc11 21.15 |
| HDFS/Hive | Hadoop 3.4.1, Hive 4.0.1 |
| 관리 DB | PostgreSQL 16 |
| 원천 | `APP.INSP_DTL` |
| 업무일자 | `2026-09-28` |
| 전체 건수 | `105,000` |
| split column | `INSP_DTL_SEQ`, 최솟값 `1`, 최댓값 `120000`, NULL `0`건 |
| 의도적 공백 | `30001`~`45000`이 없음 |
| 파티션 | 8개 중 `0002`가 0건, 나머지 7개는 각 15,000건 |
| 금액 합계 | `71,853,075` |
| 검증 결과 | source = extracted = staging = target = `105,000`, 모든 STAGING/TARGET 지표 PASS |

이 데이터는 0건 파티션이 worker로 전달되지 않는지와 2노드 분산 추출을 동시에 확인하도록 구성했습니다.
8개 파티션의 기대 범위와 건수는 다음과 같습니다.

| partition ID | 조건 | 기대 건수 |
|---|---|---:|
| `0000` | `1 <= seq < 15001` | 15,000 |
| `0001` | `15001 <= seq < 30001` | 15,000 |
| `0002` | `30001 <= seq < 45001` | 0 |
| `0003` | `45001 <= seq < 60001` | 15,000 |
| `0004` | `60001 <= seq < 75001` | 15,000 |
| `0005` | `75001 <= seq < 90001` | 15,000 |
| `0006` | `90001 <= seq < 105001` | 15,000 |
| `0007` | `105001 <= seq <= 120000` | 15,000 |

### 2.2 테스트 Job의 NiFi 설정 예시

다음 JSON은 검증 Job의 전체 설정 예시입니다. `names`를 생략했으므로 기본 이름을 사용합니다.

```json
{
  "common_params": {
    "CONTROL.API.URL": "http://<api-host>:8080/v1",
    "CONTROL.API.AUTHORIZATION": "Bearer <nifi-token>",
    "CONTROL.API.TIMEOUT": "30 secs",
    "CONTROL.LISTEN.PORT": "9443",
    "META.JDBC.URL": "jdbc:postgresql://<meta-host>:5432/nifiops",
    "META.JDBC.USER": "nifi_runtime",
    "META.JDBC.PASSWORD": "<secret>",
    "META.JDBC.DRIVER.PATH": "/opt/nifi/jdbc/postgresql-42.7.3.jar",
    "ORACLE.JDBC.URL": "jdbc:oracle:thin:@//<oracle-host>:1521/ORCLPDB1",
    "ORACLE.JDBC.USER": "NIFI_READER",
    "ORACLE.JDBC.PASSWORD": "<secret>",
    "ORACLE.JDBC.DRIVER.PATH": "/opt/nifi/jdbc/ojdbc11.jar",
    "ORACLE.POOL.MAX": "8",
    "ORACLE.NUMBER.DEFAULT.PRECISION": "38",
    "ORACLE.NUMBER.DEFAULT.SCALE": "10",
    "HADOOP.CONF.FILES": "/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml",
    "HDFS.STAGE.ROOT": "/data/nifi/stage",
    "HDFS.PERMISSIONS.UMASK": "027",
    "EXTRACT.FETCH.SIZE": "5000",
    "EXTRACT.ROWS.PER.FILE": "500000",
    "EXTRACT.QUERY.TIMEOUT": "60 min",
    "HIVE.JDBC.URL": "jdbc:hive2://<hs2-host>:10000/default?hive.resultset.use.unique.column.names=false",
    "HIVE.JDBC.USER": "nifi",
    "HIVE.JDBC.PASSWORD": "<secret>",
    "HIVE.POOL.MAX": "4",
    "HIVE.QUERY.TIMEOUT": "1800",
    "CLEANUP.BATCH": "50"
  },
  "job_params": {
    "JOB.KEY": "ORACLE_INSP_DTL_DAILY",
    "BUSINESS.KEY": "2026-09-28",
    "SRC.OWNER": "APP",
    "SRC.TABLE": "INSP_DTL",
    "SRC.COLUMNS": "INSP_DTL_SEQ, BASE_DT, ITEM_CD, CAST(AMOUNT AS NUMBER(18,2)) AS AMOUNT, REG_TS, NOTE",
    "SRC.SPLIT.COLUMN": "INSP_DTL_SEQ",
    "SRC.BASE.WHERE": "BASE_DT = TO_DATE('${load.business.key}', 'YYYY-MM-DD')",
    "DQ.AMOUNT.COLUMN": "AMOUNT",
    "DQ.TIMESTAMP.COLUMN": "REG_TS",
    "PARTITION.COUNT": "8",
    "HIVE.STAGE.TABLE.PREFIX": "TMP_INSP_DTL_",
    "ALLOW.EMPTY.SOURCE": "false",
    "HIVE.STAGE.DB": "stg",
    "HIVE.STAGE.DDL.COLUMNS": "INSP_DTL_SEQ DECIMAL(19,0), BASE_DT TIMESTAMP, ITEM_CD STRING, AMOUNT DECIMAL(18,2), REG_TS TIMESTAMP, NOTE STRING",
    "HIVE.TARGET.DB": "dw",
    "HIVE.TARGET.TABLE": "insp_dtl",
    "TARGET.PARTITION.CLAUSE": "PARTITION (base_dt='${load.business.key}')",
    "HIVE.INSERT.COLUMNS": "INSP_DTL_SEQ, ITEM_CD, AMOUNT, REG_TS, NOTE",
    "TARGET.BUSINESS.WHERE": "base_dt = '${load.business.key}'",
    "DQ.PK.COLUMN": "INSP_DTL_SEQ"
  }
}
```

테스트 테이블과 Hive staging schema의 대응은 다음과 같습니다.

| Oracle 추출 결과 | Hive staging | 용도 |
|---|---|---|
| `INSP_DTL_SEQ` | `DECIMAL(19,0)` | split 및 PK 중복 검사 |
| `BASE_DT` | `TIMESTAMP` | 원천 업무일자 컬럼 |
| `ITEM_CD` | `STRING` | 업무 데이터 |
| `CAST(AMOUNT AS NUMBER(18,2))` | `DECIMAL(18,2)` | 합계 검증 |
| `REG_TS` | `TIMESTAMP` | 최소·최대 시각 검증 |
| `NOTE` | `STRING` | 문자열 적재 검증 |

### 2.3 대응하는 Load Control API 설정 예시

NiFi 예시와 직접 연관되는 API 설정은 다음과 같습니다. 전체 logging/monitor 설정은
[`config.example.yaml`](../load-control-api/config/config.example.yaml)을 사용합니다.

```yaml
server:
  host: 0.0.0.0
  port: 8080
  workers: 4

database:
  url: postgresql+asyncpg://load_control_api:<secret>@<meta-host>:5432/nifiops
  migration_url: postgresql+asyncpg://nifi_ops_migrator:<secret>@<meta-host>:5432/nifiops
  listen_dsn: postgresql://load_control_api:<secret>@<meta-host>:5432/nifiops
  pool_size: 10
  max_overflow: 5
  tx_attempts: 3

auth:
  token_digests:
    nifi: ["<sha256-of-nifi-token>"]
    operator: ["<sha256-of-operator-token>"]

nifi:
  receiver_url: http://<nifi-lb>:9443
  timeout_seconds: 10

recovery:
  run_timeout: PT6H
  extract_query_timeout: PT60M
  stale: PT90M
  mode: FAIL
  max_attempts: 3
  validation_stale: PT2H
  publish_stale: PT2H
  sweeper_interval: PT1M

dispatch:
  max_attempts: 20
  backoff_min: PT5S
  backoff_max: PT5M
  ack_timeout: PT10M
  lease: PT60S
  poll_interval: PT5S
  batch: 20

cleanup:
  success_retention: P3D
  failed_retention: P14D
  max_batch: 200
```

두 설정을 함께 적용할 때 다음 값이 서로 맞아야 합니다.

- `CONTROL.API.URL`의 host/port와 API `server` 또는 앞단 LB 주소
- `CONTROL.LISTEN.PORT=9443`과 API `nifi.receiver_url`의 port
- `EXTRACT.QUERY.TIMEOUT=60 min`과 API `extract_query_timeout=PT60M`
- API `stale=PT90M`이 query timeout보다 큰 관계
- NiFi의 Bearer token 원문과 API `auth.token_digests.nifi`의 SHA-256 digest

### 2.4 정상 실행 후 기대값

| 확인 위치 | 기대값 |
|---|---|
| `load_run` | 최종 `SUCCESS`, source/extracted/staging/target 모두 `105000` |
| `load_partition` | 8개 모두 `SUCCESS`; `0002`는 실제 추출 없이 등록 시점 성공 |
| `load_file` | 활성 파티션 7개의 Parquet chunk 원장 |
| HDFS | `/data/nifi/stage/ORACLE_INSP_DTL_DAILY/run_id=<run_id>`와 `_SUCCESS` |
| Hive staging | run 전용 `stg.tmp_insp_dtl_<run-id-hex>` |
| Hive target | `dw.insp_dtl`의 `base_dt='2026-09-28'` 파티션 105,000건 |
| validation | 건수, NULL, PK 중복, 금액 합계, 시각 최소·최대 모두 PASS |

## 3. 6개 Oracle 테이블 적용 방법과 현재 제약

> [!IMPORTANT]
> 현재 빌더는 설정 JSON 하나당 Job PG 전체를 하나 만듭니다. Oracle 테이블 6개를 처리하려면 설정 파일
> 6개와 서로 다른 `JOB.KEY` 6개가 필요하고, 빌더를 6번 실행해야 합니다. 각 Job에는 PG-00·10·20·40·50·60·70·90이
> 모두 생성됩니다. 공통 PG-05와 공통 Parameter Context만 공유합니다.

즉, `deploy_job_flow.py`가 생성 템플릿 역할을 하므로 NiFi UI에서 Processor를 직접 복사할 필요는 없지만,
실행 결과는 동일한 Processor 구성을 가진 Job PG 6개입니다. **한 개의 PG에서 FlowFile 변수만 바꿔 6개
테이블을 처리하는 구조는 현재 구현되어 있지 않습니다.** 실행 중 Parameter Context 값을 다른 테이블 값으로
바꾸는 것도 이미 흐르는 FlowFile과 섞일 수 있으므로 대안으로 사용하면 안 됩니다.

### 3.1 테이블·Oracle 파티션·split 컬럼 지원 상태

| 요구값 | 현재 설정 | 지원 여부와 적용 방법 |
|---|---|---|
| Oracle owner | `SRC.OWNER` | Job별 변경 가능 |
| Oracle 테이블명 | `SRC.TABLE` | Job별 변경 가능 |
| Oracle 물리 파티션명 | 전용 Parameter 없음 | `PARTITION(P_...)` 직접 지정은 현재 미지원 |
| 업무/파티션 조건 | `SRC.BASE.WHERE` | Job별 변경 가능. 파티션 키 조건을 주면 Oracle partition pruning 가능 |
| Sqoop `--split-by` 컬럼 | `SRC.SPLIT.COLUMN` | Job별 변경 가능하며 병렬 범위 분할에 이미 사용됨 |
| 논리 분할 수 | `PARTITION.COUNT` | Job별 manifest partition 수 |
| Hive target 파티션 | `TARGET.PARTITION.CLAUSE` | Job별 변경 가능. Oracle 물리 파티션과 다른 설정 |

여기서 “index column”은 Oracle 인덱스 객체 이름이 아니라 Sqoop `--split-by`에 해당하는 **컬럼명**입니다.
예를 들어 원천이 자동으로 유일한 `LOAD_SEQ`를 채운다면 다음처럼 설정합니다.

```json
{
  "SRC.TABLE": "TABLE_01",
  "SRC.SPLIT.COLUMN": "LOAD_SEQ",
  "PARTITION.COUNT": "8"
}
```

현재 구현에서 `SRC.SPLIT.COLUMN`은 다음 위치에 실제로 적용됩니다.

1. PG-10이 같은 SCN에서 `MIN(LOAD_SEQ)`, `MAX(LOAD_SEQ)`, NULL 수를 계산합니다.
2. 최솟값~최댓값을 `PARTITION.COUNT`개의 연속 범위로 나눕니다.
3. 각 범위의 예상 건수를 manifest에 저장합니다.
4. PG-20이 `LOAD_SEQ >= lower`와 `< upper` 조건으로 병렬 SELECT합니다. 마지막 범위만 `<= upper`입니다.
5. API가 파티션별 실제 건수를 예상 건수와 비교하고 전체 완료를 판정합니다.

따라서 자동 증가·유일·숫자형·업무 범위 내 NOT NULL인 컬럼은 이 방식에 적합합니다. Oracle 인덱스는
Optimizer가 선택하며 Flow 설정에 인덱스 이름을 넣지 않습니다. 업무 조건이 `BASE_DT`이고 split 컬럼이
`LOAD_SEQ`라면 `(BASE_DT, LOAD_SEQ)` 형태의 인덱스 또는 파티션별 local index를 DBA와 검토합니다.

적용 전에 각 테이블에서 다음을 확인합니다.

```sql
SELECT COUNT(*) AS total_count,
       COUNT(LOAD_SEQ) AS non_null_count,
       COUNT(DISTINCT LOAD_SEQ) AS distinct_count,
       MIN(LOAD_SEQ) AS min_value,
       MAX(LOAD_SEQ) AS max_value
  FROM APP.TABLE_01
 WHERE BASE_DT = DATE '2026-09-28';
```

`total_count = non_null_count = distinct_count`여야 가장 안전합니다. 값 분포가 한쪽에 몰려 있으면 범위별
건수가 달라져 병렬 처리 시간이 긴 파티션 하나에 의해 결정될 수 있으므로 manifest 결과도 확인합니다.

### 3.2 Oracle 물리 파티션명 처리

현재 SQL은 다음 형태입니다.

```sql
FROM #{SRC.OWNER}.#{SRC.TABLE} AS OF SCN <snapshot_scn>
WHERE #{SRC.BASE.WHERE}
```

따라서 `SRC.TABLE`에 `TABLE_01 PARTITION(P_20260928)` 같은 SQL 조각을 억지로 넣지 않습니다. 식별자 검증,
cleanup 추적, 설정 리뷰가 어려워집니다. 현재 코드 변경 없이 적용하려면 `SRC.BASE.WHERE`에 파티션 키 조건을
넣어 Oracle이 partition pruning하도록 합니다.

```json
{
  "SRC.TABLE": "TABLE_01",
  "SRC.BASE.WHERE": "BASE_DT = TO_DATE('${load.business.key}', 'YYYY-MM-DD')"
}
```

반드시 `PARTITION(P_20260928)`처럼 물리 파티션명을 직접 지정해야 한다면 빌더에 별도의
`SRC.PARTITION.NAME`을 추가하고 PG-10/20의 모든 원천 SQL을 함께 변경해야 합니다. 현재 매뉴얼과 빌더에는
이 기능이 구현되어 있지 않습니다. 반면 `TARGET.PARTITION.CLAUSE`는 Hive 게시 대상을 정하는 값이며 Oracle
파티션명으로 사용되지 않습니다.

### 3.3 6개 Job 설정 구성

공통 접속값은 여섯 파일에서 완전히 같아야 합니다. Job별로 다음 값을 각각 정의합니다.

| 구분 | Job별 고유 또는 검토가 필요한 값 |
|---|---|
| 식별 | `JOB.KEY`, `BUSINESS.KEY` |
| 원천 | `SRC.OWNER`, `SRC.TABLE`, `SRC.COLUMNS`, `SRC.BASE.WHERE`, `SRC.SPLIT.COLUMN` |
| 병렬도 | `PARTITION.COUNT` |
| 품질 | `DQ.AMOUNT.COLUMN`, `DQ.TIMESTAMP.COLUMN`, `DQ.PK.COLUMN` |
| staging | `HIVE.STAGE.TABLE.PREFIX`, `HIVE.STAGE.DDL.COLUMNS` |
| target | `HIVE.TARGET.DB`, `HIVE.TARGET.TABLE`, `TARGET.PARTITION.CLAUSE`, `HIVE.INSERT.COLUMNS`, `TARGET.BUSINESS.WHERE` |

예시 파일 배치:

```text
config/jobs/
├── table-01.json  # JOB.KEY=ORACLE_TABLE_01_DAILY, SRC.TABLE=TABLE_01, SRC.SPLIT.COLUMN=TABLE_01_SEQ
├── table-02.json  # JOB.KEY=ORACLE_TABLE_02_DAILY, SRC.TABLE=TABLE_02, SRC.SPLIT.COLUMN=TABLE_02_SEQ
├── table-03.json
├── table-04.json
├── table-05.json
└── table-06.json
```

각 파일로 빌더를 한 번씩 실행합니다.

```bash
python3 nifi-flow/deploy_job_flow.py http://<nifi-host>:<port>/nifi-api config/jobs/table-01.json
python3 nifi-flow/deploy_job_flow.py http://<nifi-host>:<port>/nifi-api config/jobs/table-02.json
python3 nifi-flow/deploy_job_flow.py http://<nifi-host>:<port>/nifi-api config/jobs/table-03.json
python3 nifi-flow/deploy_job_flow.py http://<nifi-host>:<port>/nifi-api config/jobs/table-04.json
python3 nifi-flow/deploy_job_flow.py http://<nifi-host>:<port>/nifi-api config/jobs/table-05.json
python3 nifi-flow/deploy_job_flow.py http://<nifi-host>:<port>/nifi-api config/jobs/table-06.json
```

결과는 `JOB_ORACLE_TABLE_01_DAILY`부터 `JOB_ORACLE_TABLE_06_DAILY`까지 6개의 독립 Job PG입니다. Job마다
Trigger, queue, Controller Service, 오류 처리와 상태가 분리되고, root의 PG-05만 6개 Job route를 공유합니다.

6개 Job을 동시에 실행할 때는 기본 PG-20 Concurrent Tasks가 노드당 4이므로 2노드 기준 최대 48개의
파티션 SELECT가 동시에 시작될 수 있습니다(`6 Job × 2 node × 4 task`). Job별 `ORACLE.POOL.MAX`와 Oracle
전체 승인 세션을 함께 계산하고, 필요하면 schedule을 분산하거나 Concurrent Tasks를 낮춰 빌더를 수정합니다.

### 3.4 단일 Processor 세트가 필요한 경우

한 벌의 Processor가 6개 테이블 설정을 FlowFile attribute로 받아 처리하게 하려면 별도 리팩터링이 필요합니다.
테이블 설정을 run 생성 시 API의 `load_run.parameters`에 저장하고, validation callback과 partition reissue에도
그 설정을 다시 전달해야 합니다. PG-05도 Job별 route가 아닌 공통 route로 바뀌어야 하며, PG-40~60의 정적
Parameter 참조를 run별 attribute로 교체해야 합니다. 현재 구현에 설정 JSON만 추가해서 달성할 수 있는 범위가
아니므로, 이 구조가 필수라면 별도 개발·회귀 시험 항목으로 잡습니다.

## 4. NiFi 빌더 최상위 구조

```json
{
  "names": {},
  "common_params": {},
  "job_params": {}
}
```

### 4.1 `names`

대부분 생략합니다. 여러 Job이 같은 `common_context`와 `control_receiver`를 사용해야 합니다.

| 키 | 기본값 | 의미 |
|---|---|---|
| `process_group` | `JOB_<JOB.KEY>` | root에 생성할 Job PG 이름 |
| `job_context` | `PC_JOB_<JOB.KEY>` | Job Parameter Context |
| `common_context` | `PC_SQOOP_REPLACEMENT_COMMON` | 공유 Parameter Context |
| `control_receiver` | `PG-05 Control Receiver` | 공유 수신 PG 이름 |

## 5. NiFi `common_params`

### 5.1 Load Control 연결

| Parameter | 예 | 의미와 결정 기준 |
|---|---|---|
| `CONTROL.API.URL` | `http://api-lb:8080/v1` | API server 또는 LB의 `/v1`까지 포함한 주소 |
| `CONTROL.API.AUTHORIZATION` | `Bearer <nifi-token>` | `nifi` role token의 원문. 전체 값이 sensitive parameter |
| `CONTROL.API.TIMEOUT` | `30 secs` | NiFi가 API 응답을 기다리는 시간. 일반 판정 요청 기준 |
| `CONTROL.LISTEN.PORT` | `9443` | PG-05 `HandleHttpRequest`가 모든 NiFi 노드에서 여는 포트 |
| `CLEANUP.BATCH` | `50` | PG-70이 한 주기에 요청하는 cleanup 후보 수 |

API worker의 `nifi.receiver_url`은 `http://<NiFi LB>:<CONTROL.LISTEN.PORT>`여야 합니다. NiFi→API와
API worker→NiFi 방향의 방화벽을 각각 확인합니다.

### 5.2 관리 PostgreSQL

NiFi는 `load_event` INSERT만 수행합니다. 상태 테이블은 API만 갱신합니다.

| Parameter | 의미 |
|---|---|
| `META.JDBC.URL` | `jdbc:postgresql://<host>:5432/<db>` |
| `META.JDBC.USER` | `nifi_runtime` 권장 |
| `META.JDBC.PASSWORD` | NiFi runtime 계정 비밀번호 |
| `META.JDBC.DRIVER.PATH` | 모든 NiFi 노드의 PostgreSQL JDBC jar 경로 |

### 5.3 Oracle

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
넘으면 안 됩니다. 여러 Job의 최악 동시 실행까지 합산합니다.

정밀도 없는 `NUMBER`는 `SRC.COLUMNS`에서 `CAST(... AS NUMBER(p,s))`로 명시하는 것이 안전합니다. 기본
scale보다 소수 자릿수가 많으면 오류 없이 반올림될 수 있습니다.

### 5.4 HDFS와 추출

| Parameter | 예 | 의미와 결정 기준 |
|---|---|---|
| `HADOOP.CONF.FILES` | `/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml` | 모든 NiFi 노드의 설정 파일 |
| `HDFS.STAGE.ROOT` | `/data/nifi/stage` | API가 run별 경로를 만드는 상위 경로 |
| `HDFS.PERMISSIONS.UMASK` | `027` | PutHDFS 파일 umask |
| `EXTRACT.FETCH.SIZE` | `5000` | Oracle JDBC fetch size. 메모리와 round trip 절충 |
| `EXTRACT.ROWS.PER.FILE` | `500000` | Parquet FlowFile/chunk당 최대 행 수 |
| `EXTRACT.QUERY.TIMEOUT` | `60 min` | 파티션 SQL 최대 실행 시간 |

`EXTRACT.ROWS.PER.FILE`이 너무 작으면 HDFS small file과 API chunk 보고가 많아지고, 너무 크면 NiFi content
claim과 메모리 부담이 커집니다. 실제 row 크기로 파일 크기를 측정해 결정합니다.

API의 `recovery.extract_query_timeout`을 같은 값으로 두고 `recovery.stale`은 반드시 더 크게 둡니다.

### 5.5 Hive

| Parameter | 예 | 의미와 결정 기준 |
|---|---|---|
| `HIVE.JDBC.URL` | `jdbc:hive2://hs2:10000/default?hive.resultset.use.unique.column.names=false` | HiveServer2 URL. query option 필수 |
| `HIVE.JDBC.USER` | `nifi` | Hive 접속 사용자 |
| `HIVE.JDBC.PASSWORD` | secret/빈 값 | 환경 인증 방식에 맞춤 |
| `HIVE.POOL.MAX` | `4` | Job별 Hive 연결 pool 상한 |
| `HIVE.QUERY.TIMEOUT` | `1800` | Hive statement timeout(초) |

NiFi JVM 시간대와 Hive의 `hive.local.time.zone`은 같아야 합니다.

## 6. NiFi `job_params`

### 6.1 Job과 원천 범위

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

`SRC.OWNER`, `SRC.TABLE`, column 및 SQL 조각은 Parameter Context를 변경할 수 있는 관리자만 수정해야 합니다.
FlowFile에서 받은 자유 입력을 SQL 식별자나 조건으로 사용하면 안 됩니다.

### 6.2 데이터 품질

| Parameter | 의미 |
|---|---|
| `DQ.AMOUNT.COLUMN` | 원천/staging/target의 합계를 비교할 숫자 컬럼 |
| `DQ.TIMESTAMP.COLUMN` | 최솟값·최댓값을 비교할 시각 컬럼 |
| `DQ.PK.COLUMN` | staging/target에서 중복 수를 검사할 key 컬럼 |

현재 Flow는 `SOURCE_COUNT`, `NULL_SPLIT_COUNT`, `DUP_PK_COUNT`, `AMOUNT_SUM`, `MIN_TS`, `MAX_TS`를
기본 지표로 사용합니다. 업무상 허용 오차가 필요하면 PG-40/60의 지표 SQL과 API에 기록하는 result를 함께
변경합니다.

### 6.3 staging과 target

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
범위를 성공으로 오인할 수 있습니다. 변경 검토 시 두 값을 한 쌍으로 봅니다.

## 7. Parameter 상호 제약

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

## 8. Load Control API 설정

설정 우선순위는 환경 변수 > `config.yaml` > 기본값입니다. 환경 변수의 중첩 구분자는 `__`입니다.
예: `LCA_DATABASE__URL`.

### 8.1 `server`

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

현재 프로젝트 전제는 HTTP지만 API 자체 TLS 옵션은 구현되어 있습니다. LB에서 TLS/mTLS를 종료한다면 앱의 TLS
필드는 비우고 proxy 신뢰 범위를 제한합니다.

### 8.2 `database`

| 키 | 의미 |
|---|---|
| `url` | API/worker 런타임 async SQLAlchemy URL |
| `migration_url` | Alembic DDL 계정 URL. 없으면 `url` 사용 |
| `listen_dsn` | worker의 asyncpg `LISTEN` 전용 DSN. 없으면 polling만 사용 |
| `pool_size` | 프로세스당 상시 pool 크기 |
| `max_overflow` | pool 초과 임시 연결 수 |
| `tx_attempts` | deadlock/serialization 트랜잭션 재시도 횟수 |

최대 DB 연결 수를 계산할 때 `server workers × (pool_size + max_overflow)`와 worker 인스턴스의 pool,
LISTEN 전용 연결, migration/운영 연결을 합산합니다.

### 8.3 `auth`

> [!IMPORTANT]
> 여기서 `nifi` token은 NiFi UI나 NiFi REST API에 로그인할 때 사용하는 token이 아닙니다. NiFi Flow가
> Load Control API로 보내는 요청을 인증하기 위해 프로젝트에서 임의로 생성하는 공유 비밀값입니다.
> NiFi가 비보안·무인증 모드여도 이 token은 별도로 설정합니다.

```yaml
auth:
  token_digests:
    nifi: ["<nifi-token-sha256-hex>"]
    operator: ["<operator-token-sha256-hex>"]
```

#### role의 의미와 권한 경계

`token_digests`의 key가 role이고, 각 목록은 그 role로 허용할 token의 SHA-256
digest들입니다. API는 요청으로 받은 token 원문을 매번 SHA-256으로 변환해 허용된
role 목록과 비교합니다. token이 없으면 `401 UNAUTHENTICATED`, token은 있지만 해당 API의
role에 없으면 `403 FORBIDDEN`을 반환합니다.

| role | 사용 주체 | 허용 범위 |
|---|---|---|
| `nifi` | NiFi PG-10·20·40·50·60·70·90 | run/manifest 생성, partition/chunk 보고, 검증·게시 결과, 실패·cleanup 보고, 조회 |
| `operator` | TUI, 운영자 curl/운영 도구 | 조회·monitor·cleanup, dispatch 재전송, `PUBLISH_UNKNOWN` 수동 확정 |

같은 digest를 `nifi`와 `operator`에 모두 넣으면 하나의 token이 두 권한을 모두 갖습니다.
기능적으로는 동작하지만 NiFi token이 유출되면 수동 확정 권한까지 얻게 되므로
운영에서는 반드시 두 token을 다르게 생성합니다. 현재 구현의 감사 기록은 `operator`
role까지만 식별하고 개인을 구분하지는 않습니다. 여러 운영자의 개인별 추적이 필요하면
앞단 프록시의 SSO/OIDC 또는 개인별 token 기능을 별도로 적용해야 합니다.

- 원문 token은 API 설정의 `monitor` 또는 NiFi sensitive parameter에만 둡니다.
- 여섯 Job은 공통 Parameter Context를 사용하므로 `nifi` token 하나를 공유합니다. Job별 발급은 필요 없습니다.
- `operator` token은 `nifi` token과 다르게 생성해 운영자 전용 권한을 분리합니다.

원문 token은 충분히 긴 난수로 생성합니다. 다음 명령은 256-bit 값을 64자리 hex 문자열로 만듭니다.

```bash
openssl rand -hex 32
```

출력된 값을 비밀 저장소에 보관하고, API 설정에는 원문이 아니라 SHA-256 digest만 넣습니다.

digest 생성:

```bash
cd load-control-api
PYTHONPATH=src .venv/bin/python -m load_control.security '<token>'
```

예를 들어 위에서 만든 원문이 `<generated-nifi-token>`이면 설정 위치는 다음과 같습니다.

Load Control API `config.yaml`:

```yaml
auth:
  token_digests:
    nifi:
      - "<sha256-of-generated-nifi-token>"
    operator:
      - "<sha256-of-separately-generated-operator-token>"
```

NiFi 빌더 JSON의 공통 Parameter:

```json
{
  "common_params": {
    "CONTROL.API.AUTHORIZATION": "Bearer <generated-nifi-token>"
  }
}
```

`Bearer ` 접두사를 빼거나 API에 원문을 넣으면 인증되지 않습니다. 반대로 API의 `token_digests`를 비우면
인증이 해제되는 것이 아니라 보호된 API가 모두 401/403을 반환합니다.

#### token 만료와 수명 관리

현재 구현은 JWT가 아닌 임의의 난수 원문을 사용하는 정적 opaque token 방식입니다.
`AuthSettings`에는 digest 목록만 있고 `expires_at`이나 JWT `exp`를 검증하는 로직이 없습니다.
따라서 token은 해당 digest를 설정에서 제거하고 API server를 재시작할 때까지 유효합니다.

NiFi 서비스 token에 자동 만료를 강제하면 교체 실패 시 전체 적재가 멈출 수 있으므로
현재 범위에서는 요청 시간 기준 자동 만료를 필수로 두지 않습니다. 다만 만료가 없다고 같은
token을 영구적으로 쓰는 것은 아닙니다. 충분히 긴 난수, 망 제한, 조직 정책에 따른 정기
교체, 유출 의심 시 즉시 폐기를 함께 적용합니다. `operator` token은 상태를 수동으로
변경할 수 있으므로 `nifi` token보다 접근 대상을 더 엄격하게 제한합니다.

통신 방향별 인증은 다음과 같습니다.

| 통신 | 현재 인증 |
|---|---|
| NiFi Flow → Load Control API | 공통 `nifi` Bearer token 필수 |
| 운영자/TUI → Load Control API | `operator` 또는 허용된 조회 token |
| Load Control worker → NiFi PG-05 | 현재 Bearer token 없음; 내부망·방화벽으로 제한 |
| 사용자 → NiFi UI/REST API | NiFi 자체 보안 설정이며 위 API token과 무관 |

#### 무중단 token 교체

1. 새 원문 token을 생성하고 digest를 계산합니다.
2. 기존 digest를 지우지 않은 채 같은 role 목록에 신규 digest를 추가합니다.
3. API server를 순차 재시작합니다. 설정은 시작 시 로드되므로 파일만 수정해서는 반영되지 않습니다.
4. `nifi` token은 공통 Parameter Context의 `CONTROL.API.AUTHORIZATION`을 `Bearer <신규 token>`으로
   변경합니다. `operator` token은 TUI `monitor.operator_token`과 운영 도구의 비밀을 변경합니다.
5. 신규 token으로 API 조회와 필수 운영 기능을 검증합니다.
6. 이전 digest를 제거하고 API server를 다시 순차 재시작합니다.

공통 Parameter Context 변경은 모든 Job에 영향을 줄 수 있으므로 실행 중 run이 없는 시간에
수행합니다. 현재 NiFi→API 통신은 HTTP이므로 token이 평문 패킷에 포함됩니다. API 포트를
NiFi 노드와 승인된 관리망에서만 열고, 가능하면 앞단 LB/프록시에서 TLS를 종료합니다.

### 8.4 `nifi`

| 키 | 의미 |
|---|---|
| `receiver_url` | API worker가 PG-05로 호출할 base URL |
| `timeout_seconds` | 한 번의 NiFi HTTP 호출 timeout |
| `client_cert`, `client_key`, `ca_bundle` | 선택적 mTLS client 설정 |

worker는 `receiver_url`이 없으면 시작하지 않습니다.

### 8.5 `recovery`

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

`REISSUE`는 같은 SCN을 다시 읽으므로 Oracle undo가 충분하다는 증거가 있을 때만 사용합니다.

### 8.6 `dispatch`

| 키 | 기본 | 의미 |
|---|---:|---|
| `max_attempts` | `20` | 전송과 ACK 재전송을 포함한 최대 시도 |
| `backoff_min` | `PT5S` | 첫 backoff |
| `backoff_max` | `PT5M` | 최대 backoff |
| `ack_timeout` | `PT10M` | 202 이후 실제 처리 ACK 대기 |
| `lease` | `PT60S` | dispatch 선점 시간. `nifi.timeout_seconds`보다 길게 |
| `poll_interval` | `PT5S` | NOTIFY 유실 대비 polling |
| `batch` | `20` | 한 번에 lease하고 병렬 전송할 수 |

### 8.7 `cleanup`, `worker`, `monitor`, `logging`

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

### 8.8 `clients`

운영 조회 도구(`bin/oracle.sh`, `bin/hive.sh`, `bin/hdfs.sh`)의 접속 정보입니다. API server·worker는 이
섹션을 읽지 않습니다. 쓰지 않는 도구는 `null`로 둡니다. 자세한 내용은 [부록 A. 운영 조회 도구 사용법](./appendix-a-query-tools.md)에 있습니다.

| 키 | 기본 | 설명 |
|---|---|---|
| `oracle.dsn`, `user`, `password` | 필수 | `host:port/service_name`. NiFi `ORACLE.JDBC.URL`·`USER`와 같은 읽기 전용 계정을 권장합니다 |
| `oracle.current_schema` | `null` | 스키마를 붙이지 않은 이름을 찾을 스키마(예: `APP`) |
| `oracle.call_timeout`, `arraysize` | `PT10M`, `1000` | 문장 하나의 최대 실행 시간, fetch 단위 |
| `hive.host`, `port`, `database`, `user` | 필수, `10000`, `default`, `nifi` | NiFi `HIVE.JDBC.URL`과 같은 HS2 |
| `hive.password` | `null` | `auth`가 `LDAP`·`CUSTOM`이면 필수(또는 `bin/hive.sh -W`) |
| `hive.auth`, `transport`, `http_path` | `NONE`, `binary`, `cliservice` | 서버 `hive.server2.authentication`(NONE·LDAP·CUSTOM·NOSASL), 전송 방식 |
| `hive.connect_timeout`, `query_timeout` | `30`, `PT30M` | 연결 초, 문장마다 보내는 `hive.query.timeout.seconds` |
| `hdfs.namenode_urls` | 필수 | WebHDFS 주소 목록(`dfs.namenode.http-address`). HA면 standby를 건너뜁니다 |
| `hdfs.user`, `home` | `nifi`, `/` | simple 인증 `user.name`, 대화형 시작 디렉터리 |
| `hdfs.datanode_hosts` | `{}` | 읽기·쓰기 redirect의 DataNode 이름을 바꿀 `{이름: IP}` |
| `hdfs.timeout_seconds` | `30` | HTTP timeout |

## 9. 변경 영향도

| 변경 | 영향 |
|---|---|
| 공통 Parameter | PG-05와 모든 Job에 영향. NiFi update-request가 참조 구성요소를 잠시 정지시킬 수 있음 |
| Job Parameter | 해당 Job 재생성 또는 Context 변경 필요 |
| `HDFS.STAGE.ROOT`/stage prefix | 기존 cleanup 대상이 PG-70 안전 검사에서 거부될 수 있음 |
| `JOB.KEY` | 새 Job으로 간주. PG-05 route와 HDFS 경로 변경 |
| target partition clause | 데이터 교체 범위 변경. 별도 리뷰와 비운영 검증 필수 |
| recovery timeout | sweeper의 자동 조치 시점 변경 |
| token | API digest와 NiFi/TUI 원문을 겹치는 기간에 순차 교체 |

설정을 확정했으면 [설치 및 적용 절차](./03-installation-and-apply.md)로 진행합니다.
