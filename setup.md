# NiFi Flow 배포 Setup 가이드 (HTTP·무인증 NiFi)

이 문서는 **NiFi가 HTTP로 열려 있고 로그인 인증이 없는 환경**에서 Sqoop 대체 Flow를 처음 배포하는
절차를 순서대로 정리한 것입니다. 각 단계에서 **어떤 설정 파일의 어떤 값을 어떻게 채우는지**를 구체적으로
적었습니다. 예시는 원천 테이블 `APP.INSP_DTL`을 Hive `dw.insp_dtl`로 적재하는 Job입니다.

> 상세 설명과 근거는 `docs/02-configuration.md`(설정 레퍼런스)와 `docs/03-installation-and-apply.md`
> (설치 및 적용 절차)에 있습니다. 이 문서는 그 두 문서를 실행 순서대로 압축한 체크리스트입니다.

## 전체 순서

| 단계 | 작업 | 결과물 |
|---|---|---|
| 0 | 환경값 수집 | 호스트·포트·계정 목록 |
| 1 | Oracle 계정·권한 준비 | `NIFI_READER` 계정 |
| 2 | HDFS·Hive 준비 | stage 경로, `stg`·`dw` DB, target 테이블 |
| 3 | PostgreSQL 준비 | `nifiops` DB와 계정 3개 |
| 4 | API token 생성 | NiFi token 원문과 digest, operator token 원문과 digest |
| 5 | Load Control API 설정·기동 | `load-control-api/config/config.yaml` |
| 6 | NiFi 노드 준비 | JDBC jar, Hadoop 설정 파일 |
| 7 | Job 설정 파일 작성 | `/secure/path/job-insp-dtl.json` |
| 8 | 값 교차 점검 | 체크리스트 통과 |
| 9 | Flow 배포 | Job PG, flow ID JSON |
| 10 | UI 점검 | 점검 통과 |
| 11 | 첫 실행 | 시험 run `SUCCESS` |
| 12 | 추가 Job 배포·운영 전환 | Job PG N개, 운영 schedule |

---

## 0단계. 환경값 수집

먼저 아래 표를 채웁니다. 이후 모든 설정 파일은 이 값으로 작성합니다.

| 항목 | 예시 | 내 환경 값 |
|---|---|---|
| NiFi REST API URL | `http://nifi-host:8080/nifi-api` | |
| NiFi 노드 목록 | `nifi1`, `nifi2` | |
| NiFi LB(또는 대표 노드) 주소 | `nifi-lb.internal` | |
| PG-05 수신 포트(`CONTROL.LISTEN.PORT`) | `9443` (NiFi에서 사용하지 않는 포트) | |
| Load Control API 호스트·포트 | `api-host:8080` | |
| PostgreSQL 호스트·DB | `meta:5432/nifiops` | |
| Oracle JDBC URL | `jdbc:oracle:thin:@//oracle-host:1521/ORCLPDB1` | |
| HiveServer2 | `hs2-host:10000` | |
| HDFS stage 경로 | `/data/nifi/stage` | |
| NiFi 노드의 JDBC jar 경로 | `/opt/nifi/jdbc/` | |
| NiFi 노드의 Hadoop 설정 경로 | `/etc/hadoop/conf/` | |

NiFi가 HTTP·무인증이면 배포 스크립트는 별도 로그인 없이 REST API를 바로 호출합니다. 배포를 실행할 호스트에서
다음 명령이 JSON을 반환해야 합니다.

```bash
curl -fsS http://nifi-host:8080/nifi-api/flow/about
```

> [!NOTE]
> NiFi 자체에 인증이 없어도, NiFi Flow가 **Load Control API**를 호출할 때 쓰는 Bearer token(4단계)은
> 반드시 만들어야 합니다. 둘은 서로 관계없는 값입니다.

---

## 1단계. Oracle 준비

### 1-1. 읽기 전용 계정과 권한 (DBA 작업)

```sql
CREATE USER NIFI_READER IDENTIFIED BY "<oracle-password>";
GRANT CREATE SESSION TO NIFI_READER;
GRANT SELECT     ON APP.INSP_DTL TO NIFI_READER;
GRANT FLASHBACK  ON APP.INSP_DTL TO NIFI_READER;   -- AS OF SCN 조회에 필요
GRANT SELECT     ON SYS.V_$DATABASE TO NIFI_READER; -- run 시작 시 CURRENT_SCN 조회
```

원천 테이블이 여러 개면 `SELECT`와 `FLASHBACK`을 테이블마다 부여합니다.

### 1-2. NIFI_READER 계정으로 권한 확인

```sql
SELECT TO_CHAR(CURRENT_SCN) AS SNAPSHOT_SCN FROM V$DATABASE;

SELECT COUNT(*) FROM APP.INSP_DTL AS OF SCN <위_SCN>
 WHERE BASE_DT = DATE '2026-09-28';
```

### 1-3. split column 검사

`SRC.SPLIT.COLUMN`으로 쓸 컬럼(Sqoop의 `--split-by`에 해당)을 검사합니다.

```sql
SELECT COUNT(*)                        AS total_rows,
       COUNT(*) - COUNT(INSP_DTL_SEQ)  AS null_split_rows,
       COUNT(DISTINCT INSP_DTL_SEQ)    AS distinct_split_rows,
       MIN(INSP_DTL_SEQ), MAX(INSP_DTL_SEQ)
  FROM APP.INSP_DTL
 WHERE BASE_DT = DATE '2026-09-28';
```

- `null_split_rows = 0`이어야 합니다.
- `total_rows = distinct_split_rows`이어야 합니다.
- split column은 숫자형이어야 합니다.

> 여기서 확인한 값은 7단계의 `SRC.SPLIT.COLUMN`, `SRC.BASE.WHERE`에 그대로 씁니다.

---

## 2단계. HDFS·Hive 준비

### 2-1. HDFS stage 경로

```bash
hdfs dfs -mkdir -p /data/nifi/stage
hdfs dfs -chown nifi:nifi /data/nifi/stage     # NiFi 실행 사용자
hdfs dfs -ls -d /data/nifi/stage
```

이 경로는 7단계의 `HDFS.STAGE.ROOT`에 씁니다.

### 2-2. Hive DB와 target 테이블

staging 테이블은 Flow가 run마다 직접 만들므로, 여기서는 DB와 target 테이블만 만듭니다.

```sql
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS dw;

CREATE EXTERNAL TABLE IF NOT EXISTS dw.insp_dtl (
  INSP_DTL_SEQ DECIMAL(19,0),
  ITEM_CD      STRING,
  AMOUNT       DECIMAL(18,2),
  REG_TS       TIMESTAMP,
  NOTE         STRING
)
PARTITIONED BY (base_dt STRING)
STORED AS PARQUET;
```

- `stg`는 7단계의 `HIVE.STAGE.DB`에 씁니다.
- `dw`와 `insp_dtl`은 7단계의 `HIVE.TARGET.DB`와 `HIVE.TARGET.TABLE`에 씁니다.
- partition 컬럼 `base_dt`는 7단계의 `TARGET.PARTITION.CLAUSE`와 `TARGET.BUSINESS.WHERE`에 씁니다.
- NiFi JVM timezone과 Hive `hive.local.time.zone`을 같은 값(예: `Asia/Seoul`)으로 맞춥니다.

---

## 3단계. PostgreSQL 준비

계정 3개를 만듭니다. **migration을 실행하기 전에** 계정을 먼저 만들어야 migration이 권한(GRANT)까지
부여합니다.

```sql
CREATE DATABASE nifiops;

CREATE ROLE nifi_ops_migrator LOGIN PASSWORD '<migrator-password>';  -- DDL(migration) 전용
CREATE ROLE load_control_api  LOGIN PASSWORD '<api-password>';       -- API server·worker 런타임
CREATE ROLE nifi_runtime      LOGIN PASSWORD '<nifi-db-password>';   -- NiFi PG-90 (load_event INSERT만)

\c nifiops
GRANT CREATE ON DATABASE nifiops TO nifi_ops_migrator;
```

| 계정 | 쓰는 곳 |
|---|---|
| `nifi_ops_migrator` | API `config.yaml`의 `database.migration_url` |
| `load_control_api` | API `config.yaml`의 `database.url`, `database.listen_dsn` |
| `nifi_runtime` | Job 설정 JSON의 `META.JDBC.USER`와 `META.JDBC.PASSWORD` |

---

## 4단계. Load Control API token 생성

token은 두 개를 **서로 다르게** 만듭니다. 원문은 비밀 저장소에 보관합니다.

```bash
openssl rand -hex 32   # → NIFI_TOKEN (예: 3f9c...a1)
openssl rand -hex 32   # → OPERATOR_TOKEN
```

digest는 API를 설치한 뒤(5-1) 다음 명령으로 계산합니다.

```bash
cd load-control-api
PYTHONPATH=src .venv/bin/python -m load_control.security '<NIFI_TOKEN>'      # → NIFI_DIGEST
PYTHONPATH=src .venv/bin/python -m load_control.security '<OPERATOR_TOKEN>'  # → OPERATOR_DIGEST
```

| 값 | 넣는 곳 |
|---|---|
| `NIFI_TOKEN` 원문 | Job JSON `common_params.CONTROL.API.AUTHORIZATION` = `Bearer <NIFI_TOKEN>` |
| `NIFI_DIGEST` | API `config.yaml` → `auth.token_digests.nifi` |
| `OPERATOR_TOKEN` 원문 | API `config.yaml` → `monitor.operator_token` (TUI 사용 시) |
| `OPERATOR_DIGEST` | API `config.yaml` → `auth.token_digests.operator` |

---

## 5단계. Load Control API 설정·기동

### 5-1. 설치

```bash
cd load-control-api
bin/install.sh --online          # 폐쇄망: 인터넷 장비에서 bin/download-packages.sh 후 디렉터리 복사, bin/install.sh
cp config/config.example.yaml config/config.yaml
chmod 600 config/config.yaml
```

설치가 끝나면 4단계의 digest 계산을 실행합니다.

### 5-2. `config/config.yaml` 작성

예시 파일에서 **반드시 바꿔야 하는 항목**만 표시했습니다. 나머지 항목은 예시 값을 그대로 둬도 됩니다.

```yaml
server:
  host: 0.0.0.0
  port: 8080                     # ← 7단계 CONTROL.API.URL의 포트와 같아야 함
  workers: 4

database:
  url:           postgresql+asyncpg://load_control_api:<api-password>@meta:5432/nifiops
  migration_url: postgresql+asyncpg://nifi_ops_migrator:<migrator-password>@meta:5432/nifiops
  listen_dsn:    postgresql://load_control_api:<api-password>@meta:5432/nifiops
  pool_size: 10
  max_overflow: 5
  tx_attempts: 3

auth:
  token_digests:
    nifi:
      - "<NIFI_DIGEST>"          # ← 4단계. 원문이 아닌 digest
    operator:
      - "<OPERATOR_DIGEST>"

nifi:
  receiver_url: http://nifi-lb.internal:9443   # ← http://<NiFi LB 또는 노드>:<CONTROL.LISTEN.PORT>
  timeout_seconds: 10

recovery:
  run_timeout: PT6H
  extract_query_timeout: PT60M   # ← 7단계 EXTRACT.QUERY.TIMEOUT("60 min")과 같은 시간
  stale: PT90M                   # ← 반드시 extract_query_timeout보다 크게
  mode: FAIL
  max_attempts: 3
  validation_stale: PT2H
  publish_stale: PT2H
  sweeper_interval: PT1M

cleanup:
  success_retention: P3D
  failed_retention: P14D
  max_batch: 200                 # ← 7단계 CLEANUP.BATCH(50) 이상

monitor:
  api_url: null
  token: null
  operator_token: "<OPERATOR_TOKEN>"   # TUI를 쓸 때만
```

- `clients` 섹션은 운영 조회 도구(`bin/oracle.sh`, `bin/hive.sh`, `bin/hdfs.sh`)용입니다. 쓰지 않는 도구는 `null`로 둡니다.
- 모르는 키가 있으면 API가 시작되지 않으므로 오타에 주의합니다.

### 5-3. migration과 기동

```bash
bin/migrate.sh                   # nifi_ops 스키마 생성 + 3단계 계정 GRANT
bin/migrate.sh current           # head revision 확인
bin/start.sh
bin/status.sh --wait 30
curl -fsS http://127.0.0.1:8080/readyz        # {"status":"ok"}
curl -fsS -H 'Authorization: Bearer <NIFI_TOKEN>' 'http://127.0.0.1:8080/v1/runs?limit=1'
```

마지막 명령이 401 또는 403을 반환하면 4단계의 token 원문과 digest가 서로 맞지 않는 것입니다.

---

## 6단계. NiFi 노드 준비

**모든 NiFi 노드**에서 같은 경로에 파일을 배치합니다.

```bash
# 각 NiFi 노드에서
sudo mkdir -p /opt/nifi/jdbc
sudo cp ojdbc11.jar               /opt/nifi/jdbc/   # → ORACLE.JDBC.DRIVER.PATH
sudo cp postgresql-42.7.3.jar     /opt/nifi/jdbc/   # → META.JDBC.DRIVER.PATH
sudo chown -R nifi:nifi /opt/nifi/jdbc
ls /etc/hadoop/conf/core-site.xml /etc/hadoop/conf/hdfs-site.xml   # → HADOOP.CONF.FILES
```

추가로 다음을 확인합니다.

```bash
# CFM Hive 구성요소가 설치되어 있는지 (없으면 배포가 중단됨)
curl -fsS http://nifi-host:8080/nifi-api/flow/processor-types | grep -o 'PutClouderaHiveQL' | head -1
curl -fsS http://nifi-host:8080/nifi-api/flow/controller-service-types | grep -o 'ClouderaHiveConnectionPool' | head -1

# CONTROL.LISTEN.PORT(9443)를 다른 프로세스가 쓰고 있지 않은지 (각 NiFi 노드에서)
ss -ltn | grep ':9443' || echo "9443 free"

# 같은 이름의 Job PG가 이미 있는지 (root 기준)
curl -fsS http://nifi-host:8080/nifi-api/flow/process-groups/root | grep -o '"name":"JOB_ORACLE_INSP_DTL_DAILY"'
```

API worker 호스트에서 NiFi 노드의 9443 포트로, NiFi 노드에서 API 호스트의 8080 포트로 접속할 수 있도록
방화벽을 엽니다.

---

## 7단계. Job 설정 파일 작성

### 7-1. 예시 복사

설정 파일에는 비밀번호와 token 원문이 들어가므로 저장소 밖에 두고 권한을 600으로 설정합니다.

```bash
mkdir -p /secure/path
cp nifi-flow/job-config.example.json /secure/path/job-insp-dtl.json
chmod 600 /secure/path/job-insp-dtl.json
```

파일은 세 부분으로 나뉩니다. 아래는 구조를 설명하기 위한 그림이며, JSON은 `//` 주석을 허용하지 않으므로
실제 파일에는 주석을 쓰지 않습니다.

```text
{
  "names":         { },   // 선택. 보통 생략(기본 이름 사용)
  "common_params": { },   // 모든 Job이 공유하는 접속값 → 공통 Parameter Context
  "job_params":    { }    // 이 Job(테이블) 전용 값 → Job Parameter Context
}
```

### 7-2. `names` (보통 생략)

생략하면 다음 기본 이름을 씁니다.

| 대상 | 기본 이름 |
|---|---|
| Job PG | `JOB_<JOB.KEY>` |
| Job Parameter Context | `PC_JOB_<JOB.KEY>` |
| 공통 Parameter Context | `PC_SQOOP_REPLACEMENT_COMMON` |
| 공유 수신 PG | `PG-05 Control Receiver` |

Job PG 이름만 바꾸려면 `names`를 쓰기보다 배포 스크립트의 세 번째 인자를 쓰는 편이 간단합니다.

### 7-3. `common_params` — 공통 접속값

> [!IMPORTANT]
> 공통 Context는 모든 Job이 공유합니다. 새 Job을 배포할 때마다 그 Job 파일의 값으로 공통 Context가 **덮어써지므로**,
> 모든 Job 파일의 `common_params`는 글자 하나까지 같아야 합니다.

```json
"common_params": {
  "CONTROL.API.URL": "http://api-host:8080/v1",
  "CONTROL.API.AUTHORIZATION": "Bearer <NIFI_TOKEN>",
  "CONTROL.API.TIMEOUT": "30 secs",
  "CONTROL.LISTEN.PORT": "9443",

  "META.JDBC.URL": "jdbc:postgresql://meta:5432/nifiops",
  "META.JDBC.USER": "nifi_runtime",
  "META.JDBC.PASSWORD": "<nifi-db-password>",
  "META.JDBC.DRIVER.PATH": "/opt/nifi/jdbc/postgresql-42.7.3.jar",

  "ORACLE.JDBC.URL": "jdbc:oracle:thin:@//oracle-host:1521/ORCLPDB1",
  "ORACLE.JDBC.USER": "NIFI_READER",
  "ORACLE.JDBC.PASSWORD": "<oracle-password>",
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

  "HIVE.JDBC.URL": "jdbc:hive2://hs2-host:10000/default?hive.resultset.use.unique.column.names=false",
  "HIVE.JDBC.USER": "nifi",
  "HIVE.JDBC.PASSWORD": "<hive-password 또는 빈 문자열>",
  "HIVE.POOL.MAX": "4",
  "HIVE.QUERY.TIMEOUT": "1800",

  "CLEANUP.BATCH": "50"
}
```

(위 블록의 빈 줄은 읽기 쉽게 넣은 것이며 JSON에서도 허용됩니다.)

| Parameter | 채우는 방법 | 출처 |
|---|---|---|
| `CONTROL.API.URL` | `http://<API 호스트>:<server.port>/v1` — `/v1`까지 포함 | 5단계 |
| `CONTROL.API.AUTHORIZATION` | `Bearer ` + NIFI_TOKEN **원문** (digest 아님, `Bearer ` 접두사 필수) | 4단계 |
| `CONTROL.API.TIMEOUT` | 기본 `30 secs` | — |
| `CONTROL.LISTEN.PORT` | PG-05가 모든 NiFi 노드에서 여는 HTTP 포트. API `nifi.receiver_url`의 포트와 같게 | 0·5단계 |
| `META.JDBC.*` | `nifi_runtime` 계정 정보와 PostgreSQL JDBC jar 경로 | 3·6단계 |
| `ORACLE.JDBC.URL` | `jdbc:oracle:thin:@//<host>:<port>/<service>` | 0단계 |
| `ORACLE.JDBC.USER`, `PASSWORD` | `NIFI_READER` 계정 정보 | 1단계 |
| `ORACLE.JDBC.DRIVER.PATH` | ojdbc11 jar 경로 (모든 노드에서 같은 경로) | 6단계 |
| `ORACLE.POOL.MAX` | Job 하나의 Oracle 연결 상한. DBA가 승인한 세션 수 이내 | DBA |
| `ORACLE.NUMBER.DEFAULT.*` | precision 없는 `NUMBER`의 기본 처리값. 보통 그대로 둠 | — |
| `HADOOP.CONF.FILES` | core-site와 hdfs-site 경로를 쉼표로 구분 | 6단계 |
| `HDFS.STAGE.ROOT` | 2-1에서 만든 경로 | 2단계 |
| `EXTRACT.QUERY.TIMEOUT` | 파티션 SQL 하나의 최대 실행 시간. API `extract_query_timeout`과 같은 시간 | 5단계 |
| `EXTRACT.ROWS.PER.FILE` | Parquet 파일 하나에 담을 최대 행 수 | — |
| `HIVE.JDBC.URL` | `jdbc:hive2://<hs2>:<port>/default?hive.resultset.use.unique.column.names=false` (뒤의 옵션 필수) | 0단계 |
| `HIVE.JDBC.USER`, `PASSWORD` | HS2 접속 계정. 인증이 없으면 password는 `""` | — |
| `CLEANUP.BATCH` | API `cleanup.max_batch` 이하 | 5단계 |

이름이 `PASSWORD` 또는 `AUTHORIZATION`으로 끝나는 Parameter는 배포 스크립트가 자동으로 sensitive로 등록합니다.

### 7-4. `job_params` — 이 Job(테이블) 전용 값

```json
"job_params": {
  "JOB.KEY": "ORACLE_INSP_DTL_DAILY",
  "BUSINESS.KEY": "2026-09-28",

  "SRC.OWNER": "APP",
  "SRC.TABLE": "INSP_DTL",
  "SRC.COLUMNS": "INSP_DTL_SEQ, BASE_DT, ITEM_CD, CAST(AMOUNT AS NUMBER(18,2)) AS AMOUNT, REG_TS, NOTE",
  "SRC.SPLIT.COLUMN": "INSP_DTL_SEQ",
  "SRC.BASE.WHERE": "BASE_DT = TO_DATE('${load.business.key}', 'YYYY-MM-DD')",
  "PARTITION.COUNT": "8",
  "ALLOW.EMPTY.SOURCE": "false",

  "DQ.AMOUNT.COLUMN": "AMOUNT",
  "DQ.TIMESTAMP.COLUMN": "REG_TS",
  "DQ.PK.COLUMN": "INSP_DTL_SEQ",

  "HIVE.STAGE.DB": "stg",
  "HIVE.STAGE.TABLE.PREFIX": "TMP_INSP_DTL_",
  "HIVE.STAGE.DDL.COLUMNS": "INSP_DTL_SEQ DECIMAL(19,0), BASE_DT TIMESTAMP, ITEM_CD STRING, AMOUNT DECIMAL(18,2), REG_TS TIMESTAMP, NOTE STRING",

  "HIVE.TARGET.DB": "dw",
  "HIVE.TARGET.TABLE": "insp_dtl",
  "TARGET.PARTITION.CLAUSE": "PARTITION (base_dt='${load.business.key}')",
  "HIVE.INSERT.COLUMNS": "INSP_DTL_SEQ, ITEM_CD, AMOUNT, REG_TS, NOTE",
  "TARGET.BUSINESS.WHERE": "base_dt = '${load.business.key}'"
}
```

각 값은 아래 순서로 정합니다.

**① 식별자**

| Parameter | 채우는 방법 |
|---|---|
| `JOB.KEY` | 대문자·숫자·`_`만 허용(`[A-Z0-9_]{1,200}`). Job PG 이름, API route, HDFS 경로의 키가 되므로 **배포 후 바꾸지 않습니다** |
| `BUSINESS.KEY` | 적재할 업무일자. `YYYY-MM-DD` 형식만 허용. 첫 시험은 비운영 일자로 |

**② 원천(Oracle)**

| Parameter | 채우는 방법 |
|---|---|
| `SRC.OWNER`, `SRC.TABLE` | 스키마와 테이블 이름. `PARTITION(...)` 같은 SQL 조각을 넣지 않습니다 |
| `SRC.COLUMNS` | SELECT 목록. 정밀도 없는 `NUMBER`는 `CAST(x AS NUMBER(p,s)) AS x`로 정밀도를 명시 |
| `SRC.SPLIT.COLUMN` | 1-3에서 검사한 split 컬럼 이름. 인덱스 이름이 아닌 **컬럼명** |
| `SRC.BASE.WHERE` | 업무 범위 조건. 업무일자는 `${load.business.key}`로 참조. Oracle 파티션 키 조건을 쓰면 partition pruning이 됩니다 |
| `PARTITION.COUNT` | split 범위를 몇 개로 나눌지. 보통 4~8 |
| `ALLOW.EMPTY.SOURCE` | 원천이 0건일 때 성공으로 볼지 여부. `false`면 0건을 실패로 처리 |

**③ 품질 검증 컬럼** — 모두 `SRC.COLUMNS`의 결과 컬럼 이름 중에서 고릅니다.

| Parameter | 채우는 방법 |
|---|---|
| `DQ.AMOUNT.COLUMN` | 합계를 비교할 숫자 컬럼 |
| `DQ.TIMESTAMP.COLUMN` | 최솟값·최댓값을 비교할 시각 컬럼 |
| `DQ.PK.COLUMN` | 중복을 검사할 key 컬럼 (보통 split 컬럼) |

**④ Hive staging** — Flow가 run마다 만드는 임시 external 테이블에 쓰는 값입니다.

| Parameter | 채우는 방법 |
|---|---|
| `HIVE.STAGE.DB` | 2-2에서 만든 staging DB |
| `HIVE.STAGE.TABLE.PREFIX` | 임시 테이블 접두사. Job마다 다르게 (`TMP_<테이블>_`) |
| `HIVE.STAGE.DDL.COLUMNS` | `SRC.COLUMNS`의 **결과 컬럼과 이름·순서·개수가 정확히 같게**, Hive 타입으로 |

`SRC.COLUMNS` → `HIVE.STAGE.DDL.COLUMNS` 대응 규칙:

| Oracle 타입 | Hive 타입 |
|---|---|
| `NUMBER(p,0)` / `CAST(... AS NUMBER(p,0))` | `DECIMAL(p,0)` |
| `NUMBER(p,s)` | `DECIMAL(p,s)` |
| `DATE`, `TIMESTAMP` | `TIMESTAMP` |
| `VARCHAR2`, `CHAR`, `CLOB` | `STRING` |

**⑤ Hive target(게시)**

| Parameter | 채우는 방법 |
|---|---|
| `HIVE.TARGET.DB`, `HIVE.TARGET.TABLE` | 2-2에서 만든 target |
| `TARGET.PARTITION.CLAUSE` | 교체할 파티션. **비워 두면 테이블 전체를 overwrite**하므로 반드시 채웁니다 |
| `HIVE.INSERT.COLUMNS` | staging에서 target으로 넣을 컬럼. target 컬럼 순서대로 쓰고, **정적 partition 컬럼(`base_dt`)과 target에 없는 컬럼(`BASE_DT`)은 뺍니다** |
| `TARGET.BUSINESS.WHERE` | target 검증 범위. `TARGET.PARTITION.CLAUSE`와 **같은 범위**를 WHERE 형태로 |

### 7-5. JSON 문법 확인

```bash
python3 -m json.tool /secure/path/job-insp-dtl.json > /dev/null && echo OK
```

---

## 8단계. 값 교차 점검

배포 전에 아래 관계를 하나씩 확인합니다.

| # | 확인할 관계 | 예시 값 |
|---|---|---|
| 1 | JSON `CONTROL.API.URL`의 호스트·포트 = API `server` 주소(또는 LB) | `api-host:8080` |
| 2 | JSON `CONTROL.API.AUTHORIZATION`의 token을 digest로 바꾼 값 = API `auth.token_digests.nifi` | 5-3의 curl 응답이 200 |
| 3 | JSON `CONTROL.LISTEN.PORT` = API `nifi.receiver_url`의 포트 | `9443` |
| 4 | JSON `EXTRACT.QUERY.TIMEOUT` = API `recovery.extract_query_timeout` | `60 min` = `PT60M` |
| 5 | API `recovery.stale` > `extract_query_timeout` | `PT90M` > `PT60M` |
| 6 | JSON `CLEANUP.BATCH` ≤ API `cleanup.max_batch` | `50` ≤ `200` |
| 7 | `SRC.COLUMNS`의 결과 컬럼 = `HIVE.STAGE.DDL.COLUMNS` (이름·순서·개수) | 6개 = 6개 |
| 8 | `TARGET.PARTITION.CLAUSE`와 `TARGET.BUSINESS.WHERE`가 같은 범위 | 둘 다 `base_dt='${load.business.key}'` |
| 9 | 여러 Job 파일의 `common_params`가 모두 같음 | 아래 diff 명령 |
| 10 | NiFi JVM timezone = Hive `hive.local.time.zone` | `Asia/Seoul` |

여러 Job 파일의 `common_params`가 같은지 비교하는 방법:

```bash
for f in /secure/path/job-*.json; do
  python3 -c "import json,sys;print(json.dumps(json.load(open(sys.argv[1]))['common_params'],sort_keys=True))" "$f" | md5sum
done   # 해시가 모두 같아야 함
```

---

## 9단계. Flow 배포

저장소 root에서 실행합니다. NiFi에 인증이 없으므로 URL만 주면 됩니다.

```bash
cd /data1/projects/nifi-sqoop-removal

python3 nifi-flow/deploy_job_flow.py \
  http://nifi-host:8080/nifi-api \
  /secure/path/job-insp-dtl.json \
  > /secure/path/job-insp-dtl-flow-ids.json
```

선택 인자는 다음과 같습니다.

```bash
# Job PG 이름을 지정하고, 특정 PG 안에 생성
python3 nifi-flow/deploy_job_flow.py \
  http://nifi-host:8080/nifi-api \
  /secure/path/job-insp-dtl.json \
  INSP_DTL_LOAD --parent-id <상위 PG id> \
  > /secure/path/job-insp-dtl-flow-ids.json
```

- `<상위 PG id>`는 NiFi UI에서 해당 PG로 들어갔을 때 URL의 `processGroupId=` 값입니다.
- PG-05는 `--parent-id`를 줘도 root에 만들어집니다.

배포 스크립트가 하는 일은 다음과 같습니다.

1. 공통 Context와 Job Context를 만듭니다.
2. Job PG와 자식 PG 8개(00·10·20·40·50·60·70·90)를 만들고 연결합니다.
3. Controller Service를 만들고 enable합니다.
4. root의 PG-05를 만들거나 기존 PG-05에 이 Job의 route를 등록합니다.

Trigger(00)는 **DISABLED** 상태로 만들어집니다. 출력된 flow ID JSON은 변경 기록용으로 보관합니다.

**배포가 중간에 실패하면** 자동으로 되돌리지 않습니다. 반드시 먼저 지운 뒤 다시 배포합니다. 배포 때 PG 이름이나
`--parent-id`를 줬다면 지울 때도 같은 값을 줍니다.

```bash
python3 nifi-flow/remove_job_flow.py \
  http://nifi-host:8080/nifi-api \
  /secure/path/job-insp-dtl.json
# 이름이나 --parent-id를 줬다면: ... INSP_DTL_LOAD --parent-id <상위 PG id>
```

제거 스크립트는 NiFi 객체만 지웁니다. PostgreSQL 원장, HDFS 파일, Hive target은 그대로 남습니다.

---

## 10단계. NiFi UI 점검

`http://nifi-host:8080/nifi`에서 다음을 확인합니다.

**Job PG (`JOB_ORACLE_INSP_DTL_DAILY`)**
- [ ] 자식 PG가 8개 있습니다
- [ ] `validate-in`, `reissue-in` Input Port가 연결되어 있습니다
- [ ] 모든 Controller Service가 enabled이고, invalid 표시(⚠)가 있는 Processor가 없습니다
- [ ] `00_Generate_Trigger`가 disabled입니다

**Parameter Context**
- [ ] `PC_JOB_ORACLE_INSP_DTL_DAILY`가 `PC_SQOOP_REPLACEMENT_COMMON`을 상속합니다
- [ ] 자식 PG 8개 모두에 Job Context가 지정되어 있습니다
- [ ] `*PASSWORD`와 `CONTROL.API.AUTHORIZATION`이 sensitive입니다

**PG-05 (root)**
- [ ] root에 `PG-05 Control Receiver`가 하나만 있고 running입니다
- [ ] `validate-ORACLE_INSP_DTL_DAILY`와 `reissue-ORACLE_INSP_DTL_DAILY` Output Port가 Job PG에 연결되어 있습니다

수신 포트는 등록하지 않은 경로로만 시험합니다. **404**가 오면 정상입니다.

```bash
curl -i -X POST http://nifi-lb.internal:9443/not-registered
```

등록된 validate·reissue 경로에는 임의 요청을 보내지 마십시오. 실제 FlowFile이 만들어질 수 있습니다.

---

## 11단계. 첫 실행 (시험)

1. Job PG를 **Start**합니다. Trigger가 disabled라 run은 시작되지 않습니다.
2. Job Context의 `BUSINESS.KEY`를 비운영 시험 일자로 설정합니다.
3. `00_Generate_Trigger`를 **Enable**한 뒤 우클릭 → **Run Once**를 실행합니다.
4. 바로 Trigger를 다시 **Disable**합니다.
5. 진행 상황을 확인합니다.
   ```bash
   curl -fsS -H 'Authorization: Bearer <NIFI_TOKEN>' 'http://api-host:8080/v1/runs?limit=1'
   cd load-control-api && bin/monitor.sh       # TUI
   tail -f logs/server.log logs/worker.log
   ```
6. 다음 결과가 나오면 성공입니다.
   - run 상태가 `SUCCESS`입니다.
   - source, extracted, staging, target 건수가 모두 같습니다.
   - Hive `dw.insp_dtl`의 `base_dt='<시험일자>'` 파티션에 데이터가 들어 있습니다.

PG별 상세 확인 방법은 `docs/04-process-groups.md`, 장애 대응은 `docs/06-validation-and-operations.md`를 참고합니다.

---

## 12단계. 추가 Job 배포와 운영 전환

### 12-1. 테이블 추가

테이블마다 설정 파일을 하나씩 만들어 7~11단계를 반복합니다. 두 번째 예시로
`nifi-flow/job-config.ord-hist.example.json`(`SALES.ORD_HIST`)이 있습니다.

- `common_params`는 첫 Job 파일에서 **그대로 복사**합니다.
- `job_params`는 테이블에 맞게 모두 새로 작성합니다.
- `JOB.KEY`와 `HIVE.STAGE.TABLE.PREFIX`는 Job마다 달라야 합니다.

```bash
python3 nifi-flow/deploy_job_flow.py http://nifi-host:8080/nifi-api /secure/path/job-ord-hist.json \
  > /secure/path/job-ord-hist-flow-ids.json
```

1단계 Oracle 권한(`SELECT`, `FLASHBACK`)과 2단계 Hive target 테이블도 테이블마다 준비합니다.

### 12-2. 정기 실행 전환

- `00_Generate_Trigger`의 schedule을 운영 시간으로 바꾸고 enable합니다.
- 업무일자를 자동으로 계산하려면 Job Context의 `BUSINESS.KEY`에 EL을 넣습니다. 예: 전일
  ```text
  ${now():toNumber():minus(86400000):format('yyyy-MM-dd')}
  ```
- 검증 기간에는 Sqoop과 NiFi가 같은 target 파티션에 동시에 게시하지 않도록 기존 Sqoop schedule을 조정합니다.
- 배포 commit, 설정 파일 checksum, flow ID JSON, 검증 run ID를 변경 기록에 남깁니다.

### 12-3. 롤백

1. Trigger를 disable하고 Job PG를 stop합니다.
2. API(또는 TUI)에서 진행 중인 run을 확인합니다. `PUBLISH_UNKNOWN`이면 다시 실행하지 말고 target을 직접 확인합니다.
3. `remove_job_flow.py`로 Flow를 제거합니다.
4. 기존 Sqoop schedule을 복구합니다.
