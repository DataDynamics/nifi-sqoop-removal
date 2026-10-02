# V4 Oracle 원천 Flow 사용 매뉴얼

`poc/build_flow_v4.py`로 만드는 V4 Flow의 구성, 준비, 설치, 실행, 확인, 오류 대응 방법을 설명한다. 설계 근거는 `nifi-sqoop-removal-guide.md`(가이드), 시험 결과는 `poc/REVIEW.md` 7장에 있다.

## 1. 개요

V4는 V3(PostgreSQL 원천)와 같은 구조와 Load Control API 계약을 쓰고 원천만 Oracle로 바꾼 Flow다. 작업 시작 시 Oracle SCN을 하나 고정하고, 원천 지표·manifest 계산과 모든 파티션 추출이 같은 SCN(`AS OF SCN`)을 읽는다. 추출 뒤에는 Hive staging 검증, `INSERT OVERWRITE` 게시, target 검증까지 진행해 run을 `SUCCESS`로 끝낸다.

```mermaid
flowchart LR
    T[PG-00 Trigger] -->|start-run| C[PG-10 Run Coordinator]
    C -->|partitions<br/>Round Robin| W[PG-20 Extract Worker]
    R[PG-05 Control Receiver<br/>root 공통] -->|validate| S[PG-40 Staging Validation]
    S -->|staging-valid| P[PG-50 Publish]
    P -->|published| V[PG-60 Target Validation]
    R -->|reissue| W
    CL[PG-70 Cleanup<br/>1시간 주기]
    C & W & S & R & P & V & CL -.->|errors| E[PG-90 Error and Event]
    C & W & S & P & V & CL & E -->|InvokeHTTP| API[[Load Control API]]
    API -->|POST /validate, /reissue| R
    C & W -->|AS OF SCN| O[(Oracle)]
    W -->|Parquet| H[(HDFS)]
    S & P & V & CL -->|HiveQL| HV[(Hive)]
    CL -->|DeleteHDFS| H
```

| 항목 | 내용 |
|---|---|
| 구현 범위 | 가이드 2장의 전 단계: 추출(PG-00, 10, 20), API 호출 수신(PG-05), staging 검증(PG-40), 게시(PG-50), target 검증(PG-60), staging·run 경로 정리(PG-70), 오류 처리(PG-90) |
| 미구현 | PG-20 `ValidateRecord`, PG-90 DLQ·알림(가이드 8장, 14장 선택 항목) |
| 필요 NiFi | **Cloudera CFM**(Hive 구성요소 `ClouderaHiveConnectionPool`, `PutClouderaHiveQL`). Apache NiFi에는 Hive 번들이 없어 빌더가 멈춘다 |
| 검증 환경 | Cloudera CFM 4.12(NiFi 2.6.0) 2노드 클러스터, Oracle Database 23ai Free, ojdbc11 21.15, PostgreSQL 16, Apache Hadoop 3.4.1 HDFS, Apache Hive 4.0.1 HiveServer2(REVIEW.md 7.9). 추출 단계는 Apache NiFi 2.4.0에서도 검증(7.4~7.7) |
| 구성 | Job PG(상위 1 + 자식 8): Processor 73개, Connection 136개, Port 19개. root PG-05(Job 공통): Processor 6개, Job마다 Output Port 2개와 root 연결 2개 |

운영 적용에 남은 일은 `TODO.md`에 있다.

## 2. 구성

### 2.1 Process Group과 Processor

모든 Processor에는 역할을 설명하는 한글 COMMENT가 있고, 각 PG에는 역할·흐름·입출력·주의점을 적은 Label이 있다.

| PG | Processor | 역할 |
|---|---|---|
| PG-00 Trigger | 00_Generate_Trigger, 01_Set_Trigger_Attributes, 02_Validate_Trigger | 업무일자(`BUSINESS.KEY`)로 실행 요청을 만들고 형식을 검사한다. 00은 DISABLED로 만들어진다 |
| PG-10 Run Coordinator | 11_Build_Run_Body, 12_Create_Run, 13_Set_Run_Attrs | API에 run을 만든다. 같은 업무일자의 활성 run이 있으면 409 |
| | 14_Query_Current_SCN, 15_Extract_SCN | `V$DATABASE.CURRENT_SCN`을 조회해 `load.snapshot.scn`에 넣는다 |
| | 16_Query_Source_Manifest | 같은 SCN으로 원천 지표(건수, 금액 합계, `MIN_TS`/`MAX_TS`)와 파티션 범위·예상 건수를 SQL 한 문장으로 계산한다 |
| | 17_Build_Manifest_Body, 18_Register_Manifest | manifest를 API에 등록한다. 0건 파티션은 API가 바로 SUCCESS 처리한다 |
| | 19_Split_Dispatch_Partitions, 20_Extract_Partition_Attrs | API가 돌려준 실행 대상 파티션을 FlowFile 하나씩으로 나눈다 |
| PG-20 Extract Worker | 30_Set_Claim_Token, 31_Build_Claim_Body, 32_Claim_Partition, 33_Is_Owner | 파티션 소유권을 API에서 claim한다. 33은 SCN이 숫자인지도 검사한다 |
| | 34_Execute_Partition_Query | `AS OF SCN`으로 파티션 범위를 조회해 Parquet chunk로 만든다. **재시도하지 않는다** |
| | 35_Set_Chunk_Attrs, 36_PutHDFS | `part-<파티션>-<chunk>.parquet`을 run 경로에 기록한다 |
| | 37_Build_Chunk_Report, 38_Report_Chunk | chunk마다 API에 보고한다. 파티션·run 완료 판정은 API가 한다 |
| PG-05 Control Receiver(root) | 05_Listen_Control, 06_Validate_Request, 07_Respond_400, 08_Respond_202, 09_Extract_Control_Body, 10_Route_By_Job_Action | 모든 Job이 공유한다. API worker의 `POST /validate/<JOB.KEY>`, `POST /reissue/<JOB.KEY>`를 받아 해당 Job PG의 `validate-in`(→PG-40), `reissue-in`(→PG-20)으로 보낸다. 등록된 Job만 받고 나머지는 404 |
| PG-40 Staging Validation | 40_Set_Validation_Stage ~ 46_PutHDFS_SUCCESS_Marker | API `/validation/start` CAS에 성공한 요청만 원천 기대값을 받아 run 경로에 `_SUCCESS`를 쓴다 |
| | 47_Build_External_DDL, 48_Create_External_Table | run 경로를 LOCATION으로 하는 external staging 테이블을 만든다 |
| | 49_Query_Stage_Metrics, 4A_Build_Validations_Body, 4B_Report_Validations | staging 지표(`STAGE_COUNT`, `NULL_SPLIT_COUNT`, `DUP_PK_COUNT`, `AMOUNT_SUM`, `MIN_TS`, `MAX_TS`)와 PASS/FAIL을 SQL 한 문장으로 계산해 API에 기록한다 |
| | 4C_Empty_Json, 4D_Stage_Validated, 4E_Is_Stage_Validated | API가 저장된 지표로 다시 판정해 `STAGING_VALIDATED`로 바꾼다 |
| PG-50 Publish | 50_Set_Publish_Token ~ 53_Is_Publish_Owner | publish token으로 게시 소유권을 claim한다 |
| | 54_Build_Insert_Overwrite_SQL, 55_Insert_Overwrite | target 파티션을 `INSERT OVERWRITE`로 교체한다. **재시도하지 않는다** |
| | 56_Body_PUBLISHED, 56U_Body_PUBLISH_UNKNOWN, 57_Report_Publish_Result, 58_Is_Published | 결과를 API에 직접 보고한다. 55의 failure·retry는 모두 `PUBLISH_UNKNOWN`(7.1) |
| PG-60 Target Validation | 60_Set_Target_Stage ~ 66_Is_Success | target 업무 범위의 지표를 같은 형식으로 기록하고 `/success`를 요청한다 |
| PG-70 Cleanup | 70_Generate_Cleanup_Trigger ~ 74_Extract_Run_Attrs | 1시간마다(Primary Node) API에 보존 기간이 지난 run을 묻고 run별로 나눈다 |
| | 75_Check_Cleanup_Target | 지울 경로가 `#{HDFS.STAGE.ROOT}/#{JOB.KEY}/run_id=<runId>`와 정확히 같고 table 이름이 접두사·형식에 맞을 때만 진행한다 |
| | 76_Build_Drop_SQL ~ 7A_Report_Cleanup | staging table DROP, HDFS run 경로 삭제, API에 정리 기록 |
| PG-90 Error and Event | 90_Normalize_Error ~ 97_LogMessage | 모든 실패를 오류 코드로 정리하고, run·파티션 실패를 API에 보고하고, `load_event`에 기록한다 |

### 2.2 Controller Service

| 이름 | 용도 |
|---|---|
| `CS_DBCP_ORACLE` | 원천 Oracle. `oracle.jdbc.OracleDriver`, 검사 쿼리 `SELECT 1 FROM DUAL`, 최대 연결 `#{ORACLE.POOL.MAX}` |
| `CS_DBCP_META` | 관리 DB(PostgreSQL). PG-90의 `load_event` INSERT 전용 |
| JSON/Parquet Reader·Writer | manifest JSON 처리, Parquet chunk 기록 |
| `CS_HTTP_CONTEXT_MAP`(`StandardHttpContextMap`) | root PG-05 안에 있다. `HandleHttpRequest`/`HandleHttpResponse` |
| `CS_HIVE3_DBCP` | HiveServer2(`ClouderaHiveConnectionPool`). 48·55(`PutClouderaHiveQL`)와 49·61(`ExecuteSQLRecord`, JSON)이 같이 쓴다 |

### 2.3 데이터 흐름과 결과

1. Trigger가 실행 요청을 만들면 PG-10이 run을 만들고 SCN을 고정한다.
2. 16이 계산한 파티션 범위로 manifest를 등록한다. 범위는 split 컬럼의 최소~최대를 `PARTITION.COUNT`개로 나눈 하한 포함·상한 미포함 구간이고, 마지막 파티션만 상한을 포함한다.
3. PG-20이 파티션마다 claim → 추출 → HDFS 기록 → chunk 보고를 한다.
4. 마지막 chunk 보고에서 API가 run을 `EXTRACTED_VALIDATED`로 판정하고 검증 호출(dispatch)을 예약한다.
5. API worker가 PG-05를 호출하면 PG-40이 `/validation/start`로 run을 `STAGE_VALIDATING`으로 바꾸고 `_SUCCESS`를 기록한다.
6. PG-40이 staging 테이블 `#{HIVE.STAGE.DB}.<HIVE.STAGE.TABLE.PREFIX><run_id 하이픈 제거>`를 만들고 지표를 비교한다. 모두 PASS면 `STAGING_VALIDATED`.
7. PG-50이 `INSERT OVERWRITE TABLE #{HIVE.TARGET.DB}.#{HIVE.TARGET.TABLE} #{TARGET.PARTITION.CLAUSE}`로 게시한다. `PUBLISHED`.
8. PG-60이 target 지표를 비교하고 `/success`를 요청한다. 모두 PASS면 API가 `SUCCESS`와 `RUN_SUCCESS`를 기록한다.
9. 보존 기간(SUCCESS 3일, 실패·`TIMED_OUT` 14일)이 지나면 PG-70이 staging table과 run 경로를 지우고 API가 `cleaned_at`과 `RUN_CLEANED`를 남긴다(7.6).

HDFS 결과 경로:

```text
#{HDFS.STAGE.ROOT}/<JOB.KEY>/run_id=<run_id>/part-<partition_id>-<chunk>.parquet
#{HDFS.STAGE.ROOT}/<JOB.KEY>/run_id=<run_id>/_SUCCESS
```

## 3. 사전 준비

### 3.1 NiFi

- Parquet·Hadoop NAR(`nifi-parquet-nar`, `nifi-hadoop-nar`, `nifi-hadoop-libraries-nar`)가 있어야 한다(CFM 기본 배포본에 포함)
- ojdbc(`ORACLE.JDBC.DRIVER.PATH`)와 PostgreSQL JDBC(`META.JDBC.DRIVER.PATH`) 파일을 모든 노드의 같은 경로에 둔다
- `HADOOP.CONF.FILES`에 지정할 `core-site.xml`, `hdfs-site.xml`을 둔다
- Hive 구성요소(`nifi-cdf-hive-nar`)가 있는 Cloudera CFM이어야 한다
- `CONTROL.LISTEN.PORT`(PG-05 수신 포트)가 비어 있어야 한다
- Cloudera CFM 4.12(NiFi 2.6.0)는 기본 배포본에 필요한 NAR가 모두 있다
- 클러스터에서는 PG-05가 모든 노드에서 `CONTROL.LISTEN.PORT`를 연다. API `nifi.receiver_url`은 노드 하나 또는 그 앞의 LB로 둔다. PG-05는 root에 하나뿐이므로 Job이 여러 개여도 포트는 하나다

### 3.2 Oracle

조회 계정에는 다음 권한만 준다.

```sql
CREATE USER NIFI_READER IDENTIFIED BY <pw>;
GRANT CREATE SESSION TO NIFI_READER;
GRANT SELECT ON APP.INSP_DTL TO NIFI_READER;
GRANT FLASHBACK ON APP.INSP_DTL TO NIFI_READER;     -- AS OF SCN
GRANT SELECT ON SYS.V_$DATABASE TO NIFI_READER;     -- 14 SCN 조회
```

`V$DATABASE` 권한을 줄 수 없으면 14의 SQL을 `SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) AS SNAPSHOT_SCN FROM DUAL`로 바꾸고 `DBMS_FLASHBACK` 실행 권한을 준다.

함께 확인할 것:

| 항목 | 기준 |
|---|---|
| `UNDO_RETENTION`, undo 크기 | run 최대 소요시간보다 길어야 한다. 부족하면 `ORA-01555`로 run이 `FAILED_SNAPSHOT_EXPIRED`가 된다 |
| 인덱스 | `(업무 조건 컬럼, split 컬럼)` 인덱스. 16과 34가 범위 스캔을 한다 |
| split 컬럼 NULL | 업무 조건 범위에 NULL이 있으면 API가 manifest를 거부한다(V4는 NULL 파티션을 만들지 않음) |
| 세션 수 | `노드 수 × 34 Concurrent Tasks + 여유`가 승인 세션 수 이하가 되게 `ORACLE.POOL.MAX`를 정한다 |

### 3.3 Hive

- `HIVE.STAGE.DB`, `HIVE.TARGET.DB` 데이터베이스와 target 테이블을 미리 만든다. target 테이블 정의는 승인된 schema이며 빌더가 만들지 않는다. 시험에 쓴 정의:

```sql
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS dw;
CREATE EXTERNAL TABLE dw.insp_dtl (
  INSP_DTL_SEQ DECIMAL(19,0), ITEM_CD STRING, AMOUNT DECIMAL(18,2), REG_TS TIMESTAMP, NOTE STRING)
PARTITIONED BY (base_dt STRING) STORED AS PARQUET TBLPROPERTIES ('external.table.purge'='true');
```

- Hive 계정(`HIVE.JDBC.USER`): staging DB에 external table 생성, target 테이블 `INSERT OVERWRITE`, run 경로(`HDFS.STAGE.ROOT` 아래) 읽기
- **시간대**: Hive가 Parquet timestamp를 해석하는 시간대(`hive.local.time.zone`)를 NiFi JVM 시간대와 같게 둔다. 다르면 건수는 맞아도 `MIN_TS`/`MAX_TS`가 FAIL이 된다(7.3)
- `HIVE.JDBC.URL`에 `hive.resultset.use.unique.column.names=false`를 둔다. 없으면 49·61 결과 컬럼 이름에 table alias가 붙어 4A·62 Jolt가 지표를 찾지 못한다

### 3.4 Load Control API

`load-control-api/README.md` 기준으로 준비한다.

```bash
cd load-control-api
bin/install.sh                       # .venv 생성(airgap: packages/의 wheel 사용)
cp config/config.example.yaml config/config.yaml
# NiFi용 토큰의 digest를 만들어 config/config.yaml auth.token_digests.nifi에 넣는다
PYTHONPATH=src .venv/bin/python -m load_control.security "<nifi-token>"
bin/migrate.sh                       # 관리 DB migration
bin/start.sh                         # API와 worker(dispatcher + sweeper). 상태는 bin/status.sh, 중지는 bin/stop.sh
```

`config/config.yaml`의 `nifi.receiver_url`은 `http://<nifi-host>:<CONTROL.LISTEN.PORT>`로 둔다. 정리 보존 기간은 `cleanup.success_retention`(기본 `P3D`)과 `cleanup.failed_retention`(기본 `P14D`)이다. migration `0002_run_cleanup`이 `load_run.cleaned_at`을 추가한다. `curl <API>/readyz`가 `{"status":"ok"}`면 준비된 것이다.

## 4. 설정 파일

`poc/config.v4.example.json`을 복사해 값을 채운다. 암호와 토큰이 들어가므로 권한을 `600`으로 두고 저장소에 넣지 않는다.

### 4.1 `names` (선택)

이름은 가이드 2장·3장 규칙으로 정해지므로 보통 `names`를 두지 않는다. 바꿀 때만 키를 적는다.

| 키 | 기본값 | 의미 |
|---|---|---|
| `process_group` | `JOB_<JOB.KEY>` (예: `JOB_ORACLE_INSP_DTL_DAILY`) | Job PG 이름. PG-90이 `load_event.process_group`에 기록한다 |
| `job_context` | `PC_JOB_<JOB.KEY>` | Job Parameter Context(Job PG와 자식 PG 모두에 지정됨) |
| `common_context` | `PC_SQOOP_REPLACEMENT_COMMON` | 공통 Parameter Context. root PG-05와 모든 Job이 쓴다. 바꾼다면 모든 Job config에 같게 둔다 |
| `control_receiver` | `PG-05 Control Receiver` | root 공통 PG-05 이름 |

### 4.2 `common_params`

| Parameter | 예시 | 의미 |
|---|---|---|
| `CONTROL.API.URL` | `http://api-host:8080/v1` | Load Control API 기본 URL |
| `CONTROL.API.AUTHORIZATION` | `Bearer <token>` | Sensitive. `Bearer `를 포함한 전체 값을 넣는다(속성에 문자열을 덧붙일 수 없음) |
| `CONTROL.API.TIMEOUT` | `30 secs` | `InvokeHTTP` 타임아웃 |
| `CONTROL.API.RETRY.MAX` | `5` | API 호출 재시도 기준값. Retry Count는 Parameter를 참조할 수 없어 V4 빌더는 이 값을 읽지 않고 5회로 고정한다(7.1) |
| `CONTROL.LISTEN.PORT` | `9443` | PG-05 수신 포트 |
| `CLEANUP.BATCH` | `50` | PG-70이 한 주기에 정리할 최대 run 수 |
| `META.JDBC.URL`, `META.JDBC.USER`, `META.JDBC.PASSWORD`, `META.JDBC.DRIVER.PATH` | | 관리 DB(PostgreSQL). NiFi 계정은 `load_event` INSERT 권한만 있으면 된다 |
| `ORACLE.JDBC.URL` | `jdbc:oracle:thin:@//host:1521/SERVICE` | 원천 Oracle |
| `ORACLE.JDBC.USER`, `ORACLE.JDBC.PASSWORD`, `ORACLE.JDBC.DRIVER.PATH` | | 조회 계정과 ojdbc 경로 |
| `ORACLE.POOL.MAX` | `8` | `CS_DBCP_ORACLE` 최대 연결 수 |
| `ORACLE.NUMBER.DEFAULT.PRECISION` | `38` | 정밀도 없는 `NUMBER`를 decimal로 쓸 때의 precision |
| `ORACLE.NUMBER.DEFAULT.SCALE` | `10` | 같은 경우의 scale. 작게 두면 오류 없이 반올림된다(7.2) |
| `HADOOP.CONF.FILES` | `/etc/hadoop/conf/core-site.xml,...` | PutHDFS 설정 파일 |
| `HDFS.STAGE.ROOT` | `/data/nifi/stage` | run 경로의 상위 디렉터리 |
| `HDFS.PERMISSIONS.UMASK` | `027` | 기록 파일 umask. Hive 계정이 읽을 수 있어야 한다 |
| `HIVE.JDBC.URL` | `jdbc:hive2://hs2:10000/default?hive.resultset.use.unique.column.names=false` | HiveServer2. `?` 뒤 설정은 빼지 않는다(3.3) |
| `HIVE.JDBC.USER`, `HIVE.JDBC.PASSWORD` | | Hive 계정 |
| `HIVE.POOL.MAX` | `4` | `CS_HIVE3_DBCP` 최대 연결 수 |
| `HIVE.QUERY.TIMEOUT` | `1800` | Hive 문장 타임아웃(초). 48·55와 49·61에 쓰인다 |
| `EXTRACT.FETCH.SIZE` | `5000` | JDBC fetch size |
| `EXTRACT.ROWS.PER.FILE` | `500000` | Parquet 파일(chunk) 하나의 최대 행 수 |
| `EXTRACT.QUERY.TIMEOUT` | `60 min` | 34 쿼리 타임아웃 |
| `PARTITION.RETRY.MAX` | `3` | V4 빌더는 사용하지 않는다(34는 재시도하지 않음, 7.4). V3와 설정 형식을 맞추기 위해 남아 있다 |

### 4.3 `job_params`

| Parameter | 예시 | 의미 |
|---|---|---|
| `JOB.KEY` | `ORACLE_INSP_DTL_DAILY` | Job 식별자. HDFS 경로, PG-05 수신 경로에 쓰인다 |
| `BUSINESS.KEY` | `2026-09-28` | 업무일자(`YYYY-MM-DD`). 실행마다 바꾼다 |
| `SRC.OWNER`, `SRC.TABLE` | `APP`, `INSP_DTL` | 원천 테이블 |
| `SRC.COLUMNS` | `INSP_DTL_SEQ, BASE_DT, ITEM_CD, CAST(AMOUNT AS NUMBER(18,2)) AS AMOUNT, REG_TS, NOTE` | 추출 컬럼(순서 고정). 정밀도 없는 `NUMBER`는 반드시 CAST |
| `SRC.SPLIT.COLUMN` | `INSP_DTL_SEQ` | 파티션 분할 컬럼(숫자) |
| `SRC.BASE.WHERE` | `BASE_DT = TO_DATE('${load.business.key}', 'YYYY-MM-DD')` | 업무 조건. `${load.business.key}`로 업무일자를 참조 |
| `DQ.AMOUNT.COLUMN` | `AMOUNT` | 금액 합계 지표 컬럼 |
| `DQ.TIMESTAMP.COLUMN` | `REG_TS` | `MIN_TS`/`MAX_TS` 지표 컬럼 |
| `PARTITION.COUNT` | `8` | 파티션 수 |
| `HIVE.STAGE.TABLE.PREFIX` | `TMP_INSP_DTL_` | staging 테이블 이름 접두사(API가 run별 이름을 만듦) |
| `ALLOW.EMPTY.SOURCE` | `false` | 원천 0건을 허용할지 |
| `DQ.PK.COLUMN` | `INSP_DTL_SEQ` | `DUP_PK_COUNT` 지표의 업무 PK 컬럼 |
| `HIVE.STAGE.DB` | `stg` | staging external table을 만들 DB |
| `HIVE.STAGE.DDL.COLUMNS` | `INSP_DTL_SEQ DECIMAL(19,0), BASE_DT TIMESTAMP, ITEM_CD STRING, AMOUNT DECIMAL(18,2), REG_TS TIMESTAMP, NOTE STRING` | staging 컬럼 정의. 이름은 `SRC.COLUMNS`의 결과 컬럼과, 타입은 Parquet 타입(7.3)과 맞춘다 |
| `HIVE.TARGET.DB`, `HIVE.TARGET.TABLE` | `dw`, `insp_dtl` | 게시 대상 |
| `TARGET.PARTITION.CLAUSE` | `PARTITION (base_dt='${load.business.key}')` | 교체할 target 파티션. 비우면 테이블 전체를 덮어쓴다 |
| `HIVE.INSERT.COLUMNS` | `INSP_DTL_SEQ, ITEM_CD, AMOUNT, REG_TS, NOTE` | staging에서 target으로 넣을 컬럼(파티션 컬럼 제외, target 순서) |
| `TARGET.BUSINESS.WHERE` | `base_dt = '${load.business.key}'` | target 검증(61) 범위. 교체한 파티션과 같아야 한다 |

## 5. 설치와 실행

### 5.1 Flow 생성

```bash
python3 poc/build_flow_v4.py http://<nifi-host>:<port>/nifi-api my-config.v4.json > flow_ids.json
```

- NiFi root 아래에 상위 PG(Job PG), 자식 PG 8개, Job Parameter Context, Controller Service를 만들고 Controller Service를 enable한다
- 공통 Parameter Context는 없으면 만들고, 있으면 값만 config로 맞춘다(다른 Job이 쓰고 있으므로 지우지 않는다)
- root에 PG-05가 없으면 만든다. 그다음 PG-05를 몇 초 멈추고 이 Job의 route·Output Port·root 연결을 추가한 뒤 Allowed Paths를 갱신하고 다시 시작한다. PG-05는 빌더가 시작하므로 따로 Start하지 않는다
- 표준 출력은 생성한 PG·Processor id와 `receiver`(PG-05 id), `receiver_jobs`(등록된 Job 목록)다. 이후 조작에 쓰므로 저장해 둔다
- 같은 이름의 PG가 이미 있으면 먼저 5.5로 지운다

생성 직후 Controller Service가 enable되는 동안 34 등이 잠시 INVALID로 보인다. 몇 초 뒤 모두 VALID가 되는지 확인한다.

### 5.2 시작

NiFi UI에서 Job PG `JOB_<JOB.KEY>`(예: `JOB_ORACLE_INSP_DTL_DAILY`)를 Start한다. REST API로는 다음과 같다.

```bash
curl -X PUT -H 'Content-Type: application/json' \
  -d '{"id":"<process_group id>","state":"RUNNING"}' \
  http://<nifi-host>:<port>/nifi-api/flow/process-groups/<process_group id>
```

Trigger(00)는 DISABLED라 PG를 시작해도 실행되지 않는다.

### 5.3 실행(업무일자 1회)

1. Job Parameter Context의 `BUSINESS.KEY`를 실행할 업무일자로 바꾼다
2. 00_Generate_Trigger를 Enable한다(상태가 STOPPED가 됨)
3. 00_Generate_Trigger에서 **Run Once**를 실행한다

00은 Primary Node에서만 실행되도록 만들어지므로 클러스터에서도 Run Once 한 번에 run 하나만 생긴다.

PG-70의 70(정리 트리거)은 DISABLED가 아니므로 상위 PG를 Start하면 바로 한 번 실행된다. 무엇을 지울지는 API가 정하므로 안전하다.

> **주의**: 00을 enable한 상태로 상위 PG를 다시 Start하면 00도 RUNNING이 되어 바로 한 번 실행된다(스케줄 1일). 같은 업무일자의 run이 진행 중이면 `DUPLICATE_ACTIVE_RUN`으로 거부되어 데이터에는 영향이 없지만, PG를 다시 시작하기 전에 00을 STOPPED 또는 DISABLED로 둔다.

### 5.4 정기 실행

00의 스케줄(기본 Timer 1일)을 운영 일정(예: CRON)으로 바꾸고 RUNNING으로 둔다. 업무일자를 자동으로 정하려면 01_Set_Trigger_Attributes의 `load.business.key` 식을 날짜 EL(예: `${now():toNumber():minus(86400000):format('yyyy-MM-dd')}`)로 바꾼다.

### 5.5 삭제

```bash
python3 poc/teardown_flow.py http://<nifi-host>:<port>/nifi-api my-config.v4.json
```

`names`에 적힌 Job PG와 Job Parameter Context를 지운다. root PG-05에서는 이 Job의 route·Output Port·연결만 지우고 Allowed Paths를 갱신한다. 마지막 Job이었으면 PG-05와 공통 Parameter Context도 지운다. 관리 DB 기록과 HDFS 파일은 지우지 않는다.

### 5.6 Job 추가

같은 NiFi에 다른 Job을 올릴 때는 config를 하나 더 만들어 5.1을 실행한다. 바꿀 값:

| 키 | 예 |
|---|---|
| `job_params.JOB.KEY` | `ORACLE_INSP_DTL_DAILY_B`(대문자·숫자·`_`). Job PG `JOB_ORACLE_INSP_DTL_DAILY_B`와 Context `PC_JOB_ORACLE_INSP_DTL_DAILY_B`가 이 값으로 정해진다 |
| `job_params` 원천·target | `SRC.*`, `HIVE.STAGE.TABLE.PREFIX`, `HIVE.TARGET.TABLE` 등 |

`common_params`는 기존 Job과 같게 둔다. 빌더가 공통 Context 값을 이 config로 덮어쓰기 때문이다. 등록되면 API의 `/validate/<JOB.KEY>` 호출이 그 Job PG로 간다. 두 Job을 동시에 실행해 각자 검증·게시까지 끝나는 것을 확인했다(REVIEW 7.11).

## 6. 실행 확인

### 6.1 run 상태

| 상태 | 의미 |
|---|---|
| `CREATED` | run 생성, manifest 등록 전 |
| `EXTRACTING` | manifest 등록, 파티션 추출 중 |
| `EXTRACTED_VALIDATED` | 모든 파티션 성공, 검증 호출 대기 |
| `STAGE_VALIDATING` | PG-40이 staging 검증 중 |
| `STAGING_VALIDATED` | staging 지표 모두 PASS, 게시 대기 |
| `PUBLISHING` | PG-50이 게시 소유권을 얻고 `INSERT OVERWRITE` 중 |
| `PUBLISHED` | 게시 완료, target 검증 중 |
| `SUCCESS` | target 지표 모두 PASS. 정상 종료 |
| `FAILED_STAGE_VALIDATION` | staging DDL·지표 조회 실패 또는 지표 FAIL |
| `PUBLISH_UNKNOWN` | 게시 결과 불명. **운영자가 확정해야 한다**(7.5) |
| `FAILED_PUBLISH` | 게시 실패(운영자 확정 포함) |
| `FAILED_TARGET_VALIDATION` | target 지표 조회 실패 또는 지표 FAIL |
| `FAILED_MANIFEST` | run 생성 후 SCN 조회·manifest 계산·등록 실패 |
| `FAILED_EXTRACT` | 파티션 하나 이상 실패 |
| `FAILED_SNAPSHOT_EXPIRED` | 파티션 쿼리가 `ORA-01555`/`ORA-08180`으로 실패 |
| `TIMED_OUT` | API sweeper가 시간 초과로 정리 |

### 6.2 관리 DB 조회

```sql
-- run
SELECT run_id, status, snapshot_scn, source_count, expected_partition_count,
       success_partition_count, extracted_count, error_stage, error_code
  FROM nifi_ops.load_run ORDER BY started_at DESC LIMIT 5;

-- 파티션
SELECT partition_id, status, lower_bound, upper_bound, expected_row_count,
       actual_row_count, file_count, attempt_count
  FROM nifi_ops.load_partition WHERE run_id = '<run_id>' ORDER BY partition_id;

-- 지표(SOURCE, STAGING, TARGET)
SELECT stage, metric_name, expected_value, actual_value, result
  FROM nifi_ops.load_validation WHERE run_id = '<run_id>' ORDER BY stage, metric_name;

-- 검증 호출
SELECT dispatch_type, status, last_http_status FROM nifi_ops.load_dispatch WHERE run_id = '<run_id>';

-- 오류·경고(INFO는 API의 상태 전이 기록)
SELECT event_time, process_group, event_level, event_name, error_code, partition_id, message
  FROM nifi_ops.load_event WHERE run_id = '<run_id>' AND event_level <> 'INFO' ORDER BY event_time;
```

정상 실행이면 run `SUCCESS`, `source_count = extracted_count = staging_count = target_count`, 모든 파티션 `SUCCESS`, STAGING·TARGET 지표 모두 `PASS`, dispatch `VALIDATE_RUN`이 `ACKED`(202)다.

### 6.3 결과 확인

```sql
-- Hive
SHOW PARTITIONS dw.insp_dtl;
SELECT count(*), sum(amount), min(reg_ts), max(reg_ts) FROM dw.insp_dtl WHERE base_dt = '2026-09-28';
SHOW TABLES IN stg;   -- run마다 tmp_<prefix><run_id> external table이 남는다
```

staging 테이블과 run 경로는 보존 기간이 지나면 PG-70이 지운다(7.6). 정리된 run은 `load_run.cleaned_at`이 채워진다.

### 6.4 같은 업무일자 다시 실행

같은 업무일자에 활성 run(실패·성공으로 끝나지 않은 run)이 있으면 새 run이 거부된다(`DUPLICATE_ACTIVE_RUN`). 끝난 run은 그대로 다시 Trigger하면 새 `run_id`·새 SCN으로 실행되고, target 파티션은 `INSERT OVERWRITE`로 교체된다. `PUBLISH_UNKNOWN`은 끝난 상태가 아니므로 먼저 7.5로 확정한다.

## 7. 오류와 대응

### 7.1 오류 코드

NiFi 오류는 PG-90이 `load_event`(process_group=상위 PG 이름)와 NiFi 로그(97_LogMessage)에 남긴다. 파티션·run 실패는 API가 `PARTITION_FAILED`, `RUN_FAILED`로 함께 남긴다.

| 이벤트·코드 | 수준 | 원인 | 대응 |
|---|---|---|---|
| `DUPLICATE_ACTIVE_RUN` | WARN | 같은 업무일자의 활성 run이 있음 | 기존 run 상태를 확인한다. 기존 run 변화 없음 |
| `CLAIM_MISMATCH` | WARN | 재발행 뒤 이전 시도의 chunk 보고가 늦게 도착 | 정상 경합. 조치 없음 |
| `ORA-01555` | ERROR | 고정 SCN의 undo가 사라짐. run `FAILED_SNAPSHOT_EXPIRED` | undo 보존 시간·크기를 늘리거나 변경이 적은 시간에 실행한 뒤 새로 Trigger한다 |
| `ORA-nnnnn`(그 밖) | ERROR | 권한(`ORA-00942`, `ORA-01031`), SQL 문법, 연결 오류 | 메시지와 34·14·16의 SQL을 확인하고 고친 뒤 새로 Trigger한다 |
| `SQL_ERROR` | ERROR | ORA 코드가 없는 추출 오류. 예: `Cannot encode decimal with precision 41 as max precision 38` | 7.2. 해당 컬럼을 큰 precision으로 CAST한다 |
| `CHUNK_WRITE_FAILED` | ERROR | PutHDFS 실패(경로 권한, HDFS 장애) | HDFS 상태와 `HDFS.STAGE.ROOT` 권한을 확인한다 |
| `API_UNREACHABLE`, `HTTP_<code>` | ERROR | API 호출 재시도를 모두 소진 | API 상태를 확인한다. 진행 중 run은 sweeper가 `TIMED_OUT`으로 정리한다 |
| `INVALID_SCN`(SQL 오류 메시지 안) | ERROR | SCN 속성이 숫자가 아님 | 14·15 결과를 확인한다 |
| `STAGING_METRIC_FAILED`(API, WARN) → `STAGE_VALIDATION_FAILED` | ERROR | staging 지표 FAIL. 메시지에 API의 `reasons`(예: `FAIL DUP_PK_COUNT`) | `load_validation`의 STAGING 행을 보고 원인(데이터, DDL 타입, 시간대)을 고친 뒤 새로 Trigger한다 |
| `CLEANUP_FAILED` | ERROR | PG-70이 지우지 못함. 메시지에 경로·table 이름. 75 검사 거부(경로가 현재 `HDFS.STAGE.ROOT`와 다름), Hive DROP·DeleteHDFS 실패 | 다음 주기에 자동으로 다시 한다. 75 거부는 계속 반복되므로 7.6의 수동 정리를 한다 |
| `STAGE_VALIDATION_FAILED`(메시지 `processor routed failure`) | ERROR | 48 DDL 또는 49 조회 실패(문법, 권한, 테이블 이름 형식) | NiFi bulletin에서 Hive 오류를 확인한다. PutClouderaHiveQL은 오류 attribute를 남기지 않는다 |
| `PUBLISH_UNKNOWN`(API) + `PUBLISH_FAILED`(NiFi) | ERROR | 55 failure·retry | 7.5 |
| `TARGET_METRIC_FAILED`(API, WARN) → `TARGET_VALIDATION_FAILED` | ERROR | target 지표 FAIL | `TARGET.BUSINESS.WHERE`와 `TARGET.PARTITION.CLAUSE`가 같은 범위인지, 다른 run이 같은 파티션을 덮었는지 확인한다. 자동 재게시하지 않는다 |

Processor 내장 재시도 횟수는 빌더에 고정돼 있다: API 호출(`InvokeHTTP`) `Retry`·`Failure` 5회, PutHDFS `failure` 3회, 48 `retry` 3회, 34·55는 0회. HiveServer2에 연결할 수 없으면 PutClouderaHiveQL은 failure로 보내지 않고 FlowFile을 큐에 되돌려 계속 다시 시도한다(REVIEW 7.9). 바꾸려면 빌더의 `retry=(...)` 값을 고친다. API가 잠시 중단돼도 이 재시도(penalty 증가)로 기다렸다가 재기동 후 이어서 진행한다(REVIEW 7.4).

### 7.2 정밀도 없는 `NUMBER`

`SRC.COLUMNS`에서 CAST하지 않은 정밀도 없는 `NUMBER`는 `ORACLE.NUMBER.DEFAULT.PRECISION`/`SCALE`로 기록된다(REVIEW 7.6).

| 경우 | 결과 |
|---|---|
| 기본값(38, 10) | `decimal(38,10)`. 소수 11자리부터 반올림 |
| 소수 자릿수 > scale | **오류 없이 반올림**. scale `0`이면 1.37 → 1. 건수 검증으로는 드러나지 않는다 |
| 정수부 자릿수 > precision − scale | 파티션 `SQL_ERROR` 실패, run `FAILED_EXTRACT` |

### 7.3 타입과 시간대

| Oracle | Parquet |
|---|---|
| `NUMBER(19)` | `decimal(19,0)`. Hive DDL은 `DECIMAL(19,0)` 또는 `BIGINT`로 CAST |
| `NUMBER(p,s)`, `CAST(... AS NUMBER(p,s))` | `decimal(p,s)` |
| `DATE`, `TIMESTAMP` | `timestamp[ms, UTC]`. `DATE`도 timestamp가 되고, 시간대 없는 값은 NiFi JVM 시간대로 해석되어 UTC로 저장된다. 마이크로초 이하는 버려진다 |
| `VARCHAR2` | string |

NiFi JVM `-Duser.timezone`과 Hive `hive.local.time.zone`을 같게 둔다. 시험 환경은 둘 다 `America/New_York`이고, Hive에서 읽은 `REG_TS`가 Oracle 값과 같았다(REVIEW 7.9). 다르면 `MIN_TS`/`MAX_TS` 지표가 FAIL이 된다(가이드 4장).

| Parquet | Hive DDL(`HIVE.STAGE.DDL.COLUMNS`) |
|---|---|
| `decimal(p,s)` | `DECIMAL(p,s)` |
| `timestamp[ms]` | `TIMESTAMP` |
| string | `STRING` |

### 7.4 재처리 원칙

- 파티션 쿼리(34)는 재시도하지 않는다. 같은 SCN으로 긴 쿼리를 반복하지 않기 위해서다
- 실패한 run의 일부 파티션만 다시 읽지 않는다. 새로 Trigger해 새 `run_id`·새 SCN으로 전체를 다시 실행한다
- 실패한 run의 HDFS 파일은 그 run 경로에만 남고 `_SUCCESS`가 없다. 다음 run과 섞이지 않는다
- 게시(55)는 재시도하지 않는다. target 검증이 실패해도 자동으로 다시 게시하지 않는다
- API를 `recovery.mode=REISSUE`로 띄운 경우에만 sweeper가 멈춘 파티션을 같은 `run_id`·같은 SCN으로 재발행한다(가이드 13장)

### 7.5 `PUBLISH_UNKNOWN` 확정

55가 failure·retry로 가면 SQL이 실행됐는지 알 수 없으므로 run은 `PUBLISH_UNKNOWN`이 된다. 같은 업무일자의 새 run도 막힌다. Hive query history와 target 지표를 확인한 뒤 operator 토큰으로 확정한다.

```bash
curl -X POST -H "Authorization: Bearer <operator-token>" -H 'Content-Type: application/json' \
  -d '{"resolution":"FAILED_PUBLISH","reason":"SemanticException으로 컴파일 단계에서 실패, target 변경 없음"}' \
  <API>/v1/runs/<run_id>/publish-unknown/resolve
```

target이 실제로 바뀌었고 지표가 맞으면 `"resolution":"PUBLISHED"`로 확정한다. 이 경우 PG-60이 실행되지 않으므로 target 검증은 운영자가 따로 한다.

### 7.6 정리(PG-70)

- 대상: 끝난 run 중 보존 기간이 지나고 아직 정리하지 않은 run. `SUCCESS`는 3일, `FAILED_*`·`TIMED_OUT`은 14일(API `cleanup` 설정). 진행 중인 run과 `PUBLISH_UNKNOWN`은 지우지 않는다
- 지우는 것: `#{HIVE.STAGE.DB}.<stage table>`(external table이라 DROP만으로는 파일이 남음)과 HDFS run 경로 전체. target 테이블은 건드리지 않는다
- 실패하면 다음 주기(1시간)에 같은 run을 처음부터 다시 처리한다. DROP `IF EXISTS`와 없는 경로 삭제는 성공으로 끝난다
- 대상 확인: `curl -H "Authorization: Bearer <token>" "<API>/v1/cleanup/candidates?jobKey=<JOB.KEY>"`

75에서 거부된 run(예: `HDFS.STAGE.ROOT`를 바꾸기 전 run)은 운영자가 직접 지우고 기록한다.

```bash
hdfs dfs -rm -r '<hdfsRunPath>'                       # 경로를 두 번 확인한다
beeline -e "DROP TABLE IF EXISTS <HIVE.STAGE.DB>.<stageTable>"
curl -X POST -H "Authorization: Bearer <operator-token>" -H 'Content-Type: application/json' \
  -d '{"droppedTable":"<db.table>","deletedPath":"<hdfsRunPath>"}' <API>/v1/runs/<run_id>/cleanup
```

## 8. Oracle 시험 환경 구성

Oracle이 없는 환경에서 시험할 때 쓴 구성이다(REVIEW 7.4).

```bash
docker run -d --name nifi-poc-oracle --restart unless-stopped -p 1521:1521 \
  -e ORACLE_PASSWORD=<sys-pw> gvenzl/oracle-free:23-slim
docker logs -f nifi-poc-oracle      # "DATABASE IS READY TO USE" 확인
```

시험 데이터(업무일자 `2026-09-28` 105,000건, seq 30001~45000 공백으로 0건 파티션 1개, 다른 일자 5,000건):

```sql
-- sqlplus sys/<sys-pw>@//localhost:1521/FREEPDB1 as sysdba
CREATE USER APP IDENTIFIED BY <pw> QUOTA UNLIMITED ON USERS DEFAULT TABLESPACE USERS;
CREATE TABLE APP.INSP_DTL (
  INSP_DTL_SEQ NUMBER(19), BASE_DT DATE NOT NULL, ITEM_CD VARCHAR2(10) NOT NULL,
  AMOUNT NUMBER NOT NULL, REG_TS TIMESTAMP NOT NULL, NOTE VARCHAR2(4000));
INSERT /*+ APPEND */ INTO APP.INSP_DTL
SELECT g, DATE '2026-09-28', 'C' || MOD(g,17), ROUND(MOD(g,1000) * 1.37, 2),
       TIMESTAMP '2026-09-28 00:00:00' + NUMTODSINTERVAL(g,'SECOND'),
       CASE WHEN MOD(g,10) = 0 THEN NULL ELSE 'note ' || g END
  FROM (SELECT LEVEL g FROM DUAL CONNECT BY LEVEL <= 120000) WHERE g NOT BETWEEN 30001 AND 45000;
COMMIT;
INSERT INTO APP.INSP_DTL
SELECT CASE WHEN g <= 5 THEN NULL ELSE 200000 + g END, DATE '2026-09-27', 'X', 1, SYSTIMESTAMP, NULL
  FROM (SELECT LEVEL g FROM DUAL CONNECT BY LEVEL <= 5000);
COMMIT;
CREATE INDEX APP.IX_INSP_DTL_BASE_SEQ ON APP.INSP_DTL(BASE_DT, INSP_DTL_SEQ);
EXEC DBMS_STATS.GATHER_TABLE_STATS('APP','INSP_DTL');
-- NIFI_READER 생성과 권한은 3.2
```

설정 파일은 `ORACLE.JDBC.URL=jdbc:oracle:thin:@//localhost:1521/FREEPDB1`로 둔다. 기대 결과는 105,000건, `SUM(AMOUNT)` 71,853,075, 파티션 8개 중 0002가 0건이다.

## 9. HDFS·Hive 시험 환경 구성

`poc/hdfs-hive/compose.yaml`이 단일 NameNode·DataNode(Apache Hadoop 3.4.1)와 HiveServer2(Apache Hive 4.0.1, Derby metastore, Tez local)를 host network로 띄운다. 주소는 NiFi 노드에서 보이는 libvirt 내부 주소 `192.168.122.1`이다(다른 환경이면 `conf/*.xml`의 주소를 바꾼다).

```bash
cd poc/hdfs-hive
docker volume create nifi-poc-hdfs-hive_hive
docker run --rm -u root -v nifi-poc-hdfs-hive_hive:/opt/hive/data --entrypoint chown apache/hive:4.0.1 hive:hive /opt/hive/data
docker compose up -d namenode datanode
docker exec nifi-poc-namenode hdfs dfs -mkdir -p /data/nifi/stage /tmp/hive /user \
  /warehouse/tablespace/managed/hive /warehouse/tablespace/external/hive
docker exec nifi-poc-namenode hdfs dfs -chmod -R 1777 /tmp
docker compose up -d hiveserver2
# 3.3의 DB·target 테이블 생성
docker exec nifi-poc-hiveserver2 beeline -u jdbc:hive2://192.168.122.1:10000/ -n hive -e "..."
# 모든 NiFi 노드에 conf/core-site.xml, conf/hdfs-site.xml을 복사하고 HADOOP.CONF.FILES로 지정한다
```

- HDFS 권한 검사는 끈다(`dfs.permissions.enabled=false`). NiFi(`nifi`)가 쓴 파일을 Hive(`hive`)가 읽기 위해서다. 운영도 권한 검사를 하지 않는다
- Hive 시간대는 NiFi 노드와 같은 `America/New_York`(`hive.local.time.zone`)
- Hive 4는 DB 디렉터리가 이미 있으면 `CREATE DATABASE`가 실패한다. metastore를 새로 만들었다면 빈 `/warehouse/tablespace/managed/hive/<db>.db`를 지우고 만든다

## 10. 참고

| 문서 | 내용 |
|---|---|
| `poc/REVIEW.md` 7장 | V4 변경점, 시험 결과(정상·중복·HDFS 실패·API 중단·재발행·`NUMBER`·`ORA-01555`, CFM 클러스터, Hive 단계) |
| `nifi-sqoop-removal-guide.md` 7·8·10~12·16장 | PG-10·20·40·50·60 설계, SCN과 manifest SQL, 지표 SQL, 재시도 분류 |
| `load-control-api-design.md` | API 계약, 완료 판정, outbox, sweeper |
| `load-control-api/README.md` | API 설치·실행·테스트 |
| `TODO.md` | 운영 적용에 남은 일 |
