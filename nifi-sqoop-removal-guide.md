# CFM 4.12.0 Sqoop 제거 통합 설계 및 NiFi Flow 구현 명세

> 기준: CFM 4.12.0 / Apache NiFi 2.6.0, Oracle → HDFS Parquet → Hive External Staging → `INSERT OVERWRITE`
>
> 완료 판정과 상태 기록은 Load Control API(Python FastAPI)가 담당한다. API 계약, 판정 트랜잭션, FastAPI 구현은 [Load Control API 설계](./load-control-api-design.md)(이하 "API 설계")에 있다.

## 1. 구현 범위와 전제

이 문서는 Sqoop 제거를 위한 아키텍처 설계와 NiFi Canvas 구현 명세를 하나로 통합한다. 설계 원칙, 상태 및 검증 모델부터 Process Group, Processor, Controller Service, Parameter Context, PostgreSQL DDL, 오류 처리와 로그까지 포함한다.

역할은 다음과 같이 나눈다.

- NiFi: 원천 조회, Parquet 변환, HDFS 기록, Hive DDL·DQ·`INSERT OVERWRITE` 실행. 결과는 모두 Load Control API에 보고한다.
- Load Control API: PostgreSQL 원장 기록, 불변식 검증, 파티션·run 완료 판정, 상태 전이(CAS), 검증 flow 호출(outbox), timeout 정리(sweeper).
- PostgreSQL `nifi_ops`: 원장. 업무 테이블에 쓰는 주체는 API 하나이고, NiFi는 관측 이벤트(`load_event`)만 직접 기록한다.

예시 이름은 다음 규칙을 사용한다.

```text
Process Group : PG-번호-기능
Processor     : 번호_동사_대상
ControllerSvc : CS_종류_용도
Parameter     : 대문자 점 표기(예: SRC.TABLE)
Attribute     : load.* / partition.* / error.* / event.*
```

테이블 DDL, 컬럼 목록, 업무 WHERE 조건은 실제 대상별로 확정해야 한다. 테이블명과 컬럼명은 JDBC bind parameter가 될 수 없으므로 승인된 Parameter Context에서만 공급한다.

---

## 설계 개요와 핵심 보장

### AS-IS와 TO-BE

#### AS-IS 처리 구조

```mermaid
flowchart LR
    A1[(Oracle 원천)] --> A2[NiFi 1.x<br/>Kylo ImportSqoop]
    A2 --> A3{Sqoop Job<br/>YARN MapReduce}
    A3 --> A31[Mapper 0]
    A3 --> A32[Mapper 1]
    A3 --> A33[Mapper N]
    A31 --> A4[(HDFS 공용<br/>적재 경로)]
    A32 --> A4
    A33 --> A4
    A4 --> A5[Hive External<br/>임시 테이블]
    A5 --> A6[INSERT OVERWRITE]
    A6 --> A7[(Hive 원본 테이블)]
    A7 --> A8[Oracle 원천 건수와<br/>적재 건수 비교]

    classDef source fill:#e8f1ff,stroke:#2563eb,color:#111827;
    classDef storage fill:#ecfdf5,stroke:#059669,color:#111827;
    classDef control fill:#fff7ed,stroke:#ea580c,color:#111827;
    class A1 source;
    class A4,A7 storage;
    class A2,A3,A5,A6,A8 control;
```

#### TO-BE 처리 구조

```mermaid
flowchart TB
    B1[(Oracle 원천<br/>고정 Snapshot SCN)] --> B2[PG-10 Coordinator<br/>범위 Manifest 계산]
    B2 --> B3{PG-20<br/>병렬 Worker}

    subgraph WORKERS["NiFi Cluster 병렬 추출"]
        direction LR
        B31[ExecuteSQLRecord<br/>Partition 0]
        B32[ExecuteSQLRecord<br/>Partition 1]
        B33[ExecuteSQLRecord<br/>Partition N]
    end

    B3 --> B31
    B3 --> B32
    B3 --> B33
    B31 --> B4[(run_id별 HDFS<br/>격리 Staging)]
    B32 --> B4
    B33 --> B4
    B4 -->|chunk마다 보고| B5[Load Control API<br/>File·Partition·Run 완료 판정]
    B5 -->|run당 1회 호출| B6[NiFi 검증 flow<br/>Hive External Staging<br/>Count·Schema·DQ 검증]
    B6 --> B7{모든 검증<br/>PASS?}
    B7 -->|예| B8[API Publish Token CAS<br/>단일 INSERT OVERWRITE]
    B8 --> B9[(Hive 원본 테이블)]
    B9 --> B10[Target 사후 검증]
    B10 --> B11[Run SUCCESS]
    B7 -->|아니요| BX[전체 Run 실패<br/>게시 차단]

    BM[(PostgreSQL<br/>Run·Partition·File·Validation·Dispatch·Event)]
    B2 -. Manifest 등록 .-> B5
    B6 -. 검증 결과 .-> B5
    B8 -. 게시 소유권 .-> B5
    B10 -. 최종 결과 .-> B5
    B5 == 유일한 원장 writer ==> BM

    classDef source fill:#e8f1ff,stroke:#2563eb,color:#111827;
    classDef storage fill:#ecfdf5,stroke:#059669,color:#111827;
    classDef control fill:#fff7ed,stroke:#ea580c,color:#111827;
    classDef success fill:#f0fdf4,stroke:#16a34a,color:#166534;
    classDef failure fill:#fef2f2,stroke:#dc2626,color:#991b1b;
    class B1 source;
    class B4,B9,BM storage;
    class B2,B3,B5,B6,B7,B8,B10 control;
    class B11 success;
    class BX failure;
```

AS-IS에서는 Sqoop/YARN이 Mapper 실행과 전체 Job 실패를 담당한다. TO-BE에서는 NiFi가 병렬 Worker를 실행하고, Load Control API가 PostgreSQL Manifest를 원장으로 완료를 판정한다. staging 검증을 통과하고 API에서 publish token을 얻은 단 하나의 FlowFile만 최종 테이블을 변경한다.

Sqoop Mapper가 제공하던 분할 조회와 전체 Job 실패 의미를 NiFi Processor의 단순 병렬 실행만으로 대체해서는 안 된다. TO-BE는 데이터 처리 영역과 제어 영역을 분리한다.

- Data plane (NiFi): Oracle 조회, Record 변환, HDFS 파일 기록, Hive 검증 조회와 게시 SQL 실행
- Control plane (Load Control API + PostgreSQL): Run/Partition/File 상태, 완료 판정, 검증 flow 호출, 게시 소유권과 복구

### 불변 실행 식별자와 격리

매 실행에 UUID `run_id`를 발급하고 FlowFile, PostgreSQL 관리 행, HDFS 경로와 로그에 동일하게 사용한다.

```text
job_key       = 적재 Job의 영구 식별자
business_key  = 업무일자 또는 적재 범위 식별자
run_id        = 한 번의 실행을 나타내는 UUID
snapshot_scn  = 해당 run이 읽는 Oracle 고정 SCN
partition_id  = split 범위 식별자
```

```text
/data/nifi/stage/<job_key>/run_id=<run_id>/part-<partition_id>-<chunk_index>.parquet
```

파일은 run root 바로 아래에 평탄하게 둔다. 파일명에 `partition_id`와 `chunk_index`가 들어가므로 하위 디렉터리 없이도 고유하다. `part=<partition_id>/` 같은 하위 디렉터리를 두면 run root를 `LOCATION`으로 지정한 비파티션 external table이 Hive 설정(`hive.mapred.supports.subdirectories`, `mapreduce.input.fileinputformat.input.dir.recursive`)에 따라 하위 파일을 읽지 않아 0건이 될 수 있다. 또 `key=value` 형식은 Hive partition 디렉터리로 오인될 수 있다.

실패한 run의 파일은 다른 실행 및 최종 테이블과 섞이지 않는다. 재실행은 이전 run을 수정하지 않고 새 `run_id`를 사용한다.

### Oracle 읽기 일관성

여러 JDBC Connection이 서로 다른 시점의 데이터를 읽지 않도록 시작 시점에 SCN을 한 번 고정한다. source metrics, partition 예상 건수와 실제 데이터 조회는 모두 동일한 `AS OF SCN`을 사용한다.

Flashback Query를 사용할 수 없다면 Oracle snapshot table, 불변 업무 마감 조건 또는 원천 변경이 없는 배치 구간을 사용한다(PostgreSQL 등 Oracle 이외 원천은 아래 참조). 추출 전후 `COUNT(*)`가 같다는 사실만으로 동일 시점 데이터는 보장되지 않는다.

#### Oracle 이외 원천 (PostgreSQL)

PostgreSQL에는 `AS OF SCN`에 해당하는 과거 시점 조회가 없다. `pg_export_snapshot()`으로 내보낸 snapshot은 내보낸 트랜잭션이 열려 있는 동안에만 쓸 수 있는데, NiFi Connection Pool의 서로 다른 connection과 Processor 사이에서 그 트랜잭션을 유지할 수 없다. 따라서 다음 중 하나를 필수 전제로 둔다.

- 불변 업무 마감 조건: 대상 `business_key` 범위의 행이 적재 시점에 더 이상 변경되지 않음을 업무적으로 보장
- 원천 측 snapshot table: 마감 시점에 원천에서 별도 테이블로 복제한 뒤 그 테이블을 추출

SQL에서는 `AS OF SCN` 절을 제거하고, 7.2의 12~14(SCN 조회·검증) 단계를 생략하거나 마감 확인 조회로 대체한다. PostgreSQL JDBC driver는 autocommit=false일 때만 Fetch Size(server-side cursor)를 적용한다. 그러므로 추출 `ExecuteSQLRecord`에 `Set Auto Commit=false`를 설정한다. 설정하지 않으면 파티션 결과 전체를 NiFi 메모리에 적재한다. 이 구성은 NiFi 2.4.0 + PostgreSQL 16 PoC에서 105,000건, 8파티션으로 검증했다.

### 범위 파티셔닝

`INSP_DTL_SEQ` 범위는 하한 포함·상한 미포함으로 생성하고 마지막 범위만 최댓값을 포함한다.

```text
partition 0   : seq >= b0   AND seq < b1
partition 1   : seq >= b1   AND seq < b2
partition N-1 : seq >= bN-1 AND seq <= max_seq
```

NULL은 사전 실패 또는 별도 `IS NULL` 파티션 중 하나로 명시한다. 각 range의 expected count를 동일 SCN에서 계산하고 그 합이 source count와 같은지 Worker 실행 전에 확인한다.

### 완료 판정의 원장

PostgreSQL의 Run, Partition, File Manifest가 원장이고, 원장에 쓰는 주체는 Load Control API 하나다. NiFi Worker는 PutHDFS가 성공한 chunk마다 API에 보고한다. API는 보고마다 run 행을 잠근 트랜잭션에서 다음을 판정한다.

```text
파티션 완료:
  받은 chunk 수 = fragment.count, chunk_index가 0..n-1로 연속
  SUM(record_count) = partition.expected_row_count

run 완료:
  manifest partition 수 = run.expected_partition_count
  모든 partition 상태 = SUCCESS
  SUM(partition.actual_row_count) = run.source_count
```

run 완료 CAS(`EXTRACTING → EXTRACTED_VALIDATED`)에 성공한 단 하나의 보고만, 같은 트랜잭션에서 검증 flow 호출(outbox)을 예약한다. NiFi Queue, FlowFile 수, cache 신호는 판정에 쓰지 않는다. 따라서 중복 보고, NiFi 재기동, API 재기동이 있어도 잘못 게시되지 않는다. 판정 트랜잭션의 상세는 API 설계 3장, 호출 전달 보장은 4장을 따른다.

### 검증 및 게시 원칙

검증은 다음 네 경계를 통과한다.

1. Source: Oracle SCN 기준 count, min/max, NULL, 업무 집계
2. Extract: partition/file count, HDFS 성공 여부, row count 합계
3. Staging: Hive external table count, schema, PK 중복과 업무 집계
4. Target: `INSERT OVERWRITE` 후 동일 업무 범위의 count와 품질 지표

건수만 일치하면 누락과 중복이 상쇄될 수 있으므로 PK NULL/중복, 주요 금액 합계, 코드별 건수 및 필요한 경우 canonical hash를 함께 사용한다. 원천 0건은 기본적으로 게시하지 않는다.

게시 직전 API의 `POST /publish/claim`이 `STAGING_VALIDATED → PUBLISHING` 상태를 publish token으로 compare-and-set하고, `claimed=true`를 받은 단 하나의 FlowFile만 `INSERT OVERWRITE`를 실행한다. Hive 결과가 불명확한 timeout은 `PUBLISH_UNKNOWN`으로 남기고 자동 재실행하지 않는다.

### 실패 원칙

- 파티션 하나라도 최종 실패하면 전체 run을 실패시키고 게시를 금지한다.
- 일시적 Oracle/HDFS 오류만 제한 재시도한다.
- `ORA-01555`, 권한, SQL, schema 및 검증 오류는 즉시 실패한다.
- 한 run의 일부 파티션만 새 SCN으로 다시 읽지 않는다.
- 늦게 완료된 다른 파티션은 실패 run의 격리 경로에만 남고 성공 상태를 되돌리지 못한다.
- stale partition과 timeout은 API sweeper가 PostgreSQL manifest를 기준으로 정리한다. 기본 정책은 run 실패 후 새 `run_id`로 재실행이고, 재발행을 켜면 동일 `run_id`와 SCN으로만 재발행한다(13장).

---

## 2. 최상위 Canvas

```mermaid
flowchart LR
    T[PG-00 Trigger] --> C[PG-10 Run Coordinator]
    C -->|partition FlowFiles| W[PG-20 Extract Workers]
    API[[Load Control API<br/>FastAPI]] -->|POST /validate/jobKey| RC[PG-05 Control Receiver]
    RC -->|validate-in| S[PG-40 Staging Validation]
    S -->|validated| P[PG-50 Publish]
    P --> V[PG-60 Target Validation]
    API -.->|POST /reissue/jobKey 선택| RC
    RC -.->|reissue-in| W

    C -- runs, manifest --> API
    W -- claim, chunks, fail --> API
    S -- start, validations --> API
    P -- publish claim, result --> API
    V -- validations, success --> API
    API --- M[(PostgreSQL nifi_ops)]

    C -. event .-> A[PG-90 Audit and Notify]
    W -. event .-> A
    S -. event .-> A
    P -. event .-> A
    V -. event .-> A
    A --- M

    O[(Oracle Source)] --- C
    O --- W
    H[(HDFS)] --- W
    H --- S
    Q[(Hive)] --- S
    Q --- P
    Q --- V
```

가이드 초안의 PG-30 Partition and Run Gate(Wait/Notify)와 PG-70 Recovery Monitor는 없다. 완료 판정은 API가 하고, 검증 flow는 API의 호출을 PG-05가 받아 시작한다(9장). 복구는 API sweeper가 담당한다(13장).

실행 정책은 다음과 같다.

| 영역 | 실행 노드 | Concurrent Tasks |
|---|---|---:|
| Trigger, Coordinator | Primary Node | 1 |
| Oracle Extract, PutHDFS, API 보고 | All Nodes | 노드당 `WORKER.CONCURRENT.TASKS` 기준값 |
| Control Receiver(`HandleHttpRequest`), Staging Validation, Publish, Target Validation | All Nodes | 1 |
| Audit writer | All Nodes | 2~4 |

API는 NiFi LB 주소 하나로 검증 flow를 호출하므로 어느 노드가 요청을 받을지 정할 수 없다. 그래서 PG-40~60은 All Nodes로 스케줄한다. Primary Node로 제한하면 다른 노드가 받은 FlowFile이 처리되지 않는다. 중복 실행은 Primary Node가 아니라 API의 CAS(`/validation/start`, `/publish/claim`)가 막는다.

Coordinator에서 Worker로 가는 Connection은 `Round Robin` Load Balance를 설정한다. 그 외 제어 Connection은 load balance를 사용하지 않는다.

Concurrent Tasks는 정수 스케줄링 설정이라 Parameter(`#{...}`)나 Expression Language(`${...}`)를 참조할 수 없다. REST API에서도 정수 필드로 정의되어 있다. `WORKER.CONCURRENT.TASKS`는 환경별 기준값으로 관리하고, 배포 스크립트나 운영 절차에서 해당 Processor의 Concurrent Tasks에 정수로 입력한다.

---

## 3. Parameter Context

### 3.1 `PC_SQOOP_REPLACEMENT_COMMON`

| Parameter | 예시 | Sensitive | 용도 |
|---|---|---:|---|
| `CONTROL.API.URL` | `https://load-control.internal:8443/v1` | N | Load Control API base URL |
| `CONTROL.API.TOKEN` | 미표시 | Y | API Bearer 토큰(role=`nifi`) |
| `CONTROL.API.TIMEOUT` | `30 sec` | N | `InvokeHTTP` Socket Read Timeout |
| `CONTROL.API.RETRY.MAX` | `5` | N | API 호출 재시도 횟수. penalty와 곱해 API 재기동 시간보다 길게 |
| `CONTROL.LISTEN.PORT` | `9443` | N | API→NiFi 호출 수신 포트(PG-05). 모든 Job이 공유 |
| `META.JDBC.URL` | `jdbc:postgresql://meta:5432/nifiops` | N | 관리 DB. PG-90 이벤트 기록 전용 |
| `META.JDBC.USER` | `nifi_runtime` | N | `load_event` INSERT 권한만 가진 계정 |
| `META.JDBC.PASSWORD` | 미표시 | Y | 관리 DB 암호 |
| `ORACLE.JDBC.URL` | `jdbc:oracle:thin:@//host:1521/service` | N | 원천 Oracle |
| `ORACLE.JDBC.USER` | `nifi_reader` | N | 원천 조회 계정 |
| `ORACLE.JDBC.PASSWORD` | 미표시 | Y | 원천 암호 |
| `ORACLE.JDBC.DRIVER.PATH` | `/opt/nifi/jdbc/ojdbc11.jar` | N | JDBC Driver |
| `HIVE.JDBC.URL` | 환경별 HiveServer2 URL | N | HiveQL 실행 |
| `HIVE.USER` | service account | N | Hive 계정 |
| `HADOOP.CONF.FILES` | `core-site.xml,hdfs-site.xml` 절대경로 | N | PutHDFS |
| `HDFS.AUTH.MODE` | `simple` | N | 비-Ker버 HDFS 인증 방식 문서화 |
| `HDFS.STAGE.ROOT` | `/data/nifi/stage` | N | staging root |
| `HDFS.PERMISSIONS.UMASK` | `027` | N | PutHDFS 생성 파일/경로 umask |
| `HDFS.REPLICATION` | 환경 기본값 또는 `3` | N | 필요 시 PutHDFS replication override |
| `WORKER.CONCURRENT.TASKS` | `2` | N | 노드당 추출 병렬도 기준값. Concurrent Tasks에는 참조할 수 없으므로 배포 시 정수로 입력 |
| `ORACLE.POOL.MAX` | `8` | N | 전체 노드 정책과 맞춤 |
| `EXTRACT.FETCH.SIZE` | `5000` | N | JDBC fetch size |
| `EXTRACT.ROWS.PER.FILE` | `500000` | N | chunk 행 수, 부하 시험으로 조정 |
| `EXTRACT.QUERY.TIMEOUT` | `60 min` | N | 파티션 query timeout |
| `PARTITION.RETRY.MAX` | `3` | N | 일시 오류 재시도 |
| `ALLOW.EMPTY.SOURCE` | `false` | N | 0건 overwrite 방지. `POST /runs`로 API에도 전달 |
| `FAILED.RETENTION.DAYS` | `14` | N | 실패 staging 보존 |
| `SUCCESS.RETENTION.DAYS` | `3` | N | 성공 staging 보존 |

run timeout, stale 판정, dispatch 재시도 같은 제어 설정은 NiFi Parameter가 아니라 API 설정(`LCA_*` 환경변수, API 설계 9.4)이다. `EXTRACT.QUERY.TIMEOUT`은 NiFi와 API 양쪽에 같은 값을 둔다. API는 이 값으로 stale 여부를 판단한다.

### 3.2 `PC_JOB_<JOB_NAME>`

| Parameter | 예시 | 설명 |
|---|---|---|
| `JOB.KEY` | `ORACLE_INSP_DTL_DAILY` | 관리용 고유 키 |
| `SRC.OWNER` | `APP` | Oracle owner |
| `SRC.TABLE` | `INSP_DTL` | Oracle table |
| `SRC.COLUMNS` | `COL_A,COL_B,...,INSP_DTL_SEQ` | 순서를 고정한 컬럼 목록 |
| `SRC.SPLIT.COLUMN` | `INSP_DTL_SEQ` | split-by 대체 컬럼 |
| `SRC.BASE.WHERE` | `BASE_DT = ?` | 승인된 고정 조건 템플릿 |
| `SRC.BUSINESS.KEY.TYPE` | `91` | JDBC type, DATE=91 등 |
| `PARTITION.COUNT` | `8` | 논리 파티션 수 |
| `SPLIT.NULL.POLICY` | `FAIL` | `FAIL` 또는 `SEPARATE` |
| `HIVE.STAGE.DB` | `STG_DB` | 임시 external DB |
| `HIVE.STAGE.TABLE.PREFIX` | `TMP_INSP_DTL_` | run별 테이블 prefix |
| `HIVE.STAGE.DDL.COLUMNS` | 실제 Hive DDL 컬럼 | external table schema |
| `HIVE.TARGET.DB` | `DW` | target DB |
| `HIVE.TARGET.TABLE` | `INSP_DTL` | target table |
| `HIVE.INSERT.COLUMNS` | 명시적 SELECT 컬럼 | `SELECT *` 금지 |
| `TARGET.PARTITION.CLAUSE` | `PARTITION (BASE_DT='...')` | 전체 overwrite면 빈 값 |
| `DQ.SOURCE.SQL` | 승인된 집계 SQL | count 외 검증 |
| `DQ.STAGE.SQL` | 대응 Hive SQL | 동일 의미의 집계 |
| `DQ.TARGET.SQL` | 대응 target SQL | 게시 후 검증 |

JDBC type 주요 값은 `NUMERIC=2`, `BIGINT=-5`, `VARCHAR=12`, `DATE=91`, `TIMESTAMP=93`이다. 실제 Oracle 컬럼 타입에 맞춰 지정한다.

---

## 4. Controller Services

| 이름 | 구현 | 주요 설정 |
|---|---|---|
| `CS_DBCP_ORACLE` | `HikariCPConnectionPool` | URL/계정/ojdbc, Max Total=`${ORACLE.POOL.MAX}`, validation query=`SELECT 1 FROM DUAL` |
| `CS_DBCP_META` | `HikariCPConnectionPool` | 관리 DB, `load_event` INSERT 전용(PG-90), Max Total 4~8 |
| `CS_HIVE3_DBCP` | CFM 제공 Hive Connection Pool | HiveServer2의 실제 인증 방식 적용. Apache NiFi 2.x에는 Hive 구성요소가 없으므로 CFM 4.12.0 제공 이름을 확정 |
| `CS_JSON_WRITER_ARRAY` | `JsonRecordSetWriter` | Output Grouping=`Array`, pretty print=false |
| `CS_PARQUET_WRITER` | `ParquetRecordSetWriter` | Schema=`Inherit Record Schema`, compression=`SNAPPY` |
| `CS_PARQUET_READER` | `ParquetReader` | ValidateRecord에서 기록 결과 schema를 다시 읽음 |
| `CS_SCHEMA_REGISTRY` | 조직 표준 Schema Registry | target Avro schema를 버전으로 고정 |
| `CS_SSL_CLIENT` | `StandardRestrictedSSLContextService` | NiFi→API `InvokeHTTP` mTLS. truststore에 API 서버 CA, keystore에 NiFi client 인증서 |
| `CS_SSL_SERVER` | `StandardRestrictedSSLContextService` | API→NiFi `HandleHttpRequest` 수신 TLS. Client Auth=Required로 API client 인증서 검증 |
| `CS_HTTP_CONTEXT_MAP` | `StandardHttpContextMap` | `HandleHttpRequest`/`HandleHttpResponse` 요청 연결 보관, Request Expiration 1 min |

가이드 초안의 `CS_DMC_SERVER`(`MapCacheServer`)와 `CS_DMC_CLIENT`(`MapCacheClientService`)는 Wait/Notify를 쓰지 않으므로 두지 않는다.

운영 데이터에는 schema inference를 사용하지 않는다. Oracle JDBC schema를 상속하되, Oracle `NUMBER`, `DATE`, `TIMESTAMP`, CLOB 처리 결과가 Hive DDL과 일치하는지 사전 시험하고 필요하면 `ConvertRecord`를 추가해 명시적 schema로 변환한다.

추출 `ExecuteSQLRecord`에는 `Use Avro Logical Types=true`를 명시한다. 기본값 `false`이면 DATE, TIMESTAMP, DECIMAL이 문자열로 기록되어 Hive DDL과 어긋난다.

시간대가 없는 원천 `DATE`/`TIMESTAMP`는 JDBC가 NiFi JVM 기본 시간대로 해석한다. 그 결과 Parquet에는 UTC로 변환된 `TIMESTAMP_MILLIS (isAdjustedToUTC=true)`로 기록된다. NiFi 2.4.0 PoC에서 JVM 시간대가 KST일 때 원천 `2026-09-28 00:00:01`이 `2026-09-27T15:00:01Z`로 저장됐고, 마이크로초 이하 정밀도는 버려졌다. Hive가 이 값을 읽는 방식은 Hive 버전과 parquet timestamp 설정에 따라 다르며, 건수 검증으로는 이 차이를 잡을 수 없다. 따라서 다음을 지킨다.

- NiFi JVM `-Duser.timezone`과 Hive parquet timestamp 해석 설정을 환경 표준으로 확정한다.
- 대표 TIMESTAMP/DATE 컬럼의 `MIN`/`MAX`를 Source, Staging, Target DQ 지표에 포함해 문자열 값으로 비교한다.
- 마이크로초 이상 정밀도가 업무상 필요한 컬럼은 추출 SQL에서 문자열로 변환하거나 명시적 schema로 정밀도를 확인한다.

### 4.1 PostgreSQL 관리 및 로그 스키마

PostgreSQL 13 이상을 기준으로 한다. 식별자는 따옴표 없이 소문자로 생성한다. 이 절의 DDL이 원본이며, 운영 DB에는 Load Control API 저장소의 Alembic baseline migration으로 배포한다(API 설계 9.9).

`load_run`, `load_partition`, `load_file`, `load_validation`, `load_dispatch`는 API만 쓴다. NiFi는 `load_event`에만 INSERT한다. 이 절의 SQL에서 `:name` 형식은 API의 bind parameter, `?`는 NiFi `PutSQL` parameter다.

#### Schema와 Run 테이블

```sql
CREATE SCHEMA IF NOT EXISTS nifi_ops;

CREATE TABLE nifi_ops.load_run (
    run_id                       uuid PRIMARY KEY,
    job_key                      varchar(200) NOT NULL,
    business_key                 varchar(200) NOT NULL,
    status                       varchar(40) NOT NULL,
    snapshot_scn                 numeric(38, 0),
    source_count                 bigint,
    source_null_split_count      bigint,
    source_min_split             numeric(38, 0),
    source_max_split             numeric(38, 0),
    expected_partition_count     integer,
    success_partition_count      integer NOT NULL DEFAULT 0,
    failed_partition_count       integer NOT NULL DEFAULT 0,
    extracted_count              bigint NOT NULL DEFAULT 0,
    staging_count                bigint,
    target_count                 bigint,
    hdfs_run_path                varchar(1000),
    stage_table_name             varchar(255),
    publish_token                uuid,
    retry_of_run_id              uuid REFERENCES nifi_ops.load_run(run_id),
    version_no                   integer NOT NULL DEFAULT 0,
    parameters                   jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at                   timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at                 timestamptz NOT NULL DEFAULT clock_timestamp(),
    extract_completed_at         timestamptz,
    publish_started_at           timestamptz,
    published_at                 timestamptz,
    completed_at                 timestamptz,
    error_stage                  varchar(80),
    error_code                   varchar(100),
    error_message                varchar(2000),
    CONSTRAINT ck_load_run_status CHECK (status IN (
        'CREATED', 'EXTRACTING',
        'EXTRACTED_VALIDATED', 'STAGE_VALIDATING', 'STAGING_VALIDATED',
        'PUBLISHING', 'PUBLISHED', 'SUCCESS',
        'FAILED_MANIFEST', 'FAILED_EXTRACT',
        'FAILED_STAGE_VALIDATION', 'FAILED_PUBLISH',
        'PUBLISH_UNKNOWN', 'FAILED_TARGET_VALIDATION',
        'FAILED_SNAPSHOT_EXPIRED', 'TIMED_OUT'
    )),
    CONSTRAINT ck_load_run_counts CHECK (
        COALESCE(source_count, 0) >= 0
        AND COALESCE(expected_partition_count, 0) >= 0
        AND success_partition_count >= 0
        AND failed_partition_count >= 0
        AND extracted_count >= 0
    )
);

-- 동일 Job과 업무키에는 활성 run 하나만 허용한다.
CREATE UNIQUE INDEX uq_load_run_active
    ON nifi_ops.load_run (job_key, business_key)
    WHERE status IN (
        'CREATED', 'EXTRACTING',
        'EXTRACTED_VALIDATED', 'STAGE_VALIDATING', 'STAGING_VALIDATED',
        'PUBLISHING', 'PUBLISHED', 'PUBLISH_UNKNOWN'
    );

CREATE INDEX ix_load_run_status_heartbeat
    ON nifi_ops.load_run (status, heartbeat_at);

CREATE INDEX ix_load_run_job_started
    ON nifi_ops.load_run (job_key, started_at DESC);
```

Partial unique index가 활성 실행 lock 역할을 한다. 최종 상태로 변경되면 같은 `job_key + business_key`의 새 run을 생성할 수 있다. `POST /runs`가 이 index 위반(SQLSTATE `23505`)을 409 `DUPLICATE_ACTIVE_RUN`으로 응답한다.

`STAGE_VALIDATING`은 검증 flow가 시작됐음을 나타낸다. API는 `EXTRACTED_VALIDATED → STAGE_VALIDATING` CAS에 성공한 호출에만 검증 시작을 허락해, outbox가 같은 run을 두 번 전달해도 검증이 한 번만 실행되게 한다.

#### Partition Manifest

```sql
CREATE TABLE nifi_ops.load_partition (
    run_id                  uuid NOT NULL
                            REFERENCES nifi_ops.load_run(run_id) ON DELETE RESTRICT,
    partition_id            varchar(40) NOT NULL,
    lower_bound             numeric(38, 0),
    upper_bound             numeric(38, 0),
    upper_inclusive         boolean NOT NULL DEFAULT false,
    is_null_partition       boolean NOT NULL DEFAULT false,
    status                  varchar(20) NOT NULL DEFAULT 'PENDING',
    expected_row_count      bigint NOT NULL,
    actual_row_count        bigint,
    fragment_count          integer,
    file_count              integer,
    byte_count              bigint,
    attempt_count           integer NOT NULL DEFAULT 0,
    claim_token             uuid,
    worker_node             varchar(200),
    started_at              timestamptz,
    heartbeat_at            timestamptz,
    completed_at            timestamptz,
    error_code              varchar(100),
    error_message           varchar(2000),
    PRIMARY KEY (run_id, partition_id),
    CONSTRAINT ck_load_partition_status CHECK (status IN (
        'PENDING', 'RUNNING', 'RETRY', 'SUCCESS', 'FAILED', 'TIMED_OUT'
    )),
    CONSTRAINT ck_load_partition_counts CHECK (
        expected_row_count >= 0
        AND COALESCE(actual_row_count, 0) >= 0
        AND COALESCE(fragment_count, 0) >= 0
        AND COALESCE(file_count, 0) >= 0
        AND COALESCE(byte_count, 0) >= 0
        AND attempt_count >= 0
    ),
    CONSTRAINT ck_load_partition_bounds CHECK (
        is_null_partition
        OR (lower_bound IS NOT NULL AND upper_bound IS NOT NULL
            AND lower_bound <= upper_bound)
    )
);

CREATE INDEX ix_load_partition_status
    ON nifi_ops.load_partition (run_id, status);

CREATE INDEX ix_load_partition_recovery
    ON nifi_ops.load_partition (status, heartbeat_at)
    WHERE status IN ('RUNNING', 'RETRY');
```

#### HDFS File Manifest

```sql
CREATE TABLE nifi_ops.load_file (
    run_id                  uuid NOT NULL,
    partition_id            varchar(40) NOT NULL,
    chunk_index             integer NOT NULL,
    fragment_identifier     varchar(100),
    fragment_count          integer NOT NULL,
    hdfs_path               varchar(1500) NOT NULL,
    record_count            bigint NOT NULL,
    byte_count              bigint,
    checksum                varchar(128),
    status                  varchar(20) NOT NULL DEFAULT 'WRITTEN',
    written_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, partition_id, chunk_index),
    FOREIGN KEY (run_id, partition_id)
        REFERENCES nifi_ops.load_partition(run_id, partition_id)
        ON DELETE RESTRICT,
    CONSTRAINT uq_load_file_path UNIQUE (hdfs_path),
    CONSTRAINT ck_load_file_status CHECK (status IN ('WRITTEN', 'VERIFIED', 'FAILED')),
    CONSTRAINT ck_load_file_counts CHECK (
        chunk_index >= 0 AND fragment_count > 0
        AND record_count >= 0 AND COALESCE(byte_count, 0) >= 0
    )
);

CREATE INDEX ix_load_file_partition_status
    ON nifi_ops.load_file (run_id, partition_id, status);
```

API `POST .../chunks`가 run과 partition 행을 잠근 뒤 다음 UPSERT를 실행한다. 동일 chunk 재시도는 행을 추가하지 않고 결과를 갱신한다. 파티션이 이미 `SUCCESS`인데 값이 다른 보고가 오면 API가 409 `CHUNK_CONFLICT`로 거부하고 트랜잭션을 rollback한다.

```sql
INSERT INTO nifi_ops.load_file (
    run_id, partition_id, chunk_index,
    fragment_identifier, fragment_count,
    hdfs_path, record_count, byte_count, status
) VALUES (
    CAST(:run_id AS uuid), :partition_id, :chunk_index,
    :fragment_identifier, :chunk_count,
    :hdfs_path, :record_count, :byte_count, 'WRITTEN'
)
ON CONFLICT (run_id, partition_id, chunk_index)
DO UPDATE SET
    fragment_identifier = EXCLUDED.fragment_identifier,
    fragment_count      = EXCLUDED.fragment_count,
    hdfs_path           = EXCLUDED.hdfs_path,
    record_count        = EXCLUDED.record_count,
    byte_count          = EXCLUDED.byte_count,
    status              = 'WRITTEN',
    updated_at          = clock_timestamp();
```

#### Validation 결과

```sql
CREATE TABLE nifi_ops.load_validation (
    validation_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 uuid NOT NULL
                           REFERENCES nifi_ops.load_run(run_id) ON DELETE RESTRICT,
    stage                  varchar(30) NOT NULL,
    metric_name            varchar(150) NOT NULL,
    expected_value         text,
    actual_value           text,
    tolerance              text,
    result                 varchar(10) NOT NULL,
    query_version          varchar(50) NOT NULL,
    details                jsonb NOT NULL DEFAULT '{}'::jsonb,
    measured_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_load_validation_stage CHECK (stage IN (
        'SOURCE', 'PARTITION', 'HDFS', 'STAGING', 'TARGET'
    )),
    CONSTRAINT ck_load_validation_result CHECK (result IN ('PASS', 'FAIL', 'WARN'))
);

CREATE INDEX ix_load_validation_run_stage
    ON nifi_ops.load_validation (run_id, stage, result);

CREATE UNIQUE INDEX uq_load_validation_metric
    ON nifi_ops.load_validation (run_id, stage, metric_name, query_version);
```

`expected_value`와 `actual_value`는 count뿐 아니라 hash 및 문자열 지표도 저장할 수 있도록 `text`로 둔다. 숫자 비교와 허용 오차 판정은 검증 SQL에서 수행하고 결과를 함께 저장한다.

#### Dispatch Outbox

run 완료 CAS와 같은 트랜잭션에서 검증 flow 호출을 예약하는 outbox다. API worker의 dispatcher가 commit 후 NiFi로 전달한다(API 설계 4장).

```sql
CREATE TABLE nifi_ops.load_dispatch (
    dispatch_id         uuid PRIMARY KEY,
    run_id              uuid NOT NULL
                        REFERENCES nifi_ops.load_run(run_id) ON DELETE RESTRICT,
    dispatch_type       varchar(30) NOT NULL,
    partition_id        varchar(40),
    status              varchar(20) NOT NULL DEFAULT 'PENDING',
    attempt_count       integer NOT NULL DEFAULT 0,
    next_attempt_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_http_status    integer,
    last_error          varchar(2000),
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at             timestamptz,
    acked_at            timestamptz,
    CONSTRAINT ck_load_dispatch_type CHECK (dispatch_type IN (
        'VALIDATE_RUN', 'REISSUE_PARTITION'
    )),
    CONSTRAINT ck_load_dispatch_status CHECK (status IN (
        'PENDING', 'SENT', 'ACKED', 'DEAD'
    )),
    CONSTRAINT ck_load_dispatch_partition CHECK (
        (dispatch_type = 'VALIDATE_RUN' AND partition_id IS NULL)
        OR (dispatch_type = 'REISSUE_PARTITION' AND partition_id IS NOT NULL)
    )
);

-- run당 검증 호출은 하나만 존재한다.
CREATE UNIQUE INDEX uq_load_dispatch_validate
    ON nifi_ops.load_dispatch (run_id)
    WHERE dispatch_type = 'VALIDATE_RUN';

CREATE INDEX ix_load_dispatch_due
    ON nifi_ops.load_dispatch (status, next_attempt_at)
    WHERE status IN ('PENDING', 'SENT');
```

`PENDING`은 전송 대기, `SENT`는 NiFi가 2xx로 수신함, `ACKED`는 검증 flow가 `/validation/start`로 실제 시작을 알림, `DEAD`는 최대 시도 초과다. `uq_load_dispatch_validate`는 run 행 잠금과 CAS가 깨지더라도 검증 호출이 두 번 예약되지 않게 하는 마지막 방어선이다.

#### PostgreSQL 로그 테이블

```sql
CREATE TABLE nifi_ops.load_event (
    event_id               uuid PRIMARY KEY,
    event_time             timestamptz NOT NULL DEFAULT clock_timestamp(),
    event_level            varchar(10) NOT NULL,
    event_name             varchar(80) NOT NULL,
    run_id                 uuid,
    job_key                varchar(200),
    business_key           varchar(200),
    partition_id           varchar(40),
    chunk_index            integer,
    process_group          varchar(100),
    processor_name         varchar(150),
    processor_id           varchar(100),
    node_id                varchar(200),
    attempt_no             integer,
    row_count              bigint,
    byte_count             bigint,
    duration_ms            bigint,
    error_class            varchar(40),
    error_code             varchar(100),
    message                varchar(2000),
    details                jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT ck_load_event_level CHECK (event_level IN (
        'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'
    )),
    CONSTRAINT ck_load_event_values CHECK (
        COALESCE(attempt_no, 0) >= 0
        AND COALESCE(row_count, 0) >= 0
        AND COALESCE(byte_count, 0) >= 0
        AND COALESCE(duration_ms, 0) >= 0
    )
);

-- 실행 추적과 오류 검색을 위한 B-tree index
CREATE INDEX ix_load_event_run_time
    ON nifi_ops.load_event (run_id, event_time DESC);

CREATE INDEX ix_load_event_job_time
    ON nifi_ops.load_event (job_key, event_time DESC);

CREATE INDEX ix_load_event_error
    ON nifi_ops.load_event (event_time DESC, event_name)
    WHERE event_level IN ('WARN', 'ERROR');

-- 대용량 시계열 검색과 보존 삭제 지원
CREATE INDEX ix_load_event_time_brin
    ON nifi_ops.load_event USING brin (event_time);
```

`load_event.run_id`에는 의도적으로 Foreign Key를 두지 않는다. Run 정리 또는 비정상 초기화 상황에서도 운영 로그가 독립적으로 남고, 이벤트 기록 실패가 제어 트랜잭션을 방해하지 않게 하기 위해서다.

로그 보존 예시는 다음과 같다. 운영에서는 PostgreSQL scheduler 또는 외부 운영 Job으로 실행한다.

```sql
DELETE FROM nifi_ops.load_event
 WHERE event_time < clock_timestamp() - interval '90 days';

VACUUM (ANALYZE) nifi_ops.load_event;
```

이벤트량이 일 수백만 건 이상이면 `event_time` 월 단위 partition table로 전환하고 다음 달 partition을 미리 생성한다.

#### Claim과 상태 전이

파티션 claim, publish claim, 모든 상태 전이는 API가 조건부 `UPDATE ... RETURNING`으로 실행한다. 가이드 초안의 `claim_partition`/`claim_publish` PostgreSQL 함수는 만들지 않는다. 같은 claim token의 재요청을 성공으로 돌려주는 멱등 규칙이 필요하기 때문이다. 함수 버전은 claim 응답을 잃고 재요청하면 `false`를 반환해 Worker가 스스로 종료하고, 파티션이 sweeper까지 멈춘다.

파티션 claim은 다음 SQL로 구현한다.

```sql
UPDATE nifi_ops.load_partition p
   SET status = 'RUNNING',
       claim_token = CAST(:claim_token AS uuid),
       worker_node = :worker_node,
       attempt_count = p.attempt_count
                       + CASE WHEN p.claim_token = CAST(:claim_token AS uuid) THEN 0 ELSE 1 END,
       started_at = COALESCE(p.started_at, clock_timestamp()),
       heartbeat_at = clock_timestamp(),
       error_code = NULL,
       error_message = NULL
  FROM nifi_ops.load_run r
 WHERE p.run_id = CAST(:run_id AS uuid)
   AND p.partition_id = :partition_id
   AND r.run_id = p.run_id
   AND r.status = 'EXTRACTING'
   AND (p.status IN ('PENDING', 'RETRY')
        OR (p.status = 'RUNNING' AND p.claim_token = CAST(:claim_token AS uuid)))
RETURNING p.status, p.attempt_count;
```

반환 행이 있으면 `claimed=true`다. API는 이 문장 앞에 `load_run` 행을 `FOR UPDATE`로 잠근다. 모든 상태 변경에서 잠금 순서(`load_run` → `load_partition` → `load_file` → `load_dispatch`)를 고정해 deadlock을 피한다(API 설계 9.5).

#### 권한 예시

```sql
-- 역할은 DBA가 사전에 생성한다.
-- Load Control API 런타임 계정: 원장과 outbox 쓰기
GRANT USAGE ON SCHEMA nifi_ops TO load_control_api;
GRANT SELECT, INSERT, UPDATE ON
    nifi_ops.load_run, nifi_ops.load_partition, nifi_ops.load_file,
    nifi_ops.load_validation, nifi_ops.load_dispatch
    TO load_control_api;
GRANT INSERT, SELECT ON nifi_ops.load_event TO load_control_api;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA nifi_ops TO load_control_api;

-- NiFi 런타임 계정: 관측 이벤트만 기록
GRANT USAGE ON SCHEMA nifi_ops TO nifi_runtime;
GRANT INSERT ON nifi_ops.load_event TO nifi_runtime;
```

NiFi 계정이 원장을 직접 바꿀 수 없어야 "원장 writer는 API 하나"라는 전제가 운영 중에도 유지된다. 두 런타임 계정 모두 `DELETE`, `TRUNCATE`, `DROP` 권한을 받지 않는다. 스키마 변경은 migration 전용 계정이 하고, 보존 삭제는 별도 DBA 역할로 수행한다.

---

## 5. 공통 FlowFile Attributes

| Attribute | 생성 위치 | 예시/의미 |
|---|---|---|
| `load.run.id` | API `POST /runs` 응답 | UUID, 실행 불변 키 |
| `load.job.key` | Trigger | `ORACLE_INSP_DTL_DAILY` |
| `load.business.key` | Trigger | `2026-09-28` |
| `load.snapshot.scn` | Snapshot query | 숫자 문자열 |
| `load.source.count` | Source metrics | 전체 source count |
| `load.partition.planned` | Coordinator | `#{PARTITION.COUNT}`(+NULL 파티션), manifest 요청의 `plannedPartitionCount` |
| `load.hdfs.path` | API `POST /runs` 응답 | run 전용 root |
| `load.stage.table` | API `POST /runs` 응답 | 안전한 run suffix 포함 |
| `load.dispatch.id` | 검증 호출 수신 | outbox dispatch UUID, `/validation/start`에 전달 |
| `partition.id` | Manifest 응답 split | `0003` 또는 `NULL` |
| `partition.lower` | Manifest 응답 split | 포함 하한 |
| `partition.upper` | Manifest 응답 split | 상한 |
| `partition.upper.inclusive` | Manifest 응답 split | `true/false` |
| `partition.is.null` | Manifest 응답 split | NULL 전용 파티션 여부 |
| `partition.expected.rows` | Manifest 응답 split | 동일 SCN 예상 건수 |
| `partition.claim.token` | Worker | 중복 worker 방지 UUID |
| `partition.retry.count` | RetryFlowFile | 재시도 횟수 |
| `chunk.index` | Extract output | `${fragment.index}` |
| `chunk.count` | Extract output | `${fragment.count}`, API 보고의 `chunkCount` |
| `chunk.record.count` | Extract output | `${record.count}` |
| `api.response` | `InvokeHTTP` | 2xx 응답 본문(`Response Body Attribute Name`), EL `jsonPath()`로 분기 |
| `invokehttp.status.code` | `InvokeHTTP` | HTTP 상태 코드, 409/422 구분 |
| `invokehttp.response.body` | `InvokeHTTP` | 2xx가 아닐 때의 응답 본문, `$.code`로 오류 코드 확인 |
| `event.name` | 각 단계 | 구조화 이벤트 이름 |
| `error.stage` | 오류 경로 | `ORACLE_EXTRACT`, `HDFS_WRITE` 등 |
| `error.class` | 오류 경로 | `TRANSIENT`, `NON_RETRYABLE`, `VALIDATION` |
| `error.message` | 오류 경로 | 비밀값을 제거한 메시지 |

SCN, partition bound, count는 숫자 정규식으로 검증한 뒤 SQL에 사용한다. table/column/where 문자열을 외부 FlowFile에서 받지 않는다.

Parameter 참조는 Expression Language의 문자열 리터럴 안에서 치환되지 않는다. 예를 들어 `${record.count:equals('#{PARTITION.COUNT}')}`는 문자 그대로의 `#{PARTITION.COUNT}`와 비교하므로 항상 false가 된다(NiFi 2.4.0 PoC에서 재현). Parameter 값과 비교할 때는 먼저 `UpdateAttribute`에서 `load.partition.planned=#{PARTITION.COUNT}`처럼 attribute로 옮긴 뒤 `${record.count:equals(${load.partition.planned})}`로 비교한다.

---

## 6. PG-00 Trigger

### 6.1 Processor 흐름

```mermaid
flowchart LR
    A[00_Generate_Schedule<br/>GenerateFlowFile] --> B[01_Set_Trigger_Attributes<br/>UpdateAttribute]
    B --> C{02_Validate_Trigger<br/>RouteOnAttribute}
    C -->|valid| D[Output: start-run]
    C -->|invalid| E[PG-90 Fatal Error]
```

### 6.2 주요 Processor 설정

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 00 | `GenerateFlowFile` | Primary, 1, CRON | Custom Text=`{}`, Unique FlowFiles=true | success→01 |
| 01 | `UpdateAttribute` | Primary, 1, input driven | `load.job.key=#{JOB.KEY}`, `load.business.key=${now():format('yyyy-MM-dd','Asia/Seoul')}`, `load.trigger.type=SCHEDULE` | success→02 |
| 02 | `RouteOnAttribute` | Primary, 1 | 업무키 정규식, Job key와 필수 Parameter 존재 여부 검증 | valid→PG-10, unmatched→PG-90 Fatal |

외부에서 업무일자를 전달받는 경우 `HandleHttpRequest` 등을 직접 worker에 연결하지 않고 인증된 상위 orchestration flow가 이 Process Group의 Input Port를 호출하도록 한다.

---

## 7. PG-10 Run Coordinator

### 7.1 Processor 흐름

```mermaid
flowchart TD
    I[Input: start-run] --> A[10_Build_Run_Request<br/>AttributesToJSON]
    A --> B[11_Create_Run<br/>InvokeHTTP POST /runs]
    B -->|Original 2xx| BA[11A_Set_Run_Attrs<br/>UpdateAttribute]
    B -->|No Retry| DUP{11D_Route_Status<br/>RouteOnAttribute}
    DUP -->|409| DW[PG-90 DUPLICATE_ACTIVE_RUN WARN]
    DUP -->|기타| X0[PG-90 Fatal Error]
    B -->|Retry or Failure| BR[11R_RetryFlowFile]
    BR -->|retry| B
    BR -->|exceeded| X0
    BA --> C[12_Query_Current_SCN<br/>ExecuteSQLRecord]
    C --> D[13_Extract_SCN<br/>EvaluateJsonPath]
    D --> E{14_Validate_SCN<br/>RouteOnAttribute}
    E -->|valid| F[15_Query_Source_Metrics<br/>ExecuteSQLRecord]
    E -->|invalid| X[25_Report_Run_Fail<br/>InvokeHTTP POST /runs/id/fail]
    F --> G[16_Extract_Source_Metrics<br/>EvaluateJsonPath]
    G --> H{17_Source_Precheck<br/>RouteOnAttribute}
    H -->|empty blocked or null invalid| X
    H -->|valid| K[19_Query_Partition_Manifest<br/>ExecuteSQLRecord]
    K --> J[20_Build_Manifest_Request<br/>JoltTransformJSON]
    J --> M[21_Register_Manifest<br/>InvokeHTTP POST /runs/id/manifest]
    M -->|Response 2xx| L[23_Split_Dispatch_Partitions<br/>SplitJson]
    M -->|No Retry 422| XM[PG-90 FAILED_MANIFEST<br/>API가 상태 기록 완료]
    M -->|Retry or Failure| MR[21R_RetryFlowFile]
    MR -->|retry| M
    MR -->|exceeded| X
    L -->|split| P[24_Extract_Partition_Attrs<br/>EvaluateJsonPath]
    P --> OUT[Output: partitions]
    X --> XE[PG-90 RUN_FAILED]
```

가이드 초안과 달라진 점은 다음과 같다.

- run lock INSERT, snapshot 저장, manifest 불변식 검사, 파티션 행별 INSERT, 0건 파티션 처리, run-control 생성이 없다. 모두 API 두 번 호출(`POST /runs`, `POST /manifest`)로 대체된다.
- 파티션 행별 `PutSQL`(Fragmented=true)이 없어지므로, PoC에서 재현된 fragment livelock이 구조적으로 생기지 않는다.
- run을 만든 뒤 SCN이나 source 검증에서 실패하면 반드시 25에서 `POST /runs/{id}/fail`을 호출한다. 호출하지 않으면 run이 `CREATED`로 남아 active run lock을 잡고, API sweeper의 timeout까지 같은 업무키를 다시 실행할 수 없다.

### 7.2 주요 Processor 설정

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 10 | `UpdateAttribute` + `AttributesToJSON` | Primary, 1 | `jobKey=${load.job.key}`, `businessKey=${load.business.key}`, `hdfsRoot=#{HDFS.STAGE.ROOT}`, `stageTablePrefix=#{HIVE.STAGE.TABLE.PREFIX}`, `allowEmptySource=#{ALLOW.EMPTY.SOURCE}` 설정 후 Destination=flowfile-content, 위 다섯 attribute만 포함 | success→11 |
| 11 | `InvokeHTTP` | Primary, 1 | 9.2 공통 설정, URL=`#{CONTROL.API.URL}/runs`, `Response Body Attribute Name=api.response` | Original→11A, No Retry→11D, Retry/Failure→11R, Response→auto-terminate |
| 11A | `UpdateAttribute` | Primary, 1 | `load.run.id=${api.response:jsonPath('$.runId')}`, `load.hdfs.path=${api.response:jsonPath('$.hdfsRunPath')}`, `load.stage.table=${api.response:jsonPath('$.stageTable')}` | success→12 |
| 11D | `RouteOnAttribute` | Primary, 1 | `duplicate=${invokehttp.status.code:equals('409')}` | duplicate→PG-90 WARN 후 종료, unmatched→PG-90 Fatal |
| 11R, 21R | `RetryFlowFile` | Primary, 1 | Maximum=`#{CONTROL.API.RETRY.MAX}`, Penalize=true | retry→재호출, exceeded→PG-90 또는 25 |
| 12 | `ExecuteSQLRecord` | Primary, 1 | `CS_DBCP_ORACLE`, current SCN SQL, `CS_JSON_WRITER_ARRAY`, Max Rows=0 | success→13, failure→25 |
| 13 | `EvaluateJsonPath` | Primary, 1 | `load.snapshot.scn=$[0].SNAPSHOT_SCN`, Destination=attribute | matched→14, failure/unmatched→25 |
| 14 | `RouteOnAttribute` | Primary, 1 | `${load.snapshot.scn:matches('^[0-9]+$')}` | valid→15, unmatched→25 |
| 15 | `ExecuteSQLRecord` | Primary, 1 | `CS_DBCP_ORACLE`, source metrics SQL, JSON array writer, Query Timeout | success→16, failure→25 |
| 16 | `EvaluateJsonPath` | Primary, 1 | `load.source.count`, `load.source.min`, `load.source.max`, `load.source.null.count`와 DQ 값을 attribute로 추출 | matched→17, failure/unmatched→25 |
| 17 | `RouteOnAttribute` | Primary, 1 | empty source, NULL 정책, min/max 유효성 | valid→19, invalid→25 |
| 19 | `UpdateAttribute` + `ExecuteSQLRecord` | Primary, 1 | `load.partition.planned` 설정 후 동일 SCN manifest SQL(7.4), JSON array writer, Max Rows=0, Output Batch=0 | success→20, failure→25 |
| 20 | `JoltTransformJSON` | Primary, 1 | manifest 배열을 `partitions`로 감싸고 SCN·metric attribute를 상위 필드로 추가(아래 spec) | success→21, failure→25 |
| 21 | `InvokeHTTP` | Primary, 1 | 9.2 공통 설정, URL=`#{CONTROL.API.URL}/runs/${load.run.id}/manifest`. `Response Body Attribute Name`은 비워 응답을 content로 받음 | Response→23, No Retry→PG-90 `FAILED_MANIFEST`, Retry/Failure→21R, Original→auto-terminate |
| 23 | `SplitJson` | Primary, 1 | JsonPath Expression=`$.dispatchPartitions` | split→24, original→terminate, failure→PG-90 |
| 24 | `EvaluateJsonPath` | Primary, 1 | `partition.id=$.partitionId`, `partition.lower=$.lowerBound`, `partition.upper=$.upperBound`, `partition.upper.inclusive=$.upperInclusive`, `partition.is.null=$.isNullPartition`, `partition.expected.rows=$.expectedRowCount` | matched→Worker Output Port, failure/unmatched→PG-90 |
| 25 | `AttributesToJSON` + `InvokeHTTP` | Primary, 1 | 본문 `expectedStatus=CREATED`, `failStatus=FAILED_MANIFEST`, `errorStage`, `errorCode`, `message`. URL=`#{CONTROL.API.URL}/runs/${load.run.id}/fail` | Original→PG-90 `RUN_FAILED`, Retry/Failure→재시도 후 PG-90 ERROR |

20 `JoltTransformJSON` spec 예시(Jolt Specification은 FlowFile attribute EL을 지원한다):

```json
[
  { "operation": "shift",
    "spec": { "*": {
      "PARTITION_ID": "partitions[&1].partitionId",
      "LOWER_BOUND": "partitions[&1].lowerBound",
      "UPPER_BOUND": "partitions[&1].upperBound",
      "UPPER_INCLUSIVE": "partitions[&1].upperInclusive",
      "IS_NULL_PARTITION": "partitions[&1].isNullPartition",
      "EXPECTED_ROW_COUNT": "partitions[&1].expectedRowCount" } } },
  { "operation": "default",
    "spec": {
      "snapshotScn": "${load.snapshot.scn}",
      "sourceCount": "${load.source.count}",
      "sourceNullSplitCount": "${load.source.null.count}",
      "sourceMinSplit": "${load.source.min}",
      "sourceMaxSplit": "${load.source.max}",
      "plannedPartitionCount": "${load.partition.planned}" } }
]
```

`load.partition.planned`는 19 앞의 `UpdateAttribute`에서 `#{PARTITION.COUNT}`로 설정한다. `SPLIT.NULL.POLICY=SEPARATE`이면 Parameter를 먼저 attribute로 옮긴 뒤 `${load.partition.planned:plus(1)}`로 1을 더한다(5장).

`InvokeHTTP` 응답 처리 규칙: `Response Body Attribute Name`을 설정하면 2xx 응답 본문은 **Original** relationship의 FlowFile에 attribute로 붙는다. 응답 본문을 content로 받아야 하는 21만 Response relationship을 쓴다. 2xx가 아니면 요청 FlowFile이 Retry(5xx)나 No Retry(4xx)로 가며 `invokehttp.status.code`, `invokehttp.response.body` attribute가 붙는다.

중복 실행은 API가 active run unique index 위반을 409 `DUPLICATE_ACTIVE_RUN`으로 응답해 구분한다. NiFi는 SQLState를 해석하지 않는다. 연결 장애와 5xx만 11R에서 제한 재시도한다.

### 7.3 Source metric SQL

`snapshot_scn`은 숫자 검증을 마친 시스템 생성 값이다. 업무값은 bind parameter를 사용한다.

```sql
SELECT COUNT(*) AS SOURCE_COUNT,
       TO_CHAR(MIN(INSP_DTL_SEQ)) AS MIN_SEQ,
       TO_CHAR(MAX(INSP_DTL_SEQ)) AS MAX_SEQ,
       SUM(CASE WHEN INSP_DTL_SEQ IS NULL THEN 1 ELSE 0 END) AS NULL_SEQ_COUNT,
       COUNT(DISTINCT INSP_DTL_SEQ) AS DISTINCT_SEQ_COUNT
  FROM APP.INSP_DTL AS OF SCN ${load.snapshot.scn}
 WHERE BASE_DT = ?
```

```text
sql.args.1.type  = #{SRC.BUSINESS.KEY.TYPE}
sql.args.1.value = ${load.business.key}
```

실제 구현에서는 `APP.INSP_DTL`, 컬럼, WHERE 부분을 Job Parameter로 치환한다.

### 7.4 Manifest SQL 원칙

Oracle `CONNECT BY LEVEL <= #{PARTITION.COUNT}` 또는 관리 DB에서 경계를 생성한다. 모든 파티션은 다음 규칙을 만족해야 한다.

```text
0..N-2 : split_column >= lower AND split_column < upper
N-1    : split_column >= lower AND split_column <= upper
NULL    : split_column IS NULL, SPLIT.NULL.POLICY=SEPARATE일 때만
```

각 range의 `EXPECTED_ROW_COUNT`를 동일 SCN에서 계산한다. 결과 컬럼은 `PARTITION_ID`, `LOWER_BOUND`, `UPPER_BOUND`, `UPPER_INCLUSIVE`, `IS_NULL_PARTITION`, `EXPECTED_ROW_COUNT`로 고정한다(20의 Jolt spec과 일치). `LOWER_BOUND`, `UPPER_BOUND`, SCN은 `TO_CHAR(...)`로 문자열로 반환한다. `NUMBER(38)` 값이 JSON 숫자로 바뀌며 정밀도를 잃는 것을 막기 위해서다.

불변식은 API가 `POST /manifest`에서 한 트랜잭션으로 검증한다(API 설계 3.4).

```text
SUM(expected_row_count) = source_count
AND 파티션 수 = plannedPartitionCount
AND lower(i+1) = upper(i), 마지막 파티션만 upper_inclusive = true
AND NULL 파티션은 SPLIT.NULL.POLICY=SEPARATE일 때만 존재
```

불일치면 API가 run을 `FAILED_MANIFEST`로 기록하고 422를 반환한다. NiFi는 Worker를 시작하지 않고 PG-90에 이벤트만 남긴다. `expectedRowCount=0`인 파티션은 API가 바로 `SUCCESS`(actual=0)로 기록하고 `dispatchPartitions`에서 뺀다. 그래서 0건 파티션은 Worker로 가지 않는다.

---

## 8. PG-20 Oracle Extract Workers

### 8.1 Processor 흐름

```mermaid
flowchart TD
    I[Input: partition<br/>Round Robin] --> A[20_Set_Claim_Request<br/>UpdateAttribute + AttributesToJSON]
    A --> B[21_Claim_Partition<br/>InvokeHTTP POST claim]
    B -->|Retry or Failure| BR[21R_RetryFlowFile]
    BR -->|retry| B
    BR -->|exceeded| EV0[PG-90 ERROR<br/>sweeper가 정리]
    B -->|No Retry| EV0
    B -->|Original 2xx| D{23_Is_Owner<br/>RouteOnAttribute}
    D -->|no| DROP[Terminate duplicate or failed run]
    D -->|yes| E[24_Build_Oracle_SQL<br/>ReplaceText]
    E --> F[25_Execute_Partition_Query<br/>ExecuteSQLRecord]
    F -->|failure| ER{27_Classify_DB_Error}
    ER -->|transient| RETRY[28_RetryFlowFile]
    RETRY -->|retry| E
    RETRY -->|exceeded| FAIL[29_Report_Partition_Fail<br/>InvokeHTTP POST fail]
    ER -->|ORA-01555 or permanent| FAIL
    F -->|success| U[31_Set_Chunk_Attrs<br/>UpdateAttribute]
    U --> VAL[32_ValidateRecord<br/>ParquetReader and fixed schema]
    VAL -->|valid| H[33_PutHDFS]
    VAL -->|invalid or failure| FAIL
    H -->|failure| HR[35_Retry_HDFS]
    HR -->|retry| H
    HR -->|exceeded| FAIL
    H -->|success| J[34A_Build_Chunk_Report<br/>AttributesToJSON]
    J --> R[34B_Report_Chunk<br/>InvokeHTTP POST chunks]
    R -->|Original 2xx| LOG[34D_Log_Result<br/>RouteOnAttribute]
    LOG --> TERM[Terminate]
    R -->|No Retry 409/4xx| EV1[PG-90 WARN 또는 ERROR]
    R -->|Retry or Failure| RR[34C_RetryFlowFile]
    RR -->|retry| R
    RR -->|exceeded| EV2[PG-90 ERROR<br/>sweeper가 정리]
    FAIL --> EV3[PG-90 PARTITION_FAILED]
```

Worker에는 대기 단계가 없다. 각 chunk는 HDFS에 기록되고 API에 보고되면 끝난다. 파티션과 run의 완료는 API가 보고를 받을 때마다 판정한다(9장). 가이드 초안의 첫 fragment 분기(26), `DuplicateFlowFile`(29, 29A), partition-control FlowFile(30), file audit `PutSQL`(34), `Notify`(36)는 없다. API 보고에 `chunkCount=${fragment.count}`가 들어가므로 파티션 완료 판정에 별도 control FlowFile이 필요 없다.

### 8.2 주요 Processor 설정

PG-20의 Connection은 PG-10에서 들어오는 입력에만 Round Robin Load Balance를 적용한다. 각 Worker Processor는 All Nodes에서 동작하며 동시성은 Oracle pool 상한을 넘지 않게 한다.

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 20 | `UpdateAttribute` + `AttributesToJSON` | All Nodes, worker concurrency | `partition.claim.token=${UUID()}`, `claimToken=${partition.claim.token}`, `workerNode=${hostname(true)}`; Destination=flowfile-content, Attributes List=`claimToken,workerNode` | success→21 |
| 21 | `InvokeHTTP` | All Nodes, worker concurrency | 9.2 공통 설정, URL=`#{CONTROL.API.URL}/runs/${load.run.id}/partitions/${partition.id}/claim`, `Response Body Attribute Name=api.response` | Original→23, No Retry→PG-90, Retry/Failure→21R |
| 23 | `RouteOnAttribute` | All Nodes, worker concurrency | `owner=${api.response:jsonPath('$.claimed'):equals('true')}` | owner→24, unmatched→종료(DEBUG) |
| 24 | `ReplaceText` | All Nodes, worker concurrency | Replacement Strategy=Entire text, partition 유형별 Oracle SQL 생성 | success→25, failure→29 |
| 25 | `ExecuteSQLRecord` | All Nodes, worker concurrency | `CS_DBCP_ORACLE`, `CS_PARQUET_WRITER`, Fetch/Rows/Timeout은 8.4 표 참조 | success→31, failure→27 |
| 27 | `RouteOnAttribute` | All Nodes, worker concurrency | Oracle vendor code/SQLState로 transient, ORA-01555, permanent 분류 | transient→28, non-retryable→29 |
| 28 | `RetryFlowFile` | All Nodes, worker concurrency | Retry Attribute=`partition.retry.count`, Maximum=`#{PARTITION.RETRY.MAX}`, Penalize=true | retry→24, exceeded/failure→29 |
| 29 | `UpdateAttribute` + `AttributesToJSON` + `InvokeHTTP` | All Nodes, worker concurrency | 본문 `claimToken`, `errorStage`, `errorClass`, `errorCode`, `message`, `attempt`. URL=`.../partitions/${partition.id}/fail` | Original→PG-90 `PARTITION_FAILED`, Retry/Failure→제한 재시도 후 PG-90 ERROR |
| 31 | `UpdateAttribute` | All Nodes, worker concurrency | chunk index/count, 결정적 filename 설정(8.5) | success→32 |
| 32 | `ValidateRecord` | All Nodes, worker concurrency | Reader=`CS_PARQUET_READER`, Writer=`CS_PARQUET_WRITER`, validation schema 고정 | valid→33, invalid/failure→29 |
| 33 | `PutHDFS` | All Nodes, HDFS 부하 기준 | Hadoop config만 설정, Kerberos service 미설정, umask, replication, replace, Write and rename | success→34A, failure→35 |
| 34A | `AttributesToJSON` | All Nodes, worker concurrency | Destination=flowfile-content, chunk 보고 attribute 목록(8.5) | success→34B, failure→PG-90 |
| 34B | `InvokeHTTP` | All Nodes, worker concurrency | 9.2 공통 설정, URL=`.../partitions/${partition.id}/chunks`, `Response Body Attribute Name=api.response` | Original→34D, No Retry→PG-90, Retry/Failure→34C |
| 34C | `RetryFlowFile` | All Nodes, worker concurrency | Retry Attribute=`api.retry.count`, Maximum=`#{CONTROL.API.RETRY.MAX}`, Penalize=true | retry→34B, exceeded→PG-90 ERROR |
| 34D | `RouteOnAttribute` | All Nodes, worker concurrency | `${api.response:jsonPath('$.validationScheduled'):equals('true')}`이면 INFO `EXTRACT_VALIDATED` 로그 | 모두 종료 |
| 35 | `RetryFlowFile` | All Nodes, worker concurrency | HDFS 전용 retry attribute, 최대 횟수와 penalty 설정 | retry→33, exceeded/failure→29 |

20은 content를 claim 요청 JSON으로 바꾼다. 파티션 정보는 PG-10의 24에서 만든 `partition.*` attribute에 있고 24가 content를 Oracle SQL로 덮어쓰므로, manifest 레코드 content는 이후에 쓰지 않는다.

claim 응답을 잃고 21이 재시도하면 같은 `partition.claim.token`으로 다시 요청한다. API는 같은 token의 재요청에 `claimed=true`를 돌려주므로 Worker가 스스로 종료하지 않는다(4.1 "Claim과 상태 전이"). 이를 위해 token은 20에서 한 번만 만들고 재시도 루프에서 다시 만들지 않는다.

`claimed=false`는 다른 Worker가 이미 처리 중이거나 run이 실패 또는 종료된 경우다(`$.runStatus`로 구분). 둘 다 정상 경합이므로 DEBUG 로그만 남기고 종료한다.

### 8.3 Claim과 실패 보고

- claim은 API가 run 행을 잠근 뒤 조건부 UPDATE로 처리한다. run이 `EXTRACTING`이 아니면 `claimed=false`이므로 실패한 run의 대기 파티션은 Oracle 조회를 시작하지 않는다.
- 29의 실패 보고는 일시 오류 재시도(28, 35)를 다 쓴 뒤나 재시도 불가 오류일 때만 호출한다. API는 같은 트랜잭션에서 partition `FAILED`, run `FAILED_EXTRACT`로 바꾼다. `errorCode=ORA-01555`이면 `FAILED_SNAPSHOT_EXPIRED`로 바꾼다.
- 실패 후 같은 run의 다른 Worker는 Oracle 쿼리를 중단하지 않고 끝까지 실행한다. 이후 chunk 보고는 200(`runStatus=FAILED_*`)을 받고 종료하며, 파일은 실패 run의 격리 경로에만 남는다.
- 가이드 초안의 실패 시 Wait 해제 Notify는 필요 없다. 기다리는 FlowFile이 없기 때문이다.

### 8.4 Extract SQL 생성

중간 파티션:

```sql
SELECT #{SRC.COLUMNS}
  FROM #{SRC.OWNER}.#{SRC.TABLE} AS OF SCN ${load.snapshot.scn}
 WHERE #{SRC.BASE.WHERE}
   AND #{SRC.SPLIT.COLUMN} >= ?
   AND #{SRC.SPLIT.COLUMN} < ?
```

마지막 파티션은 `< ?` 대신 `<= ?`, NULL 파티션은 `IS NULL`을 사용한다.

```text
sql.args.1 = business key
sql.args.2 = lower bound, NUMERIC(2)
sql.args.3 = upper bound, NUMERIC(2)
```

24 `ReplaceText`는 Entire text를 위 SQL로 치환한다. 25 `ExecuteSQLRecord` 설정은 다음과 같다.

| Property | 값 |
|---|---|
| Database Connection Pooling Service | `CS_DBCP_ORACLE` |
| SQL select query | 빈 값, FlowFile content 사용 |
| Record Writer | `CS_PARQUET_WRITER` |
| Fetch Size | `#{EXTRACT.FETCH.SIZE}` |
| Max Rows Per FlowFile | `#{EXTRACT.ROWS.PER.FILE}` |
| Output Batch Size | `0` |
| Max Wait Time | `#{EXTRACT.QUERY.TIMEOUT}` |
| Use Avro Logical Types | `true` (DATE/TIMESTAMP/DECIMAL 타입 유지, 4장 참조) |
| Concurrent Tasks | `WORKER.CONCURRENT.TASKS` 기준값을 정수로 입력 (Parameter 참조 불가) |
| Execution | All Nodes |

`Output Batch Size=0`이어야 한 ResultSet의 `fragment.count`, `fragment.index`, `fragment.identifier`가 완전하게 생성된다. 파티션 크기가 너무 커 session/repository 압력이 생기면 Output Batch를 켜기보다 논리 파티션 수를 늘린다.

### 8.5 Chunk 기록과 보고

31에서 다음 속성을 만든다.

```text
chunk.index        = ${fragment.index:padLeft(6,'0')}
chunk.count        = ${fragment.count}
chunk.record.count = ${record.count}
filename           = part-${partition.id}-${chunk.index}.parquet
```

하위 디렉터리를 만들지 않으므로 별도 part path attribute는 두지 않는다(1장 경로 원칙 참조).

33 `PutHDFS`:

| Property | 값 |
|---|---|
| Hadoop Configuration Resources | `#{HADOOP.CONF.FILES}` |
| Kerberos User Service | 설정하지 않음 |
| Directory | `${load.hdfs.path}` (run root, 하위 디렉터리 없음) |
| Conflict Resolution Strategy | `replace` |
| Writing Strategy | `Write and rename` |
| Permissions umask | `#{HDFS.PERMISSIONS.UMASK}` |
| Replication | `#{HDFS.REPLICATION}` 또는 공란으로 HDFS 기본값 사용 |
| Concurrent Tasks | Worker 동시성과 HDFS 부하에 맞춰 설정 |

HDFS에는 Kerberos가 적용되지 않았으므로 `Kerberos User Service`, principal, keytab을 구성하지 않는다. NiFi 프로세스를 실행하는 OS 사용자가 HDFS client의 effective user가 되므로 staging root와 하위 경로에 필요한 POSIX 권한 또는 ACL을 사전에 부여한다. `core-site.xml`의 인증 방식과 `fs.defaultFS`가 실제 HDFS 환경을 가리키는지 확인한다. `replace`는 run 전용 경로와 결정적 파일명인 경우에만 허용한다.

34A `AttributesToJSON`은 PutHDFS **성공 이후에** content를 보고 JSON으로 바꾼다. 그 전에 `InvokeHTTP`를 호출하면 Parquet content가 요청 본문으로 전송된다. 앞단 `UpdateAttribute`에서 API 필드 이름으로 attribute를 만든 뒤 그 목록만 포함한다.

```text
claimToken         = ${partition.claim.token}
chunkIndex         = ${fragment.index}
chunkCount         = ${fragment.count}
fragmentIdentifier = ${fragment.identifier}
hdfsPath           = ${absolute.hdfs.path}/${filename}
recordCount        = ${record.count}
byteCount          = ${fileSize}
```

`chunkIndex`는 0부터 시작하는 숫자(`fragment.index`)다. 파일명에 쓰는 0 채움 값(`chunk.index`)과 구분한다. `AttributesToJSON`은 모든 값을 문자열로 만들지만, API는 숫자 문자열을 정수로 받아들인다(API 설계 9.4). API는 `hdfsPath`가 해당 run의 `hdfs_run_path` 아래인지 검증한다.

API는 보고를 받을 때마다 같은 트랜잭션에서 다음을 처리한다(API 설계 3.2).

1. run과 partition 행 잠금, claim token 확인
2. `load_file` UPSERT, partition·run `heartbeat_at` 갱신
3. 받은 chunk 수가 `chunkCount`에 도달하면 파티션 판정: row 합계가 expected와 같으면 `SUCCESS`, 다르면 `FAILED`와 run `FAILED_EXTRACT`
4. 파티션이 `SUCCESS`가 되면 run 판정: 모두 `SUCCESS`이고 합계가 source count와 같으면 `EXTRACTED_VALIDATED`와 검증 호출 예약

응답 예시:

```json
{ "recorded": true, "partitionStatus": "SUCCESS", "runStatus": "EXTRACTED_VALIDATED",
  "receivedChunks": 3, "chunkCount": 3, "validationScheduled": true }
```

NiFi는 이 응답으로 흐름을 바꾸지 않는다. 34D가 로그 수준만 정하고 FlowFile을 종료한다.

heartbeat는 claim과 chunk 보고 때만 갱신된다. `ExecuteSQLRecord`는 `Output Batch Size=0`이면 ResultSet을 끝까지 읽은 뒤 모든 chunk FlowFile을 한 번에 내보낸다. 따라서 파티션 쿼리가 실행되는 동안(최대 `EXTRACT.QUERY.TIMEOUT`)에는 heartbeat가 갱신되지 않는다. API sweeper의 stale 기준(13장)은 이 공백을 고려해 정한다.

34B가 재시도를 다 써도 보고하지 못하면 파티션은 미완료로 남는다. 이 경우 run은 API sweeper가 timeout으로 정리한다. HDFS 파일은 run 격리 경로에 있으므로 다른 run에 영향을 주지 않는다.

32 `ValidateRecord`는 Reader=`CS_PARQUET_READER`, validation schema=`CS_SCHEMA_REGISTRY`의 승인 버전, Writer=`CS_PARQUET_WRITER`로 설정한다. `invalid` 또는 `failure`가 한 건이라도 발생하면 해당 partition 전체를 실패시킨다. 대용량 재직렬화 비용이 허용되지 않으면 이 Processor를 제거할 수 있지만, 그 경우 동일 schema 검증을 staging Hive 조회에서 필수로 수행한다.

---

## 9. Load Control API 연동과 완료 판정

가이드 초안의 PG-30 Partition and Run Gate(Wait/Notify, `MapCacheServer`, 폴링 루프)는 두지 않는다. 완료 판정은 Load Control API가 chunk 보고마다 수행하고, run 완료를 확정한 보고 하나만 검증 flow 호출을 예약한다. 이 장은 NiFi가 API와 연동하는 공통 규칙을 정의한다. API 내부 동작은 API 설계 3~5장을 따른다.

### 9.1 완료 판정 흐름

```mermaid
sequenceDiagram
    participant W as PG-20 Worker (N개 병렬)
    participant A as Load Control API
    participant D as PostgreSQL
    participant K as API worker (dispatcher)
    participant V as PG-40 검증 flow

    W->>A: POST .../chunks (chunk마다)
    A->>D: run FOR UPDATE, file UPSERT, 파티션 판정, run 판정
    Note over A,D: run 완료 CAS 성공 시<br/>같은 트랜잭션에서 load_dispatch INSERT + pg_notify
    A-->>W: 200 (partitionStatus, runStatus)
    D-->>K: NOTIFY load_dispatch (commit 후)
    K->>V: POST /validate (runId, dispatchId)
    V-->>K: 202 Accepted
    V->>A: POST /runs/{id}/validation/start
    A->>D: EXTRACTED_VALIDATED → STAGE_VALIDATING CAS, dispatch ACKED
    A-->>V: started=true
```

판정 결과와 NiFi 동작은 다음과 같다.

| 상황 | API 처리 | NiFi 동작 |
|---|---|---|
| 파티션 진행 중 | file 기록 | 34D 로그 후 종료 |
| 파티션 완료, run 진행 중 | partition `SUCCESS` | 34D 로그 후 종료 |
| 마지막 파티션 완료 | run `EXTRACTED_VALIDATED`, outbox 예약 | 34D INFO 로그 후 종료. 검증 flow는 API가 시작 |
| row 수 불일치 | partition `FAILED`, run `FAILED_EXTRACT` | 34D ERROR 로그 후 종료 |
| 이미 실패한 run의 보고 | file만 기록, `runStatus=FAILED_*` | 34D DEBUG 로그 후 종료 |
| 파티션 최종 실패 보고(29) | partition `FAILED`, run `FAILED_EXTRACT` | PG-90 `PARTITION_FAILED` |

PoC에서 PG-30이 실패 후 0.1초 만에 run 실패를 확정하던 동작은 API에서 `fail` 호출 한 번으로 즉시 확정된다. 기다리는 FlowFile이 없으므로 Wait 해제 처리도 없다.

### 9.2 `InvokeHTTP` 공통 설정

| Property | 값 |
|---|---|
| HTTP Method | `POST` (조회는 `GET`) |
| HTTP URL | `#{CONTROL.API.URL}/runs/${load.run.id}/...` |
| SSL Context Service | `CS_SSL_CLIENT` |
| Connection Timeout | `5 sec` |
| Socket Read Timeout | `#{CONTROL.API.TIMEOUT}` |
| Request Content-Type | `application/json` |
| Request Body Enabled | `true` (FlowFile content가 본문) |
| Response Body Attribute Name | `api.response` (응답이 작은 호출). PG-10 21만 비움 |
| Response Body Attribute Size | `4096` |
| Response Generation Required | `false` |
| 동적 속성 `Authorization` | `Bearer #{CONTROL.API.TOKEN}` — **Sensitive 동적 속성**으로 추가해야 Sensitive Parameter를 참조할 수 있다 |
| 동적 속성 `X-Request-Id` | `${UUID()}` |
| 동적 속성 `X-Run-Id` | `${load.run.id}` |

요청 본문은 `AttributesToJSON` 또는 `JoltTransformJSON`으로 만든다. API는 알 수 없는 필드를 422로 거부하므로(API 설계 9.4) `AttributesToJSON`의 Attributes List를 명시하고 `Include Core Attributes=false`로 둔다.

### 9.3 Relationship 처리

| HTTP 결과 | Relationship | 처리 |
|---|---|---|
| 2xx | Original(`Response Body Attribute Name` 설정 시) 또는 Response | 응답 본문으로 분기 |
| 409 (`CLAIM_MISMATCH`, `CHUNK_CONFLICT`, `DUPLICATE_ACTIVE_RUN`) | No Retry | WARN 이벤트 후 종료. 재시도하지 않음 |
| 404, 422 (run 없음, 입력·불변식 위반) | No Retry | PG-90 ERROR. 입력 오류이므로 재시도하지 않음 |
| 5xx | Retry | `RetryFlowFile`(`#{CONTROL.API.RETRY.MAX}`, penalty) 후 재호출 |
| 연결 실패, timeout | Failure | Retry와 같게 처리 |

No Retry의 오류 코드는 `${invokehttp.response.body:jsonPath('$.code')}`로 확인한다. API 호출 재시도는 모든 상태 변경 호출이 멱등이기 때문에 안전하다. 같은 chunk 보고나 같은 token의 claim이 두 번 가도 결과는 한 번 호출한 것과 같다.

재시도 대기는 API 재기동 시간을 견딜 만큼 길어야 한다. 예를 들어 Penalty Duration 30초 × `CONTROL.API.RETRY.MAX` 5회면 약 2.5분이다. API가 이보다 오래 내려가면 보고가 유실되고, run은 sweeper가 timeout으로 정리한다(13장). 이 경우 데이터는 잘못 게시되지 않고 run이 실패할 뿐이다.

### 9.4 API 호출 목록

| Process Group | 호출 | 상태 전이 |
|---|---|---|
| PG-10 | `POST /runs` | run `CREATED` |
| PG-10 | `POST /runs/{id}/manifest` | `EXTRACTING`, 0건 파티션 `SUCCESS` |
| PG-10, PG-40~60 | `POST /runs/{id}/fail` | 지정 단계 실패 상태 |
| PG-20 | `POST /runs/{id}/partitions/{pid}/claim` | partition `RUNNING` |
| PG-20 | `POST /runs/{id}/partitions/{pid}/chunks` | partition `SUCCESS`, run `EXTRACTED_VALIDATED` |
| PG-20 | `POST /runs/{id}/partitions/{pid}/fail` | partition `FAILED`, run `FAILED_EXTRACT` |
| PG-40 | `POST /runs/{id}/validation/start` | `STAGE_VALIDATING` |
| PG-40, PG-60 | `POST /runs/{id}/validations` | `load_validation` 기록 |
| PG-40 | `POST /runs/{id}/stage-validated` | `STAGING_VALIDATED` |
| PG-50 | `POST /runs/{id}/publish/claim` | `PUBLISHING` |
| PG-50 | `POST /runs/{id}/publish/result` | `PUBLISHED`, `FAILED_PUBLISH`, `PUBLISH_UNKNOWN` |
| PG-60 | `POST /runs/{id}/success` | `SUCCESS` |

요청·응답 형식은 API 설계 5장을 따른다.

### 9.5 API 호출 수신: PG-05 Control Receiver

API는 검증 시작(`VALIDATE_RUN`)과 선택 기능인 파티션 재발행(`REISSUE_PARTITION`)을 NiFi에 HTTP로 요청한다. Job마다 Process Group과 Parameter Context(`PC_JOB_<JOB_NAME>`)가 다르고 한 포트는 하나의 `HandleHttpRequest`만 열 수 있다. 그래서 root 수준에 공통 수신 Process Group 하나를 두고 `jobKey`로 각 Job PG에 전달한다.

```mermaid
flowchart LR
    L[05_Listen<br/>HandleHttpRequest] --> V{06_Validate_Request<br/>RouteOnAttribute}
    V -->|invalid| R4[07_Respond_400<br/>HandleHttpResponse]
    V -->|valid| R2[08_Respond_202<br/>HandleHttpResponse]
    R2 --> J[09_Extract_Body<br/>EvaluateJsonPath]
    J --> RT{10_Route_By_Job<br/>RouteOnAttribute}
    RT -->|ORACLE_INSP_DTL_DAILY validate| P1[Output: INSP_DTL validate-in]
    RT -->|ORACLE_INSP_DTL_DAILY reissue| P2[Output: INSP_DTL reissue-in]
    RT -->|unmatched| E[PG-90 ERROR<br/>미등록 jobKey]
```

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 05 | `HandleHttpRequest` | All Nodes, 1 | Listening Port=`#{CONTROL.LISTEN.PORT}`, SSL Context Service=`CS_SSL_SERVER`, Client Authentication=`REQUIRED`, HTTP Context Map=`CS_HTTP_CONTEXT_MAP`, Allowed Paths=`/(validate\|reissue)/[A-Z0-9_]{1,200}`, Allow GET/PUT/DELETE/HEAD/OPTIONS=false | success→06 |
| 06 | `RouteOnAttribute` | All Nodes, 1 | `http.method`=POST, `http.headers.X-Run-Id`와 `X-Dispatch-Id`가 UUID 형식 | valid→08, unmatched→07 |
| 07 | `HandleHttpResponse` | All Nodes, 1 | HTTP Status Code=400 | success→PG-90 WARN |
| 08 | `HandleHttpResponse` | All Nodes, 1 | HTTP Status Code=202 | success→09 |
| 09 | `EvaluateJsonPath` + `UpdateAttribute` | All Nodes, 1 | `load.run.id=$.runId`, `load.dispatch.id=$.dispatchId`, `partition.id=$.partitionId`, `control.action=${http.request.uri:substringAfter('/'):substringBefore('/')}`, `load.job.key=${http.request.uri:substringAfterLast('/')}` | matched→10 |
| 10 | `RouteOnAttribute` | All Nodes, 1 | Job별 `${load.job.key:equals('ORACLE_INSP_DTL_DAILY'):and(${control.action:equals('validate')})}` 등 | Job PG Output Port, unmatched→PG-90 ERROR |

- 검증은 수십 분 걸릴 수 있으므로 08에서 먼저 202를 응답하고 HTTP 연결을 붙잡지 않는다. API는 2xx를 받으면 dispatch를 `SENT`로 바꾸고, 검증 flow가 `/validation/start`를 호출해야 `ACKED`가 된다. 202 응답 직후 노드가 죽어 FlowFile이 사라지면 API가 ACK timeout 뒤 다시 보낸다.
- `HandleHttpRequest`는 모든 노드에서 동작한다. API는 NiFi LB 주소(`LCA_NIFI_RECEIVER_URL`)로 호출하며, 어느 노드가 받든 Job PG의 첫 단계 CAS가 중복 실행을 막는다. 그래서 PG-40~60은 All Nodes로 스케줄한다(2장).
- 새 Job을 추가하면 10에 route 두 개(validate, reissue)와 Output Port를 추가한다. 등록되지 않은 `jobKey`는 ERROR로 남기고, API의 dispatch는 ACK timeout 뒤 재전송된다. 계속 실패하면 `DEAD`가 되어 알림이 간다.
- 05의 TLS client 인증으로 API만 호출할 수 있게 한다. 방화벽으로 수신 포트를 API 서버 대역에만 연다.

---

## 10. PG-40 Staging Validation

### 10.1 Processor 흐름

```mermaid
flowchart TD
    I[Input: validate-in<br/>PG-05에서 전달] --> ST[40S_Validation_Start<br/>InvokeHTTP POST validation/start]
    ST -->|Original 2xx| T{40T_Is_Started<br/>RouteOnAttribute}
    ST -->|Retry or Failure| SR[40R_RetryFlowFile]
    SR -->|retry| ST
    SR -->|exceeded| F0[PG-90 ERROR<br/>API가 ACK timeout 후 재전송]
    T -->|no| X[DEBUG 후 종료<br/>중복 dispatch]
    T -->|yes| U[40U_Set_Run_Attrs<br/>UpdateAttribute]
    U --> A[40_Create_SUCCESS_Marker<br/>ReplaceText + PutHDFS]
    A --> B[41_Build_Create_External_SQL<br/>ReplaceText]
    B --> C[42_Create_External_Table<br/>PutHive3QL]
    C -->|success| D[43_Query_Stage_Metrics<br/>SelectHive3QL]
    C -->|failure| F[47_Report_Stage_Fail<br/>InvokeHTTP POST fail]
    D --> E[44_Extract_Stage_Metrics]
    E --> R{45_Compare_Source_Stage}
    R -->|match or mismatch| V[46A_Report_Validations<br/>InvokeHTTP POST validations]
    V -->|mismatch| F
    V -->|match| S[46B_Stage_Validated<br/>InvokeHTTP POST stage-validated]
    S -->|stageValidated=true| O[Output: staging-valid]
    S -->|false| F
    F --> FE[PG-90 RUN_FAILED]
```

### 10.2 주요 Processor 설정

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 40S | `AttributesToJSON` + `InvokeHTTP` | All Nodes, 1 | 본문 `dispatchId=${load.dispatch.id}`, `node=${hostname(true)}`. URL=`#{CONTROL.API.URL}/runs/${load.run.id}/validation/start`, `Response Body Attribute Name=api.response`, `Response Body Attribute Size=16384` | Original→40T, No Retry→PG-90, Retry/Failure→40R |
| 40T | `RouteOnAttribute` | All Nodes, 1 | `${api.response:jsonPath('$.started'):equals('true')}` | started→40U, unmatched→DEBUG 후 종료 |
| 40U | `UpdateAttribute` | All Nodes, 1 | 응답에서 `load.hdfs.path`, `load.stage.table`, `load.business.key`, `load.snapshot.scn`, `load.source.count`, `load.extracted.count`, `validation.source.*`(`$.sourceMetrics.*`)를 `jsonPath()`로 추출 | success→40A |
| 40A | `ReplaceText` | All Nodes, 1 | Replacement Strategy=Entire text, Replacement Value=빈 값 | success→40B, failure→47 |
| 40B | `UpdateAttribute` | All Nodes, 1 | `filename=_SUCCESS`, Directory=`${load.hdfs.path}` | success→40C |
| 40C | `PutHDFS` | All Nodes, 1 | Hadoop config만 설정, Kerberos service 미설정, Write and rename, conflict=replace | success→41, failure→제한 재시도 후 47 |
| 41 | `ReplaceText` | All Nodes, 1 | 승인된 external table DDL로 전체 content 치환 | success→42, failure→47 |
| 42 | `PutHive3QL` 또는 `PutClouderaHiveQL` | All Nodes, 1 | `CS_HIVE3_DBCP`, Query Timeout, DDL 1건 | success→43, failure→47 |
| 43 | `SelectHive3QL` 또는 `ExecuteSQLRecord` | All Nodes, 1 | stage count/NULL/중복/min/max/업무 합계 SQL, JSON writer | success→44, failure→47 |
| 44 | `EvaluateJsonPath` | All Nodes, 1 | stage metrics를 `validation.stage.*` attribute로 추출 | matched→45, failure/unmatched→47 |
| 45 | `RouteOnAttribute` + `UpdateAttribute` | All Nodes, 1 | source/extracted/staging count 및 DQ 지표 비교, 지표별 `PASS`/`FAIL` attribute 설정 | 모두→46A(결과에 따라 `validation.result` 설정) |
| 46A | `JoltTransformJSON` + `InvokeHTTP` | All Nodes, 1 | `stage=STAGING`, 지표 배열(`metricName`, `expectedValue`, `actualValue`, `result`, `queryVersion`)을 본문으로 `POST /validations` | Original→`validation.result`가 PASS면 46B, FAIL이면 47 |
| 46B | `InvokeHTTP` | All Nodes, 1 | `POST /runs/${load.run.id}/stage-validated`, 본문 `{}`, `Response Body Attribute Name=api.response` | `$.stageValidated=true`→PG-50, false→47 |
| 47 | `AttributesToJSON` + `InvokeHTTP` | All Nodes, 1 | `POST /fail`, 본문 `expectedStatus=STAGE_VALIDATING`, `failStatus=FAILED_STAGE_VALIDATION`, `errorStage`, `errorCode`, `message` | Original→PG-90 `RUN_FAILED` |

검증 flow는 API 호출로 새로 시작되므로, PG-10에서 만든 attribute(SCN, source count, source DQ 지표, HDFS 경로)를 가지고 있지 않다. 그래서 40S의 `/validation/start` 응답이 검증에 필요한 값을 모두 돌려준다. source DQ 지표는 PG-10이 `/manifest` 요청의 `sourceMetrics`로 보내 API가 `load_validation`(stage=`SOURCE`)에 저장해 둔 값이다(API 설계 5.3).

40S는 반드시 첫 단계다. API는 `EXTRACTED_VALIDATED → STAGE_VALIDATING` CAS에 성공한 호출에만 `started=true`를 돌려준다. 같은 run의 검증 요청이 두 번 와도(outbox 재전송, LB 재시도) 두 번째는 40T에서 종료된다.

40은 수신 FlowFile의 content를 `ReplaceText`로 비우고 `filename=_SUCCESS`를 설정한 뒤 run root에 `Write and rename`으로 기록한다. FlowFile attribute는 유지되므로 PutHDFS 성공 관계에서 바로 41로 진행한다. 이 파일은 API가 run 완료를 확정한 뒤에만 존재한다.

41의 예시 SQL:

```sql
CREATE EXTERNAL TABLE #{HIVE.STAGE.DB}.${load.stage.table} (
  #{HIVE.STAGE.DDL.COLUMNS}
)
STORED AS PARQUET
LOCATION '${load.hdfs.path}'
```

`load.stage.table`은 `${load.run.id}`에서 하이픈을 제거한 안전한 suffix만 사용하고 정규식으로 검증한다.

Apache NiFi 2.x에는 Hive 번들이 없다. NiFi 2.4.0 배포본의 Processor 목록에서 `PutHive3QL`/`SelectHive3QL`이 없음을 확인했다. 이 문서의 `PutHive3QL`, `SelectHive3QL`, `PutClouderaHiveQL`, `CS_HIVE3_DBCP`는 CFM이 제공하는 Hive 구성요소를 가리키는 자리표시자다. 구현 전에 CFM 4.12.0 supported processors 목록(21장)에서 실제 Processor와 Controller Service 이름, 지원 속성(Query Timeout, Rollback On Failure 등)을 확정한다. 제공 구성요소가 없다면 Hive JDBC driver와 `ExecuteSQL`/`ExecuteSQLRecord`로 DDL·DML을 실행할 수 있는지 사전 시험한 뒤 대체한다.

42는 확정된 Hive 실행 Processor를 사용한다. 43은 Hive 조회 Processor가 제공되면 사용하고, 환경 표준이 Hive JDBC라면 `ExecuteSQLRecord + CS_HIVE3_DBCP`로 대체한다.

필수 stage 지표:

```sql
SELECT COUNT(*) AS STAGE_COUNT,
       SUM(CASE WHEN INSP_DTL_SEQ IS NULL THEN 1 ELSE 0 END) AS NULL_SEQ_COUNT,
       COUNT(*) - COUNT(DISTINCT <BUSINESS_PK>) AS DUP_PK_COUNT,
       MIN(INSP_DTL_SEQ) AS MIN_SEQ,
       MAX(INSP_DTL_SEQ) AS MAX_SEQ,
       SUM(<BUSINESS_AMOUNT>) AS AMOUNT_SUM,
       CAST(MIN(<BUSINESS_TIMESTAMP>) AS STRING) AS MIN_TS,
       CAST(MAX(<BUSINESS_TIMESTAMP>) AS STRING) AS MAX_TS
  FROM #{HIVE.STAGE.DB}.${load.stage.table}
```

`MIN_TS`/`MAX_TS`는 동일 의미로 계산한 원천 값과 문자열로 비교한다. 시간대 해석이 어긋나면 count는 같아도 이 지표가 불일치한다.

46A는 지표별 PASS/FAIL을 모두 보고한다. FAIL이 있어도 먼저 기록한 뒤 47로 실패를 확정한다. 46B에서 API는 NiFi의 판정을 그대로 믿지 않고, 저장된 STAGING 지표가 모두 PASS일 때만 `STAGE_VALIDATING → STAGING_VALIDATED`로 CAS 갱신한다.

---

## 11. PG-50 Publish

### 11.1 Processor 흐름

```mermaid
flowchart TD
    I[Input: staging-valid] --> T[50_Create_Publish_Token<br/>UpdateAttribute]
    T --> C[51_Claim_Publish<br/>InvokeHTTP POST publish/claim]
    C -->|Original 2xx| R{53_Is_Publish_Owner<br/>RouteOnAttribute}
    C -->|Retry or Failure| CR[51R_RetryFlowFile]
    CR -->|retry| C
    CR -->|exceeded| F0[PG-90 ERROR<br/>게시 전이므로 재실행 가능]
    R -->|no| X[Terminate duplicate publish]
    R -->|yes| B[54_Build_Insert_Overwrite_SQL<br/>ReplaceText]
    B --> P[55_PutHive3QL_INSERT_OVERWRITE]
    P -->|success| S[56_Report_PUBLISHED<br/>InvokeHTTP POST publish/result]
    P -->|failure| PA{55A_Classify_Publish_Failure}
    PA -->|pre_execution| SF[56F_Report_FAILED_PUBLISH<br/>InvokeHTTP POST publish/result]
    PA -->|unmatched| SU[56U_Report_PUBLISH_UNKNOWN<br/>InvokeHTTP POST publish/result]
    S -->|Original 2xx| O[Output: published]
    SF --> FE[PG-90 RUN_FAILED]
    SU --> UE[PG-90 PUBLISH_UNKNOWN ERROR]
```

### 11.2 주요 Processor 설정

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 50 | `UpdateAttribute` | All Nodes, 1 | `publish.token=${UUID()}`, `publishToken=${publish.token}`, 이어서 `AttributesToJSON`(Attributes List=`publishToken`) | success→51 |
| 51 | `InvokeHTTP` | All Nodes, 1 | `POST /runs/${load.run.id}/publish/claim`, `Response Body Attribute Name=api.response` | Original→53, No Retry→PG-90, Retry/Failure→51R |
| 53 | `RouteOnAttribute` | All Nodes, 1 | `${api.response:jsonPath('$.claimed'):equals('true')}` | true→54, false→중복 publish 종료 |
| 54 | `ReplaceText` | All Nodes, 1 | 승인된 target/partition/column로 `INSERT OVERWRITE` SQL 생성 | success→55, failure→56F |
| 55 | `PutHive3QL` 또는 `PutClouderaHiveQL` | All Nodes, 1 | `CS_HIVE3_DBCP`, Query Timeout, 환경 지원 시 Rollback On Failure=true | success→56, failure→55A |
| 55A | `RouteOnAttribute` | All Nodes, 1 | 오류 attribute로 실행 전 실패 여부 판별(아래 기준) | pre_execution→56F, unmatched→56U |
| 56, 56F, 56U | `AttributesToJSON` + `InvokeHTTP` | All Nodes, 1 | `POST /runs/${load.run.id}/publish/result`, 본문 `publishToken`, `outcome`(`PUBLISHED`/`FAILED_PUBLISH`/`PUBLISH_UNKNOWN`), `errorCode`, `message` | Original→PG-60(56) 또는 PG-90, Retry/Failure→제한 재시도 후 PG-90 ERROR |

51은 API가 `STAGING_VALIDATED → PUBLISHING`을 publish token으로 CAS하고 결과를 `claimed`로 돌려준다. 같은 token의 재요청은 `claimed=true`이므로, 응답을 잃고 재시도해도 게시 소유권을 잃지 않는다. token은 50에서 한 번만 만들고 재시도 루프에서 다시 만들지 않는다.

56은 token이 일치할 때만 `PUBLISHING → PUBLISHED`로 바꾼다. Hive 실행은 성공했는데 56이 재시도를 다 써도 API에 보고하지 못하면 run은 `PUBLISHING`에 남는다. API sweeper가 `PUBLISH.STALE` 경과 후 `PUBLISH_UNKNOWN`으로 바꾸고 알린다. 이 경우 자동 재게시는 하지 않는다.

54 SQL 예시:

```sql
INSERT OVERWRITE TABLE #{HIVE.TARGET.DB}.#{HIVE.TARGET.TABLE}
#{TARGET.PARTITION.CLAUSE}
SELECT #{HIVE.INSERT.COLUMNS}
  FROM #{HIVE.STAGE.DB}.${load.stage.table}
```

전체 테이블이 아니라 업무일자 파티션만 교체해야 한다면 `TARGET.PARTITION.CLAUSE`를 반드시 설정한다. SQL에는 FlowFile에서 받은 임의 identifier를 사용하지 않는다.

Hive 응답을 받지 못해 성공 여부가 불명확한 timeout은 자동 재실행하지 않고 `PUBLISH_UNKNOWN`으로 기록한다. 운영자가 Hive query history와 target 지표를 확인한 뒤 API의 운영자 엔드포인트(`POST /runs/{id}/publish-unknown/resolve`)로 확정한다.

Hive Processor의 `failure` relationship만으로는 "SQL이 실행되지 않은 실패"와 "실행 후 응답을 잃은 경우"를 구분할 수 없다. 따라서 55의 failure는 기본적으로 `PUBLISH_UNKNOWN`으로 보낸다. 55A는 실행 전에 실패했음이 확실한 경우에만 `FAILED_PUBLISH`로 분류한다.

- SQL 구문/의미 오류: SQLState class `42`, Hive `ParseException`/`SemanticException`
- 권한 오류: authorization 실패 메시지
- 연결 획득 실패: 연결 수립 단계 오류로, 문장 제출 전임이 확실한 경우

timeout, connection reset, 원인 불명 오류는 모두 `PUBLISH_UNKNOWN`이다. CFM Hive Processor가 failure FlowFile에 어떤 오류 attribute(SQLState, message)를 붙이는지는 구현 전에 확인한다. attribute가 없으면 55A를 두지 않고 모든 failure를 `PUBLISH_UNKNOWN`으로 처리한다.

---

## 12. PG-60 Target Validation

### 12.1 Processor 흐름

```mermaid
flowchart TD
    I[Input: published] --> Q[60_Query_Target_Metrics<br/>SelectHive3QL]
    Q --> E[61_Extract_Target_Metrics]
    E --> C{62_Compare_All_Stages}
    C -->|match or mismatch| V[63_Report_Validations<br/>InvokeHTTP POST validations]
    V -->|match| S[64_Report_SUCCESS<br/>InvokeHTTP POST success]
    V -->|mismatch| F[66_Report_Target_Fail<br/>InvokeHTTP POST fail]
    S -->|success=true| L[65_Log_Run_SUCCESS]
    S -->|false| F
    L --> O[Output: success]
    Q -->|failure| F
    F --> FE[PG-90 FAILED_TARGET_VALIDATION]
```

### 12.2 주요 Processor 설정

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 60 | `SelectHive3QL` 또는 `ExecuteSQLRecord` | All Nodes, 1 | target 업무 범위 count/NULL/중복/min/max/업무 합계 SQL, JSON writer | success→61, failure→66 |
| 61 | `EvaluateJsonPath` | All Nodes, 1 | target metrics를 `validation.target.*` attribute로 추출 | matched→62, failure/unmatched→66 |
| 62 | `RouteOnAttribute` + `UpdateAttribute` | All Nodes, 1 | source/extracted/stage/target count와 DQ 지표 비교, 지표별 PASS/FAIL 설정 | 모두→63 |
| 63 | `JoltTransformJSON` + `InvokeHTTP` | All Nodes, 1 | `stage=TARGET` 지표 배열을 `POST /validations` | Original→PASS면 64, FAIL이면 66 |
| 64 | `InvokeHTTP` | All Nodes, 1 | `POST /runs/${load.run.id}/success`, 본문 `targetCount`, `Response Body Attribute Name=api.response` | `$.success=true`→65, false→66 |
| 65 | `UpdateAttribute` + PG-90 port | All Nodes, 1 | `event.name=RUN_SUCCESS`, `event.level=INFO`, 최종 count/duration 설정 | success→완료 Output Port |
| 66 | `AttributesToJSON` + `InvokeHTTP` | All Nodes, 1 | `POST /fail`, 본문 `expectedStatus=PUBLISHED`, `failStatus=FAILED_TARGET_VALIDATION` | Original→PG-90 ERROR |

PG-40에서 이어진 같은 FlowFile이므로 source와 stage 지표 attribute를 그대로 비교에 쓴다.

62 조건:

```text
target_count = stage_count = extracted_count = source_count
AND target key/null/duplicate metrics pass
AND target business aggregates = stage/source aggregates
```

API는 64에서 저장된 TARGET 지표가 모두 PASS이고 `status='PUBLISHED'`일 때만 `SUCCESS`로 갱신하며, 완료시각과 모든 count를 저장한다. Target 검증 실패 시 재추출이나 overwrite를 자동 반복하지 않는다.

---

## 13. 복구: API Sweeper와 재발행

가이드 초안의 PG-70 Recovery Monitor(NiFi `GenerateFlowFile` 주기 조회 + `PutSQL` CAS)는 두지 않는다. stale 판정과 상태 정리는 Load Control API의 worker 프로세스가 수행한다(API 설계 7장). NiFi는 API가 재발행을 요청할 때만 관여한다.

### 13.1 Sweeper 규칙

| 대상 | 조건 | 동작 |
|---|---|---|
| 파티션 `RUNNING` | `heartbeat_at < now - LCA_RECOVERY_STALE` AND `started_at + LCA_EXTRACT_QUERY_TIMEOUT < now` | `LCA_RECOVERY_MODE=FAIL`: run `TIMED_OUT`. `REISSUE`: claim 초기화 후 `RETRY`, `REISSUE_PARTITION` dispatch |
| run `CREATED`, `EXTRACTING` | `started_at + LCA_RUN_TIMEOUT < now` | `TIMED_OUT`, 알림 |
| dispatch `SENT` | ACK timeout 경과, run이 아직 `EXTRACTED_VALIDATED` | `PENDING`으로 되돌려 재전송 |
| run `STAGE_VALIDATING`, `PUBLISHED` | heartbeat가 `LCA_VALIDATION_STALE`보다 오래됨 | ERROR 알림. 자동 전이하지 않음 |
| run `PUBLISHING` | `publish_started_at + LCA_PUBLISH_STALE < now` | `PUBLISH_UNKNOWN`, ERROR 알림. 자동 재실행 금지 |

- `LCA_RECOVERY_STALE`은 `EXTRACT.QUERY.TIMEOUT` + 파티션당 chunk 기록·보고 소요시간 + 여유보다 크게 잡는다. heartbeat는 claim과 chunk 보고에서만 갱신되고 파티션 쿼리 실행 중에는 갱신되지 않는다(8.5). 이 값이 query timeout(60분)보다 짧으면 정상 실행 중인 파티션을 stale로 판정한다.
- 1단계 운영은 `LCA_RECOVERY_MODE=FAIL`이다. stale 파티션이 생기면 run 전체를 실패시키고 새 `run_id`로 재실행한다.
- `ORA-01555`는 Worker가 `fail`로 보고하며, API가 run을 `FAILED_SNAPSHOT_EXPIRED`로 바꾼다. 같은 run의 일부 파티션만 새 SCN으로 읽지 않는다.

### 13.2 재발행 수신 (선택)

`LCA_RECOVERY_MODE=REISSUE`일 때만 사용한다. API는 stale 파티션의 claim을 CAS로 초기화한 뒤 `REISSUE_PARTITION` dispatch를 만들고, PG-05를 거쳐 Job PG의 `reissue-in`으로 전달한다. 요청 본문에는 Worker 실행에 필요한 값이 모두 들어 있다.

```json
{ "runId": "...", "dispatchId": "...", "partitionId": "0003",
  "businessKey": "2026-09-28", "snapshotScn": "1234567890",
  "hdfsRunPath": "/data/nifi/stage/...", "lowerBound": "45001", "upperBound": "60001",
  "upperInclusive": false, "isNullPartition": false, "expectedRowCount": 15000 }
```

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 70 | `EvaluateJsonPath` | All Nodes, 1 | 본문을 `load.*`, `partition.*` attribute로 추출(5장 이름과 동일) | matched→71 |
| 71 | `RouteOnAttribute` | All Nodes, 1 | SCN·경계·건수 숫자 정규식 검증 | valid→72, unmatched→PG-90 ERROR |
| 72 | `UpdateAttribute` + Output Port | All Nodes, 1 | `event.name=RECOVERY_REISSUED`, `event.level=WARN` | success→PG-20 입력(Round Robin) |

재발행된 FlowFile은 PG-20의 20에서 새 claim token으로 claim한다. API는 `RETRY` 상태 파티션만 claim을 허용하므로 이전 Worker가 늦게 살아나도 이전 token의 chunk 보고는 409 `CLAIM_MISMATCH`로 거부된다. 같은 `run_id + partition_id`와 같은 SCN, 같은 결정적 파일명을 쓰므로 PutHDFS `replace`로 이전 파일을 덮어쓴다. 재발행은 Oracle UNDO 보존 시간이 run 최대 시간보다 길다는 것을 확인한 뒤 켠다.

---

## 14. PG-90 Audit, Error and Notification

### 14.1 이벤트 흐름

```mermaid
flowchart LR
    I[Input event FlowFile] --> P[90_Prepare_Event_Attributes<br/>UpdateAttribute]
    P --> A[91_AttributesToJSON]
    A --> B[92_PutSQL<br/>nifi_ops.load_event]
    B -->|success| C[93_LogMessage]
    B -->|failure| D[94_Event_DLQ<br/>PutFile or Kafka]
    C --> E{95_Alert_Required}
    E -->|yes| F[96_PutEmail or enterprise alert]
    E -->|no| T[Terminate]
```

업무 상태를 바꾸는 `nifi_ops.load_run/load_partition/load_file/load_validation/load_dispatch` 기록은 각 주 흐름이 Load Control API를 동기적으로 호출해 처리한다. 상태 전이 이벤트는 API가 같은 트랜잭션에서 `load_event`에 기록한다. PG-90은 NiFi Processor 오류와 Data plane 관측 이벤트만 기록한다. 이벤트 DB 장애가 데이터 FlowFile을 무한 정지시키지 않도록 로컬 보호 DLQ 또는 운영 Kafka로 보낸다.

### 14.2 주요 Processor 설정

| ID | Processor | Scheduling | 주요 Properties | Relationship |
|---|---|---|---|---|
| 90 | `UpdateAttribute` | All Nodes, 2~4 | event_id/time/level/name, run/partition/chunk, 오류 및 처리량 속성 정규화 | success→91 |
| 91 | `AttributesToJSON` | All Nodes, 2~4 | Destination=flowfile-content, 지정 attribute 목록만 포함, pretty print=false | success→92, failure→94 |
| 92 | `PutSQL` | All Nodes, 2~4 | `CS_DBCP_META`, event INSERT prepared SQL, Batch=1, Fragmented=false | success→93, retry/failure→94 |
| 93 | `LogMessage` | All Nodes, 2~4 | Prefix=`SQOOP_REPLACEMENT`, Level=`${event.level}`, 구조화 JSON message | success→95 |
| 94 | `PutFile` 또는 운영 Kafka Publisher | All Nodes, 1~2 | 복구 가능한 DLQ 경로 또는 topic, 결정적 event filename/key | success→오류 카운터, failure→Bulletin/운영 알림 |
| 95 | `RouteOnAttribute` | All Nodes, 2~4 | ERROR, PUBLISH_UNKNOWN, 최종 실패 등 알림 대상 분기 | alert→96, unmatched→terminate |
| 96 | `PutEmail` 또는 조직 알림 Processor | All Nodes, 1 | 제목에 job/run/status, 본문에 비밀값 없는 요약 | success→terminate, failure→DLQ/Bulletin |

### 14.3 PostgreSQL 이벤트 기록

테이블은 4.1의 `nifi_ops.load_event` DDL을 사용한다. `90_Prepare_Event_Attributes`는 아래 PostgreSQL 컬럼명과 동일한 attribute를 만들고, `91_AttributesToJSON`은 해당 attribute 목록을 FlowFile content로 직렬화한다. `92_PutSQL`은 `CS_DBCP_META`, Batch Size=1, Support Fragmented Transactions=false로 설정한다.

`UpdateAttribute` 동적 Property 예시는 다음과 같다.

```text
event_id      = ${UUID()}
event_time    = ${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX","UTC")}
event_level   = ${event.level}
event_name    = ${event.name}
run_id        = ${load.run.id}
job_key       = ${load.job.key}
business_key  = ${load.business.key}
partition_id  = ${partition.id}
chunk_index   = ${chunk.index}
process_group = ${event.process.group}
processor_name = ${event.processor.name}
node_id       = ${hostname(true)}
attempt_no    = ${partition.retry.count}
row_count     = ${event.row.count}
duration_ms   = ${event.duration.ms}
error_class   = ${error.class}
error_code    = ${error.code}
message       = ${error.message}
```

`AttributesToJSON`은 Destination=`flowfile-content`, Attributes List=`event_id,event_time,event_level,event_name,run_id,job_key,business_key,partition_id,chunk_index,process_group,processor_name,node_id,attempt_no,row_count,duration_ms,error_class,error_code,message`로 설정한다. 이 content는 DLQ와 운영 분석에 사용하고, DB INSERT는 아래 prepared SQL을 사용한다.

```sql
INSERT INTO nifi_ops.load_event (
    event_id, event_time, event_level, event_name,
    run_id, job_key, business_key, partition_id, chunk_index,
    process_group, processor_name, node_id,
    attempt_no, row_count, duration_ms,
    error_class, error_code, message
) VALUES (
    CAST(? AS uuid), CAST(? AS timestamptz), ?, ?,
    CAST(NULLIF(?, '') AS uuid), ?, ?, ?, CAST(NULLIF(?, '') AS integer),
    ?, ?, ?,
    CAST(NULLIF(?, '') AS integer), CAST(NULLIF(?, '') AS bigint),
    CAST(NULLIF(?, '') AS bigint),
    NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, '')
);
```

`sql.args.1`부터 `sql.args.18`까지 위 컬럼 순서로 매핑하고 type은 문자열 전달이 가능한 `VARCHAR(12)`를 사용한다. PostgreSQL `CAST/NULLIF`가 UUID, timestamp 및 숫자 변환을 담당하므로 선택 숫자 값이 비어 있어도 INSERT가 실패하지 않는다.

생성되는 JSON 예시는 다음과 같다.

```json
{
  "event_id": "0199d100-1111-7000-8000-000000000001",
  "event_time": "2026-09-28T05:10:12.123Z",
  "event_level": "INFO",
  "event_name": "CHUNK_REPORTED",
  "run_id": "0199d100-2222-7000-8000-000000000002",
  "job_key": "ORACLE_INSP_DTL_DAILY",
  "business_key": "2026-09-28",
  "partition_id": "0003",
  "chunk_index": 2,
  "process_group": "PG-20 Extract Workers",
  "processor_name": "34B_Report_Chunk",
  "node_id": "nifi-02.example.com",
  "attempt_no": 1,
  "row_count": 500000,
  "duration_ms": 82451,
  "error_class": null,
  "error_code": null,
  "message": "partitionStatus=SUCCESS runStatus=EXTRACTING"
}
```

이벤트 INSERT 실패는 main flow 상태를 되돌리지 않고 JSON content를 DLQ에 저장하되, Run/Partition/File/Validation 상태 기록 실패는 해당 단계 자체를 실패시킨다.

### 14.4 필수 이벤트

| Level | Event | 기록 시점 | 기록 주체 |
|---|---|---|---|
| INFO | `RUN_STARTED` | run 생성(active lock 획득) | API |
| WARN | `DUPLICATE_ACTIVE_RUN` | 같은 업무키 활성 run 존재(409) | NiFi PG-90 |
| INFO | `MANIFEST_CREATED` | SCN·metric 저장, manifest 등록 | API |
| ERROR | `MANIFEST_INVALID` | 불변식 위반, `FAILED_MANIFEST` | API |
| INFO | `PARTITION_STARTED` | claim 성공 | API |
| DEBUG | `CHUNK_REPORTED` | PutHDFS 성공 후 보고; 운영 로그량에 따라 생략 가능 | NiFi PG-90 |
| INFO | `PARTITION_SUCCESS` | 파티션 row/file 판정 성공 | API |
| ERROR | `PARTITION_FAILED` | 재시도 소진, 비일시 오류, row 수 불일치 | API(상태), NiFi PG-90(Processor 오류 상세) |
| INFO | `EXTRACT_VALIDATED` | 전체 파티션 판정 성공, 검증 호출 예약 | API |
| ERROR | `DISPATCH_DEAD` | 검증 호출 최대 시도 초과 | API |
| INFO | `STAGE_VALIDATION_STARTED` | `/validation/start` CAS 성공 | API |
| INFO | `STAGE_VALIDATED` | external table 검증 성공 | API |
| INFO | `PUBLISH_STARTED` | publish CAS 획득 | API |
| INFO | `PUBLISH_FINISHED` | HiveQL 성공 보고 | API |
| ERROR | `PUBLISH_UNKNOWN` | timeout/연결 단절로 결과 불명, 또는 sweeper 판정 | API |
| INFO | `RUN_SUCCESS` | target 검증 완료 | API |
| ERROR | `RUN_FAILED` | 최종 실패 확정 | API |
| ERROR | `RUN_TIMED_OUT` | sweeper timeout | API |
| ERROR | `CONTROL_API_UNREACHABLE` | API 호출 재시도 소진 | NiFi PG-90 |
| WARN | `RECOVERY_REISSUED` | stale partition 재발행 | API, NiFi PG-90(수신) |

### 14.5 `LogMessage` 형식

```text
Log Prefix  = SQOOP_REPLACEMENT
Log Level   = ${event.level}
Log Message = {"event":"${event.name}","run_id":"${load.run.id}",
 "job_key":"${load.job.key}","business_key":"${load.business.key}",
 "partition_id":"${partition.id}","chunk_index":"${chunk.index}",
 "node":"${hostname(true)}","attempt":"${partition.retry.count}",
 "rows":"${event.row.count}","duration_ms":"${event.duration.ms}",
 "error_class":"${error.class}","error_code":"${error.code}",
 "message":"${error.message}"}
```

`error.message`는 줄바꿈 제거, 길이 제한 및 비밀값 마스킹 후 기록한다. SQL 본문 전체, JDBC URL의 credential, 원천 행 데이터는 로그에 기록하지 않는다.

### 14.6 오류 공통 경로

각 Processor의 failure 관계에는 먼저 전용 `UpdateAttribute`를 둔다.

```text
error.stage     = ORACLE_EXTRACT | HDFS_WRITE | STAGE_VALIDATE | PUBLISH ...
error.processor = 25_Execute_Partition_Query
error.class     = TRANSIENT | NON_RETRYABLE | VALIDATION | UNKNOWN
error.code      = processor가 제공한 SQLState/vendor code 또는 INTERNAL
error.message   = processor error attribute, 없으면 고정 설명
event.level     = ERROR
event.name      = PARTITION_FAILED 또는 RUN_FAILED
```

`ExecuteSQLRecord`의 `executesql.error.message`처럼 Processor가 제공하는 attribute를 사용한다. PutHDFS처럼 구체적인 오류 attribute가 없는 경우 `PutHDFS routed failure; see bulletin and provenance for run_id/partition_id`를 기록하고 NiFi Bulletin/Provenance와 상관 조회한다.

---

## 15. Connection과 Back Pressure

| Connection | Object threshold | Data threshold | 기타 |
|---|---:|---:|---|
| Coordinator → Worker | `2 × 전체 worker 수` 이상 | 제어 FlowFile이므로 100 MB | Round Robin |
| PutHDFS → 보고(34A/34B) | 1,000 | 100 MB | PutHDFS 후 content를 보고 JSON으로 바꾸므로 작음 |
| ExecuteSQLRecord → Validate/PutHDFS | 100~500 | HDFS 지연을 견디되 repository 용량의 20% 이하 | Oldest First |
| PutHDFS retry loop | 100 | 10 GB 예시 | RetryFlowFile penalty 사용 |
| API 보고 retry loop(34C) | 1,000 | 100 MB | 보고 JSON만 담긴 작은 FlowFile, RetryFlowFile penalty |
| Control Receiver → Job PG | active run 수 × 2 | 10 MB | 검증·재발행 요청 |
| Audit | 10,000 | 1 GB | 중요 이벤트 우선순위 가능 |

정확한 값은 평균 FlowFile 크기, Content Repository 용량, 동시 run 수로 산정한다. Back Pressure가 걸렸는데 Coordinator가 새 run을 계속 생성하지 않도록 활성 run lock을 유지한다.

### 15.1 `PutSQL` 사용 범위

NiFi의 `PutSQL`은 PG-90의 `load_event` INSERT(92)에만 쓴다. 원장 쓰기는 모두 API 호출이므로 가이드 초안의 manifest 행별 INSERT(Fragmented=true)와 그로 인한 livelock 문제는 생기지 않는다.

`SplitJson`이나 `ExecuteSQLRecord`가 만든 FlowFile에는 `fragment.identifier/count/index`가 남아 있다. 이벤트 FlowFile이 이 attribute를 가진 채 92에 들어오면 `PutSQL`이 fragment가 모두 모일 때까지 기다릴 수 있다. 따라서 92는 `Support Fragmented Transactions=false`를 반드시 명시한다. NiFi 2.4.0 PoC에서 Fragmented=true인 `PutSQL`이 일부 fragment만 poll되면 FlowFile을 penalize해 되돌리고, Penalty Duration이 0보다 크면 livelock이 생기는 것을 재현했다.

---

## 16. Retry 분류

| 분류 | 예 | 동작 |
|---|---|---|
| 일시적 | connection reset, 일시 HDFS unavailable, DB pool timeout | 최대 `${PARTITION.RETRY.MAX}`, penalty/backoff 후 같은 partition 재시도 |
| 스냅샷 불가 | `ORA-01555`, snapshot too old | 즉시 전체 run 실패, 새 SCN 일부 재시도 금지 |
| 영구 SQL | ORA-00942, 문법/컬럼/권한 오류 | 즉시 실패 |
| 데이터 | schema 변환 실패, expected/actual mismatch | 즉시 실패 및 파일 격리 |
| 게시 불명 | Hive timeout, connection loss after submit | `PUBLISH_UNKNOWN`, 자동 overwrite 재실행 금지 |
| API 일시 장애 | 5xx, 연결 실패, timeout | `CONTROL.API.RETRY.MAX`까지 같은 요청 재시도(멱등). 소진 시 PG-90 ERROR, run은 API sweeper가 정리 |
| API 거부 | 409, 404, 422 | 재시도하지 않음. 409는 정상 경합(WARN), 404/422는 설정·입력 오류(ERROR) |

`RetryFlowFile` 설정 예:

```text
Retry Attribute               = partition.retry.count
Maximum Retries               = #{PARTITION.RETRY.MAX}
Penalize Retries              = true
Fail on Non-numerical Overwrite = true
Reuse Mode                    = Fail on Reuse
```

Connection의 Penalty Duration으로 최소 backoff를 설정한다. 더 긴 지수 backoff가 필요하면 retry count별 `RouteOnAttribute`와 `ControlRate`/지연 queue를 사용한다. 재시도 loop마다 Retry Attribute를 다르게 둔다(`partition.retry.count`, `hdfs.retry.count`, `api.retry.count`). 같은 attribute를 쓰면 앞 loop의 횟수가 뒤 loop에 이어진다.

---

## 17. 상태 변경과 게시 안전장치

```mermaid
stateDiagram-v2
    [*] --> CREATED: POST /runs
    CREATED --> EXTRACTING: POST /manifest (불변식 PASS)
    EXTRACTING --> EXTRACTED_VALIDATED: 마지막 chunk 보고, 모든 partition SUCCESS 및 count 일치
    EXTRACTED_VALIDATED --> STAGE_VALIDATING: POST /validation/start
    STAGE_VALIDATING --> STAGING_VALIDATED: STAGING 지표 모두 PASS
    STAGING_VALIDATED --> PUBLISHING: POST /publish/claim (token CAS)
    PUBLISHING --> PUBLISHED: publish/result PUBLISHED
    PUBLISHED --> SUCCESS: TARGET 지표 모두 PASS

    CREATED --> FAILED_MANIFEST
    EXTRACTING --> FAILED_EXTRACT
    EXTRACTING --> FAILED_SNAPSHOT_EXPIRED
    CREATED --> TIMED_OUT: sweeper
    EXTRACTING --> TIMED_OUT: sweeper
    STAGE_VALIDATING --> FAILED_STAGE_VALIDATION
    STAGING_VALIDATED --> FAILED_PUBLISH
    PUBLISHING --> PUBLISH_UNKNOWN: 결과 불명 또는 sweeper
    PUBLISHING --> FAILED_PUBLISH
    PUBLISHED --> FAILED_TARGET_VALIDATION
```

모든 상태 변경은 API가 `WHERE run_id = :run_id AND status = <expected>` 조건의 `UPDATE ... RETURNING`으로 실행하고, 반환 행 수로 성공 여부를 판단한다. 중복 게시는 다음이 함께 막는다.

- active run partial unique index: 같은 업무키의 동시 run 차단
- run 행 잠금 + run 완료 CAS + `uq_load_dispatch_validate`: 검증 호출 예약 1회
- `STAGE_VALIDATING` CAS: 검증 flow 실행 1회
- publish token CAS: `INSERT OVERWRITE` 실행 1회

NiFi의 Primary Node 실행 제한에 의존하지 않는다. PG-40~60은 All Nodes에서 실행된다(2장).

### 17.1 CAS 결과 확인

가이드 초안에서는 NiFi `PutSQL`이 UPDATE 영향 행 수가 0이어도 `success`로 보내는 문제 때문에 CAS 결과를 별도로 확인해야 했다. 이제 CAS는 API 내부 코드가 반환 행으로 확인하고, NiFi에는 결과(`claimed`, `started`, `stageValidated`, `success`)만 돌려준다. NiFi는 이 boolean으로 분기한다.

| 전이 | API 엔드포인트 | NiFi 분기 attribute |
|---|---|---|
| partition `PENDING/RETRY → RUNNING` | `POST .../claim` | `$.claimed` |
| partition `RUNNING → SUCCESS`, run `EXTRACTING → EXTRACTED_VALIDATED` | `POST .../chunks` | 분기 없음(로그만) |
| run `EXTRACTED_VALIDATED → STAGE_VALIDATING` | `POST /validation/start` | `$.started` |
| run `STAGE_VALIDATING → STAGING_VALIDATED` | `POST /stage-validated` | `$.stageValidated` |
| run `STAGING_VALIDATED → PUBLISHING` | `POST /publish/claim` | `$.claimed` |
| run `PUBLISHING → PUBLISHED` | `POST /publish/result` | 2xx 여부 |
| run `PUBLISHED → SUCCESS` | `POST /success` | `$.success` |

API 쪽 구현과 동시성 테스트는 API 설계 9.5~9.6, 11장을 따른다.

---

## 18. NiFi 운영 설정

- Trigger, Coordinator: `Run Schedule=0 sec` 또는 입력 기반, `Concurrent Tasks=1`, `Execution=Primary Node`
- Worker: `Execution=All Nodes`, 입력 Connection Round Robin
- Control Receiver와 검증·게시 flow: `Execution=All Nodes`, `Concurrent Tasks=1`. 중복 방지는 API CAS가 담당
- DB pool 상한: `노드 수 × worker concurrent tasks + control 여유`가 Oracle 승인 세션 수를 넘지 않게 설정
- `InvokeHTTP` 동시 호출 수: `노드 수 × worker concurrent tasks`가 API 처리 용량과 관리 DB 연결 수 안에 들도록 API 인스턴스 수를 정한다(API 설계 9.10)
- Processor `Yield Duration`: DB/HDFS/API failure 폭주 방지를 위해 10~30초부터 시험
- Provenance: run/partition/chunk 상관 분석이 가능한 기간 유지. API 로그와 `X-Request-Id`, `run_id`로 대조
- Bulletin: ERROR/WARN 수집을 모니터링 시스템에 연계
- Parameter Context 변경 권한과 NiFi Policy를 운영자/개발자로 분리
- 민감 Parameter(`CONTROL.API.TOKEN`, DB 암호)는 버전관리 flow JSON에 평문으로 포함하지 않음. `InvokeHTTP`의 `Authorization`은 Sensitive 동적 속성으로 설정
- flow definition은 NiFi Registry 또는 조직 표준 Git 배포 절차로 승격. Load Control API와 API 계약(엔드포인트, 필드)을 함께 버전 관리한다

---

## 19. 구현 및 검증 순서

1. 관리 테이블(Alembic baseline)과 Load Control API 골격, 판정 트랜잭션, 동시성 테스트를 먼저 구현한다(API 설계 12장).
2. `PARTITION.COUNT=1`, 작은 기준 데이터로 PG-10 → PG-20 → API 보고까지 구현한다.
3. 경계값, NULL, 0건, Oracle 타입을 검증한다.
4. 2/4/8 partition으로 늘려 병렬성, API 응답 시간, run 잠금 대기, DB 부하를 측정한다.
5. outbox dispatcher와 PG-05 Control Receiver를 연결해 검증 flow 호출을 확인한다.
6. external staging DDL과 count/DQ를 구현한다.
7. 비운영 target에서 `INSERT OVERWRITE`와 `PUBLISH_UNKNOWN` 경로를 시험한다.
8. Target validation과 최종 상태를 연결한다.
9. 장애를 주입한다: NiFi 노드 종료, NiFi 재기동, API 인스턴스 종료, API worker 종료, 관리 DB 연결 차단, HDFS 오류, `ORA-01555`, 검증 호출 수신 실패.
10. API sweeper(`LCA_RECOVERY_MODE=FAIL`)와 보존/정리 flow를 활성화하고, NiFi 계정의 원장 쓰기 권한을 회수한다.

운영 승인 조건:

```text
파티션 하나가 실패하면 검증 flow 호출(dispatch)과 PutHive3QL 호출 건수는 0이다.
중복 보고, 동시 완료, API 재기동 후에도 run당 STAGE_VALIDATING 진입과 INSERT OVERWRITE는 각각 1회다.
NiFi 계정으로 load_run/load_partition/load_file/load_validation/load_dispatch를 변경할 수 없다.
API 장애 중 진행된 run은 성공으로 판정되지 않고, 복구 후 정상 판정되거나 TIMED_OUT으로 끝난다.
재발행을 켠 경우 동일 run_id와 snapshot_scn으로만 복구된다.
source = partition sum = staging = target 검증이 모두 PASS인 경우만 SUCCESS이다.
PUBLISH_UNKNOWN은 사람 또는 별도 reconciliation 없이 자동 재실행되지 않는다.
```

---

## 20. 용어집

### 플랫폼과 NiFi 구성요소

| 용어 | 정의 | 이 문서에서의 의미 |
|---|---|---|
| CFM | Cloudera Flow Management | NiFi 2.6.0을 포함하는 목표 플랫폼 버전은 CFM 4.12.0이다. |
| NiFi | Apache NiFi 데이터 흐름 자동화 플랫폼 | Sqoop을 대신하여 Oracle 병렬 조회, HDFS 기록, Hive 검증과 게시를 실행한다. 완료 판정은 하지 않는다. |
| Load Control API | 적재 상태 원장과 완료 판정을 담당하는 FastAPI 서비스 | 유일한 원장 writer. chunk 보고마다 완료를 판정하고 검증 flow를 호출한다. |
| FastAPI | Python 비동기 웹 프레임워크 | Load Control API 구현 프레임워크. Uvicorn/Gunicorn으로 실행한다. |
| Kylo | NiFi 기반 데이터 레이크 관리 플랫폼 | AS-IS에서 Kylo의 `ImportSqoop` Processor를 사용한다. |
| Sqoop | RDBMS와 Hadoop 간 대량 데이터 전송 도구 | TO-BE에서 제거하며 Mapper의 병렬 실행과 Job 완료 의미를 NiFi로 재구현한다. |
| Processor | NiFi Flow의 단일 처리 컴포넌트 | `ExecuteSQLRecord`, `PutHDFS`, `InvokeHTTP`, `HandleHttpRequest` 등이 해당한다. |
| `InvokeHTTP` | HTTP 요청을 보내고 응답으로 분기하는 Processor | NiFi→API 보고와 상태 전이 요청에 사용한다. |
| `HandleHttpRequest`/`HandleHttpResponse` | NiFi에서 HTTP 요청을 받고 응답하는 Processor 쌍 | PG-05가 API의 검증·재발행 호출을 받는다. |
| Process Group | 여러 Processor와 Connection을 묶은 논리 단위 | `PG-10 Coordinator`, `PG-20 Worker`처럼 책임별로 Flow를 분리한다. |
| FlowFile | NiFi에서 content와 attribute를 함께 운반하는 객체 | 데이터 chunk 또는 run/partition 제어 메시지를 전달한다. |
| Content | FlowFile가 가리키는 실제 데이터 | Parquet 데이터, SQL 문장 또는 제어용 JSON이 될 수 있다. |
| Attribute | FlowFile에 연결된 문자열 메타데이터 | `load.run.id`, `partition.id`, `record.count` 등 제어와 상관관계에 사용한다. |
| Connection | Processor 사이에서 FlowFile을 보관하는 Queue | Back Pressure, Prioritizer 및 cluster load balancing을 설정한다. |
| Controller Service | 여러 Processor가 공유하는 연결·직렬화 서비스 | JDBC pool, Record Reader/Writer, SSL Context, HTTP Context Map을 제공한다. |
| Parameter Context | Flow 설정값과 민감정보를 묶어 공급하는 NiFi 기능 | DB URL, table, concurrency, HDFS 경로와 timeout을 환경별로 관리한다. |
| Primary Node | NiFi cluster에서 단일 실행 Processor를 담당하는 선출 노드 | Trigger와 Coordinator가 실행된다. 중복 방지 수단으로 쓰지 않는다. |
| All Nodes | NiFi cluster의 모든 노드에서 실행되는 스케줄링 방식 | PG-20 Worker, PG-05 Control Receiver, PG-40~60이 실행된다. |
| Concurrent Tasks | 한 Processor가 동시에 실행할 수 있는 task 수 | Oracle 동시 JDBC session과 HDFS 쓰기 동시성을 결정한다. |
| Round Robin | FlowFile을 cluster 노드에 순환 분배하는 load balancing 전략 | Coordinator에서 PG-20 Worker로 가는 입력 Connection에만 적용한다. |
| Back Pressure | Queue 크기 또는 데이터량이 임계값에 도달하면 upstream 실행을 억제하는 기능 | HDFS 지연이나 DB 병목이 NiFi repository 고갈로 이어지는 것을 방지한다. |
| Provenance | FlowFile 처리 이력을 기록하는 NiFi 기능 | `run_id/partition_id/chunk_index`로 장애 경로를 추적한다. |
| Bulletin | Processor 또는 Controller Service의 경고·오류 알림 | Processor가 상세 오류 attribute를 제공하지 않을 때 운영 진단에 사용한다. |

### 실행, 분할과 상태 관리

| 용어 | 정의 | 이 문서에서의 의미 |
|---|---|---|
| Job | 반복 실행 가능한 하나의 적재 정의 | 원천·대상·컬럼·조건이 같은 논리 적재이며 `job_key`로 식별한다. |
| `job_key` | 적재 Job의 영구 식별자 | 예: `ORACLE_INSP_DTL_DAILY`; 업무일자와 분리한다. |
| `business_key` | 한 적재가 대상으로 하는 업무 범위 | 업무일자, 기준일 또는 대상 partition 값이다. |
| Run | Job이 한 번 실행된 인스턴스 | 고유한 `run_id`, snapshot SCN과 HDFS 격리 경로를 갖는다. |
| `run_id` | 한 Run을 식별하는 UUID | 모든 FlowFile, PostgreSQL 행, HDFS 경로와 로그의 최상위 상관키다. |
| Partition | 원천 조회를 병렬화하기 위해 나눈 데이터 범위 | `INSP_DTL_SEQ`의 하한 포함·상한 미포함 범위다. |
| `partition_id` | Run 내부 partition 식별자 | `0000`, `0001` 또는 NULL 전용 partition 값이다. |
| Chunk | 한 partition 결과를 파일 크기에 맞춰 나눈 단위 | 하나의 Parquet FlowFile 및 HDFS 파일과 대응한다. |
| Manifest | 처리 대상과 예상 결과를 기록한 영속 목록 | PostgreSQL의 `load_run`, `load_partition`, `load_file`이 완료 판정의 원장이며 API만 쓴다. |
| Control plane | 실행 상태와 완료·게시 결정을 관리하는 영역 | Load Control API(판정, outbox, sweeper)와 PostgreSQL Manifest가 해당한다. |
| Data plane | 실제 데이터를 읽고 변환하고 쓰는 영역 | Oracle 조회, Parquet 변환과 HDFS 기록이 해당한다. |
| Claim token | Worker가 partition 처리 소유권을 얻을 때 사용하는 UUID | API `POST .../claim`이 중복 Worker 실행을 막고, chunk 보고의 소유권 확인에도 쓴다. |
| Publish token | 최종 게시 소유권을 식별하는 UUID | API `POST /publish/claim`이 하나의 FlowFile만 `INSERT OVERWRITE`하게 한다. |
| CAS | Compare-And-Set; 기대 상태일 때만 값을 변경하는 방식 | `WHERE status='STAGING_VALIDATED'` 같은 조건부 UPDATE로 중복 상태 전이를 막는다. |
| Active Run Lock | 동일 Job과 업무키의 동시 실행을 막는 제약 | PostgreSQL partial unique index로 구현한다. |
| Heartbeat | 실행 중인 Run/Partition이 살아 있음을 나타내는 갱신 시각 | claim과 chunk 보고 때 API가 갱신하며, sweeper가 stale 작업을 판정하는 기준이다. |
| Stale | 일정 시간 heartbeat가 갱신되지 않은 상태 | 기본은 run `TIMED_OUT`. 재발행을 켜면 동일 SCN으로 claim을 회수해 재발행한다. |

### 데이터베이스와 저장소

| 용어 | 정의 | 이 문서에서의 의미 |
|---|---|---|
| SCN | Oracle System Change Number | 병렬 쿼리가 동일한 시점의 데이터를 읽도록 Run 시작 시 고정한다. |
| Flashback Query | 과거 SCN 또는 timestamp의 Oracle 데이터를 조회하는 기능 | 모든 source metric과 partition query에 동일한 `AS OF SCN`을 사용한다. |
| `ORA-01555` | 필요한 UNDO가 사라져 과거 snapshot을 읽을 수 없는 Oracle 오류 | 동일 Run의 부분 재시도를 금지하고 새 `run_id/SCN`으로 전체 재실행한다. |
| JDBC | Java Database Connectivity | NiFi가 Oracle, PostgreSQL 및 HiveServer2와 통신하는 인터페이스다. |
| Connection Pool | DB Connection을 재사용하고 동시 접속 수를 제한하는 서비스 | `CS_DBCP_ORACLE`, `CS_DBCP_META`, `CS_HIVE3_DBCP`로 구분한다. |
| Fetch Size | JDBC가 한 번에 가져오는 row 수에 대한 힌트 | Oracle 왕복 횟수와 NiFi memory 사용량을 조정한다. |
| HDFS | Hadoop Distributed File System | Oracle 추출 결과 Parquet와 `_SUCCESS` marker를 저장한다. |
| Simple Authentication | Kerberos 없이 OS 사용자명 기반으로 동작하는 Hadoop 인증 방식 | 이 프로젝트의 HDFS 인증 방식이며 NiFi OS 사용자의 POSIX/ACL 권한이 필요하다. |
| Effective User | HDFS가 요청 주체로 인식하는 사용자 | 비-Ker버 환경에서는 일반적으로 NiFi 프로세스 OS 사용자다. |
| Parquet | 컬럼 기반 파일 형식 | Oracle 추출 결과의 기본 HDFS 저장 형식이다. |
| Staging | 최종 게시 전 데이터를 격리하고 검증하는 임시 영역 | `run_id`별 HDFS 경로와 Hive external table로 구성한다. |
| External Table | 데이터 파일은 외부 경로에 두고 Hive가 metadata만 관리하는 테이블 | 검증 단계에서 해당 Run의 HDFS 경로만 참조한다. |
| Target Table | 사용자가 조회하는 최종 Hive 테이블 | 모든 사전 검증을 통과한 후에만 변경한다. |
| `INSERT OVERWRITE` | 대상 테이블 또는 partition의 기존 데이터를 새 결과로 교체하는 Hive DML | Publish token을 소유한 단일 FlowFile만 실행한다. |
| `_SUCCESS` | 데이터 쓰기 완료를 표시하는 빈 marker 파일 | 모든 partition과 file 검증이 끝난 후에만 Run root에 생성한다. |
| Write and Rename | 임시 파일을 완전히 쓴 뒤 최종 파일명으로 rename하는 방식 | 독자가 부분 파일을 읽는 것을 방지하는 PutHDFS 설정이다. |
| UPSERT | 행이 없으면 INSERT, 있으면 UPDATE하는 쓰기 방식 | 동일 chunk 재시도 시 `load_file`을 멱등하게 기록한다. |

### 완료 판정, 검증과 장애 처리

| 용어 | 정의 | 이 문서에서의 의미 |
|---|---|---|
| Barrier/Gate | 여러 병렬 작업이 모두 특정 상태에 도달할 때까지 다음 단계를 막는 장치 | API가 chunk 보고마다 Chunk→Partition→Run 순서로 판정한다. NiFi에는 대기 단계가 없다. |
| Outbox | 상태 변경과 같은 트랜잭션에 외부 호출 요청을 기록하는 패턴 | `load_dispatch`. run 완료와 검증 호출 예약을 원자적으로 묶는다. |
| Dispatch | outbox에 기록된 NiFi 호출 한 건 | `PENDING → SENT → ACKED`. 최대 시도 초과 시 `DEAD`. |
| Dispatcher | outbox를 읽어 NiFi로 전달하는 API worker 작업 | `LISTEN/NOTIFY`로 깨어나고 lease로 행을 선점한다. |
| Lease | 처리 중인 행을 일정 시간 다른 worker가 가져가지 못하게 하는 선점 방식 | dispatcher가 전송 동안 DB 트랜잭션을 열지 않게 한다. |
| Sweeper | stale·timeout 상태를 주기적으로 정리하는 API worker 작업 | 가이드 초안의 Recovery Monitor를 대체한다. |
| `Wait`/`Notify` | NiFi cache 기반 신호 대기 Processor | 가이드 초안에서 완료 wake-up에 썼으나 현재 설계에서는 쓰지 않는다. |
| Fragment | 한 ResultSet 또는 Record 묶음에서 파생된 FlowFile 집합 | `fragment.identifier/count/index`로 chunk의 소속과 순서를 식별한다. |
| `record.count` | Record Writer가 FlowFile에 기록한 row 수 | file manifest와 partition 실제 건수 합산에 사용한다. |
| DQ | Data Quality | count, schema, NULL, 중복, min/max, 업무 합계 및 hash 검증을 뜻한다. |
| Reconciliation | 서로 다른 처리 단계의 지표를 대조하는 작업 | Source=Partition 합계=Staging=Target인지 검증한다. |
| Idempotency | 같은 요청을 반복해도 최종 결과가 한 번 실행한 것과 같은 성질 | 결정적 HDFS 경로, file UPSERT, 같은 token 재요청 허용, CAS 상태 전이로 확보한다. 모든 API 상태 변경 호출의 전제다. |
| Retryable/Transient Error | 시간이 지나면 성공할 가능성이 있는 일시 오류 | connection reset, 일시적 HDFS 장애 등에 제한 재시도를 적용한다. |
| Non-retryable Error | 동일 입력으로 반복해도 해결되지 않는 오류 | SQL 문법, 권한, schema, 검증 오류와 `ORA-01555`가 해당한다. |
| Backoff | 재시도 사이의 대기시간을 점차 늘리는 방식 | DB/HDFS 장애 시 과도한 반복 호출을 막는다. |
| DLQ | Dead Letter Queue | PostgreSQL event 기록에 실패한 JSON 로그를 복구 가능하게 보관한다. |
| `PUBLISH_UNKNOWN` | Hive 게시 요청의 성공 여부를 확정할 수 없는 Run 상태 | timeout 후 자동 overwrite 재실행을 금지하고 target과 Hive 이력을 확인한다. |
| `FAILED_TARGET_VALIDATION` | 게시 후 Target 검증이 실패한 상태 | 자동 재추출·재게시하지 않고 중대 운영 오류로 처리한다. |
| `STAGE_VALIDATING` | 검증 flow가 시작된 Run 상태 | `/validation/start` CAS로 진입하며, 중복 검증 호출을 걸러 낸다. |

---

## 21. Processor 지원 근거

- CFM 4.12.0 supported processors: https://docs.cloudera.com/cfm/4.12.0/release-notes/topics/cfm-supported-processors.html
- `ExecuteSQLRecord`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.ExecuteSQLRecord/
- `PutSQL`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.PutSQL/
- `InvokeHTTP`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.InvokeHTTP/
- `HandleHttpRequest`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.HandleHttpRequest/
- `HandleHttpResponse`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.HandleHttpResponse/
- `AttributesToJSON`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.AttributesToJSON/
- `JoltTransformJSON`: https://nifi.apache.org/components/org.apache.nifi.processors.jolt.JoltTransformJSON/
- `SplitJson`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.SplitJson/
- FastAPI: https://fastapi.tiangolo.com/
- PostgreSQL `NOTIFY`: https://www.postgresql.org/docs/current/sql-notify.html
- PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
- `PutHDFS`: https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-hadoop-nar/1.28.0/org.apache.nifi.processors.hadoop.PutHDFS/
- `RetryFlowFile`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.RetryFlowFile/
- `LogMessage`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.LogMessage/
