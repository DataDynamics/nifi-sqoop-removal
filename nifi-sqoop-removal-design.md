# CFM 4.12.0 기반 Sqoop 제거 상세 설계

## 1. 설계 목표

기존 `ImportSqoop` 처리의 기능을 NiFi 2.x 기반 CFM 4.12.0으로 대체한다.

설계가 보장해야 할 핵심 조건은 다음과 같다.

1. Oracle 원천 데이터를 `INSP_DTL_SEQ` 기준으로 병렬 추출한다.
2. 어느 한 파티션이라도 최종 실패하면 해당 실행 전체를 실패로 처리한다.
3. 모든 파티션의 추출과 HDFS 기록이 검증된 후에만 Hive `INSERT OVERWRITE`를 실행한다.
4. 재시도, NiFi 재기동, 클러스터 노드 장애가 발생해도 중복 완료 신호나 부분 파일 때문에 잘못 게시하지 않는다.
5. 원천, HDFS staging, 임시 외부 테이블, 최종 테이블의 데이터 정합성을 단계별로 검증한다.
6. 실패한 실행의 파일은 최종 테이블에서 보이지 않으며, 동일 업무 건을 안전하게 재실행할 수 있다.

CFM 4.12.0은 Apache NiFi 2.6.0 기반이다. 이 설계에서는 CFM 4.12.0이 지원하는 표준 구성요소인 `ExecuteSQLRecord`, `PutHDFS`, `Wait`, `Notify`, `PutHive3QL` 또는 `PutClouderaHiveQL` 등을 사용한다.

---

## 2. 핵심 설계 원칙

### 2.1 실행 단위마다 불변의 `run_id`를 사용한다

한 번의 배치 실행마다 UUID 형식의 `run_id`를 발급한다. 모든 FlowFile, 관리 테이블 행, HDFS 경로, 파일명, 로그에 같은 `run_id`를 사용한다.

```text
job_key       = ORACLE_OWNER.TABLE_NAME:업무일자 또는 추출 조건
run_id        = 0199... UUID
snapshot_scn  = Oracle의 고정 SCN
partition_id  = 0000 ... N-1, 필요 시 NULL 파티션
```

서로 다른 실행은 절대로 같은 staging 경로를 공유하지 않는다.

```text
/data/stage/<job_key>/run_id=<run_id>/part=<partition_id>/chunk-<index>.parquet
```

### 2.2 제어 상태의 원장은 영속 DB에 둔다

NiFi Queue, Processor State, `Wait/Notify` 캐시는 작업 상태의 최종 원장으로 사용하지 않는다. 별도의 운영 메타데이터 DB에 작업과 파티션 상태를 저장한다. 기존 운영 DB가 있다면 사용할 수 있지만, 원천 업무 테이블과 분리된 스키마를 권장한다.

`Wait/Notify`는 병렬 작업이 끝났다는 신호를 빠르게 전달하는 용도로는 사용할 수 있다. 그러나 재시도 시 같은 파티션이 두 번 `Notify`될 수 있으므로, 게시 직전에는 반드시 관리 테이블을 다시 조회해 아래 조건을 확인한다.

```text
job.status = EXTRACTING
AND expected_partition_count = success_partition_count
AND failed_partition_count = 0
AND pending_partition_count = 0
```

### 2.3 추출 시점을 고정한다

병렬 JDBC Connection은 서로 다른 시각에 SQL을 시작한다. 일반적인 `READ COMMITTED`만 사용하면 원천이 변경되는 동안 파티션별로 서로 다른 상태를 읽을 수 있다.

가장 권장하는 방법은 작업 시작 시 Oracle SCN을 한 번 얻고, 모든 경계 조회, 원천 검증 조회 및 데이터 조회에 동일한 `AS OF SCN`을 사용하는 것이다.

```sql
SELECT current_scn
  FROM v$database;

SELECT COUNT(*) AS source_count,
       MIN(INSP_DTL_SEQ) AS min_seq,
       MAX(INSP_DTL_SEQ) AS max_seq,
       SUM(CASE WHEN INSP_DTL_SEQ IS NULL THEN 1 ELSE 0 END) AS null_seq_count
  FROM OWNER.SOURCE_TABLE AS OF SCN <snapshot_scn>
 WHERE <업무 추출 조건>;
```

필요 권한과 UNDO 보존 시간이 확보되어야 한다. 작업 도중 `ORA-01555`가 발생하면 새 SCN으로 일부 파티션만 재시도하지 않고 실행 전체를 실패시킨 뒤 새 `run_id`로 다시 시작한다.

Flashback Query를 사용할 수 없다면 다음 대안을 우선순위대로 적용한다.

1. Oracle에서 일관된 source snapshot/staging table을 먼저 생성하고 이를 병렬 조회한다.
2. 변경 불가능한 업무 마감 조건 또는 CDC high-watermark를 고정한다.
3. 최후 수단으로 원천 변경이 없는 배치 시간대를 사용한다.

단순히 추출 전후 `COUNT(*)`가 같다는 이유만으로 동일 스냅샷이라고 판단하면 안 된다.

### 2.4 게시 전까지 최종 테이블과 격리한다

모든 추출 결과는 `run_id`별 HDFS staging 디렉터리에 기록한다. 성공하지 않은 실행의 경로는 임시 외부 테이블만 참조하며 최종 테이블에서는 접근하지 않는다.

---

## 3. 전체 아키텍처

```text
[Primary Node Coordinator]
  1. 중복 실행 Lock 획득
  2. run_id, snapshot_scn 생성
  3. source count/min/max/DQ 측정
  4. partition manifest 생성
                 |
                 | Load-balanced connection (Round Robin)
                 v
[All Nodes Partition Workers, bounded concurrency]
  Partition FlowFile
    -> ExecuteSQLRecord (Oracle AS OF SCN + range predicate)
    -> Parquet/Avro chunks
    -> PutHDFS (run_id 전용 경로, 결정적 파일명)
    -> 파티션 건수/파일 검증
    -> partition status SUCCESS 또는 FAILED
                 |
                 v
[Primary Node Completion Controller]
  5. 영속 manifest에서 전체 상태 재조회
  6. source_count = extracted_count 확인
  7. HDFS _SUCCESS 생성
  8. run_id 경로를 참조하는 임시 external table 생성
  9. staging table COUNT/DQ 검증
 10. 검증 통과 시에만 INSERT OVERWRITE
 11. final table COUNT/DQ 검증
 12. SUCCESS 확정, 감사 로그 및 정리
```

제어 플로우는 `Primary Node only`, `Concurrent Tasks = 1`로 실행한다. 데이터 추출 플로우는 모든 NiFi 노드에서 수행하고 입력 Connection에 Round Robin Load Balance를 적용한다.

---

## 4. 관리 테이블

### 4.1 작업 실행 테이블

예시 논리 모델은 다음과 같다.

```sql
CREATE TABLE NIFI_LOAD_RUN (
    RUN_ID                    VARCHAR(64) PRIMARY KEY,
    JOB_KEY                   VARCHAR(200) NOT NULL,
    BUSINESS_KEY              VARCHAR(200),
    STATUS                    VARCHAR(30) NOT NULL,
    SNAPSHOT_SCN              DECIMAL(38,0),
    SOURCE_COUNT              BIGINT,
    SOURCE_NULL_SPLIT_COUNT   BIGINT,
    EXPECTED_PARTITION_COUNT  INTEGER,
    SUCCESS_PARTITION_COUNT   INTEGER DEFAULT 0,
    EXTRACTED_COUNT           BIGINT DEFAULT 0,
    STAGING_COUNT             BIGINT,
    TARGET_COUNT              BIGINT,
    STARTED_AT                TIMESTAMP NOT NULL,
    HEARTBEAT_AT              TIMESTAMP,
    EXTRACT_COMPLETED_AT      TIMESTAMP,
    PUBLISHED_AT              TIMESTAMP,
    COMPLETED_AT              TIMESTAMP,
    ERROR_CODE                VARCHAR(100),
    ERROR_MESSAGE             VARCHAR(4000),
    HDFS_RUN_PATH             VARCHAR(1000),
    RETRY_OF_RUN_ID           VARCHAR(64),
    VERSION_NO                INTEGER DEFAULT 0
);
```

동일 `JOB_KEY + BUSINESS_KEY`에 활성 실행이 하나만 존재하도록 unique 조건 또는 DB advisory lock을 둔다. 상태 변경은 `VERSION_NO` 또는 현재 상태 조건을 이용한 compare-and-set 방식으로 수행한다.

```sql
UPDATE NIFI_LOAD_RUN
   SET STATUS = 'PUBLISHING', VERSION_NO = VERSION_NO + 1
 WHERE RUN_ID = ?
   AND STATUS = 'STAGING_VALIDATED';
```

갱신 건수가 1일 때만 게시한다. 이 조건으로 중복 `INSERT OVERWRITE` 실행을 막는다.

### 4.2 파티션 manifest 테이블

```sql
CREATE TABLE NIFI_LOAD_PARTITION (
    RUN_ID               VARCHAR(64) NOT NULL,
    PARTITION_ID         VARCHAR(20) NOT NULL,
    LOWER_BOUND          DECIMAL(38,0),
    UPPER_BOUND          DECIMAL(38,0),
    UPPER_INCLUSIVE      CHAR(1) NOT NULL,
    IS_NULL_PARTITION    CHAR(1) DEFAULT 'N',
    STATUS               VARCHAR(20) NOT NULL,
    EXPECTED_ROW_COUNT   BIGINT,
    ACTUAL_ROW_COUNT     BIGINT,
    FILE_COUNT           INTEGER,
    BYTE_COUNT           BIGINT,
    ATTEMPT_COUNT        INTEGER DEFAULT 0,
    STARTED_AT           TIMESTAMP,
    COMPLETED_AT         TIMESTAMP,
    ERROR_CODE           VARCHAR(100),
    ERROR_MESSAGE        VARCHAR(4000),
    WORKER_NODE          VARCHAR(200),
    PRIMARY KEY (RUN_ID, PARTITION_ID)
);
```

선택적으로 파일 단위 manifest를 추가한다.

```text
NIFI_LOAD_FILE(run_id, partition_id, chunk_index, hdfs_path,
               record_count, byte_count, checksum, status)
```

이 테이블이 있으면 HDFS 기록 성공 여부와 재처리 범위를 더 정밀하게 추적할 수 있다.

### 4.3 상태 모델

```text
CREATED
  -> SNAPSHOT_FIXED
  -> EXTRACTING
  -> EXTRACTED_VALIDATED
  -> STAGING_VALIDATED
  -> PUBLISHING
  -> PUBLISHED
  -> TARGET_VALIDATED
  -> SUCCESS

각 단계 -> FAILED_EXTRACT | FAILED_STAGE_VALIDATION |
           FAILED_PUBLISH | FAILED_TARGET_VALIDATION | TIMED_OUT
```

`FAILED_*` 상태가 된 실행은 절대로 이후 성공 상태로 자동 전환하지 않는다. 늦게 끝난 파티션의 성공 이벤트도 해당 작업 상태가 `EXTRACTING`일 때만 반영한다.

---

## 5. 파티션 생성

### 5.1 기본 범위 방식

기존 Sqoop과 유사하게 `INSP_DTL_SEQ`의 최소·최대 범위를 N개로 나눈다.

```text
partition 0 : seq >= b0 AND seq < b1
partition 1 : seq >= b1 AND seq < b2
...
partition N-1: seq >= bN-1 AND seq <= max_seq
```

중간 파티션은 하한 포함, 상한 미포함으로 통일해 중복과 누락을 방지한다. 마지막 파티션만 최댓값을 포함한다.

`INSP_DTL_SEQ`가 NULL일 가능성이 있으면 다음 중 하나를 명시적으로 선택한다.

- 업무적으로 NULL이 불가능: 사전 검증에서 NULL이 1건이라도 있으면 전체 실패
- NULL이 정상 데이터: `INSP_DTL_SEQ IS NULL` 전용 파티션을 하나 더 생성

NULL 행을 아무 처리 없이 범위 조회에서 제외하면 source count와 staging count가 불일치한다.

### 5.2 데이터 쏠림 개선

MIN/MAX 균등 범위는 값 분포가 치우치거나 값 사이가 매우 듬성듬성하면 파티션별 처리량이 불균등할 수 있다. 초기에는 Sqoop과 동일한 경계로 기준 성능을 측정하고, 편차가 큰 경우 다음 방식을 사용한다.

1. Oracle 통계 또는 히스토그램을 활용한 경계
2. `NTILE(N) OVER (ORDER BY INSP_DTL_SEQ)` 기반의 사전 계산
3. 업무 날짜 등 선행 조건과 `INSP_DTL_SEQ` 범위를 조합한 복합 분할

파티션별 예상 건수를 동일 SCN에서 미리 계산해 manifest에 저장하면 skew와 누락을 함께 검증할 수 있다. 경계 산정 쿼리 비용이 큰 경우에는 전체 건수만 사전 계산하고 파티션 실제 건수의 합을 비교한다.

### 5.3 파티션 FlowFile 속성

```text
load.run.id
load.job.key
load.snapshot.scn
load.partition.id
load.partition.lower
load.partition.upper
load.partition.upper.inclusive
load.partition.expected.rows
load.hdfs.path
load.attempt
```

테이블명, 컬럼 목록, WHERE 템플릿은 Parameter Context에서 관리하고 자유 입력 FlowFile 속성으로 받지 않는다. 값 조건은 JDBC bind parameter를 사용해 SQL injection과 문자열 변환 문제를 줄인다.

---

## 6. NiFi Process Group 상세

### PG-01: Run Coordinator

실행 위치는 Primary Node only, 동시 실행 수는 1이다.

```text
Schedule/Start Event
 -> 중복 실행 Lock/활성 Run 조회
 -> run_id 생성
 -> Oracle snapshot_scn 조회
 -> 동일 SCN source metrics 조회
 -> 관리 테이블에 Run 및 Partition manifest INSERT
 -> 파티션 레코드를 FlowFile로 분리
 -> 데이터 처리 Connection으로 전송
 -> 완료 확인용 Control FlowFile 생성
```

주요 사전 검증은 다음과 같다.

- source schema와 대상 schema 호환 여부
- `INSP_DTL_SEQ` 데이터 타입과 인덱스
- source count가 0일 때의 업무 정책
- min/max 및 NULL 건수
- SCN과 예상 최대 작업시간 대비 UNDO 보존 가능성
- HDFS staging 경로가 다른 활성 run과 겹치지 않는지
- 대상 테이블에 대한 동일 업무키의 다른 게시 작업이 없는지

0건은 기술적으로 성공할 수 있지만 `INSERT OVERWRITE` 시 기존 데이터를 비우게 될 수 있으므로, 업무 정책을 반드시 분리한다. 기본 권장은 `ALLOW_EMPTY_SOURCE=false`이고 0건이면 게시하지 않고 검토 상태로 보낸다.

### PG-02: Parallel Oracle Extract

```text
Partition FlowFile
 -> 작업 상태가 EXTRACTING인지 확인
 -> partition RUNNING 선점(CAS)
 -> ExecuteSQLRecord
 -> RouteOnAttribute(fragment.index)
 -> 각 데이터 chunk에 결정적 filename 설정
 -> PutHDFS
 -> chunk/file audit
 -> 파티션 barrier
 -> 파티션 검증
 -> partition SUCCESS
```

개념 SQL은 다음과 같다.

```sql
SELECT <고정 컬럼 목록>
  FROM OWNER.SOURCE_TABLE AS OF SCN <검증된 snapshot_scn>
 WHERE <고정 업무 추출 조건>
   AND INSP_DTL_SEQ >= ?
   AND INSP_DTL_SEQ < ?;
```

마지막 파티션은 `<= ?`, NULL 파티션은 `IS NULL`을 사용한다. `SELECT *` 대신 컬럼 순서와 타입이 고정된 명시적 목록을 사용한다.

#### ExecuteSQLRecord 권장 설정

| 항목 | 권장값/원칙 |
|---|---|
| Record Writer | ParquetRecordSetWriter 또는 호환되는 Avro Writer |
| Fetch Size | Oracle JDBC 및 행 크기 기준 부하 시험 후 결정 |
| Max Rows Per FlowFile | 목표 HDFS 파일 크기(예: 256~512 MiB)에 맞는 행 수 |
| Output Batch Size | `0` |
| Query Timeout | 정상 최대시간보다 크되 무한대는 피함 |
| Concurrent Tasks | 노드 수와 DB 허용 세션을 반영해 제한 |

`Output Batch Size=0`으로 두면 쿼리의 전체 ResultSet 처리가 끝나야 결과 FlowFile이 downstream으로 전달되고, `fragment.count/index/identifier`를 사용할 수 있다. 따라서 JDBC 조회가 중간 실패한 상태에서 일부 chunk만 HDFS로 흐르는 것을 막기 쉽다. 매우 큰 파티션 때문에 세션 유지와 repository 압력이 커지면 파티션 자체를 더 작게 나누는 방식을 우선 적용한다.

#### PutHDFS 권장 설정

```text
Directory  = /data/stage/<job_key>/run_id=${load.run.id}/part=${load.partition.id}
filename   = part-${load.partition.id}-${fragment.index}.parquet
Writing Strategy = Write and rename
Conflict Resolution = replace (run_id 전용의 결정적 파일명에 한함)
```

`Write and rename`은 부분 파일 노출을 방지한다. 같은 `run_id/partition/chunk`의 재시도는 동일 스냅샷의 같은 결과를 다시 쓰므로 `replace`로 멱등 처리할 수 있다. 임의 파일명이나 공유 디렉터리에서 `replace`를 사용해서는 안 된다.

### PG-03: Partition Barrier and Validation

`ExecuteSQLRecord`가 여러 chunk를 생성하면 한 파티션의 모든 chunk가 HDFS에 기록된 후에만 파티션을 성공 처리해야 한다.

구현 예시는 다음과 같다.

1. `fragment.index=0`인 결과로 비어 있는 partition control FlowFile을 하나 만든다.
2. 모든 chunk는 `PutHDFS success` 후 `Notify`한다.
3. 신호 키는 `${load.run.id}:${load.partition.id}`, 목표 개수는 `${fragment.count}`이다.
4. `Wait`가 해제되면 HDFS 성공 chunk 수와 row count 합계를 검증한다.
5. manifest의 `EXPECTED_ROW_COUNT`와 실제 합계가 같을 때만 `SUCCESS`로 갱신한다.

행 수 합계는 다음 중 하나로 관리한다.

- 권장: 파일 audit 테이블에 chunk별 `record.count`를 멱등 upsert하고 SQL `SUM(record_count)`로 계산
- 보조: `Notify` counter delta에 `record.count`를 사용

최종 판정은 캐시 counter가 아니라 파일 audit 또는 manifest를 사용한다. `Wait expired`, HDFS 실패, 행 수 불일치는 파티션 실패로 기록한다.

### PG-04: Global Completion Controller

각 파티션 성공 시 `Notify(run_id)`를 보낼 수 있지만, Coordinator는 해제된 후 다음 SQL과 동등한 조건을 다시 확인한다.

```sql
SELECT COUNT(*) AS total_count,
       SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
       SUM(CASE WHEN status LIKE 'FAILED%' THEN 1 ELSE 0 END) AS failed_count,
       SUM(COALESCE(actual_row_count, 0)) AS extracted_count
  FROM NIFI_LOAD_PARTITION
 WHERE run_id = ?;
```

완료 조건은 다음과 같다.

```text
total_count = expected_partition_count
success_count = expected_partition_count
failed_count = 0
extracted_count = source_count
```

하나라도 만족하지 않으면 `INSERT OVERWRITE`를 실행하지 않는다. `FAILED` 신호는 즉시 실행 전체를 실패 처리할 수 있고, 완료 timeout도 둔다.

### PG-05: HDFS Staging and Hive External Table

전체 추출 검증 후에만 빈 `_SUCCESS` 파일을 생성한다. 임시 외부 테이블 이름에도 `run_id`의 안전한 축약값을 포함한다.

```sql
CREATE EXTERNAL TABLE STG_DB.T_<JOB>_<RUN_SUFFIX> (
    ...
)
STORED AS PARQUET
LOCATION '/data/stage/<job_key>/run_id=<run_id>';
```

그 후 Hive에서 다음을 검증한다.

```sql
SELECT COUNT(*) FROM STG_DB.T_<JOB>_<RUN_SUFFIX>;
SELECT COUNT(*) - COUNT(INSP_DTL_SEQ) AS null_key_count ...;
SELECT COUNT(*) - COUNT(DISTINCT <business_key>) AS duplicate_key_count ...;
SELECT MIN(INSP_DTL_SEQ), MAX(INSP_DTL_SEQ), <업무 합계> ...;
```

`staging_count = source_count = extracted_count`이고 추가 품질 규칙이 모두 통과해야 상태를 `STAGING_VALIDATED`로 바꾼다.

### PG-06: Publish

상태를 조건부로 `PUBLISHING`으로 선점한 하나의 FlowFile만 다음 HiveQL을 수행한다.

```sql
INSERT OVERWRITE TABLE TARGET_DB.TARGET_TABLE
SELECT <명시적 컬럼 목록>
  FROM STG_DB.T_<JOB>_<RUN_SUFFIX>;
```

파티션 대상이면 반드시 덮어쓸 대상 파티션을 업무키로 제한한다.

```sql
INSERT OVERWRITE TABLE TARGET_DB.TARGET_TABLE
PARTITION (BASE_DT = '<validated_business_date>')
SELECT <non_partition_columns>
  FROM STG_DB.T_<JOB>_<RUN_SUFFIX>;
```

Hive DDL/DML 실행은 환경의 인증 방식에 맞춰 `PutHive3QL` 또는 CFM의 `PutClouderaHiveQL`을 사용한다. SQL 성공 relationship만 다음 단계로 연결하고 failure는 `FAILED_PUBLISH`로 보낸다.

주의: 일반 external table에 대한 `INSERT OVERWRITE`는 사용 중인 Hive/파일시스템/테이블 형식에 따라 최종 독자가 부분 상태나 실패 영향을 볼 가능성을 별도 검증해야 한다. 더 강한 게시 원자성이 필요하면 다음 방식이 우선이다.

```text
run_id별 완성된 target shadow 경로/테이블 생성
 -> target과 동일 검증
 -> 메타스토어의 partition location 또는 table pointer를 단일 전환
 -> 이전 버전 경로는 보존 후 지연 삭제
```

Iceberg 테이블을 사용할 수 있다면 스냅샷 커밋 방식도 검토한다. 기존과 동일한 `INSERT OVERWRITE`를 유지해야 한다면 게시 시간 동안 단일 writer lock을 유지하고, 실패 시 복구할 이전 버전/파티션 백업 정책을 둔다.

### PG-07: Target Validation

게시 성공은 최종 성공이 아니다. 동일한 업무 범위로 최종 테이블을 다시 조회한다.

```text
target_count = staging_count = extracted_count = source_count
AND target DQ metrics = staging/source DQ metrics
```

검증을 통과하면 `SUCCESS`로 확정한다. 실패하면 `FAILED_TARGET_VALIDATION`으로 남기고 자동으로 source를 다시 읽거나 다시 overwrite하지 않는다. 이전 스냅샷 복원 또는 검토가 필요한 중대 장애로 알린다.

---

## 7. 검증 체계

건수 비교만으로는 같은 개수의 누락과 중복이 서로 상쇄되는 오류를 찾을 수 없다. 다음 계층을 적용한다.

### Level 1: 필수 기술 검증

- source count
- 각 파티션 예상/실제 count
- `SUM(partition actual count) = source count`
- 모든 HDFS chunk 기록 성공
- staging external table count
- target table의 해당 업무 범위 count
- 스키마 컬럼 수, 컬럼명, 타입/precision/scale
- 변환 오류 및 reject count = 0

### Level 2: 키 및 범위 검증

- `INSP_DTL_SEQ` NULL 건수
- 업무 PK NULL 건수
- 업무 PK 중복 건수
- split 컬럼 min/max
- 업무 일자별 또는 주요 코드별 group count

### Level 3: 내용 검증

- 금액/수량 컬럼의 `SUM`, `MIN`, `MAX`
- 주요 상태코드별 건수
- 안정된 canonical 표현에 대한 hash aggregate

Oracle과 Hive의 문자열, NULL, timestamp, decimal 표현이 다르므로 단순 문자열 연결 hash는 사전에 canonical 규칙을 정의해야 한다. 예를 들어 timestamp timezone, decimal scale, CHAR trailing space, NULL 토큰을 명시한다. cross-engine hash 일치 구현이 어렵다면 주요 업무 집계와 키 기반 표본 대조를 함께 사용한다.

### 검증 결과 저장

각 지표를 다음 형태로 감사 테이블에 저장한다.

```text
NIFI_LOAD_VALIDATION(
  run_id, stage, metric_name, source_value, target_value,
  tolerance, result, measured_at, query_version
)
```

검증 SQL 버전도 기록해야 나중에 같은 기준으로 재현할 수 있다.

---

## 8. 실패 및 재시도 정책

| 실패 지점 | 처리 | 전체 결과 |
|---|---|---|
| SCN/경계/원천 지표 조회 실패 | 제한 재시도 후 run 실패 | 게시 금지 |
| 한 Oracle 파티션 일시 오류 | 해당 파티션만 제한 재시도 | 재시도 소진 시 run 실패 |
| `ORA-01555` | 재시도하지 않고 즉시 실패 | 새 run_id/SCN 필요 |
| PutHDFS 일시 오류 | 같은 결정적 경로로 chunk 재시도 | 소진 시 run 실패 |
| 파티션 row count 불일치 | 파티션 및 run 실패 | 게시 금지 |
| Wait timeout | DB manifest 재조회 후 미완료면 timeout 실패 | 게시 금지 |
| NiFi 노드 장애 | stale RUNNING 파티션을 회수해 재발행 | 동일 SCN 유효 시 계속 |
| NiFi 전체 재기동 | 관리 테이블에서 미완료 run 복구 | 캐시만 믿지 않음 |
| staging Hive 검증 실패 | run 실패, 경로 격리 | 게시 금지 |
| INSERT OVERWRITE 실패 | publish 실패, 자동 성공 전환 금지 | 복구 정책 수행 |
| target 사후 검증 실패 | 중대 오류 알림 및 격리 | 성공 처리 금지 |

재시도는 `RetryFlowFile` 등을 사용해 횟수를 제한하고 지수형 backoff를 적용한다. 인증 실패, SQL 문법, 스키마 불일치, 권한 오류, `ORA-01555`와 같은 비일시 오류는 반복 재시도하지 않는다.

한 파티션 실패 후 이미 실행 중인 다른 파티션을 즉시 강제 취소하기는 어렵다. 다른 파티션이 늦게 성공하더라도 실패한 run 경로에만 기록되게 하고, 관리 테이블의 run 상태가 `EXTRACTING`이 아니면 성공 집계와 게시를 진행하지 않는다.

---

## 9. 재기동 복구

Primary Node에서 별도 Recovery Monitor를 주기적으로 실행한다.

```text
1. EXTRACTING이면서 heartbeat가 임계시간을 넘은 run 조회
2. manifest의 PENDING 또는 stale RUNNING 파티션 조회
3. snapshot SCN이 아직 사용 가능한지 확인
4. 사용 가능하면 같은 partition FlowFile 재발행
5. 불가능하면 run을 FAILED_SNAPSHOT_EXPIRED로 종료
6. PUBLISHING 상태가 오래된 건은 Hive job/history와 target 상태를 조회하고 수동/자동 복구 정책 적용
```

완료 여부가 불명확한 `INSERT OVERWRITE`를 단순 재실행하지 않는다. 먼저 Hive 실행 결과와 대상 검증을 확인한다.

---

## 10. 병렬도와 자원 제어

NiFi의 설정값만 보고 실제 Oracle 세션 수를 과소평가하면 안 된다.

```text
최대 추출 세션 수
≈ NiFi 노드 수 × 노드별 ExecuteSQLRecord Concurrent Tasks

전체 Oracle 세션
≈ 추출 세션 + 제어/검증 세션 + 재시도 여유
```

권장 제약은 다음과 같다.

- DBCP/HikariCP 최대 Connection 수로 상한을 강제한다.
- 파티션 Queue에 object/data size Back Pressure를 설정한다.
- `ExecuteSQLRecord` 뒤 Queue도 HDFS 지연을 견딜 수 있도록 제한한다.
- Oracle의 CPU, I/O, active session, undo 사용량과 NiFi repository/HDFS 처리량을 함께 관찰한다.
- `P=1, 2, 4, 8...` 순서로 부하 시험하고 가장 느린 파티션 시간을 기록한다.
- 파티션 수는 worker 동시성보다 크게 잡을 수 있지만 지나치게 작은 HDFS 파일을 만들지 않는다.

기존 Sqoop mapper 수가 8이었다고 해서 NiFi 각 노드에 Concurrent Tasks 8을 설정하면 안 된다. 3노드라면 최대 24개의 쿼리가 동시에 실행될 수 있다.

---

## 11. 보안 및 운영

- Oracle/Hive/HDFS 자격증명은 Parameter Provider 또는 민감 Parameter로 관리한다.
- Oracle 테이블명과 컬럼명은 승인된 Parameter Context에서만 가져온다.
- HDFS/Hive는 Kerberos service account와 최소 권한 Ranger policy를 사용한다.
- HDFS run 경로에는 업무별 ACL을 적용한다.
- 로그에는 비밀번호와 민감 데이터 값을 기록하지 않는다.
- 모든 오류에는 `run_id`, `partition_id`, processor, attempt, error class를 포함한다.
- 성공 staging은 보존기간 후 삭제하고 실패 staging은 조사기간 동안 격리 보존한다.
- 임시 external table도 성공/실패 정책에 따라 DROP하되 데이터 경로 삭제와 분리한다.

권장 모니터링 지표는 다음과 같다.

```text
run elapsed time, pending/running/failed partition count
partition rows/sec, bytes/sec, skew ratio(max duration / median duration)
Oracle active JDBC sessions, query time, ORA errors, undo pressure
NiFi queue count/bytes, back pressure, repository usage
HDFS write latency/failure, file count/average size
validation mismatch, publish duration, recovery count
```

---

## 12. 구현 선택 요약

| 영역 | 권장 선택 |
|---|---|
| 실행 조정 | Primary Node only Coordinator + 영속 관리 테이블 |
| 원천 일관성 | 모든 쿼리에 동일 Oracle Flashback SCN |
| 분할 | `INSP_DTL_SEQ` half-open range + 명시적 NULL 정책 |
| 병렬 처리 | Load-balanced FlowFiles + bounded `ExecuteSQLRecord` |
| 파일 형식 | 기존 Hive schema와 호환되는 Parquet 우선 |
| HDFS 쓰기 | run 격리 경로 + 결정적 파일명 + write-and-rename |
| 파티션 완료 | fragment barrier + 영속 file/partition audit 확인 |
| 전체 완료 | manifest의 모든 파티션 SUCCESS 및 count 합계 일치 |
| 임시 테이블 | run_id 경로만 참조하는 external table |
| 게시 | 검증 후 단일 `INSERT OVERWRITE`, CAS로 중복 방지 |
| 최종 성공 | target 사후 검증까지 통과한 경우에만 SUCCESS |
| 재처리 | 기존 run을 수정하지 않고 새 run_id 사용 |

---

## 13. 필수 테스트 시나리오

1. 정상 1/4/8/16 파티션 실행 및 결과 동일성
2. `INSP_DTL_SEQ` 최솟값/최댓값/경계값 중복·누락 확인
3. split 컬럼 NULL 데이터 처리
4. 빈 원천 데이터의 게시 차단 정책
5. 심한 data skew에서 timeout과 처리량 확인
6. 한 Oracle 파티션을 의도적으로 실패시켜 전체 publish 미실행 확인
7. 일부 PutHDFS 성공 후 한 chunk 실패 및 멱등 재시도
8. 동일 partition success 이벤트 중복 전달 시 중복 완료 방지
9. Worker NiFi 노드 강제 종료 후 파티션 회수
10. 전체 NiFi 재기동 후 관리 테이블 기반 복구
11. `Wait/Notify` 캐시 초기화 후에도 잘못 publish되지 않음 확인
12. 추출 중 Oracle 데이터 변경 시 동일 SCN 결과 확인
13. 의도적인 `ORA-01555`에서 전체 run 실패 확인
14. staging count 불일치 시 overwrite 미실행 확인
15. Hive `INSERT OVERWRITE` 실패 시 대상 복구 절차 확인
16. publish 직전 중복 Coordinator 실행 시 한 건만 CAS 성공 확인
17. target count는 같지만 키 중복/누락이 있는 데이터의 Level 2/3 검출
18. Kerberos ticket 갱신, Ranger 권한 거부, HDFS quota 초과 시험

운영 전 합격 기준은 최소한 다음과 같다.

```text
어떤 단일 파티션/노드/HDFS write 실패에서도 INSERT OVERWRITE가 실행되지 않는다.
재시도 및 재기동으로 같은 run의 파일 또는 성공 카운트가 중복되어도 오게시되지 않는다.
동일 SCN 기준 source/staging/target 검증 결과가 일치한다.
실패 run은 성공 run과 경로 및 상태가 완전히 격리된다.
```

---

## 14. 구현 전 확정할 항목

다음 값이 정해지면 Processor별 Property와 실제 SQL을 확정할 수 있다.

- Oracle 버전, Flashback Query 권한, 예상 UNDO 보존시간
- 원천 테이블 DDL, PK, `INSP_DTL_SEQ` 타입/NULL 가능 여부/인덱스/분포
- 전체 건수, 평균 행 크기, 일 최대 데이터량, 목표 처리시간
- Full load인지 업무일자/조건별 load인지
- 현재 Sqoop의 실제 query, where, boundary-query, mapper 수, 파일 형식
- HDFS 배포판과 인증 방식, 목표 파일 크기
- Hive 버전, target table DDL, external/managed/ACID/Iceberg 여부, partition 구조
- `INSERT OVERWRITE` 대상이 전체 테이블인지 특정 partition인지
- 원천 0건 허용 여부와 허용 시 overwrite 정책
- 건수 외에 반드시 비교할 업무 합계/키/코드별 지표
- NiFi 클러스터 노드 수와 Oracle 허용 동시 세션 수
- 운영 메타데이터 DB 위치와 보존기간

---

## 15. 참고 자료

- Cloudera CFM 4.12.0 supported processors: https://docs.cloudera.com/cfm/4.12.0/release-notes/topics/cfm-supported-processors.html
- Cloudera CFM 4.12.0 download/version information: https://docs.cloudera.com/cfm/4.12.0/release-notes/topics/cfm-download-locations.html
- Apache NiFi `ExecuteSQLRecord`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.ExecuteSQLRecord/
- Apache NiFi `GenerateTableFetch`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.GenerateTableFetch/
- Apache NiFi `Wait`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.Wait/
- Apache NiFi `Notify`: https://nifi.apache.org/components/org.apache.nifi.processors.standard.Notify/
- Apache NiFi `PutHDFS`: https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-hadoop-nar/1.28.0/org.apache.nifi.processors.hadoop.PutHDFS/
- Oracle Flashback Query/read consistency: https://docs.oracle.com/cd/B28359_01/server.111/b28318/consist.htm
