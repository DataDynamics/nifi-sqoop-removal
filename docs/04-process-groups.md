# Process Group별 동작 원리와 검증

Processor 이름과 속성의 최종 기준은 `nifi-flow/deploy_job_flow.py`다. 아래 검증은 비운영 데이터로 실행하고,
NiFi queue, bulletin, provenance, API 원장을 함께 확인한다.

## 1. 공통 구현 규칙

### 1.1 FlowFile attribute

주요 attribute는 다음과 같다.

| Attribute | 생성 위치 | 용도 |
|---|---|---|
| `load.job.key` | PG-00/PG-05 | Job 식별 |
| `load.business.key` | PG-00/API 응답 | 업무 범위 |
| `load.run.id` | PG-10/API worker 본문 | run 추적 |
| `load.snapshot.scn` | PG-10/API 재발행 본문 | 같은 시점 Oracle 조회 |
| `load.hdfs.path` | PG-10/API 응답 | run 전용 HDFS 경로 |
| `load.stage.table` | PG-10/API 응답 | run 전용 staging table |
| `partition.id`, `partition.lower`, `partition.upper` | manifest/재발행 | 파티션 범위 |
| `partition.claim.token` | PG-20 | 처리 소유권 |
| `publish.token` | PG-50 | 게시 소유권 |
| `load.stage` | 각 단계 입구 | PG-90 실패 분류 |
| `api.response` | InvokeHTTP | API 판정과 오류 본문 |

### 1.2 API 호출

- `ReplaceText`로 요청 JSON을 만든 뒤 `InvokeHTTP`를 호출한다.
- Authorization은 sensitive dynamic property로 `#{CONTROL.API.AUTHORIZATION}`만 참조한다.
- `X-Request-Id=${UUID()}`, `X-Run-Id=${load.run.id}`를 보낸다.
- 5xx와 연결 실패는 5회 재시도하며, 4xx는 재시도하지 않고 `errors`로 보낸다.
- 상태 변경 API는 멱등이므로 같은 FlowFile 재시도가 안전하다.

### 1.3 오류 경로

각 PG의 단계 입구에서 `load.stage`를 정한다. 실패 relationship은 해당 PG의 `errors` Output Port로 모이고,
상위 Job PG에서 PG-90으로 연결된다.

## 2. PG-00 Trigger

### 목적

업무일자를 가진 빈 FlowFile 하나로 run을 시작한다. 클러스터 중복 생성을 막기 위해 Trigger는 Primary
Node에서만 실행하며 배포 직후 disabled다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 00 | `00_Generate_Trigger` | 스케줄 또는 Run Once로 빈 FlowFile 생성 |
| 01 | `01_Set_Trigger_Attributes` | Job key, business key, `load.stage=RUN_CREATE` 설정 |
| 02 | `02_Validate_Trigger` | 업무일자가 `YYYY-MM-DD` 형식이면 `start-run`, 아니면 `errors` |

업무일자는 이후 SQL에 들어가므로 02가 입력 경계다. 형식 검사만 수행하므로 실제 존재하는 날짜인지까지
검증하지는 않는다.

### 관련 설정

- `JOB.KEY`
- `BUSINESS.KEY`
- 00의 scheduling period와 Primary Node 실행 설정

### 단계 검증

1. 00이 disabled인지 확인한다.
2. 올바른 `BUSINESS.KEY`로 Run Once 후 PG-10 입력 queue로 FlowFile 하나만 이동하는지 확인한다.
3. 두 NiFi 노드에서 run이 중복 생성되지 않는지 API `load_run`을 확인한다.
4. 시험 환경에서 잘못된 형식을 넣으면 PG-90에 `RUN_CREATE_FAILED` 이벤트만 남고 run은 생성되지 않아야 한다.

## 3. PG-10 Run Coordinator

### 목적

run을 만들고 Oracle SCN을 고정한 뒤, 같은 SCN에서 원천 지표와 파티션 manifest를 계산해 API에 등록한다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 11 | `11_Build_Run_Body` | run 생성 JSON 작성 |
| 12 | `12_Create_Run` | `POST /v1/runs` |
| 13 | `13_Set_Run_Attrs` | run ID, HDFS 경로, staging table 추출; stage=`MANIFEST` |
| 14 | `14_Query_Current_SCN` | Oracle 현재 SCN 조회 |
| 15 | `15_Extract_SCN` | SCN을 attribute로 추출 |
| 16 | `16_Query_Source_Manifest` | 원천 지표, 경계, 파티션 예상 건수를 같은 SCN에서 계산 |
| 17 | `17_Build_Manifest_Body` | Oracle JSON 배열을 API manifest 형식으로 변환 |
| 18 | `18_Register_Manifest` | `POST /v1/runs/{id}/manifest` |
| 19 | `19_Split_Dispatch_Partitions` | 실행 대상 파티션을 FlowFile로 분리 |
| 20 | `20_Extract_Partition_Attrs` | 경계와 예상 건수를 attribute로 추출 |

### 동작 원리

14의 SCN은 16과 모든 PG-20 query에서 재사용된다. 16은 한 SQL 안에서 다음을 계산한다.

- `SOURCE_COUNT`
- split NULL 수, min, max
- 금액 합계, timestamp min/max
- `PARTITION.COUNT`개의 연속 범위
- 각 범위의 예상 row count

API는 manifest 등록 시 파티션 ID 중복, 계획 수, 예상 건수 합계, 0건 허용, NULL, 범위 연속성, 마지막
상한 포함 여부를 검사한다. 위반하면 `FAILED_MANIFEST`로 commit한 뒤 422를 반환한다.

### 관련 설정

- `JOB.KEY`, `BUSINESS.KEY`, `ALLOW.EMPTY.SOURCE`
- `HDFS.STAGE.ROOT`, `HIVE.STAGE.TABLE.PREFIX`
- `SRC.OWNER`, `SRC.TABLE`, `SRC.BASE.WHERE`, `SRC.SPLIT.COLUMN`
- `PARTITION.COUNT`
- `DQ.AMOUNT.COLUMN`, `DQ.TIMESTAMP.COLUMN`
- Oracle pool/driver 설정

### 단계 검증

API/DB:

```sql
SELECT run_id, status, snapshot_scn, source_count, expected_partition_count,
       success_partition_count, hdfs_run_path, stage_table_name
  FROM nifi_ops.load_run
 ORDER BY started_at DESC LIMIT 1;

SELECT partition_id, lower_bound, upper_bound, upper_inclusive,
       expected_row_count, status
  FROM nifi_ops.load_partition
 WHERE run_id = '<run_id>'
 ORDER BY partition_id;
```

검증 기준:

- run은 `EXTRACTING`이다.
- 파티션 수가 `PARTITION.COUNT`와 같다.
- 예상 건수 합계가 `source_count`와 같다.
- 경계가 연속이고 마지막 파티션만 상한을 포함한다.
- 0건 파티션은 이미 `SUCCESS`이며 PG-20 queue로 가지 않는다.
- `SOURCE` 지표에 원천 건수/금액/시각 범위가 저장된다.

Oracle의 독립 쿼리로 같은 SCN의 건수를 비교한다.

```sql
SELECT COUNT(*)
  FROM APP.INSP_DTL AS OF SCN <snapshot_scn>
 WHERE BASE_DT = DATE '2026-09-28';
```

## 4. PG-20 Extract Worker

### 목적

각 파티션의 소유권을 얻은 worker 하나만 Oracle을 조회하고, 결과를 Parquet chunk로 HDFS에 기록한 뒤
API에 보고한다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 30 | `30_Set_Claim_Token` | UUID claim token 생성, stage=`EXTRACT` |
| 31 | `31_Build_Claim_Body` | token과 worker node JSON 작성 |
| 32 | `32_Claim_Partition` | `POST .../claim` |
| 33 | `33_Is_Owner` | `claimed=true`이고 경계/SCN이 숫자인 경우만 진행 |
| 34 | `34_Execute_Partition_Query` | Oracle `AS OF SCN`, 범위 조회, Parquet chunk 생성 |
| 35 | `35_Set_Chunk_Attrs` | 파일명과 stage=`CHUNK_WRITE` 설정 |
| 36 | `36_PutHDFS` | Write and rename, conflict replace, 실패 3회 재시도 |
| 37 | `37_Build_Chunk_Report` | HDFS 기록 결과를 chunk JSON으로 변환 |
| 38 | `38_Report_Chunk` | `POST .../chunks` |

### 동작 원리

claim은 `PENDING/RETRY → RUNNING` CAS다. 같은 token 재요청은 응답 유실 재시도로 보고 다시 성공한다.
다른 worker가 이미 처리 중이거나 run이 끝났으면 `claimed=false`로 정상 종료한다.

34의 핵심 설정:

| 속성 | 값/의미 |
|---|---|
| Fetch Size | `EXTRACT.FETCH.SIZE` |
| Max Rows Per Flow File | `EXTRACT.ROWS.PER.FILE` |
| Output Batch Size | `0`; 모든 chunk에 `fragment.count`가 필요 |
| Max Wait Time | `EXTRACT.QUERY.TIMEOUT` |
| Use Avro Logical Types | `true`; decimal/timestamp 타입 유지 |
| Default Decimal Precision/Scale | Oracle 기본 precision/scale Parameter |
| 재시도 | 없음; 실패를 PG-90에 즉시 보고 |

36 성공 후에만 37이 content를 JSON으로 바꾼다. 순서가 바뀌면 Parquet 대신 JSON이 HDFS에 쓰이거나
Parquet binary가 API body로 전송될 수 있다.

API는 `load_file`의 모든 `WRITTEN` chunk를 집계한다. chunk index가 `0..n-1`로 완전하고 row 합계가
예상 건수와 같아야 파티션을 `SUCCESS`로 만든다. 모든 파티션 성공과 전체 건수 일치를 만족한 마지막
보고가 run을 `EXTRACTED_VALIDATED`로 전이하고 검증 dispatch를 예약한다.

### 단계 검증

```bash
hdfs dfs -ls '<HDFS.STAGE.ROOT>/<JOB.KEY>/run_id=<run_id>'
hdfs dfs -du -h '<HDFS.STAGE.ROOT>/<JOB.KEY>/run_id=<run_id>'
```

```sql
SELECT partition_id, status, expected_row_count, actual_row_count,
       file_count, attempt_count, worker_node, error_code
  FROM nifi_ops.load_partition
 WHERE run_id = '<run_id>' ORDER BY partition_id;

SELECT partition_id, chunk_index, fragment_count, record_count,
       byte_count, status, hdfs_path
  FROM nifi_ops.load_file
 WHERE run_id = '<run_id>' ORDER BY partition_id, chunk_index;
```

검증 기준:

- 각 활성 파티션은 한 worker만 claim한다.
- HDFS 파일 수와 `load_file`의 WRITTEN 행 수가 같다.
- 파티션별 `actual_row_count = expected_row_count`다.
- 완료 뒤 run은 `EXTRACTED_VALIDATED`, 검증 dispatch는 한 행이다.
- 이 시점에는 `_SUCCESS`가 아직 없거나 PG-40이 시작하며 생성된다.

장애 검증은 비운영에서 한 파티션의 HDFS 경로/권한을 의도적으로 잘못 설정하는 방식보다 별도 시험 Job과
격리된 경로를 사용하는 것이 안전하다. 실패 시 게시 단계가 시작되지 않아야 한다.

## 5. PG-05 Control Receiver

### 목적

Load Control worker의 validate/reissue HTTP 요청을 받아 Job별 PG로 라우팅한다. root에 하나만 존재하며
모든 Job이 공유한다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 05 | `05_Listen_Control` | 등록된 `/validate/<JOB>`, `/reissue/<JOB>` POST 수신 |
| 06 | `06_Validate_Request` | method와 `X-Run-Id`, `X-Dispatch-Id` UUID 검사 |
| 07 | `07_Respond_400` | 잘못된 요청 400 |
| 08 | `08_Respond_202` | 정상 요청 즉시 202 |
| 09 | `09_Extract_Control_Body` | run/dispatch/재발행 정보를 attribute로 추출 |
| 10 | `10_Route_By_Job_Action` | Job별 validate/reissue Output Port로 전달 |

### 동작 원리

202는 수신 확인일 뿐 처리 ACK가 아니다. validate의 ACK는 PG-40 `/validation/start`, reissue의 ACK는
PG-20의 새 claim이다. 202 직후 NiFi 노드가 종료되면 API sweeper가 `ack_timeout` 뒤 dispatch를 다시
보낸다.

### 관련 설정

- `CONTROL.LISTEN.PORT`
- API `nifi.receiver_url`, `nifi.timeout_seconds`
- builder가 관리하는 Job route와 Allowed Paths

### 단계 검증

- `worker.log`에서 `dispatch_sending`과 `dispatch_sent httpStatus=202`를 확인한다.
- `load_dispatch`가 `PENDING → SENT → ACKED`로 바뀌는지 확인한다.
- PG-05 provenance에서 `X-Run-Id`, `X-Dispatch-Id`와 request URI를 확인한다.
- validate FlowFile이 해당 Job의 `validate-in`으로 하나만 들어가는지 확인한다.
- 미등록 경로가 404인지 확인한다.

```sql
SELECT dispatch_id, dispatch_type, status, attempt_count,
       sent_at, acked_at, last_http_status, last_error
  FROM nifi_ops.load_dispatch
 WHERE run_id = '<run_id>' ORDER BY created_at;
```

## 6. PG-40 Staging Validation

### 목적

검증 실행 소유권을 획득하고 HDFS run 경로를 Hive external table로 연결한 뒤 원천 기대값과 staging
지표를 비교한다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 40 | `40_Set_Validation_Stage` | stage=`VALIDATION_START` |
| 41 | `41_Build_Start_Body` | dispatch ID와 node JSON 작성 |
| 42 | `42_Validation_Start` | `POST /validation/start`, dispatch ACK |
| 43 | `43_Is_Started` | 첫 요청의 `started=true`만 진행 |
| 44 | `44_Set_Run_Attrs` | API가 반환한 경로, table, 기대값 설정; stage=`STAGE_VALIDATION` |
| 45 | `45_Empty_Content` | `_SUCCESS`용 빈 content |
| 46 | `46_PutHDFS_SUCCESS_Marker` | run 경로에 `_SUCCESS` 기록 |
| 47 | `47_Build_External_DDL` | run 경로를 LOCATION으로 하는 DDL 작성 |
| 48 | `48_Create_External_Table` | staging external table 생성 |
| 49 | `49_Query_Stage_Metrics` | staging 지표 계산과 PASS/FAIL |
| 4A | `4A_Build_Validations_Body` | STAGING 지표 JSON 작성 |
| 4B | `4B_Report_Validations` | `POST /validations` |
| 4C | `4C_Empty_Json` | 판정 요청 `{}` 작성 |
| 4D | `4D_Stage_Validated` | `POST /stage-validated` |
| 4E | `4E_Is_Stage_Validated` | true면 PG-50, false면 PG-90 |

### 동작 원리

검증 FlowFile은 API worker가 새로 만들기 때문에 PG-10의 attribute를 갖지 않는다. 42 응답에서 HDFS 경로,
staging table, business key, 원천/추출 건수, SOURCE 지표를 다시 받는다.

API는 49가 계산한 PASS/FAIL을 다시 읽어 FAIL이 하나라도 있거나 지표가 비어 있으면
`STAGING_VALIDATED`로 전이하지 않는다.

기본 지표:

| 지표 | 통과 조건 |
|---|---|
| `STAGE_COUNT` | staging = source = extracted |
| `NULL_SPLIT_COUNT` | 0 |
| `DUP_PK_COUNT` | 0 |
| `AMOUNT_SUM` | source 합계와 같음 |
| `MIN_TS`, `MAX_TS` | source 범위와 같음 |

### 단계 검증

```bash
hdfs dfs -test -e '<run path>/_SUCCESS' && echo OK
```

```sql
DESCRIBE FORMATTED stg.<stage_table>;
SELECT COUNT(*) FROM stg.<stage_table>;
```

```sql
SELECT metric_name, expected_value, actual_value, result
  FROM nifi_ops.load_validation
 WHERE run_id = '<run_id>' AND stage = 'STAGING'
 ORDER BY metric_name;
```

검증 기준:

- dispatch는 `ACKED`, run은 최종적으로 `STAGING_VALIDATED`다.
- staging LOCATION이 정확히 해당 run 경로다.
- `_SUCCESS`가 존재하고 Hive row count가 `source_count`와 같다.
- 모든 지표가 PASS다.
- 같은 dispatch를 다시 받아도 43에서 종료되고 staging/publish가 중복 실행되지 않는다.

## 7. PG-50 Publish

### 목적

publish token으로 게시 실행자를 하나만 선택하고, staging 데이터를 target에 `INSERT OVERWRITE`한다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 50 | `50_Set_Publish_Token` | UUID token, stage=`PUBLISH` |
| 51 | `51_Build_Claim_Body` | token JSON 작성 |
| 52 | `52_Claim_Publish` | `POST /publish/claim` |
| 53 | `53_Is_Publish_Owner` | `claimed=true`만 진행 |
| 54 | `54_Build_Insert_Overwrite_SQL` | 게시 SQL 작성 |
| 55 | `55_Insert_Overwrite` | 재시도 없이 Hive 게시 |
| 56 | `56_Body_PUBLISHED` | 성공 결과 JSON |
| 56U | `56U_Body_PUBLISH_UNKNOWN` | failure/retry의 결과 불명 JSON |
| 57 | `57_Report_Publish_Result` | `POST /publish/result` |
| 58 | `58_Is_Published` | `PUBLISHED`만 PG-60, 그 밖은 errors |

### 동작 원리

52의 `STAGING_VALIDATED → PUBLISHING` CAS에 성공한 token 하나만 55를 실행한다. `INSERT OVERWRITE`는
자동 재시도하지 않는다.

`PutClouderaHiveQL` 실패 relationship만으로 SQL이 제출되지 않았는지, 실행 중 끊겼는지 구분할 수 없다.
따라서 성공 이외에는 `PUBLISH_UNKNOWN`으로 보고한다. 운영자가 Hive query history와 target을 확인하기
전에는 재실행하지 않는다.

HiveServer2 연결 자체가 불가능하면 Processor가 FlowFile을 input queue로 되돌릴 수 있다. 이 경우 SQL 제출
전이므로 연결 복구 뒤 이어서 처리된다. queue와 bulletin을 함께 본다.

### 관련 설정

- `HIVE.TARGET.DB`, `HIVE.TARGET.TABLE`
- `TARGET.PARTITION.CLAUSE`
- `HIVE.INSERT.COLUMNS`
- Hive pool/timeout
- API `recovery.publish_stale`

### 단계 검증

```sql
SHOW PARTITIONS dw.insp_dtl;
SELECT COUNT(*), SUM(amount), MIN(reg_ts), MAX(reg_ts)
  FROM dw.insp_dtl
 WHERE base_dt = '2026-09-28';
```

```sql
SELECT status, publish_started_at, published_at, error_code, error_message
  FROM nifi_ops.load_run WHERE run_id = '<run_id>';
```

검증 기준:

- 게시 중 `PUBLISHING`, 정상 보고 뒤 `PUBLISHED`다.
- 의도한 target partition만 교체되었다.
- 같은 publish token 재요청은 게시를 다시 실행하지 않는다.
- 다른 token은 `claimed=false`다.
- 의도적 실패 시험은 격리된 target에서 수행하며 `PUBLISH_UNKNOWN` 운영 절차까지 검증한다.

## 8. PG-60 Target Validation

### 목적

게시된 target의 업무 범위를 다시 측정하고 최종 `SUCCESS`를 요청한다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 60 | `60_Set_Target_Stage` | stage=`TARGET_VALIDATION` |
| 61 | `61_Query_Target_Metrics` | `TARGET.BUSINESS.WHERE` 범위 지표 계산 |
| 62 | `62_Build_Validations_Body` | TARGET 지표 JSON 작성 |
| 63 | `63_Report_Validations` | `POST /validations` |
| 64 | `64_Empty_Json` | success 요청 body 작성 |
| 65 | `65_Report_Success` | `POST /success` |
| 66 | `66_Is_Success` | false면 PG-90, true면 정상 종료 |

### 동작 원리

API는 저장된 TARGET 지표가 모두 PASS일 때만 `PUBLISHED → SUCCESS`로 전이한다. 실패해도 자동 재게시하지
않는다. target 범위가 잘못되었을 가능성이 있으므로 운영자가 원인을 분석한 뒤 새 run 또는 별도 복구를
결정한다.

### 단계 검증

```sql
SELECT stage, metric_name, expected_value, actual_value, result
  FROM nifi_ops.load_validation
 WHERE run_id = '<run_id>' AND stage IN ('SOURCE','STAGING','TARGET')
 ORDER BY stage, metric_name;
```

검증 기준:

- SOURCE/STAGING/TARGET의 의미가 같은 지표가 일치한다.
- 모든 TARGET 지표가 PASS다.
- run은 `SUCCESS`, `completed_at`이 채워진다.
- `source_count = extracted_count = staging_count = target_count`다.

## 9. PG-70 Cleanup

### 목적

API가 보존 기간으로 선정한 종료 run의 staging table과 HDFS run 경로만 삭제하고 결과를 기록한다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 70 | `70_Generate_Cleanup_Trigger` | 1시간 주기, Primary Node |
| 71 | `71_Set_Cleanup_Stage` | stage=`CLEANUP`, 안전 비교용 prefix |
| 72 | `72_Get_Cleanup_Candidates` | `GET /cleanup/candidates` |
| 73 | `73_Split_Runs` | 후보를 run별 FlowFile로 분리 |
| 74 | `74_Extract_Run_Attrs` | 경로/table/run 정보 추출 |
| 75 | `75_Check_Cleanup_Target` | 현재 설정의 정확한 경로와 table prefix 검사 |
| 76 | `76_Build_Drop_SQL` | `DROP TABLE IF EXISTS` 작성 |
| 77 | `77_Drop_Stage_Table` | staging table 삭제 |
| 78 | `78_Delete_Run_Path` | HDFS run 경로 재귀 삭제 |
| 79 | `79_Build_Cleanup_Body` | 삭제 결과 JSON |
| 7A | `7A_Report_Cleanup` | `POST /runs/{id}/cleanup` |

### 동작 원리

API는 `SUCCESS`, `FAILED_*`, `TIMED_OUT` 중 보존 기간이 지난 run만 반환한다. 진행 중 상태와
`PUBLISH_UNKNOWN`은 대상이 아니다.

75는 HDFS 경로가 정확히 `<root>/<job>/run_id=<runId>`인지, table 이름이 현재 prefix와 형식에 맞는지
검사한다. `DeleteHDFS`가 glob을 받을 수 있으므로 이 검사는 삭제 안전 경계다.

### 단계 검증

```bash
curl -fsS -H 'Authorization: Bearer <token>' \
  '<API>/v1/cleanup/candidates?jobKey=<JOB.KEY>&limit=10'
```

검증 기준:

- 보존 기간 전 run은 반환되지 않는다.
- cleanup 대상의 staging table과 run 경로만 삭제된다.
- target은 변하지 않는다.
- `load_run.cleaned_at`과 `RUN_CLEANED` 이벤트가 기록된다.
- 경로/prefix가 현재 설정과 다른 대상은 삭제하지 않고 PG-90에 `CLEANUP_FAILED`를 남긴다.

운영 전 cleanup 시험은 비운영 Job에서 보존 기간을 짧게 설정하고 만든 전용 run만 대상으로 수행한다.

## 10. PG-90 Error and Event

### 목적

모든 PG의 실패를 같은 형식으로 정규화하고, 단계에 따라 run/partition 실패를 API에 보고하며,
`load_event`와 NiFi 로그에 상세를 남긴다.

### Processor 흐름

| 번호 | Processor | 동작 |
|---|---|---|
| 90 | `90_Normalize_Error` | stage, code, level, class, message 정규화 |
| 91 | `91_Route_Failure_Report` | run 실패 / partition 실패 / 이벤트만 분기 |
| 92·93 | `92_Build_Run_Fail_Body`, `93_Report_Run_Fail` | 단계 실패 API 보고 |
| 94·95 | `94_Build_Partition_Fail_Body`, `95_Report_Partition_Fail` | 파티션 최종 실패 API 보고 |
| 96 | `96_Insert_Load_Event` | `nifi_ops.load_event` INSERT |
| 97 | `97_LogMessage` | `SQOOP_REPLACEMENT` JSON 한 줄 로그 |

### 실패 보고 규칙

| `load.stage` | 보고 | 결과 |
|---|---|---|
| `RUN_CREATE` | 이벤트만 | run이 아직 없을 수 있음 |
| `MANIFEST` | run 실패 | `FAILED_MANIFEST`; API가 이미 422로 기록했으면 중복 보고 생략 |
| `EXTRACT`, `CHUNK_WRITE` | claim 성공 시 partition 실패 | `FAILED_EXTRACT` 또는 snapshot expired |
| `VALIDATION_START` | 이벤트만 | API dispatch가 재전송할 수 있음 |
| `STAGE_VALIDATION` | run 실패 | `FAILED_STAGE_VALIDATION` |
| `PUBLISH` | 이벤트만 | PG-50의 57이 직접 결과 보고 |
| `TARGET_VALIDATION` | run 실패 | `FAILED_TARGET_VALIDATION` |
| `CLEANUP` | 이벤트만 | 다음 cleanup 주기 재시도 |

### 오류 코드 정규화

- HTTP 409: API 응답의 code, WARN
- 다른 HTTP 3xx~5xx: `HTTP_<status>`
- API 연결 실패: `API_UNREACHABLE`
- SQL 메시지의 `ORA-nnnnn`: 해당 Oracle code
- ORA code 없는 SQL 오류: `SQL_ERROR`
- 그 밖: `<load.stage>_FAILED`

### 단계 검증

```sql
SELECT event_time, event_level, event_name, error_class, error_code,
       partition_id, process_group, message
  FROM nifi_ops.load_event
 WHERE run_id = '<run_id>'
 ORDER BY event_time;
```

```bash
grep 'SQOOP_REPLACEMENT' <NIFI_HOME>/logs/nifi-app.log
grep '<run_id>' load-control-api/logs/*.log
```

검증 기준:

- 상태 실패 이벤트와 Processor 상세 이벤트가 모두 추적 가능하다.
- 409 정상 경합은 WARN이다.
- `ORA-01555`/`ORA-08180`은 run을 `FAILED_SNAPSHOT_EXPIRED`로 만든다.
- 93/95 자체 실패가 PG-90으로 순환하지 않고 96/97로 계속 진행한다.
- `PutHDFS`/`PutClouderaHiveQL` 상세 원인은 bulletin과 provenance로 연결해 찾을 수 있다.

## 11. 단계별 성공 체크 요약

| PG | 성공 증적 |
|---|---|
| PG-00 | FlowFile 1개, 유효 business key |
| PG-10 | run `EXTRACTING`, SCN/manifest/원천 지표 저장 |
| PG-20 | 모든 파티션 `SUCCESS`, HDFS chunk와 ledger 일치 |
| PG-05 | dispatch `ACKED`, 올바른 Job route |
| PG-40 | `_SUCCESS`, staging table, STAGING 지표 PASS |
| PG-50 | run `PUBLISHED`, 의도한 target partition 교체 |
| PG-60 | TARGET 지표 PASS, run `SUCCESS` |
| PG-70 | 허용 대상만 삭제, `cleaned_at` 기록 |
| PG-90 | 오류 코드·상태·이벤트·로그가 같은 run ID로 추적 가능 |
