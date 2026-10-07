# nifi-sqoop-removal-guide.md 검토 및 NiFi 2.4.0 PoC 결과

> 1~5장은 Load Control API 도입 이전 구조(PG-30 Wait/Notify, NiFi `PutSQL`로 원장 직접 기록)로 수행한 PoC 기록이다. 현재 구조(Load Control API 연동)로 다시 수행한 결과는 6장에 있다. `poc/build_flow.py`는 현재 구조로 바뀌었고, 이전 빌더는 git 이력(커밋 `ec5f074` 이전)에 있다.

- 검토일: 2026-10-01
- 실행 환경: Apache NiFi 2.4.0 (단일 노드, `http://10.0.1.50:10001`), PostgreSQL 16
- 대체 사항: 원천 Oracle → PostgreSQL `srcdb.app.insp_dtl`, HDFS → PutHDFS + `fs.defaultFS=file:///`
- 추가 설치: `extensions/`에 `nifi-parquet-nar`, `nifi-hadoop-nar`, `nifi-hadoop-libraries-nar` 2.4.0, `jdbc/`에 PostgreSQL·ojdbc 드라이버
- PoC Flow 생성기: `poc/build_flow.py` (Process Group `SQOOP_REPLACEMENT_POC`, Processor 75개)
- 구현 범위: PG-00 Trigger → PG-10 Coordinator → PG-20 Worker → PG-30 Partition/Run Gate → `EXTRACTED_VALIDATED` + `_SUCCESS`, PG-90 실패/이벤트
- 미구현: PG-40 이후(Hive staging, `INSERT OVERWRITE`, Target 검증), PG-70 Recovery. 이 환경에는 Hive가 없고 Apache NiFi 2.x에는 Hive Processor도 없다.

## 1. 시험 결과

| 시나리오 | 결과 |
|---|---|
| 정상 실행 (105,000건, 8 파티션, 5,000행/파일) | `EXTRACTED_VALIDATED`, 8/8 SUCCESS, 21개 Parquet. 원천과 count·distinct·min/max·`SUM(amount)`·NULL 수 일치 |
| 0건 파티션 (seq 30001~45000 공백) | Worker로 보내지 않고 바로 SUCCESS 처리, Run Wait counter 정상 |
| 파티션 건수가 파일 행 수의 정확한 배수 (15,000/5,000) | 빈 4번째 FlowFile 없이 3파일 생성, `fragment.count=3` |
| 동일 업무일자 중복 실행 | partial unique index로 차단, 기존 활성 run 상태 변화 없음, `RUN_FAILED` 이벤트 기록 |
| 파티션 하나의 HDFS 쓰기 실패 주입 (재시도 3회 소진) | 파티션 0004만 FAILED, run `FAILED_EXTRACT`, `_SUCCESS` 미생성. Run Gate가 실패 후 0.1초 만에 확정(RUN.WAIT.TIMEOUT 대기 없음) |
| claim CAS (SQL 단위 시험) | 동일 파티션 2회 claim 시 첫 번째만 `true` |
| Trigger 검증 실패 | `RUN_FAILED`(TRIGGER/VALIDATION) 이벤트 후 종료 |

## 2. 가이드 결함: 실제로 재현했거나 확인한 항목

| # | 위치 | 문제 | 조치 및 권고 |
|---|---|---|---|
| 1 | 4.1 `claim_partition` | `started_at`이 `load_run`과 `load_partition` 양쪽에 있어 `column reference "started_at" is ambiguous` 오류로 **함수 생성이 실패**함 | 가이드 수정 완료 (`p.started_at`, `p.attempt_count`) |
| 2 | 7.2 #22, 15.1 | `PutSQL` Fragmented=true는 fragment 일부만 poll되면 FlowFile을 penalize해 되돌린다. penalized FlowFile은 다음 poll에서 빠지므로 **영구 대기(livelock)**가 발생함. 두 번째 실행에서 재현됨 (`PutSQL.java:724-726`) | 해당 PutSQL의 Penalty Duration=0 sec로 즉시 해소됨. 근본 대책은 split 전에 manifest 전체를 SQL 한 문장으로 INSERT하는 것 (fragment 의존 제거) |
| 3 | 8.1 / 8.2 #29 | `DuplicateFlowFile`에는 `success` relationship 하나뿐이다. `original`/`duplicate` 분기는 존재하지 않음 | `RouteOnAttribute ${copy.index:equals('1')}`로 분기 (PoC에서 검증) |
| 4 | 4장 `CS_DMC_CLIENT` | NiFi 2.x에서 `DistributedMapCacheClientService`가 `MapCacheClientService`로, 서버는 `MapCacheServer`로 이름이 바뀜 | 컴포넌트 이름 갱신 |
| 5 | 10·11·12장 | `PutHive3QL`/`SelectHive3QL`은 Apache NiFi 2.x에 없음 (2.4.0 목록에서 확인) | CFM 4.12.0 지원 Processor 목록에서 실제 이름(예: Cloudera Hive 계열)을 확정해야 함 |
| 6 | 4장, 8.4 | Parquet로 쓴 TZ 없는 `timestamp`가 JVM 시간대(KST) 기준으로 UTC 변환되어 `isAdjustedToUTC=true`, **millis 정밀도**로 저장됨. `2026-09-28 00:00:01` → `2026-09-27T15:00:01Z`. 건수 검증으로는 탐지되지 않음 | Hive에서 읽은 값과 원천 값을 min/max timestamp DQ로 비교. NiFi JVM `-Duser.timezone`과 Hive parquet timestamp 설정을 함께 확정. micro/nano 정밀도가 필요한 컬럼은 문자열 변환 검토 |
| 7 | 8.4 | `ExecuteSQLRecord` 기본값(Use Avro Logical Types=false)이면 DATE/TIMESTAMP/DECIMAL이 문자열로 기록됨 | `dbf-user-logical-types=true` 명시 (PoC에서 DECIMAL(18,2)/DATE 유지 확인) |
| 8 | 7.4 | Manifest 불변식(`SUM(expected)=source_count`) 검사를 언급하지만 7.1/7.2 Processor 흐름에는 해당 단계가 없음 | Manifest SQL에 `SUM() OVER()`를 추가하고 split 전에 RouteOnAttribute로 검사 (PoC 18번) |
| 9 | 2장 표, 8.4 표 | Concurrent Tasks에 `${WORKER.CONCURRENT.TASKS}`/`#{...}`를 사용함. Concurrent Tasks는 정수 스케줄 설정이라 Parameter 참조 대상이 아님 (REST DTO가 Integer) | 배포 스크립트나 환경별 값으로 직접 설정 |
| 10 | 전반 | EL 문자열 리터럴 안의 Parameter(`'#{PARTITION.COUNT}'`)는 치환되지 않음 (PoC에서 2회 재현) | Parameter 값을 UpdateAttribute로 attribute에 옮긴 뒤 EL에서 비교 |

## 3. 설계상 위험 (환경 제약으로 실행 검증은 못 함)

1. **Heartbeat 갱신 지점 없음**: `heartbeat_at`은 claim 시점과 run 생성 시점에만 갱신된다. `RECOVERY.STALE.MINUTES=15` < `EXTRACT.QUERY.TIMEOUT=60 min`이므로 정상적인 장기 run이나 파티션이 Recovery Monitor에서 stale로 판정되어 재발행될 수 있다. PoC에서는 file audit UPSERT에서 partition과 run heartbeat를 함께 갱신했다. 단일 chunk 쓰기가 15분을 넘는 경우에 대한 대책은 별도로 필요하다.
2. **Hive external table과 하위 디렉터리**: 파일이 `run_id=<id>/part=<pid>/` 하위 디렉터리에 있다. 비파티션 external table의 `LOCATION`을 run root로 잡으면 Hive 설정(`hive.mapred.supports.subdirectories`, `mapreduce.input.fileinputformat.input.dir.recursive`)에 따라 0건으로 읽힐 수 있다. 파일명이 이미 고유하므로 run root에 평탄하게 쓰는 방식을 권장한다.
3. **PutSQL CAS 결과 미확인**: PutSQL은 영향 행 수가 0이어도 success로 보낸다. 34(partition SUCCESS), 42/64(run CAS)가 실제로 갱신됐는지 흐름상 알 수 없다. 최종 판정은 DB 재조회로 보호되지만, CAS가 중요한 단계는 `UPDATE ... RETURNING`을 `ExecuteSQLRecord`로 실행해 결과를 분기하는 방식을 권장한다.
4. **실패 후 남는 control FlowFile**: 실패 파티션의 partition-control은 `PARTITION.WAIT.TIMEOUT`까지 Wait에 남는다(PoC에서 1건 관찰). 실패 경로에서 해당 chunk signal을 Notify로 해제하면 즉시 정리된다. Run Wait 쪽은 PoC처럼 실패 시 `delta=${load.partition.count}`로 Notify하면 즉시 해제된다.
5. **PUBLISH_UNKNOWN 판별**: Hive Processor의 failure relationship만으로는 "실행 전 실패"와 "응답 유실"을 구분할 수 없다. 기본값은 PUBLISH_UNKNOWN으로 두고, 명확한 parse/권한 오류만 FAILED_PUBLISH로 분류하는 것이 안전하다.
6. **PostgreSQL을 원천으로 쓰는 경우**: `AS OF SCN`에 대응하는 기능이 없다. 불변 업무 마감 조건이 필수다. 또한 pgjdbc는 autocommit=false일 때만 fetch size(cursor)를 적용하므로 `esql-auto-commit=false`가 필요하다. 이 설정이 없으면 파티션 전체를 메모리에 적재한다.
7. **확인한 정상 동작**: PutSQL 2.4는 실패 시 `error.sql.state`/`error.code`/`error.message`를 붙인다. 따라서 7.2의 "unique violation(23505)과 연결 장애 구분"은 구현 가능하다. 다만 가이드 14.6의 공통 UpdateAttribute가 같은 이름(`error.code`, `error.message`)을 덮어쓰지 않도록 주의해야 한다.

## 4. 재현 방법

```bash
# NiFi 2.4.0 기동 후
python3 poc/build_flow.py http://10.0.1.50:10001/nifi-api poc/config.example.json   # 암호와 경로를 채운 사본 사용
# Trigger(00_Generate_Trigger)는 정지 상태로 두고 Run Once로 실행
```

## 5. 가이드 반영 현황 (2026-10-01)

| 항목 | 반영 위치 |
|---|---|
| 2장 #1~#10, 3장 7번 | 2장, 3.1, 4장, 5장, 7.1~7.4, 8.1~8.4, 10.2, 15.1 (커밋 `28d7e7e`) |
| 3장 1. heartbeat | 3.1 `RECOVERY.STALE.MINUTES`(15→90), 8.5 file audit SQL에 heartbeat 갱신, 13.2 stale 기준 |
| 3장 2. Hive 하위 디렉터리 | 1장 경로 원칙, 8.5 PutHDFS Directory, README 경로. PoC 빌더도 평탄 레이아웃으로 변경 후 재실행 검증(21파일, 105,000건 일치) |
| 3장 3. PutSQL CAS 결과 | 17.1 신설, 9.2 #34·#42, 10.2 #46, 11.2 #56, 12.2 #64 |
| 3장 4. 실패 후 남은 control FlowFile | 9.2 #33 `run_failed` 경로, 9장 Wait 해제 Notify |
| 3장 5. PUBLISH_UNKNOWN 판별 | 11.2 #55A, 판별 기준 |
| 3장 6. PostgreSQL 원천 | 1장 "Oracle 이외 원천 (PostgreSQL)" |

3장 3~5번(PutSQL CAS 결과, Wait 해제, PUBLISH 판별)은 API 연동 구조에서 문제 자체가 없어졌거나 API가 처리한다(6장).

## 6. API 연동 구조 PoC (2026-10-01)

`poc/build_flow.py`를 Load Control API 연동 구조(가이드 7~10장)로 바꾸고 같은 데이터로 다시 실행했다.

- 실행 환경: Apache NiFi 2.4.0(단일 노드, HTTP `127.0.0.1:18888`), PostgreSQL 16, Load Control API(`load-control-api`, api 1 프로세스 + worker 1 프로세스, `config.yaml`)
- 추가 설치: 1장과 같음(`nifi-parquet-nar`, `nifi-hadoop-nar`, `nifi-hadoop-libraries-nar` 2.4.0, PostgreSQL JDBC 42.7.3)
- 데이터: `srcdb.app.insp_dtl` 업무일자 `2026-09-28` 105,000건(seq 1~120000 중 30001~45000 공백), 다른 일자 500건
- Flow: Processor 85개, Connection 135개. PG-00 → PG-10 → PG-20 → (API 판정·outbox) → PG-05 → PG-40 입구(`/validation/start`, `_SUCCESS`). Hive가 없으므로 `STAGE_VALIDATING`까지
- NiFi↔API는 평문 HTTP로 연결했다. 운영에서는 SSL Context Service(mTLS)를 붙인다.

### 6.1 시험 결과

| 시나리오 | 결과 |
|---|---|
| 정상 실행(105,000건, 8파티션, 5,000행/파일) | run `STAGE_VALIDATING`, 8/8 SUCCESS, Parquet 21개, `_SUCCESS` 생성, 검증 dispatch `ACKED`. Parquet를 직접 읽어 count·distinct·min/max·`SUM(amount)`(65,611,875.00)·NULL 수가 원천과 일치, DECIMAL(18,2)/DATE 타입 유지 |
| 0건 파티션(seq 30001~45000) | API가 manifest 등록 시 SUCCESS 처리, Worker로 보내지 않음 |
| 파티션 건수가 파일 행 수의 배수(15,000/5,000) | 파티션당 3파일, 빈 FlowFile 없음 |
| 동일 업무일자 중복 실행 | API 409 → `DUPLICATE_ACTIVE_RUN` WARN 이벤트, 기존 run 상태 변화 없음 |
| 파티션 0004 HDFS 쓰기 실패 주입(재시도 3회 소진) | 0004만 FAILED, run `FAILED_EXTRACT`, 검증 dispatch 0건, `_SUCCESS` 미생성. 나머지 chunk의 늦은 실패 보고는 멱등 처리 |
| API 중단 중 실행 시작(약 10초 후 재기동) | `InvokeHTTP` Failure(Connection refused) → `RetryFlowFile` 재시도 → API 재기동 후 run 생성부터 `STAGE_VALIDATING`까지 자동 진행 |
| 파티션 0004 chunk 보고 차단 후 sweeper REISSUE 모드 | 0004가 RUNNING으로 정체 → sweeper가 RETRY로 초기화·이전 chunk 기록 무효화·`REISSUE_PARTITION` dispatch → PG-05 `/reissue` 수신 → 재 claim(attempt 2, dispatch `ACKED`) → 파티션 SUCCESS → run 완료(105,000건, WRITTEN 21파일) |

이전 구조(1장)와 비교하면 PG-30(Wait/Notify)과 DMC Controller Service가 없어졌고, 원장 쓰기 `PutSQL`이 모두 `InvokeHTTP`로 바뀌었다. 재발행 경로는 이전 PoC에서 구현하지 못했던 부분이다.

### 6.2 이번 PoC에서 찾은 결함

| # | 위치 | 문제 | 조치 |
|---|---|---|---|
| 11 | 가이드 8.2 #20 | 같은 `UpdateAttribute` 안에서 `partition.claim.token=${UUID()}`를 만들고 `claimToken=${partition.claim.token}`으로 참조했다. `UpdateAttribute`는 모든 속성을 들어온 attribute 기준으로 평가하므로 `claimToken`이 빈 값이 되어 claim 7건이 모두 422(UUID 형식 오류)를 받았다 | token 생성과 복사를 두 `UpdateAttribute`로 분리(가이드 8.2, 빌더 30/30B) |
| 12 | 가이드 9.2, 3.1 | `Authorization` 동적 속성을 `Bearer #{CONTROL.API.TOKEN}`으로 쓰면 NiFi가 400으로 거부한다. Sensitive 속성은 Parameter 참조 외의 텍스트를 가질 수 없다 | Parameter를 `CONTROL.API.AUTHORIZATION`(값 `Bearer <token>`)으로 바꾸고 속성 값은 `#{CONTROL.API.AUTHORIZATION}`만 둔다 |
| 13 | 빌더 14번 | `RouteOnAttribute` EL의 닫는 괄호 위치 오류로 "Found multiple Expressions" 검증 실패 | 수정. 빌드 후 모든 Processor의 `validationStatus`를 확인한다 |
| 14 | 빌더 오류 경로 | 오류 경로가 `event.name`을 지정하지 않아 앞 단계의 `RUN_CREATED`가 오류 이벤트 이름으로 남았다 | 오류 경로마다 `event.name`, `event.level`을 지정 |

### 6.3 재현 방법

```bash
# 1. 관리 DB migration과 API·worker 실행(load-control-api/README.md)
LCA_CONFIG=config.yaml alembic upgrade head
python -m load_control.server --config config.yaml
python -m load_control.worker --config config.yaml     # nifi.receiver_url = http://<nifi-host>:<CONTROL.LISTEN.PORT>

# 2. NiFi Flow 생성(config.example.json 사본에 API URL, 인증 헤더, DB, 경로를 채운다)
python3 poc/build_flow.py http://<nifi-host>:<port>/nifi-api my-config.json
# 00_Generate_Trigger를 제외한 Processor를 시작하고, Trigger는 Run Once로 실행한다

# 3. 다시 만들 때
python3 poc/teardown_flow.py http://<nifi-host>:<port>/nifi-api
```


## 7. TLS on/off 검증 (2026-10-01, NiFi 2.4.0)

설정:

- API → NiFi: `config.yaml`의 `nifi.tls`(`enabled`, `verify`, `ca_bundle`, `client_cert`/`client_key`). `enabled: false`면 `receiver_url`은 `http://`이어야 한다.
- API 서버: `server.tls`(`enabled`, `certfile`, `keyfile`, `ca_certs`, `client_cert_required`).
- PoC 빌더: `config.json`의 `tls.api_client`(InvokeHTTP용 truststore → `CS_SSL_API_CLIENT`), `tls.receiver`(HandleHttpRequest용 keystore → `CS_SSL_RECEIVER`). 둘 다 기본 off.

결과:

| 시나리오 | 결과 |
|---|---|
| TLS 모두 off(기존 구성 회귀) | run `STAGE_VALIDATING`, dispatch `ACKED` |
| API https(자체 서명) + NiFi truststore, NiFi 수신 https(자체 서명) + worker `verify: false` | run `STAGE_VALIDATING`, dispatch `ACKED`, worker 시작 시 `nifi_tls_verification_disabled` WARNING |
| curl로 NiFi 수신 포트 확인 | 검증 on: exit 60(인증서 거부), `-k`: 400(TLS 통과, 잘못된 경로), 평문 http: 연결 실패 |

확인한 제약:

- NiFi `InvokeHTTP`는 인증서 검증을 건너뛰는 옵션이 없다(SSL Context Service만 받는다). API가 https면 자체 서명 인증서라도 truststore에 넣어야 한다. 빌더는 `tls.api_client`에 `verify` 키가 있으면 거부한다.
- 검증 skip은 API(worker) → NiFi 방향에서만 가능하다(`nifi.tls.verify: false`). 운영에서는 `ca_bundle`로 검증하는 것을 권장한다.
- 단위 테스트 `tests/test_tls.py`가 자체 서명 + skip, 검증 실패 재시도, CA 검증, mTLS를 실제 HTTPS 서버로 확인한다.

## 8. 테이블 목록으로 Job PG N개 생성 (2026-10-07, NiFi 2.4.0)

`build_flow.py`가 root에 Processor를 바로 만들지 않고 최상위 PG 하나 안에 모든 PG를 만든다. `config.json`의 `jobs`(테이블 목록)를 받아 Job PG를 한 번에 만든다.

```text
root
└── SQOOP_REPLACEMENT (common context, 공유 Controller Service: DBCP META/SRC, JSON/Parquet writer, API SSL)
    ├── 05_CONTROL_RECEIVER   HandleHttpRequest 1개 → 10_Route_By_Job → Output Port to_<JOB.KEY> × N
    ├── JOB_<JOB.KEY> × N     control_in → 09C_Route_By_Action(validate→PG-40 입구, reissue→70) + PG-00/10/20
    └── 90_EVENT_LOGGER       events_in ← 모든 PG의 events 포트 → load_event INSERT
```

- Receiver는 노드당 포트를 하나만 열 수 있어 하나만 둔다. Job PG N개가 Receiver와 connection N개로 연결된다(Job당 route 1, Output Port 1, connection 1).
- Job PG 안의 processor 구성은 모두 같고 Parameter Context(`PC_JOB_<JOB.KEY>`, common 상속)만 다르다.
- 원천 pool(`CS_DBCP_SRC`)은 모든 Job이 공유한다. 최대 연결 수는 `SRC.JDBC.MAX.CONNS`(기본 Job당 5, 최소 10).
- `load_event.process_group`에 이벤트가 난 PG 이름(`JOB_<JOB.KEY>`, `05_CONTROL_RECEIVER`)이 들어간다.
- 예전 형식(`job_params` 하나)도 Job 1개로 받는다. `teardown_flow.py`는 최상위 PG와 common을 상속한 Job context를 모두 지운다.

결과(테이블 2개: `insp_dtl` 105,000건 8파티션, `insp_hist` 35,000건 4파티션, 동시에 trigger):

| 항목 | 결과 |
|---|---|
| 생성 | Job PG 2개(각 processor 77개), Receiver 10개, Event Logger 2개. invalid 0 |
| PG 사이 connection | Receiver `to_PG_INSP_DTL_DAILY` → `JOB_PG_INSP_DTL_DAILY.control_in`, `to_APP_INSP_HIST` → `JOB_APP_INSP_HIST.control_in`, events × 3 → `events_in` |
| 실행 | 두 run 모두 `STAGE_VALIDATING`, dispatch `ACKED`, Job별 `_SUCCESS` marker |
| 미등록 jobKey(`/validate/NO_SUCH_JOB`) | 202 응답 후 `05_CONTROL_RECEIVER`의 `CONTROL_UNKNOWN_JOB` 이벤트(API는 ACK timeout 뒤 재전송) |
| `/reissue/APP_INSP_HIST` | `JOB_APP_INSP_HIST`로만 전달되어 `RECOVERY_REISSUED` 기록 |
| 실행 중 teardown | 최상위 PG와 context 3개 삭제, root 비어 있음 |
| 예전 `job_params` 설정 | Job PG 1개로 생성 |
