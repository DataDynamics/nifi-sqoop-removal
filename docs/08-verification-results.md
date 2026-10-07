# 검증 결과

`nifi-flow/deploy_job_flow.py`로 만든 Flow와 Load Control API를 실제 클러스터에서 실행해 확인한 결과입니다.

## 1. 시험 환경

| 구성 | 버전·구성 |
|---|---|
| NiFi | Cloudera CFM 4.12(NiFi 2.6.0) 2노드 클러스터(비보안). 추출 단계는 Apache NiFi 2.4.0 단일 노드에서도 확인 |
| 원천 | Oracle Database 23ai Free(컨테이너), ojdbc11 21.15 |
| HDFS·Hive | Apache Hadoop 3.4.1 단일 노드, Apache Hive 4.0.1 HiveServer2(`test-environment/hdfs-hive`, 인증 없음) |
| 관리 DB | PostgreSQL 16 |
| 데이터 | `APP.INSP_DTL` 업무일자 `2026-09-28` 105,000건(seq 30001~45000 공백 → 0건 파티션 1개), 파티션 8개 |

## 2. 시나리오

| 시나리오 | 결과 |
|---|---|
| 정상 실행 | `SUCCESS`. 원천 = 추출 = staging = target = 105,000건. STAGING·TARGET 지표 12개(건수, NULL, PK 중복, 금액 합계 71,853,075, 시각 최소·최대) 모두 PASS. 0건 파티션은 Worker로 가지 않고 바로 성공 |
| 클러스터 분산 | 파티션 7개가 두 노드에 나뉘어 추출됨. Trigger는 Primary Node에서만 실행되어 run 1개 |
| 같은 업무일자 중복 실행 | 409 `DUPLICATE_ACTIVE_RUN`(WARN). 기존 run 영향 없음 |
| 파티션 하나의 HDFS 쓰기 실패 | 파티션 `FAILED`, run `FAILED_EXTRACT`. 검증 호출·`_SUCCESS` 없음 |
| chunk 보고(38) 실패 | 2초 안에 파티션 `FAILED`, run `FAILED_EXTRACT`(오류 코드 그대로). 이전에는 `recovery.stale`(90분)까지 `EXTRACTING`에 머물렀습니다 |
| API 중단 중 실행 | NiFi가 재시도로 기다렸다가 API 재기동 후 이어서 진행 |
| `ORA-01555`(undo 소진) | 파티션 쿼리 재시도 없이 run `FAILED_SNAPSHOT_EXPIRED`. 검증 호출 없음. 새 run은 새 SCN으로 성공 |
| 정밀도 없는 `NUMBER` | 기본(38,10)은 정상. scale 0이면 **오류 없이 반올림**되어 건수는 맞고 금액 합계만 다름(71,853,600 vs 71,853,075). 정수부 초과(precision 41)는 파티션 `SQL_ERROR` → `FAILED_EXTRACT` |
| staging 지표 FAIL(PK 중복) | `FAILED_STAGE_VALIDATION`. 게시 없음 |
| 게시 실패(없는 컬럼) | `PUBLISH_UNKNOWN`. 운영자 확정(`/publish-unknown/resolve`)으로 `FAILED_PUBLISH` |
| target 지표 FAIL | `FAILED_TARGET_VALIDATION`. 재게시 없음 |
| HiveServer2 중단 중 실행 | DDL 단계에서 대기, Hive 재기동 후 이어서 `SUCCESS` |
| 파티션 재발행(`recovery.mode=REISSUE`) | 멈춘 파티션이 22초 뒤 같은 SCN으로 재발행되어 2번째 시도에서 성공, run `SUCCESS`. 늦게 도착한 첫 시도의 보고는 409 `CLAIM_MISMATCH`(WARN). 파일 중복 없음 |
| Job 2개 공유(root PG-05) | 두 Job을 동시에 실행해 각각 `SUCCESS`. 한 Job을 지우면 그 경로는 404, 다른 Job은 계속 수신 |
| 정리(PG-70) | 보존 기간이 지난 run의 staging 테이블과 HDFS 경로 삭제. 경로가 현재 `HDFS.STAGE.ROOT`와 다른 run은 거부(삭제 없음). HDFS 경로가 없는 run도 정상 처리 |
| Load Control API 테스트 | 140개 중 139개 통과(동시 완료, 중복 보고, 동시 claim, dispatcher 경합, deadlock 재시도, TUI 화면·운영 작업 포함). 나머지 1개는 서버 전체의 LISTEN 연결 수를 세는 테스트라 같은 DB 서버에 다른 worker가 돌던 시험 환경에서 제외 |

## 3. 설계에 반영한 제품 동작

시험에서 확인했고 Flow·문서에 반영한 NiFi·Hive 동작입니다. Flow를 고칠 때 다시 확인합니다.

| 동작 | 반영 |
|---|---|
| EL 문자열 리터럴 안의 Parameter(`'#{P}'`, `literal('#{P}')`)는 치환되지 않습니다 | Parameter 값은 `UpdateAttribute`로 attribute에 옮겨 비교(PG-70 71) |
| Sensitive 동적 속성에는 Parameter 참조 외 문자를 붙일 수 없습니다 | `CONTROL.API.AUTHORIZATION`에 `Bearer `까지 넣습니다 |
| `InvokeHTTP`의 Response Body Attribute Name을 쓰면 2xx가 아닌 응답 본문도 그 attribute에 들어갑니다 | PG-90이 `api.response`에서 오류 코드를 읽습니다 |
| 한 `UpdateAttribute` 안에서 방금 만든 attribute는 참조할 수 없습니다 | token 생성과 본문 생성을 다른 Processor로 나눕니다 |
| `PutSQL` Fragmented Transactions=true는 일부 fragment만 들어오면 livelock이 생길 수 있습니다 | PG-90 96은 false |
| 클러스터에서 Run Once는 모든 노드에서 실행됩니다 | Trigger(00)와 정리 트리거(70)는 Primary Node |
| `PutClouderaHiveQL`은 실패해도 오류 attribute를 남기지 않습니다 | 게시 실패는 모두 `PUBLISH_UNKNOWN` |
| `PutClouderaHiveQL`은 HiveServer2 연결 실패 시 FlowFile을 큐에 되돌려 계속 재시도합니다 | Hive 중단은 대기 후 자동 진행 |
| Hive JDBC는 결과 컬럼 이름에 테이블 별칭을 붙입니다 | `HIVE.JDBC.URL`에 `hive.resultset.use.unique.column.names=false` |
| 빈 결과에서 Hive `SUM`은 NULL | 지표 SQL에 `COALESCE` |
| Parquet timestamp는 NiFi JVM 시간대 기준 UTC로 저장됩니다 | NiFi JVM 시간대 = Hive `hive.local.time.zone` |
| `DeleteHDFS`는 glob을 받고, 없는 경로는 성공으로 처리합니다 | 경로를 정확히 비교한 뒤 삭제(PG-70 75) |
| root 연결을 지우려면 양 끝(PG-05 Output Port, Job PG Input Port)이 모두 멈춰 있어야 합니다 | 제거 스크립트가 Job PG를 먼저 멈춥니다 |
| REST로 Parameter Context를 바꿀 때 `inheritedParameterContexts`를 빼면 상속 해제로 처리됩니다 | 변경 요청에 상속 목록을 함께 보냅니다 |

## 4. 확인하지 않은 것

운영 적용 전에 확인할 항목은 [TODO.md](../TODO.md)에 있습니다. 주요 항목:

- CDP HiveServer2(Hive 3)와 ACID managed target 테이블
- 운영 규모 데이터의 처리 시간·부하, NiFi 노드 장애
- 운영 Oracle의 undo 보존 시간, 인덱스, 세션 한도
