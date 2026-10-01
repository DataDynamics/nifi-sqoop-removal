# nifi-sqoop-removal-guide.md 검토 및 NiFi 2.4.0 PoC 결과

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

2장의 #1~#10과 3장 7번은 `nifi-sqoop-removal-guide.md` 본문에 반영했다. 반영 위치: 2장, 3.1, 4장, 5장, 7.1~7.4, 8.1~8.4, 10.2, 15.1. 3장의 1~6번(heartbeat, Hive 하위 디렉터리, PutSQL CAS 결과, 실패 후 남는 control FlowFile, PUBLISH_UNKNOWN 판별, PostgreSQL 원천)은 아직 반영하지 않았다.
