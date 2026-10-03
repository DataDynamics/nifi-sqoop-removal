# 운영 적용 TODO

시험 환경에서 확인한 구성([검증 결과](./poc/VERIFICATION.md))을 운영에 적용하기 전에 남은 일이다.

## 1. 플랫폼

- [ ] CDP HiveServer2(Hive 3)로 다시 시험한다. 시험은 Apache Hive 4.0.1이었다. target이 ACID managed table이면 external staging에서 `INSERT OVERWRITE`가 되는지 확인한다
- [ ] 운영 Oracle 버전에 맞는 ojdbc와 관리 DB용 PostgreSQL JDBC를 모든 NiFi 노드의 같은 경로에 둔다
- [ ] `HADOOP.CONF.FILES`(core-site, hdfs-site)를 모든 노드의 같은 경로에 두고 `HDFS.STAGE.ROOT`를 정한다
- [ ] NiFi JVM 시간대와 Hive `hive.local.time.zone`을 같은 값으로 정한다

## 2. 운영 Oracle

- [ ] 조회 계정 권한: 대상 테이블 `SELECT`·`FLASHBACK`, `SYS.V_$DATABASE` `SELECT`
- [ ] `UNDO_RETENTION`과 undo 크기가 run 최대 소요시간보다 넉넉한지 DBA와 확인
- [ ] 대상 테이블마다 `(업무 조건 컬럼, split 컬럼)` 인덱스와 16 SQL 실행 계획 확인
- [ ] 승인된 동시 세션 수에 맞춰 `ORACLE.POOL.MAX`와 34 Concurrent Tasks를 정한다
- [ ] split 컬럼에 NULL이 있는 테이블을 찾는다(NULL이 있으면 manifest가 거부된다)

## 3. Job 정의

- [ ] Job마다 `job_params`를 확정한다: 원천 테이블·컬럼(정밀도 없는 `NUMBER`는 CAST), split 컬럼, 업무 조건, DQ 컬럼, 파티션 수, staging·target 정의
- [ ] Hive staging·target DDL 타입을 Parquet 결과와 맞춘다
- [ ] CLOB·BLOB·`RAW`·`INTERVAL` 등 시험하지 않은 타입이 있는 테이블은 따로 시험한다
- [ ] 마이크로초 이하 정밀도가 필요한 컬럼은 문자열로 추출한다(Parquet은 밀리초)

## 4. Load Control API 배포

- [ ] 관리 DB(`nifi_ops`)를 만들고 migration 계정으로 `bin/migrate.sh` 실행
- [ ] 런타임 계정 분리: API는 원장 쓰기, NiFi는 `load_event` INSERT만(둘 다 DELETE·DDL 없음)
- [ ] API 2개 이상(LB 뒤)과 worker 2개를 systemd로 띄운다(`bin/systemd/install.sh`)
- [ ] 토큰(nifi, operator)을 발급해 digest를 `config.yaml`에 넣는다
- [ ] 운영값 확정: `recovery`(`mode`, `run_timeout`, `stale`), `cleanup` 보존 기간, `dispatch`. 재발행(`REISSUE`) 사용 여부
- [ ] 메트릭 수집과 알림: `DEAD` dispatch, `PUBLISH_UNKNOWN`, `TIMED_OUT`, `FAILED_*`, 5xx 증가
- [ ] `load_event` 보존 삭제 Job, `logs/*.out` logrotate
- [ ] 방화벽: API 포트는 NiFi 노드·운영자 대역, NiFi PG-05 포트는 API worker 호스트만

## 5. NiFi 설정

- [ ] 환경별 Concurrent Tasks·재시도 횟수를 정해 빌더 값에 반영한다(이 값들은 Parameter로 바꿀 수 없다)
- [ ] Back Pressure, Provenance 보존 기간, Bulletin 수집 연계
- [ ] NiFi Policy: 운영자와 개발자 분리, Parameter Context 변경 권한 제한
- [ ] Trigger 스케줄과 업무일자 계산식을 운영 일정에 맞춘다

## 6. 운영 환경 재시험

- [ ] 운영과 같은 구성(실제 HDFS·Hive, 운영 Oracle)에서 [검증 결과](./poc/VERIFICATION.md) 2장 시나리오를 다시 수행
- [ ] 시험하지 못한 장애: 파티션 처리 중 NiFi 노드 종료·재기동, API 인스턴스 1개 종료, worker 종료, 관리 DB 연결 차단
- [ ] 운영 규모 데이터로 2/4/8 파티션 처리 시간과 Oracle·API·관리 DB 부하를 재고 `PARTITION.COUNT`, `EXTRACT.ROWS.PER.FILE`, Concurrent Tasks를 정한다

## 7. 전환

- [ ] 대상 Job 목록과 전환 순서(작은 테이블부터)
- [ ] AS-IS Sqoop과 병행 실행해 건수·합계·최소·최대·샘플 행 비교. 병행 중에는 별도 target에 게시
- [ ] 되돌림 절차: Kylo `ImportSqoop` Flow 재활성화 기준
- [ ] 운영 Runbook 확정([통합 검증과 운영·복구](./docs/06-validation-and-operations.md) 기반)
- [ ] 전환 후 Kylo Sqoop Flow 제거

## 선택 기능

- [ ] PG-20 `ValidateRecord`(승인된 schema로 Parquet 검증)
- [ ] PG-90 이벤트 DB 장애 대비 DLQ, ERROR 알림
- [ ] `PutHDFS` 실패 원인 세분화(지금은 `CHUNK_WRITE_FAILED` 하나)
