
특히 `--split-by` 컬럼에 적절한 인덱스가 없으면 여러 Mapper가 각각 넓은 범위의 테이블을 스캔할 수 있다. 이 경우 DB I/O가 급증하면서 병렬도를 높일수록 오히려 성능이 나빠질 수 있다.

---

## 6. 데이터 정합성 관점

병렬 Mapper는 일반적으로 서로 다른 JDBC Connection을 사용한다. Source DB의 데이터가 import 실행 중 변경되면 Mapper별 조회 시점이 달라질 수 있다.

```text
10:00:00 Mapper 0 조회 시작
10:00:03 원천 데이터 UPDATE
10:00:05 Mapper 1 조회 시작
```

따라서 하나의 Sqoop 작업 결과가 반드시 동일한 시점의 DB Snapshot을 의미하지는 않는다. Sqoop의 일반적인 import 기본 Isolation Level은 `READ_COMMITTED`이다.

정확한 시점 일관성이 필요한 경우 다음 방안을 별도로 검토해야 한다.

- DB Snapshot 또는 Flashback Query
- 모든 Mapper에 동일한 기준 시각 조건 적용
- CDC 기준점 사용
- 업무 중지 시간대에 배치 실행
- Snapshot 또는 Staging 테이블 생성 후 import
- DB 및 Connector별 Transaction Isolation 지원 범위 확인

---

## 7. 병렬 처리 튜닝 절차

`--num-mappers` 값을 단순히 크게 설정하지 말고 다음 순서로 측정하는 것이 안전하다.

1. 전체 데이터 건수와 용량을 확인한다.
2. 목표 처리시간을 정의한다.
3. 균등하게 분포한 `split-by` 컬럼을 선정한다.
4. 분할 컬럼의 인덱스와 SQL 실행계획을 확인한다.
5. `-m 1`, `-m 2`, `-m 4`, `-m 8` 순서로 단계별 성능을 측정한다.
6. DB 동시 세션, CPU, I/O, Lock 및 Transaction Log를 확인한다.
7. YARN Queue의 Container 할당 및 대기시간을 확인한다.
8. Mapper별 처리 건수, 출력 크기 및 종료시간을 비교한다.
9. HDFS 결과 파일의 개수와 평균 크기를 확인한다.
10. 실패 후 재실행 시 중복 및 부분 반영 여부를 검증한다.

Mapper 수는 다음 제약을 모두 고려하여 산정해야 한다.

```text
권장 Mapper 수
≤ DB가 허용하는 동시 세션 수
≤ YARN Queue에서 동시에 할당 가능한 컨테이너 수
≤ 운영상 허용 가능한 출력 파일 수
```

---

## 8. NiFi 전환을 위한 AS-IS 조사 항목

Sqoop 제거 및 NiFi 2.x 기반 대체 구조를 설계하기 전에 기존 Sqoop 명령과 NiFi Flow에서 다음 정보를 수집해야 한다.

| AS-IS 항목 | 확인 목적 |
|---|---|
| `--num-mappers`, `-m` | Sqoop 작업 하나당 요청 병렬도 확인 |
| `--split-by` | 데이터 병렬 분할 기준 확인 |
| `--boundary-query` | 분할 범위 계산 방식 확인 |
| `--query`와 `$CONDITIONS` | Mapper별 SQL 분할 방식 확인 |
| NiFi Concurrent Tasks | 동시에 실행 가능한 Sqoop 작업 수 확인 |
| Process Group 동시 실행 수 | 전체 Job 병렬도 확인 |
| YARN Queue | 실제 자원 할당 한도 확인 |
| JDBC 사용자 | DB별 동시 세션과 권한 확인 |
| Incremental 옵션 | 증분 기준과 Checkpoint 관리 방식 확인 |
| `--direct` | DB 전용 고속 Connector 사용 여부 확인 |
| Import/Export 구분 | 읽기 병렬화 또는 쓰기 병렬화 여부 확인 |
| 출력 파일 형식 | Text, Avro, SequenceFile, Parquet 등 확인 |
| 대상 경로와 파일 수 | 후속 처리 및 작은 파일 영향 확인 |
| 재실행 정책 | 중복 적재와 부분 반영 처리 방식 확인 |
| 검증 로직 | 건수, 합계 및 무결성 검증 방식 확인 |

### 8.1 AS-IS 최대 DB 부하 계산

기존 환경의 잠재적인 DB 동시 부하는 다음 식을 기준으로 산정한다.

```text
예상 최대 DB 동시 연결 수
≈ 동시에 실행되는 Sqoop Job 수
  × Job당 Mapper 수
  + 경계/메타데이터 조회 Connection
  + NiFi가 별도로 사용하는 Connection
```

NiFi 전환 설계에서도 이 값을 초과하지 않도록 Processor Concurrent Tasks, Connection Pool 크기, Partition 수 및 Back Pressure를 함께 제한해야 한다.

---

## 9. 핵심 요약

Sqoop의 병렬 처리는 단순한 멀티스레드 복사가 아니다. 분할 컬럼을 기준으로 데이터 범위를 나누고, 각 범위를 독립적인 YARN Map Task와 JDBC Connection으로 처리하는 분산 병렬 모델이다.

핵심 특성은 다음과 같다.

- `--num-mappers`는 요청 병렬도이며 실제 동시 실행 수는 YARN 자원에 의해 제한된다.
- Import는 `--split-by` 컬럼의 min/max 범위를 Mapper별로 나누어 처리한다.
- 분할 컬럼의 데이터 분포와 인덱스가 병렬 처리 효율을 결정한다.
- Mapper 수 증가는 DB Connection, SQL, Network 및 HDFS 부하를 함께 증가시킨다.
- 각 Mapper가 별도의 Connection을 사용하므로 동일 시점 Snapshot이 자동으로 보장되지 않는다.
- Export는 여러 Mapper가 독립된 Transaction으로 DB에 반영하므로 전체 원자성이 보장되지 않는다.
- NiFi가 여러 Sqoop 작업을 동시에 호출하면 `Job 동시성 × Mapper 수`만큼 DB 부하가 증폭될 수 있다.
- NiFi 2.x 기반 전환에서는 병렬도뿐 아니라 분할 방식, Checkpoint, 정합성, 실패 원자성 및 재처리 정책을 함께 재설계해야 한다.

---

## 10. 참고 자료

- [Apache Sqoop 1.4.6 User Guide](https://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html)
- [Apache Sqoop Import Documentation Source](https://apache.googlesource.com/sqoop/+/refs/heads/trunk/src/docs/user/import.txt)
- [Cloudera: Creating a Sqoop Import Command](https://docs.cloudera.com/cdp-private-cloud-base/7.3.2/migrating-data-into-hive/topics/hive_create_a_sqoop_import_command.html)
- [Apache Hadoop: MapReduce NextGen and YARN](https://hadoop.apache.org/docs/r2.7.3/hadoop-yarn/hadoop-yarn-site/)
