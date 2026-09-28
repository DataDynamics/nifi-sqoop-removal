# Sqoop 병렬 처리 구조 및 NiFi 전환 시 고려사항

## 1. 문서 목적

본 문서는 NiFi 1.x 환경에서 호출하던 Apache Sqoop을 제거하고, NiFi 2.x 기반의 Cloudera CFM 4.12.0 환경으로 전환하기 위한 사전 분석 자료이다.

우선 Sqoop이 YARN 환경에서 데이터를 병렬로 처리하는 방식과 주요 설정, 성능 및 정합성 특성을 정리하고, 향후 NiFi 기반 대체 구조를 설계할 때 보존해야 하는 핵심 요구사항을 도출한다.

---

## 2. Sqoop 병렬 처리 개요

Sqoop의 병렬 처리는 하나의 Sqoop 작업을 여러 Map Task로 분할하여 동시에 수행하는 방식이다.

```text
NiFi
 └─ Sqoop 명령 실행
     └─ YARN에 MapReduce Application 제출
         ├─ Mapper 0 ─ JDBC Connection 0 ─ 데이터 구간 A 처리
         ├─ Mapper 1 ─ JDBC Connection 1 ─ 데이터 구간 B 처리
         ├─ Mapper 2 ─ JDBC Connection 2 ─ 데이터 구간 C 처리
         └─ Mapper 3 ─ JDBC Connection 3 ─ 데이터 구간 D 처리
```

일반적인 Sqoop import/export는 Reducer가 없는 Map-only Job이다. YARN은 각 Mapper를 컨테이너에 배치하고 실행, 재시도 및 모니터링을 담당한다.

NiFi는 일반적으로 Sqoop 명령을 외부 프로세스로 호출한 뒤 종료 코드와 로그를 기다릴 뿐, Sqoop 내부의 데이터 분할이나 Mapper 실행을 직접 담당하지 않는다.

### 2.1 병렬 처리 계층

Sqoop을 NiFi에서 호출하는 구조에서는 병렬성을 다음 두 계층으로 구분해야 한다.

1. **Sqoop 내부 병렬성**
   - 하나의 Sqoop 작업에서 `--num-mappers`만큼 Map Task를 생성한다.
   - 각 Mapper는 일반적으로 독립적인 JDBC Connection을 사용한다.

2. **NiFi 호출 병렬성**
   - NiFi Processor의 Concurrent Tasks 또는 Flow 구성에 따라 여러 Sqoop 작업이 동시에 실행될 수 있다.

예를 들어 NiFi가 Sqoop 작업 3건을 동시에 실행하고, 각 작업이 `--num-mappers 8`을 사용한다면 이론적으로 최대 약 24개의 Mapper JDBC 작업이 동시에 발생할 수 있다.

```text
예상 최대 Mapper JDBC 동시성
≈ 동시에 실행되는 Sqoop Job 수 × Job당 Mapper 수
```

여기에 Sqoop의 경계 조회, 메타데이터 조회 및 NiFi가 별도로 사용하는 JDBC Connection도 추가로 고려해야 한다.

---

## 3. Import 병렬 처리

### 3.1 병렬도 지정

Sqoop import의 병렬도는 `-m` 또는 `--num-mappers` 옵션으로 지정한다.

```bash
sqoop import \
  --connect jdbc:oracle:thin:@dbhost:1521/ORCL \
  --table ORDERS \
  --split-by ORDER_ID \
  --num-mappers 4 \
  --target-dir /data/orders
```

```text
-m 1    → 단일 Mapper
-m 4    → Mapper 4개
-m 8    → Mapper 8개
```

병렬도를 명시하지 않으면 일반적으로 기본값은 4이다. 다만 Mapper가 4개 생성된다는 것이 YARN에서 4개가 반드시 동시에 실행된다는 의미는 아니다.

```text
실제 동시 실행 Mapper 수
= min(
    --num-mappers,
    YARN에서 할당 가능한 컨테이너 수,
    YARN Queue 자원 한도,
    클러스터 가용 용량
  )
```

YARN 자원이 부족하면 Mapper 8개가 생성되어도 일부 Mapper는 컨테이너 할당을 기다린 뒤 실행될 수 있다.

### 3.2 `--split-by` 기반 데이터 분할

Sqoop은 지정된 분할 컬럼의 최솟값과 최댓값을 조회한 뒤, 해당 값의 범위를 Mapper 수만큼 나눈다.

예를 들어 다음 조건을 가정한다.

```text
분할 컬럼 : ORDER_ID
최솟값    : 1
최댓값    : 1,000,000
Mapper 수 : 4
```

Sqoop은 개념적으로 다음과 같은 SQL을 병렬 실행한다.

```sql
-- Mapper 0
SELECT * FROM ORDERS
 WHERE ORDER_ID >= 1
   AND ORDER_ID < 250001;

-- Mapper 1
SELECT * FROM ORDERS
 WHERE ORDER_ID >= 250001
   AND ORDER_ID < 500001;

-- Mapper 2
SELECT * FROM ORDERS
 WHERE ORDER_ID >= 500001
   AND ORDER_ID < 750001;

-- Mapper 3
SELECT * FROM ORDERS
 WHERE ORDER_ID >= 750001
   AND ORDER_ID <= 1000000;
```

기본적인 경계 조회는 개념적으로 다음과 같다.

```sql
SELECT MIN(ORDER_ID), MAX(ORDER_ID)
  FROM ORDERS;
```

분할 경계를 별도로 제어해야 하면 `--boundary-query`를 사용할 수 있다.

```bash
--split-by ORDER_ID \
--boundary-query "SELECT MIN(ORDER_ID), MAX(ORDER_ID)
                    FROM ORDERS
                   WHERE WORK_DT = '2026-09-28'"
```

### 3.3 분할 컬럼 선정 기준

`--split-by` 컬럼의 선택은 Sqoop 병렬 처리 성능을 좌우하는 핵심 요소이다.

적절한 분할 컬럼은 다음 특성을 갖는다.

- 값이 비교적 균등하게 분포한다.
- 전체 데이터 범위를 잘 대표한다.
- 범위 조건 검색에 활용할 수 있는 인덱스가 있다.
- 실행 중 값의 변경 가능성이 작다.
- 가능하면 숫자 또는 날짜 계열이다.
- Mapper별 처리 건수가 비슷하게 분배된다.

다음과 같이 값이 한쪽 구간에 몰린 컬럼은 분할 기준으로 적합하지 않다.

| 값 범위 | 데이터 건수 |
|---|---:|
| 1~250 | 100건 |
| 251~500 | 500건 |
| 501~750 | 1,000건 |
| 751~1,000 | 1억 건 |

이 경우 Mapper는 4개이지만 마지막 Mapper가 대부분의 데이터를 처리한다. 작업 전체 완료 시간은 가장 늦게 종료되는 Mapper에 의해 결정되므로 실질적인 병렬 처리 효과가 거의 없다.

이러한 현상을 **Data Skew** 또는 **Split Skew**라고 한다.

### 3.4 Primary Key와 분할 컬럼

`--split-by`를 지정하지 않으면 Sqoop은 일반적으로 테이블의 단일 컬럼 Primary Key를 분할 컬럼으로 사용한다.

적절한 Primary Key 또는 분할 컬럼이 없으면 단일 Mapper를 명시해야 한다.

```bash
--num-mappers 1
```

지원되는 환경에서는 다음 옵션을 사용할 수도 있다.

```bash
--autoreset-to-one-mapper
```

복합 Primary Key는 자동 병렬 분할 기준으로 사용하기 어렵기 때문에 별도의 단일 분할 컬럼을 지정하는 것이 일반적이다.

### 3.5 Free-form Query 병렬 처리

`--query`를 사용하는 경우 병렬 처리를 위해 SQL에 반드시 `$CONDITIONS` 토큰을 포함해야 한다.

```bash
sqoop import \
  --connect jdbc:oracle:thin:@dbhost:1521/ORCL \
  --query "
    SELECT O.ORDER_ID,
           O.CUSTOMER_ID,
           O.AMOUNT
      FROM ORDERS O
     WHERE O.WORK_DT = '2026-09-28'
       AND \$CONDITIONS
  " \
  --split-by O.ORDER_ID \
  --num-mappers 4 \
  --target-dir /data/orders
```

각 Mapper는 `$CONDITIONS`를 자신의 담당 범위 조건으로 치환한다.

```text
Mapper 0 → AND ORDER_ID >= 1      AND ORDER_ID < 250001
Mapper 1 → AND ORDER_ID >= 250001 AND ORDER_ID < 500001
Mapper 2 → AND ORDER_ID >= 500001 AND ORDER_ID < 750001
Mapper 3 → AND ORDER_ID >= 750001 AND ORDER_ID <= 1000000
```

`$CONDITIONS` 또는 적절한 `--split-by`가 없으면 쿼리를 안전하게 병렬 분할할 수 없다. 단일 Mapper로 실행할 때에도 Sqoop의 쿼리 형식상 `$CONDITIONS`가 필요한 경우가 있으므로 기존 명령을 정확히 확인해야 한다.

### 3.6 결과 파일

각 Mapper는 독립적인 출력 파일을 생성한다.

```text
/data/orders/
 ├─ part-m-00000
 ├─ part-m-00001
 ├─ part-m-00002
 ├─ part-m-00003
 └─ _SUCCESS
```

따라서 일반적인 Map-only import에서는 Mapper 수가 많아질수록 결과 파일 수도 증가한다.

후속 처리에는 다음과 같은 영향을 줄 수 있다.

- 작은 파일 증가
- NameNode 메타데이터 증가
- 후속 작업의 파일 Open 비용 증가
- 파일 단위 처리 시 NiFi FlowFile 수 증가 가능성
- 별도 Compaction 또는 Merge 작업 필요

---

## 4. Export 병렬 처리

Sqoop export는 HDFS 측 파일 또는 Input Split을 여러 Mapper가 나누어 읽고, 각 Mapper가 별도의 DB Connection을 사용하여 대상 테이블에 쓰는 방식이다.

```bash
sqoop export \
  --connect jdbc:oracle:thin:@dbhost:1521/ORCL \
  --table ORDERS_STG \
  --export-dir /data/orders \
  --num-mappers 4
```

```text
HDFS 파일 또는 Input Split
 ├─ Mapper 0 → JDBC Connection 0 → INSERT/UPDATE
 ├─ Mapper 1 → JDBC Connection 1 → INSERT/UPDATE
 ├─ Mapper 2 → JDBC Connection 2 → INSERT/UPDATE
 └─ Mapper 3 → JDBC Connection 3 → INSERT/UPDATE
```

Export의 처리 성능은 다음과 같은 DB 작업에 크게 영향을 받는다.

- 인덱스 갱신
- Unique Key 및 Foreign Key 검사
- Trigger 실행
- Undo/Redo 또는 Transaction Log 기록
- 테이블 및 블록 경합
- 동시 INSERT/UPDATE Lock
- DB Connection 제한

Mapper 수를 늘리면 처리량이 증가할 수 있지만, DB가 병목인 상태에서는 오히려 전체 성능이 저하될 수 있다.

### 4.1 Export 트랜잭션 특성

각 Mapper는 별도의 DB Connection과 별도의 트랜잭션을 사용한다. 따라서 Sqoop export 전체는 하나의 원자적 트랜잭션으로 처리되지 않는다.

```text
Mapper 0 → Commit 성공
Mapper 1 → Commit 성공
Mapper 2 → 실패
Mapper 3 → 일부 Commit 후 실패
```

이 경우 DB에 일부 데이터가 반영된 상태로 전체 작업이 실패할 수 있다.

따라서 운영 환경에서는 일반적으로 다음과 같은 패턴을 적용한다.

```text
Sqoop Export
 → Staging Table 적재
 → 건수 및 무결성 검증
 → MERGE 또는 테이블 교체
 → 실패 시 Staging 정리
```

NiFi로 전환할 때도 Sqoop export를 단순한 병렬 JDBC Write로 치환하면 부분 반영 문제가 그대로 발생할 수 있다. Staging, 검증, Commit 경계 및 재처리 정책을 함께 설계해야 한다.

---

## 5. 병렬도를 높여도 성능이 개선되지 않는 이유

Sqoop 작업 시간은 일반적으로 다음 구간 중 가장 느린 구간에 의해 결정된다.

```text
RDBMS 읽기/쓰기
      ↓
네트워크
      ↓
YARN Mapper 자원
      ↓
HDFS 읽기/쓰기
```

| 제약 영역 | 병렬도 증가에 따른 영향 |
|---|---|
| 원천 DB | 동시 SQL과 Connection 증가 |
| 대상 DB | Lock, Index, Trigger 및 Transaction Log 부하 증가 |
| YARN | Container, CPU 및 Memory 경쟁 증가 |
| 네트워크 | DB와 Hadoop 간 대역폭 포화 가능성 |
| HDFS | 쓰기 처리량 경쟁 및 작은 파일 증가 |
| 분할 데이터 | 불균등 분포 시 특정 Mapper만 장시간 실행 |
| SQL 구조 | 적절한 인덱스가 없으면 Mapper별 Full Scan 가능 |

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

