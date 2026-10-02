# 기존 Sqoop 동작과 전환 시 지켜야 할 점

Sqoop을 NiFi로 바꿀 때 "Sqoop이 해 주던 일" 중 무엇을 직접 만들어야 하는지 정리한다.

## 1. Sqoop은 어떻게 병렬로 읽나

```text
NiFi → Sqoop 명령 → YARN MapReduce Job
                     ├─ Mapper 0 ─ JDBC 연결 0 ─ 범위 A
                     ├─ Mapper 1 ─ JDBC 연결 1 ─ 범위 B
                     └─ Mapper N ─ JDBC 연결 N ─ 범위 N
```

1. `--split-by` 컬럼의 최솟값·최댓값을 조회한다.
2. 그 범위를 `--num-mappers` 개로 나눈다(하한 포함, 상한 미포함, 마지막만 상한 포함).
3. Mapper마다 별도 JDBC 연결로 자기 범위를 읽어 HDFS에 `part-m-0000N` 파일을 쓴다.
4. 하나라도 실패하면 YARN이 Job 전체를 실패로 처리한다.

```sql
-- Mapper 0                                   -- Mapper 3(마지막)
WHERE ORDER_ID >= 1 AND ORDER_ID < 250001      WHERE ORDER_ID >= 750001 AND ORDER_ID <= 1000000
```

## 2. 알고 있어야 할 특성

| 특성 | 의미 |
|---|---|
| 동시 DB 연결 | `동시에 실행되는 Sqoop Job 수 × Mapper 수`만큼 원천 DB에 연결한다 |
| 분할 컬럼 | 값이 고르게 분포하고 인덱스가 있어야 한다. 한쪽에 몰리면 Mapper 하나만 오래 돈다(skew) |
| 시점 일관성 | Mapper마다 연결이 달라 **같은 시점의 데이터를 보장하지 않는다**(기본 READ COMMITTED) |
| 실패 원자성 | Mapper 결과는 각각 쓰인다. 전체 성공 여부는 YARN Job 상태로만 안다 |
| 병렬도 상한 | Mapper 수를 늘려도 DB·네트워크·HDFS 중 가장 느린 곳에서 막힌다 |

## 3. NiFi로 바꿀 때 직접 만들어야 하는 것

| Sqoop이 해 주던 일 | 이 프로젝트의 구현 |
|---|---|
| 범위 분할 | PG-10이 SQL 한 문장으로 경계와 파티션별 예상 건수를 계산 |
| Mapper 병렬 실행 | PG-20 Worker를 NiFi 클러스터 전체 노드에서 실행 |
| Job 전체 성공·실패 판정 | Load Control API가 chunk 보고마다 파티션·run 완료를 판정 |
| (없던 것) 같은 시점 읽기 | Oracle SCN 고정 + `AS OF SCN` |
| (없던 것) 단계별 정합성 검증 | 원천·staging·target 지표 비교 |

NiFi Processor를 병렬로 돌리는 것만으로는 "모든 범위가 성공했을 때만 게시"를 보장할 수 없다. 그래서 완료 판정을 API로 분리했다.

## 4. 전환 전 AS-IS 조사 항목

| 항목 | 확인 목적 |
|---|---|
| `--num-mappers`, `--split-by`, `--boundary-query`, `--query`의 `$CONDITIONS` | 분할 방식과 병렬도 |
| NiFi에서 동시에 실행되는 Sqoop Job 수 | 원천 DB 최대 동시 연결 수(`Job 수 × Mapper 수`) |
| 증분 옵션, 추출 조건 | 업무 조건(`SRC.BASE.WHERE`)으로 옮길 내용 |
| 출력 형식, 대상 경로, 파일 수 | HDFS·Hive 구성 |
| 재실행 정책, 검증 방식 | 실패 시 처리와 검증 지표 |

NiFi로 바꾼 뒤에도 원천 DB 동시 연결 수가 기존 최대값을 넘지 않도록 `노드 수 × Worker 동시 실행 수`와 연결 풀 크기를 정한다.

## 참고

- [Apache Sqoop User Guide](https://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html)
