#!/usr/bin/env python3
"""NiFi 2.4 REST API로 Sqoop 대체 PoC Flow V1을 만든다. Load Control API 없이 NiFi가 원장을 직접 기록하는
가이드 초안 구조다(PG-00 ~ PG-30 + 오류/이벤트 경로, 하나의 PG에 평면 배치, Processor 75개).

- 원장: NiFi PutSQL/ExecuteSQLRecord가 nifi_ops 테이블에 직접 기록, 완료 판정은 PG-30 Wait/Notify + DB 재조회

- 원천: PostgreSQL (Oracle 대체, AS OF SCN 대신 불변 업무일자 조건 사용)
- 관리 DB: PostgreSQL nifi_ops 스키마 (가이드 4.1 DDL)
- HDFS: PutHDFS + core-site.xml(fs.defaultFS=file:///) 로컬 파일시스템
- Hive(PG-40 이후)는 환경에 없으므로 EXTRACTED_VALIDATED + _SUCCESS marker까지 구현

사용법: build_flow_v1.py <nifi-api-url> <config.json>   (config.v1.example.json 형식)
결과는 poc/REVIEW.md 1~5장. Load Control API 연동 구조는 build_flow_v3.py를 쓴다.
"""
import json
import sys
import urllib.error
import urllib.request

API = sys.argv[1].rstrip("/")
CFG = json.load(open(sys.argv[2]))
PG_NAME = "SQOOP_REPLACEMENT_POC"


def call(method, path, body=None):
    req = urllib.request.Request(API + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            data = r.read()
            return json.loads(data) if data else None
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code}: {e.read().decode()[:2000]}")


REV = {"version": 0, "clientId": "poc-builder"}


TYPES = {t["type"].split(".")[-1]: t for t in
         call("GET", "/flow/processor-types")["processorTypes"]
         + call("GET", "/flow/controller-service-types")["controllerServiceTypes"]}


def bundle(short):
    return TYPES[short]["bundle"], TYPES[short]["type"]


# ---------------------------------------------------------------- Parameter Context
def drop_param_ctx(name):
    for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"]:
        if pc["component"]["name"] == name:
            call("DELETE", f"/parameter-contexts/{pc['id']}?version={pc['revision']['version']}&clientId=poc-builder")


def param_ctx(name, params, inherited=None):
    comp = {"name": name, "parameters": [
        {"parameter": {"name": k, "value": v, "sensitive": k.endswith("PASSWORD")}} for k, v in params.items()]}
    if inherited:
        comp["inheritedParameterContexts"] = [{"id": inherited, "component": {"id": inherited}}]
    return call("POST", "/parameter-contexts", {"revision": REV, "component": comp})["id"]


root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
for pg in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]:
    if pg["component"]["name"] == PG_NAME:
        raise SystemExit(f"{PG_NAME} already exists ({pg['id']}); delete it first")

drop_param_ctx("PC_JOB_PG_INSP_DTL_DAILY")
drop_param_ctx("PC_SQOOP_REPLACEMENT_COMMON")
common = param_ctx("PC_SQOOP_REPLACEMENT_COMMON", CFG["common_params"])
job = param_ctx("PC_JOB_PG_INSP_DTL_DAILY", CFG["job_params"], inherited=common)

pg = call("POST", f"/process-groups/{root}/process-groups",
          {"revision": REV, "component": {"name": PG_NAME, "position": {"x": 0, "y": 0}}})
PG = pg["id"]
call("PUT", f"/process-groups/{PG}", {"revision": pg["revision"],
     "component": {"id": PG, "parameterContext": {"id": job}}})

# ---------------------------------------------------------------- Controller Services
services = {}


def cs(name, short, props):
    b, t = bundle(short)
    ent = call("POST", f"/process-groups/{PG}/controller-services",
               {"revision": REV, "component": {"type": t, "bundle": b, "name": name, "properties": props}})
    services[name] = ent
    return ent["id"]


def hikari(prefix):
    return {"hikaricp-connection-url": f"#{{{prefix}.JDBC.URL}}",
            "hikaricp-driver-classname": "org.postgresql.Driver",
            "hikaricp-driver-locations": "#{JDBC.DRIVER.PATH}",
            "hikaricp-username": f"#{{{prefix}.JDBC.USER}}",
            "hikaricp-password": f"#{{{prefix}.JDBC.PASSWORD}}",
            "hikaricp-max-total-conns": "10",
            "hikaricp-validation-query": "SELECT 1"}


META = cs("CS_DBCP_META", "HikariCPConnectionPool", hikari("META"))
SRC = cs("CS_DBCP_SRC", "HikariCPConnectionPool", hikari("SRC"))
JREAD = cs("CS_JSON_READER", "JsonTreeReader", {})
JARR = cs("CS_JSON_WRITER_ARRAY", "JsonRecordSetWriter", {"output-grouping": "output-array"})
JLINE = cs("CS_JSON_WRITER_LINE", "JsonRecordSetWriter", {"output-grouping": "output-oneline"})
PARQ = cs("CS_PARQUET_WRITER", "ParquetRecordSetWriter", {"compression-type": "SNAPPY"})
cs("CS_MAP_CACHE_SERVER", "MapCacheServer", {"Port": "4557"})
DMC = cs("CS_DMC_CLIENT", "MapCacheClientService", {"Server Hostname": "localhost", "Server Port": "4557"})

# ---------------------------------------------------------------- Processors
procs = {}
conns = []
COLW, ROWH = 460, 200


def p(key, name, short, props=None, col=0, row=0, tasks=1, penalty="5 sec", sched="0 sec"):
    b, t = bundle(short)
    ent = call("POST", f"/process-groups/{PG}/processors", {"revision": REV, "component": {
        "type": t, "bundle": b, "name": name,
        "position": {"x": col * COLW, "y": row * ROWH},
        "config": {"properties": props or {}, "concurrentlySchedulableTaskCount": tasks,
                   "schedulingPeriod": sched, "penaltyDuration": penalty, "yieldDuration": "5 sec"}}})
    procs[key] = ent
    return key


def c(src, rels, dst):
    conns.append((src, rels if isinstance(rels, list) else [rels], dst))


def ua(key, name, attrs, col, row):
    return p(key, name, "UpdateAttribute", attrs, col, row)


def ejp(key, name, paths, col, row):
    return p(key, name, "EvaluateJsonPath", {"Destination": "flowfile-attribute", **paths}, col, row)


def route(key, name, routes, col, row):
    return p(key, name, "RouteOnAttribute", {"Routing Strategy": "Route to Property name", **routes}, col, row)


def esql(key, name, pool, sql, col, row, writer=JARR, extra=None, tasks=1):
    props = {"Database Connection Pooling Service": pool, "SQL Query": sql, "esqlrecord-record-writer": writer}
    props.update(extra or {})
    return p(key, name, "ExecuteSQLRecord", props, col, row, tasks=tasks)


def putsql(key, name, sql, col, row, fragmented="false", batch="1"):
    return p(key, name, "PutSQL", {"JDBC Connection Pool": META, "putsql-sql-statement": sql,
                                    "Support Fragmented Transactions": fragmented, "Batch Size": batch}, col, row,
             # fragment 일부만 poll되면 penalize되어 영구 대기(livelock)하므로 fragmented PutSQL은 penalty 0
             penalty="0 sec" if fragmented == "true" else "5 sec")


def retry(key, name, attr, maximum, col, row):
    return p(key, name, "RetryFlowFile", {"retry-attribute": attr, "maximum-retries": maximum,
                                          "penalize-retries": "true", "Fail on Non-numerical Overwrite": "true",
                                          "reuse-mode": "fail"}, col, row)


def err(key, name, stage, cls, msg, col, row, code="INTERNAL"):
    """오류 경로 전용 UpdateAttribute (가이드 14.6) -> 공통 실패 처리."""
    ua(key, name, {"error.stage": stage, "error.processor": name, "error.class": cls,
                   "error.code": code, "error.message": msg}, col, row)
    c(key, "success", "F1")


RUN = "CAST('${load.run.id}' AS uuid)"
NUM = "'^-?[0-9]+$'"
SRC_TABLE = "#{SRC.OWNER}.#{SRC.TABLE}"
SPLIT = "#{SRC.SPLIT.COLUMN}"

# ===== PG-00 Trigger (row 0)
p("P00", "00_Generate_Trigger", "GenerateFlowFile",
  {"generate-ff-custom-text": "{}", "Unique FlowFiles": "false"}, 0, 0, sched="1 day")
ua("P01", "01_Set_Trigger_Attributes", {
    "load.job.key": "#{JOB.KEY}",
    "load.business.key": "#{BUSINESS.KEY}",
    "load.trigger.type": "SCHEDULE"}, 1, 0)
route("P02", "02_Validate_Trigger", {
    "valid": "${load.business.key:matches('^[0-9]{4}-[0-9]{2}-[0-9]{2}$'):and(${load.job.key:isEmpty():not()})}"}, 2, 0)
err("E02", "02E_Invalid_Trigger", "TRIGGER", "VALIDATION", "invalid business key or job key", 3, 0)
c("P00", "success", "P01"); c("P01", "success", "P02"); c("P02", "valid", "P10"); c("P02", "unmatched", "E02")

# ===== PG-10 Run Coordinator (rows 1-4)
ua("P10", "10_Create_Run_Identity", {
    "load.run.id": "${UUID()}",
    "load.started.at": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSX\",\"UTC\")}",
    "error.run.status": "FAILED_MANIFEST"}, 0, 1)
ua("P10B", "10B_Derive_Run_Paths", {
    "load.hdfs.path": "#{HDFS.STAGE.ROOT}/#{JOB.KEY}/run_id=${load.run.id}",
    "load.stage.table": "tmp_insp_dtl_${load.run.id:replace('-','')}",
    "event.name": "RUN_STARTED", "event.level": "INFO"}, 1, 1)
putsql("P11", "11_Insert_Run_Lock",
       "INSERT INTO nifi_ops.load_run (run_id, job_key, business_key, status, hdfs_run_path, stage_table_name, parameters)\n"
       f"VALUES ({RUN}, '${{load.job.key}}', '${{load.business.key}}', 'CREATED', '${{load.hdfs.path}}',\n"
       "        '${load.stage.table}', '{\"partition_count\": #{PARTITION.COUNT}}'::jsonb)", 2, 1)
retry("R11", "11R_Retry_Run_Lock", "meta.retry.count", "#{PARTITION.RETRY.MAX}", 2, 2)
err("E11", "11E_Run_Lock_Failed", "RUN_LOCK", "NON_RETRYABLE",
    "run lock insert failed (duplicate active run or DB error)", 3, 1, code="DUPLICATE_OR_DB")
c("P10", "success", "P10B"); c("P10B", "success", "P11")
c("P11", "success", "P12"); c("P11", "success", "EV")
c("P11", "retry", "R11"); c("R11", "retry", "P11"); c("R11", ["retries_exceeded", "failure"], "E11")
c("P11", "failure", "E11")

esql("P12", "12_Query_Source_Metrics", SRC,
     f"SELECT COUNT(*) AS source_count,\n       MIN({SPLIT}) AS min_seq,\n       MAX({SPLIT}) AS max_seq,\n"
     f"       COUNT(*) - COUNT({SPLIT}) AS null_seq_count,\n       COUNT(DISTINCT {SPLIT}) AS distinct_seq_count,\n"
     f"       SUM(#{{DQ.AMOUNT.COLUMN}}) AS amount_sum\n  FROM {SRC_TABLE}\n WHERE #{{SRC.BASE.WHERE}}", 0, 2)
ejp("P13", "13_Extract_Source_Metrics", {
    "load.source.count": "$[0].source_count", "load.source.min": "$[0].min_seq",
    "load.source.max": "$[0].max_seq", "load.source.null.count": "$[0].null_seq_count",
    "load.source.distinct.count": "$[0].distinct_seq_count", "load.source.amount": "$[0].amount_sum"}, 1, 2)
route("P14", "14_Source_Precheck", {
    "valid": "${load.source.count:gt(0):and(${load.source.null.count:equals('0')})"
             f":and(${{load.source.min:matches({NUM})}}):and(${{load.source.max:matches({NUM})}})}}"}, 2, 3)
err("E12", "12E_Source_Metrics_Failed", "SOURCE_METRICS", "NON_RETRYABLE",
    "${executesql.error.message:replaceNull('source metrics query failed')}", 0, 3)
err("E14", "14E_Source_Precheck_Failed", "SOURCE_PRECHECK", "VALIDATION",
    "empty source or NULL split key: count=${load.source.count} nulls=${load.source.null.count}", 3, 3)
c("P12", "success", "P13"); c("P12", "failure", "E12")
c("P13", "matched", "P14"); c("P13", ["unmatched", "failure"], "E14")
c("P14", "valid", "P15"); c("P14", "unmatched", "E14")

putsql("P15", "15_Update_Run_Source_Metrics",
       "UPDATE nifi_ops.load_run\n   SET status = 'EXTRACTING', source_count = ${load.source.count},\n"
       "       source_null_split_count = ${load.source.null.count},\n"
       "       source_min_split = ${load.source.min}, source_max_split = ${load.source.max},\n"
       "       heartbeat_at = clock_timestamp(), version_no = version_no + 1\n"
       f" WHERE run_id = {RUN} AND status = 'CREATED'", 4, 1)
err("E15", "15E_Update_Run_Failed", "RUN_UPDATE", "NON_RETRYABLE", "run source metric update failed", 5, 1)
c("P15", "success", "P16"); c("P15", ["failure", "retry"], "E15")

MIN, MAX, N = "${load.source.min}", "${load.source.max}", "#{PARTITION.COUNT}"
esql("P16", "16_Query_Partition_Manifest", SRC,
     "WITH b AS (\n"
     f"  SELECT g AS pid,\n         {MIN} + floor(g * ({MAX} - {MIN} + 1)::numeric / {N}) AS lo,\n"
     f"         CASE WHEN g = {N} - 1 THEN {MAX}\n"
     f"              ELSE {MIN} + floor((g + 1) * ({MAX} - {MIN} + 1)::numeric / {N}) END AS hi,\n"
     f"         g = {N} - 1 AS incl\n    FROM generate_series(0, {N} - 1) g\n), c AS (\n"
     f"  SELECT b.*, (SELECT COUNT(*) FROM {SRC_TABLE} s\n                WHERE #{{SRC.BASE.WHERE}}\n"
     f"                  AND s.{SPLIT} >= b.lo\n                  AND (s.{SPLIT} < b.hi OR (b.incl AND s.{SPLIT} = b.hi))) AS cnt\n"
     "    FROM b\n)\nSELECT lpad(pid::text, 4, '0') AS partition_id, lo::bigint AS lower_bound, hi::bigint AS upper_bound,\n"
     "       incl AS upper_inclusive, cnt AS expected_row_count, SUM(cnt) OVER () AS manifest_total\n"
     "  FROM c ORDER BY pid", 4, 2)
ejp("P17", "17_Extract_Manifest_Total", {"load.manifest.total": "$[0].manifest_total"}, 5, 2)
ua("P17B", "17B_Capture_Manifest_Count", {
    "load.partition.count": "${record.count}", "load.partition.planned": "#{PARTITION.COUNT}",
    "event.name": "MANIFEST_CREATED", "event.level": "INFO",
    "event.row.count": "${load.manifest.total}"}, 6, 2)
# 가이드 7.4의 불변식(SUM(expected) = source_count)을 Worker 시작 전에 실제로 검사하는 단계
route("P18", "18_Check_Manifest_Invariant", {
    "valid": "${load.manifest.total:equals(${load.source.count}):and(${load.partition.count:equals(${load.partition.planned})})}"}, 7, 2)
err("E16", "16E_Manifest_Failed", "MANIFEST", "VALIDATION",
    "${executesql.error.message:replaceNull('manifest invariant failed: total=${load.manifest.total} source=${load.source.count}')}",
    5, 3)
c("P16", "success", "P17"); c("P16", "failure", "E16")
c("P17", "matched", "P17B"); c("P17", ["unmatched", "failure"], "E16")
c("P17B", "success", "P18"); c("P18", "valid", "P19"); c("P18", "unmatched", "E16")

putsql("P19", "19_Update_Run_Partition_Count",
       "UPDATE nifi_ops.load_run\n   SET expected_partition_count = ${load.partition.count}, heartbeat_at = clock_timestamp()\n"
       f" WHERE run_id = {RUN} AND status = 'EXTRACTING'", 8, 1)
c("P19", "success", "P20"); c("P19", "success", "EV"); c("P19", ["failure", "retry"], "E16")
p("P20", "20_Split_Manifest", "SplitRecord",
  {"Record Reader": JREAD, "Record Writer": JLINE, "Records Per Split": "1"}, 8, 2)
ejp("P21", "21_Extract_Partition_Attrs", {
    "partition.id": "$.partition_id", "partition.lower": "$.lower_bound", "partition.upper": "$.upper_bound",
    "partition.upper.inclusive": "$.upper_inclusive", "partition.expected.rows": "$.expected_row_count"}, 8, 3)
putsql("P22", "22_Insert_Partition_Row",
       "INSERT INTO nifi_ops.load_partition\n  (run_id, partition_id, lower_bound, upper_bound, upper_inclusive, expected_row_count)\n"
       f"VALUES ({RUN}, '${{partition.id}}', ${{partition.lower}}, ${{partition.upper}},\n"
       "        ${partition.upper.inclusive}, ${partition.expected.rows})", 8, 4, fragmented="true", batch="100")
route("P23", "23_Route_By_Expected_Rows", {"has_rows": "${partition.expected.rows:gt(0)}"}, 7, 4)
putsql("P24", "24_Mark_Empty_Partition_Success",
       "UPDATE nifi_ops.load_partition\n   SET status = 'SUCCESS', actual_row_count = 0, file_count = 0, fragment_count = 0,\n"
       "       completed_at = clock_timestamp(), heartbeat_at = clock_timestamp()\n"
       f" WHERE run_id = {RUN} AND partition_id = '${{partition.id}}' AND status = 'PENDING'", 6, 4)
p("P24N", "24A_Notify_Run_Progress", "Notify", {
    "release-signal-id": "${load.run.id}", "signal-counter-name": "partitions",
    "signal-counter-delta": "1", "distributed-cache-service": DMC}, 6, 5)
ua("P25", "25_Create_Run_Gate_Control", {"gate.source": "run-control"}, 9, 2)
c("P20", "splits", "P21"); c("P20", "original", "P25"); c("P20", "failure", "E16")
c("P21", "matched", "P22"); c("P21", ["unmatched", "failure"], "E16")
c("P22", "success", "P23"); c("P22", ["failure", "retry"], "E16")
c("P23", "has_rows", "P30"); c("P23", "unmatched", "P24")
c("P24", "success", "P24N"); c("P24", ["failure", "retry"], "E16"); c("P24N", "failure", "E16")
c("P25", "success", "P60")

# ===== PG-20 Oracle(=PostgreSQL) Extract Workers (rows 6-9)
ua("P30", "30_Set_Claim_Token", {
    "partition.claim.token": "${UUID()}", "partition.worker.node": "${hostname(true)}",
    "error.run.status": "FAILED_EXTRACT"}, 0, 6)
esql("P31", "31_Claim_Partition", META,
     f"SELECT nifi_ops.claim_partition({RUN}, '${{partition.id}}',\n"
     "       CAST('${partition.claim.token}' AS uuid), '${partition.worker.node}') AS claimed", 1, 6)
ejp("P32", "32_Extract_Claim_Result", {"partition.claimed": "$[0].claimed"}, 2, 6)
route("P33", "33_Is_Owner", {
    "owner": f"${{partition.claimed:equals('true'):and(${{partition.lower:matches({NUM})}})"
             f":and(${{partition.upper:matches({NUM})}})}}"}, 3, 6)
p("P33X", "33X_Log_Duplicate_Worker", "LogMessage", {
    "log-level": "warn", "log-prefix": "SQOOP_REPLACEMENT ",
    "log-message": "duplicate worker terminated run=${load.run.id} partition=${partition.id}"}, 3, 7)
err("E31", "31E_Claim_Failed", "PARTITION_CLAIM", "NON_RETRYABLE",
    "${executesql.error.message:replaceNull('claim failed')}", 1, 7)
c("P30", "success", "P31"); c("P31", "success", "P32"); c("P31", "failure", "E31")
c("P32", "matched", "P33"); c("P32", ["unmatched", "failure"], "E31")
c("P33", "owner", "P34"); c("P33", "unmatched", "P33X")

esql("P34", "34_Execute_Partition_Query", SRC,
     f"SELECT #{{SRC.COLUMNS}}\n  FROM {SRC_TABLE}\n WHERE #{{SRC.BASE.WHERE}}\n   AND {SPLIT} >= ${{partition.lower}}\n"
     f"   AND {SPLIT} ${{partition.upper.inclusive:equals('true'):ifElse('<=','<')}} ${{partition.upper}}",
     4, 6, writer=PARQ, tasks=4, extra={
         "esql-max-rows": "#{EXTRACT.ROWS.PER.FILE}", "esql-output-batch-size": "0",
         "esql-fetch-size": "#{EXTRACT.FETCH.SIZE}", "Max Wait Time": "#{EXTRACT.QUERY.TIMEOUT}",
         # PostgreSQL JDBC는 autocommit=false일 때만 fetch size(cursor)를 적용한다
         "esql-auto-commit": "false",
         # DATE/TIMESTAMP/DECIMAL을 Parquet logical type으로 유지
         "dbf-user-logical-types": "true"})
route("P34C", "34A_Classify_DB_Error", {
    "transient": "${executesql.error.message:find('(?i)(connection|timed out|timeout|I/O error|SQLState: 08|57P0[1-3])')}"}, 4, 7)
retry("R34", "34R_Retry_Partition_Query", "partition.retry.count", "#{PARTITION.RETRY.MAX}", 5, 7)
err("E34", "34E_Partition_Query_Failed", "SOURCE_EXTRACT", "NON_RETRYABLE",
    "${executesql.error.message:replaceNull('partition query failed')}", 4, 8)
c("P34", "success", "P35"); c("P34", "failure", "P34C")
c("P34C", "transient", "R34"); c("P34C", "unmatched", "E34")
c("R34", "retry", "P34"); c("R34", ["retries_exceeded", "failure"], "E34")

route("P35", "35_First_Fragment", {"first": "${fragment.index:equals('0')}"}, 6, 6)
p("P36", "36_Duplicate_First_Fragment", "DuplicateFlowFile", {"Number of Copies": "1"}, 6, 7)
# DuplicateFlowFile은 success 하나만 있으므로 copy.index로 원본/복제본을 분기한다
route("P37", "37_Split_Control_Copy", {"control": "${copy.index:equals('1')}"}, 6, 8)
ua("P38", "38_Set_Chunk_Attrs", {
    "chunk.index": "${fragment.index:padLeft(6,'0')}", "chunk.record.count": "${record.count}",
    # Hive external table이 하위 디렉터리를 읽지 않을 수 있으므로 run root에 평탄하게 기록
    "filename": "part-${partition.id}-${fragment.index:padLeft(6,'0')}.parquet"}, 7, 6)
p("P39", "39_PutHDFS", "PutHDFS", {
    "Hadoop Configuration Resources": "#{HADOOP.CONF.FILES}", "Directory": "${load.hdfs.path}",
    "Conflict Resolution Strategy": "replace", "writing-strategy": "writeAndRename",
    "Permissions umask": "#{HDFS.PERMISSIONS.UMASK}"}, 8, 6, tasks=4)
retry("R39", "39R_Retry_HDFS", "hdfs.retry.count", "#{PARTITION.RETRY.MAX}", 8, 7)
err("E39", "39E_HDFS_Write_Failed", "HDFS_WRITE", "TRANSIENT",
    "PutHDFS routed failure; see bulletin and provenance for run_id/partition_id", 9, 7)
# file audit UPSERT + partition/run heartbeat 갱신 (가이드에 없는 heartbeat 갱신 지점 보완)
putsql("P41", "41_Upsert_File_Audit",
       "WITH f AS (\n  INSERT INTO nifi_ops.load_file (run_id, partition_id, chunk_index, fragment_identifier, fragment_count,\n"
       "                                 hdfs_path, record_count, byte_count, status)\n"
       f"  VALUES ({RUN}, '${{partition.id}}', ${{fragment.index}}, '${{fragment.identifier}}', ${{fragment.count}},\n"
       "          '${absolute.hdfs.path}/${filename}', ${record.count}, ${fileSize}, 'WRITTEN')\n"
       "  ON CONFLICT (run_id, partition_id, chunk_index) DO UPDATE SET\n"
       "      fragment_identifier = EXCLUDED.fragment_identifier, fragment_count = EXCLUDED.fragment_count,\n"
       "      hdfs_path = EXCLUDED.hdfs_path, record_count = EXCLUDED.record_count,\n"
       "      byte_count = EXCLUDED.byte_count, status = 'WRITTEN', updated_at = clock_timestamp()\n"
       "  RETURNING 1\n), p AS (\n  UPDATE nifi_ops.load_partition SET heartbeat_at = clock_timestamp()\n"
       f"   WHERE run_id = {RUN} AND partition_id = '${{partition.id}}' RETURNING 1\n)\n"
       f"UPDATE nifi_ops.load_run SET heartbeat_at = clock_timestamp() WHERE run_id = {RUN}", 9, 6)
err("E41", "41E_File_Audit_Failed", "FILE_AUDIT", "NON_RETRYABLE", "load_file upsert failed", 10, 7)
p("P42", "42_Notify_Chunk", "Notify", {
    "release-signal-id": "${load.run.id}:${partition.id}", "signal-counter-name": "chunks",
    "signal-counter-delta": "1", "distributed-cache-service": DMC,
    "attribute-cache-regex": "^(load\\.run\\.id|partition\\.id)$"}, 10, 6)
c("P35", "first", "P36"); c("P35", "unmatched", "P38")
c("P36", "success", "P37"); c("P37", "control", "P40"); c("P37", "unmatched", "P38")
c("P38", "success", "P39"); c("P39", "success", "P41"); c("P39", "failure", "R39")
c("R39", "retry", "P39"); c("R39", ["retries_exceeded", "failure"], "E39")
c("P41", "success", "P42"); c("P41", ["failure", "retry"], "E41"); c("P42", "failure", "E41")

# ===== PG-30 Partition Gate (rows 9-11)
ua("P40", "40_Create_Partition_Control", {
    "partition.chunk.count": "${fragment.count}", "gate.source": "partition-control"}, 0, 9)
p("P50", "50_Wait_Chunk_Signals", "Wait", {
    "release-signal-id": "${load.run.id}:${partition.id}", "signal-counter-name": "chunks",
    "target-signal-count": "${partition.chunk.count}", "expiration-duration": "#{PARTITION.WAIT.TIMEOUT}",
    "distributed-cache-service": DMC, "wait-mode": "keep"}, 1, 9)
esql("P51", "51_Query_File_Audit", META,
     "SELECT COUNT(*) AS file_count, COALESCE(SUM(record_count), 0) AS row_count,\n"
     "       COALESCE(SUM(byte_count), 0) AS byte_count,\n"
     "       COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_file_count,\n"
     f"       (SELECT status FROM nifi_ops.load_run WHERE run_id = {RUN}) AS run_status\n"
     f"  FROM nifi_ops.load_file WHERE run_id = {RUN} AND partition_id = '${{partition.id}}'", 2, 9)
ejp("P52", "52_Extract_File_Totals", {
    "audit.file.count": "$[0].file_count", "audit.row.count": "$[0].row_count", "audit.byte.count": "$[0].byte_count",
    "audit.failed.count": "$[0].failed_file_count", "audit.run.status": "$[0].run_status"}, 3, 9)
route("P53", "53_Is_Partition_Complete", {
    "complete": "${audit.file.count:equals(${partition.chunk.count}):and(${audit.row.count:equals(${partition.expected.rows})})"
                ":and(${audit.failed.count:equals('0')}):and(${audit.run.status:equals('EXTRACTING')})}",
    "pending": "${audit.file.count:lt(${partition.chunk.count}):and(${audit.failed.count:equals('0')})"
               ":and(${audit.run.status:equals('EXTRACTING')})}"}, 4, 9)
retry("R53", "53R_Delay_And_Recheck", "gate.poll.count", "#{GATE.POLL.MAX}", 4, 10)
err("E53", "53E_Partition_Mismatch", "PARTITION_GATE", "VALIDATION",
    "partition mismatch: files=${audit.file.count}/${partition.chunk.count} rows=${audit.row.count}/"
    "${partition.expected.rows} run=${audit.run.status}", 5, 10)
putsql("P54", "54_Mark_Partition_SUCCESS",
       "UPDATE nifi_ops.load_partition\n   SET status = 'SUCCESS', actual_row_count = ${audit.row.count},\n"
       "       file_count = ${audit.file.count}, fragment_count = ${partition.chunk.count},\n"
       "       byte_count = ${audit.byte.count}, completed_at = clock_timestamp(), heartbeat_at = clock_timestamp()\n"
       f" WHERE run_id = {RUN} AND partition_id = '${{partition.id}}'\n"
       "   AND claim_token = CAST('${partition.claim.token}' AS uuid) AND status = 'RUNNING'", 5, 9)
p("P55", "55_Notify_Run_Progress", "Notify", {
    "release-signal-id": "${load.run.id}", "signal-counter-name": "partitions",
    "signal-counter-delta": "1", "distributed-cache-service": DMC}, 6, 9)
ua("EV55", "55E_Event_Partition_Success", {
    "event.name": "PARTITION_SUCCESS", "event.level": "INFO", "event.row.count": "${audit.row.count}"}, 7, 9)
c("P40", "success", "P50"); c("P50", ["success", "expired"], "P51"); c("P50", "failure", "E53")
c("P51", "success", "P52"); c("P51", "failure", "E53")
c("P52", "matched", "P53"); c("P52", ["unmatched", "failure"], "E53")
c("P53", "complete", "P54"); c("P53", "pending", "R53"); c("P53", "unmatched", "E53")
c("R53", "retry", "P51"); c("R53", ["retries_exceeded", "failure"], "E53")
c("P54", "success", "P55"); c("P54", ["failure", "retry"], "E53")
c("P55", "success", "EV55"); c("P55", "failure", "EV55"); c("EV55", "success", "EV")

# ===== PG-30 Run Gate (rows 11-12)
p("P60", "60_Wait_Run_Signals", "Wait", {
    "release-signal-id": "${load.run.id}", "signal-counter-name": "partitions",
    "target-signal-count": "${load.partition.count}", "expiration-duration": "#{RUN.WAIT.TIMEOUT}",
    "distributed-cache-service": DMC, "wait-mode": "keep"}, 0, 11)
esql("P61", "61_Query_Run_Manifest", META,
     "SELECT r.status AS run_status, r.source_count, r.expected_partition_count,\n"
     "       COUNT(p.partition_id) AS total_partition_count,\n"
     "       COUNT(*) FILTER (WHERE p.status = 'SUCCESS') AS success_partition_count,\n"
     "       COUNT(*) FILTER (WHERE p.status IN ('FAILED', 'TIMED_OUT')) AS failed_partition_count,\n"
     "       COUNT(*) FILTER (WHERE p.status IN ('PENDING', 'RUNNING', 'RETRY')) AS pending_partition_count,\n"
     "       COALESCE(SUM(p.actual_row_count), 0) AS extracted_count\n"
     "  FROM nifi_ops.load_run r LEFT JOIN nifi_ops.load_partition p ON p.run_id = r.run_id\n"
     f" WHERE r.run_id = {RUN}\n GROUP BY r.run_id", 1, 11)
ejp("P62", "62_Extract_Run_Totals", {
    "run.status": "$[0].run_status", "run.source.count": "$[0].source_count",
    "run.expected": "$[0].expected_partition_count", "run.total": "$[0].total_partition_count",
    "run.success": "$[0].success_partition_count", "run.failed": "$[0].failed_partition_count",
    "run.pending": "$[0].pending_partition_count", "run.extracted": "$[0].extracted_count"}, 2, 11)
route("P63", "63_Is_Run_Complete", {
    "complete": "${run.status:equals('EXTRACTING'):and(${run.total:equals(${run.expected})})"
                ":and(${run.success:equals(${run.expected})}):and(${run.failed:equals('0')})"
                ":and(${run.pending:equals('0')}):and(${run.extracted:equals(${run.source.count})})}",
    "failed": "${run.failed:gt(0):or(${run.status:startsWith('FAILED')}):or(${run.status:equals('TIMED_OUT')})}",
    "pending": "${run.status:equals('EXTRACTING'):and(${run.failed:equals('0')}):and(${run.pending:gt(0)})}"}, 3, 11)
retry("R63", "63R_Delay_And_Recheck", "run.poll.count", "#{GATE.POLL.MAX}", 3, 12)
err("E63", "63E_Run_Gate_Failed", "RUN_GATE", "VALIDATION",
    "run gate mismatch: status=${run.status} success=${run.success}/${run.expected} "
    "extracted=${run.extracted} source=${run.source.count}", 4, 12)
p("P63F", "63F_Log_Run_Already_Failed", "LogMessage", {
    "log-level": "error", "log-prefix": "SQOOP_REPLACEMENT ",
    "log-message": "run gate observed failure run=${load.run.id} status=${run.status} failed=${run.failed}"}, 2, 12)
putsql("P64", "64_CAS_EXTRACTED_VALIDATED",
       "UPDATE nifi_ops.load_run\n   SET status = 'EXTRACTED_VALIDATED', success_partition_count = ${run.success},\n"
       "       extracted_count = ${run.extracted}, extract_completed_at = clock_timestamp(),\n"
       "       heartbeat_at = clock_timestamp(), version_no = version_no + 1\n"
       f" WHERE run_id = {RUN} AND status = 'EXTRACTING'", 4, 11)
ua("P66", "66_Set_SUCCESS_Marker", {
    "filename": "_SUCCESS", "event.name": "EXTRACT_VALIDATED", "event.level": "INFO",
    "event.row.count": "${run.extracted}"}, 5, 11)
p("P67", "67_PutHDFS_SUCCESS_Marker", "PutHDFS", {
    "Hadoop Configuration Resources": "#{HADOOP.CONF.FILES}", "Directory": "${load.hdfs.path}",
    "Conflict Resolution Strategy": "replace", "writing-strategy": "writeAndRename",
    "Permissions umask": "#{HDFS.PERMISSIONS.UMASK}"}, 6, 11)
err("E67", "67E_SUCCESS_Marker_Failed", "HDFS_WRITE", "TRANSIENT", "_SUCCESS marker write failed", 6, 12)
c("P60", ["success", "expired", "failure"], "P61")
c("P61", "success", "P62"); c("P61", "failure", "E63")
c("P62", "matched", "P63"); c("P62", ["unmatched", "failure"], "E63")
c("P63", "complete", "P64"); c("P63", "pending", "R63"); c("P63", "failed", "P63F"); c("P63", "unmatched", "E63")
c("R63", "retry", "P61"); c("R63", ["retries_exceeded", "failure"], "E63")
c("P64", "success", "P66"); c("P64", ["failure", "retry"], "E63")
c("P66", "success", "P67"); c("P67", "success", "EV"); c("P67", "failure", "E67")

# ===== PG-90 공통 실패 처리 (row 14)
ua("F1", "90_Prepare_Failure", {
    "event.name": "${partition.id:isEmpty():ifElse('RUN_FAILED','PARTITION_FAILED')}",
    "event.level": "ERROR",
    "error.run.status": "${error.run.status:replaceNull('FAILED_EXTRACT')}",
    # 따옴표 이스케이프, 줄바꿈 제거, 1500자 제한
    "error.message.safe": "${error.message:replaceAll(\"'\",\"''\"):replaceAll('[\\r\\n]+',' '):replaceAll('(?s)^(.{0,1500}).*$','$1')}"},
   0, 14)
putsql("F2", "91_Mark_Partition_And_Run_Failed",
       "WITH p AS (\n  UPDATE nifi_ops.load_partition\n"
       "     SET status = 'FAILED', error_code = '${error.code}', error_message = '${error.message.safe}',\n"
       "         completed_at = clock_timestamp()\n"
       "   WHERE run_id = CAST(NULLIF('${load.run.id}', '') AS uuid) AND partition_id = '${partition.id}'\n"
       "     AND status IN ('PENDING', 'RUNNING', 'RETRY')\n  RETURNING 1\n)\n"
       "UPDATE nifi_ops.load_run\n   SET status = '${error.run.status}',\n"
       "       failed_partition_count = failed_partition_count + (SELECT COUNT(*) FROM p),\n"
       "       error_stage = '${error.stage}', error_code = '${error.code}', error_message = '${error.message.safe}',\n"
       "       completed_at = clock_timestamp(), version_no = version_no + 1\n"
       " WHERE run_id = CAST(NULLIF('${load.run.id}', '') AS uuid)\n"
       "   AND status IN ('CREATED', 'SNAPSHOT_FIXED', 'EXTRACTING')", 1, 14)
# run-control Wait를 즉시 깨워 실패를 조기 확정 (Run Wait 만료까지 대기하지 않음)
p("F3", "92_Release_Run_Gate", "Notify", {
    "release-signal-id": "${load.run.id}", "signal-counter-name": "partitions",
    "signal-counter-delta": "${load.partition.count:replaceNull('1')}", "distributed-cache-service": DMC}, 2, 14)
c("F1", "success", "F2"); c("F2", ["success", "failure", "retry"], "F3"); c("F3", ["success", "failure"], "EV")

# ===== PG-90 이벤트 기록 (row 15)
putsql("EV", "95_Insert_Load_Event",
       "INSERT INTO nifi_ops.load_event (event_id, event_level, event_name, run_id, job_key, business_key,\n"
       "    partition_id, chunk_index, process_group, processor_name, node_id, attempt_no, row_count,\n"
       "    error_class, error_code, message)\n"
       "VALUES (gen_random_uuid(), '${event.level:replaceNull('INFO')}', '${event.name}',\n"
       "    CAST(NULLIF('${load.run.id}', '') AS uuid), '${load.job.key}', '${load.business.key}',\n"
       "    NULLIF('${partition.id}', ''), CAST(NULLIF('${chunk.index}', '') AS integer), 'SQOOP_REPLACEMENT_POC',\n"
       "    NULLIF('${error.processor}', ''), '${hostname(true)}', CAST(NULLIF('${partition.retry.count}', '') AS integer),\n"
       "    CAST(NULLIF('${event.row.count}', '') AS bigint), NULLIF('${error.class}', ''), NULLIF('${error.code}', ''),\n"
       "    NULLIF('${error.message.safe}', ''))", 0, 15)
p("EV2", "96_LogMessage", "LogMessage", {
    "log-level": "${event.level:replaceNull('info'):toLower()}", "log-prefix": "SQOOP_REPLACEMENT ",
    "log-message": "{\"event\":\"${event.name}\",\"run_id\":\"${load.run.id}\",\"job_key\":\"${load.job.key}\","
                   "\"business_key\":\"${load.business.key}\",\"partition_id\":\"${partition.id}\","
                   "\"rows\":\"${event.row.count}\",\"error_stage\":\"${error.stage}\",\"error_class\":\"${error.class}\","
                   "\"message\":\"${error.message.safe}\"}"}, 1, 15)
c("EV", ["success", "failure", "retry"], "EV2")

# ---------------------------------------------------------------- Connections
for src, rels, dst in conns:
    s, d = procs[src]["component"], procs[dst]["component"]
    body = {"revision": REV, "component": {
        "source": {"id": s["id"], "groupId": PG, "type": "PROCESSOR"},
        "destination": {"id": d["id"], "groupId": PG, "type": "PROCESSOR"},
        "selectedRelationships": rels,
        "backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB"}}
    if src == "P23" and rels == ["has_rows"]:
        body["component"]["loadBalanceStrategy"] = "ROUND_ROBIN"  # Coordinator -> Worker
    if dst in ("P50", "P60"):
        body["component"]["prioritizers"] = ["org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"]
    call("POST", f"/process-groups/{PG}/connections", body)

# 연결되지 않은 relationship은 auto-terminate
used = {}
for src, rels, _ in conns:
    used.setdefault(src, set()).update(rels)
for key, ent in procs.items():
    cur = call("GET", f"/processors/{ent['id']}")
    rels = {r["name"] for r in cur["component"]["relationships"]}
    auto = sorted(rels - used.get(key, set()))
    call("PUT", f"/processors/{ent['id']}", {"revision": cur["revision"], "component": {
        "id": ent["id"], "config": {"autoTerminatedRelationships": auto}}})

# ---------------------------------------------------------------- Enable services
for name, ent in services.items():
    cur = call("GET", f"/controller-services/{ent['id']}")
    call("PUT", f"/controller-services/{ent['id']}/run-status", {"revision": cur["revision"], "state": "ENABLED"})

print(json.dumps({"process_group": PG, "trigger": procs["P00"]["id"],
                  "processors": {k: v["id"] for k, v in procs.items()}}, indent=1))
