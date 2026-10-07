#!/usr/bin/env python3
"""build_flow.py가 만든 최상위 Process Group(하위 Receiver·Event Logger·Job PG 포함)과 Parameter Context를 지운다.

사용법: teardown_flow.py <nifi-api-url> [최상위 PG 이름, 기본 SQOOP_REPLACEMENT]
"""
import json
import sys
import time
import urllib.error
import urllib.request

API = sys.argv[1].rstrip("/")
PG_NAMES = {sys.argv[2] if len(sys.argv) > 2 else "SQOOP_REPLACEMENT", "SQOOP_REPLACEMENT_POC"}  # 뒤는 이전 구조
COMMON_CTX = "PC_SQOOP_REPLACEMENT_COMMON"
CID = "poc-builder"


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


root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
for pg in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]:
    if pg["component"]["name"] not in PG_NAMES:
        continue
    pid = pg["id"]
    call("PUT", f"/flow/process-groups/{pid}", {"id": pid, "state": "STOPPED"})
    # 실행 중인 thread가 끝나야 controller service를 끌 수 있다(하위 PG 포함).
    for _ in range(60):
        snap = call("GET", f"/flow/process-groups/{pid}/status?recursive=true")
        if snap["processGroupStatus"]["aggregateSnapshot"]["activeThreadCount"] == 0:
            break
        time.sleep(1)
    call("PUT", f"/flow/process-groups/{pid}/controller-services", {"id": pid, "state": "DISABLED"})
    for _ in range(60):
        svcs = call("GET", f"/flow/process-groups/{pid}/controller-services"
                           "?includeAncestorGroups=false&includeDescendantGroups=true")["controllerServices"]
        if all(sv["component"]["state"] == "DISABLED" for sv in svcs):
            break
        time.sleep(1)
    # 큐에 남은 FlowFile을 비운다
    drop = call("POST", f"/process-groups/{pid}/empty-all-connections-requests")
    req_id = drop["dropRequest"]["id"]
    for _ in range(30):
        st = call("GET", f"/process-groups/{pid}/empty-all-connections-requests/{req_id}")
        if st["dropRequest"]["finished"]:
            break
        time.sleep(1)
    call("DELETE", f"/process-groups/{pid}/empty-all-connections-requests/{req_id}")
    cur = call("GET", f"/process-groups/{pid}")
    call("DELETE", f"/process-groups/{pid}?version={cur['revision']['version']}&clientId={CID}")
    print("deleted", pid)
# Job context(PC_JOB_*)가 common을 상속하므로 Job부터 지운다.
contexts = call("GET", "/flow/parameter-contexts")["parameterContexts"]
common_ids = {pc["id"] for pc in contexts if pc["component"]["name"] == COMMON_CTX}
jobs = [pc for pc in contexts
        if {i["id"] for i in pc["component"].get("inheritedParameterContexts") or []} & common_ids]
for pc in [*jobs, *[pc for pc in contexts if pc["id"] in common_ids]]:
    cur = call("GET", f"/parameter-contexts/{pc['id']}")
    call("DELETE", f"/parameter-contexts/{pc['id']}?version={cur['revision']['version']}&clientId={CID}")
    print("deleted parameter context", pc["component"]["name"])
