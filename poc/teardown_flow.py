#!/usr/bin/env python3
"""build_flow_v4.py가 만든 Job을 지운다.

사용법: teardown_flow.py <nifi-api-url> <config.json>   (빌더에 쓴 config)

- Job PG(`JOB_<JOB.KEY>`)와 Job Parameter Context(`PC_JOB_<JOB.KEY>`)를 지운다.
- root PG-05 Control Receiver에서는 이 Job의 route·Output Port·연결만 지운다.
- 남은 Job이 없으면 PG-05와 공통 Parameter Context도 지운다.
"""
import json
import sys
import time
import urllib.error
import urllib.request

API = sys.argv[1].rstrip("/")
if len(sys.argv) != 3:
    raise SystemExit(__doc__)
CFG = json.load(open(sys.argv[2]))
NAMES = CFG.get("names", {})
RECEIVER_NAME = NAMES.get("control_receiver", "PG-05 Control Receiver")
JOB_KEY = CFG["job_params"]["JOB.KEY"]
PG_NAME = NAMES.get("process_group", f"JOB_{JOB_KEY}")
PC_COMMON = NAMES.get("common_context", "PC_SQOOP_REPLACEMENT_COMMON")
PC_JOB = NAMES.get("job_context", f"PC_JOB_{JOB_KEY}")
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


def stop_pg(pid):
    call("PUT", f"/flow/process-groups/{pid}", {"id": pid, "state": "STOPPED"})
    # 실행 중인 thread가 끝나야 controller service를 끌 수 있다.
    for _ in range(60):
        procs = call("GET", f"/process-groups/{pid}/processors")["processors"]
        if all(pr["status"]["aggregateSnapshot"]["activeThreadCount"] == 0
               and pr["component"]["state"] != "RUNNING" for pr in procs):
            return
        time.sleep(1)


def empty_queue(conn_id):
    req = call("POST", f"/flowfile-queues/{conn_id}/drop-requests")["dropRequest"]
    for _ in range(30):
        req = call("GET", f"/flowfile-queues/{conn_id}/drop-requests/{req['id']}")["dropRequest"]
        if req["finished"]:
            break
        time.sleep(1)
    call("DELETE", f"/flowfile-queues/{conn_id}/drop-requests/{req['id']}")


def delete_connection(cn):
    empty_queue(cn["id"])
    cur = call("GET", f"/connections/{cn['id']}")
    call("DELETE", f"/connections/{cn['id']}?version={cur['revision']['version']}&clientId={CID}")


def delete_pg(pid):
    stop_pg(pid)
    call("PUT", f"/flow/process-groups/{pid}/controller-services", {"id": pid, "state": "DISABLED"})
    for _ in range(60):
        svcs = call("GET", f"/flow/process-groups/{pid}/controller-services")["controllerServices"]
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


def unregister_job(receiver, job_pg):
    """root PG-05에서 이 Job의 연결·Output Port·route를 지운다. 남은 Job 목록을 돌려준다."""
    # 연결의 양 끝(PG-05 Output Port, Job PG Input Port)이 멈춰 있어야 연결을 지울 수 있다.
    stop_pg(job_pg)
    stop_pg(receiver)
    for cn in call("GET", f"/process-groups/{root}/connections")["connections"]:
        if cn["component"]["destination"]["groupId"] == job_pg:
            delete_connection(cn)
    procs = call("GET", f"/process-groups/{receiver}/processors")["processors"]
    r10 = next(p for p in procs if p["component"]["name"].startswith("10_"))
    r05 = next(p for p in procs if p["component"]["name"].startswith("05_"))
    names = {f"validate-{JOB_KEY}", f"reissue-{JOB_KEY}"}
    for port in call("GET", f"/process-groups/{receiver}/output-ports")["outputPorts"]:
        if port["component"]["name"] not in names:
            continue
        for cn in call("GET", f"/process-groups/{receiver}/connections")["connections"]:
            if cn["component"]["destination"]["id"] == port["id"]:
                delete_connection(cn)
        cur = call("GET", f"/output-ports/{port['id']}")
        call("DELETE", f"/output-ports/{port['id']}?version={cur['revision']['version']}&clientId={CID}")
    r10 = call("GET", f"/processors/{r10['id']}")
    call("PUT", f"/processors/{r10['id']}", {"revision": r10["revision"], "component": {"id": r10["id"], "config": {
        "properties": {f"validate.{JOB_KEY}": None, f"reissue.{JOB_KEY}": None}}}})
    r10 = call("GET", f"/processors/{r10['id']}")
    jobs = sorted({k.split(".", 1)[1] for k in r10["component"]["config"]["properties"]
                   if k.startswith(("validate.", "reissue."))})
    if jobs:
        r05 = call("GET", f"/processors/{r05['id']}")
        call("PUT", f"/processors/{r05['id']}", {"revision": r05["revision"], "component": {
            "id": r05["id"], "config": {"properties": {"Allowed Paths": f"/(validate|reissue)/({'|'.join(jobs)})"}}}})
        call("PUT", f"/flow/process-groups/{receiver}", {"id": receiver, "state": "RUNNING"})
    print("unregistered", JOB_KEY, "from", RECEIVER_NAME, "remaining jobs:", jobs)
    return jobs


root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
children = {pg["component"]["name"]: pg["id"]
            for pg in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]}
receiver_left = False
if PG_NAME in children and RECEIVER_NAME in children:
    if unregister_job(children[RECEIVER_NAME], children[PG_NAME]):
        receiver_left = True
    else:
        delete_pg(children[RECEIVER_NAME])
if PG_NAME in children:
    delete_pg(children[PG_NAME])
# Job context가 common을 상속하므로 Job부터 지운다. common은 PG-05나 다른 Job이 쓰면 남긴다.
contexts = {pc["component"]["name"]: pc["id"] for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"]}
for name in (PC_JOB, PC_COMMON):
    if name not in contexts:
        continue
    cur = call("GET", f"/parameter-contexts/{contexts[name]}")
    if name == PC_COMMON and (receiver_left or cur["component"].get("boundProcessGroups")
                              or any(pc["component"].get("inheritedParameterContexts")
                                     and any(i["id"] == contexts[name] for i in pc["component"]["inheritedParameterContexts"])
                                     for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"])):
        print("kept parameter context", name, "(still used)")
        continue
    call("DELETE", f"/parameter-contexts/{contexts[name]}?version={cur['revision']['version']}&clientId={CID}")
    print("deleted parameter context", name)
