#!/usr/bin/env python3
"""deploy_job_flow.py가 만든 Job을 지운다.

사용법: remove_job_flow.py <nifi-api-url> <config.json>   (배포에 쓴 config)

- Job PG(`JOB_<JOB.KEY>`)와 Job Parameter Context(`PC_JOB_<JOB.KEY>`)를 지운다.
- root PG-05 Control Receiver에서는 이 Job의 route·Output Port·연결만 지운다.
- 남은 Job이 없으면 PG-05와 공통 Parameter Context도 지운다.
"""
import json
import sys
import time
import urllib.error
import urllib.request

if len(sys.argv) != 3:
    raise SystemExit(__doc__)
API = sys.argv[1].rstrip("/")
CFG = json.load(open(sys.argv[2]))
NAMES = CFG.get("names", {})
RECEIVER_NAME = NAMES.get("control_receiver", "PG-05 Control Receiver")
JOB_KEY = CFG["job_params"]["JOB.KEY"]
PG_NAME = NAMES.get("process_group", f"JOB_{JOB_KEY}")
PC_COMMON = NAMES.get("common_context", "PC_SQOOP_REPLACEMENT_COMMON")
PC_JOB = NAMES.get("job_context", f"PC_JOB_{JOB_KEY}")
CID = "nifi-flow-remover"   # DELETE 요청의 clientId 쿼리 값. revision version과 함께 보낸다.


def call(method, path, body=None):
    """NiFi REST API를 호출하고 응답 JSON을 돌려준다(본문이 비면 None).

    HTTP 오류는 상태 코드와 본문 앞 2000자를 담아 SystemExit로 멈춘다. 중간에 멈춰도 다시 실행하면
    남은 것부터 지운다(이미 지워진 PG·Context는 건너뛴다).
    """
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
    """PG(하위 PG 포함)를 STOPPED로 바꾸고, 하위 PG까지 active thread가 0이 될 때까지 최대 60초 기다린다.

    Job PG(`JOB_<KEY>`)는 Processor를 모두 하위 PG(PG-00~90)에 두므로 바로 아래 Processor만 보면 기다리지
    않고 지나간다. 그래서 PG status의 aggregateSnapshot(하위 PG 합계)으로 thread 수를 본다.
    시간 안에 멈추지 않아도 예외 없이 돌아간다. 이후 삭제 요청이 실패하면 call()이 멈춘다.
    """
    call("PUT", f"/flow/process-groups/{pid}", {"id": pid, "state": "STOPPED"})
    # 실행 중인 thread가 끝나야 controller service를 끄고 연결·PG를 지울 수 있다.
    for _ in range(60):
        snap = call("GET", f"/flow/process-groups/{pid}/status")["processGroupStatus"]["aggregateSnapshot"]
        if snap["activeThreadCount"] == 0:
            return
        time.sleep(1)


def empty_queue(conn_id):
    """연결 하나의 큐를 비운다.

    drop-request를 만들고 끝날 때까지 최대 30초 기다린 뒤 요청을 DELETE로 정리한다.
    큐에 FlowFile이 남아 있는 연결은 지울 수 없다.
    """
    req = call("POST", f"/flowfile-queues/{conn_id}/drop-requests")["dropRequest"]
    for _ in range(30):
        req = call("GET", f"/flowfile-queues/{conn_id}/drop-requests/{req['id']}")["dropRequest"]
        if req["finished"]:
            break
        time.sleep(1)
    call("DELETE", f"/flowfile-queues/{conn_id}/drop-requests/{req['id']}")


def delete_connection(cn):
    """연결을 비우고 지운다. 삭제에는 최신 revision version이 필요하므로 다시 읽는다."""
    empty_queue(cn["id"])
    cur = call("GET", f"/connections/{cn['id']}")
    call("DELETE", f"/connections/{cn['id']}?version={cur['revision']['version']}&clientId={CID}")


def delete_pg(pid):
    """PG를 통째로 지운다.

    NiFi는 실행 중인 구성요소, 켜진 Controller Service, 비어 있지 않은 큐가 있는 PG를 지우지 않으므로
    1. PG를 멈추고 2. PG 안 Controller Service를 모두 DISABLED로 바꿔 끝날 때까지(최대 60초) 기다리고
    3. 모든 연결의 큐를 비운 뒤(empty-all-connections-requests, 최대 30초) 4. 최신 revision으로 DELETE한다.
    """
    stop_pg(pid)
    # Controller Service 비활성화는 비동기다. 참조하는 Processor가 멈춰 있어야 끝난다.
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
    """root PG-05에서 이 Job의 연결·Output Port·route를 지운다. 남은 Job 목록을 돌려준다.

    순서:
    1. Job PG와 PG-05를 멈춘다.
    2. root에서 Job PG로 들어가는 연결(PG-05 Output Port → Job PG validate-in·reissue-in)을 비우고 지운다.
    3. PG-05 안에서 이 Job의 Output Port(validate-<JOB>, reissue-<JOB>)로 가는 연결을 지우고 Port를 지운다.
    4. 10_Route_By_Job_Action에서 validate.<JOB>·reissue.<JOB> 동적 속성을 지운다(값 None이 속성 삭제다).
       연결이 먼저 지워져 있어야 해당 relationship이 사라질 수 있다.
    5. 남은 Job이 있으면 05_Listen_Control의 Allowed Paths를 남은 Job으로 줄이고 PG-05를 다시 시작한다.
       남은 Job이 없으면 PG-05를 멈춘 채로 두고, 호출한 쪽이 PG-05를 지운다.
    """
    # 연결의 양 끝(PG-05 Output Port, Job PG Input Port)이 멈춰 있어야 연결을 지울 수 있다.
    stop_pg(job_pg)
    stop_pg(receiver)
    for cn in call("GET", f"/process-groups/{root}/connections")["connections"]:
        if cn["component"]["destination"]["groupId"] == job_pg:
            delete_connection(cn)
    procs = call("GET", f"/process-groups/{receiver}/processors")["processors"]
    r10 = next(p for p in procs if p["component"]["name"].startswith("10_"))
    r05 = next(p for p in procs if p["component"]["name"].startswith("05_"))
    names = {f"validate-{JOB_KEY}", f"reissue-{JOB_KEY}"}   # register_job이 만든 Output Port 이름
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
    # 지운 뒤 남은 route 속성 이름에서 남은 Job 목록을 얻는다.
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


# 순서: PG-05에서 이 Job 등록 해제(남은 Job이 없으면 PG-05 삭제) → Job PG 삭제 → Parameter Context 삭제.
# Job PG로 들어가는 root 연결이 남아 있으면 Job PG를 지울 수 없으므로 등록 해제를 먼저 한다.
root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
children = {pg["component"]["name"]: pg["id"]
            for pg in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]}
receiver_left = False   # PG-05가 남았는지(다른 Job이 쓰는지). 남으면 공통 Context도 지우지 않는다.
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
    # 공통 Context는 PG-05가 남았거나, 아직 붙어 있는 PG가 있거나, 다른 Job Context가 상속하면 남긴다.
    if name == PC_COMMON and (receiver_left or cur["component"].get("boundProcessGroups")
                              or any(pc["component"].get("inheritedParameterContexts")
                                     and any(i["id"] == contexts[name] for i in pc["component"]["inheritedParameterContexts"])
                                     for pc in call("GET", "/flow/parameter-contexts")["parameterContexts"])):
        print("kept parameter context", name, "(still used)")
        continue
    call("DELETE", f"/parameter-contexts/{contexts[name]}?version={cur['revision']['version']}&clientId={CID}")
    print("deleted parameter context", name)
