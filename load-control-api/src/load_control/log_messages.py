"""로그 이벤트 코드 → 한글 메시지.

코드에서는 `log.info("run_created", runId=..., jobKey=...)`처럼 영어 이벤트 코드와 필드만 쓰고,
로그를 쓸 때 이 표로 한글 `message`를 붙인다(logging.add_message). 이벤트 코드는 그대로 남으므로
grep·수집 규칙은 코드로, 사람은 메시지로 읽는다. `{필드}`는 같은 로그의 필드 값으로 채우고 없으면 `-`.
표에 없는 이벤트(SQLAlchemy, uvicorn 등 외부 로그)는 원래 메시지를 그대로 쓴다.
"""

MESSAGES: dict[str, str] = {
    # 프로세스
    "server_starting": "API 서버 시작 중: {host}:{port}, 프로세스 {workers}개",
    "api_started": "API 준비 완료: DB {dbHost}:{dbPort}/{dbName}",
    "api_stopped": "API 종료",
    "auth_not_configured": "인증 토큰 digest가 없어 모든 API 호출이 거부된다",
    "worker_started": "worker 시작: NiFi 수신 주소 {receiverUrl}, 복구 모드 {recoveryMode}",
    "worker_metrics_listening": "worker 메트릭 수신 대기: {host}:{port}",
    "worker_stopping": "worker 종료 신호 수신: {signal}",
    "worker_stopped": "worker 종료",
    # API 요청(미들웨어)
    "api_request": "API 요청 수신: {method} {path}",
    "api_response": "API 응답: {method} {path} → {httpStatus} ({durationMs}ms)",
    "api_error": "API 오류 응답: {status} {code}",
    "request_invalid": "요청 형식 오류(422)",
    "auth_missing_token": "인증 토큰 없음(401): {path}",
    "auth_forbidden": "권한 없음(403): {path}",
    "unique_violation": "DB unique 제약 위반(409): {constraint}",
    "foreign_key_violation": "DB 외래키 제약 위반: {constraint}",
    "integrity_error": "DB 무결성 오류",
    "db_error": "DB 오류(503)",
    "tx_retry": "트랜잭션 재시도(SQLSTATE {sqlstate}) {attempt}/{maxAttempts}",
    # run
    "run_created": "run 생성: {jobKey} 업무일자 {businessKey}",
    "run_duplicate_active": "run 생성 거부: {jobKey} 업무일자 {businessKey}에 진행 중인 run이 있음",
    "run_failed": "run 실패 기록: {fromStatus} → {toStatus}, 단계 {stage}, 오류 {errorCode}",
    "run_fail_replayed": "run 실패 보고 재요청(이미 반영됨): 상태 {status}",
    "run_success": "run 성공: target {targetCount}건(원천 {sourceCount}건)",
    "run_cleaned": "정리 기록: staging {droppedTable}, 경로 {deletedPath}",
    "cleanup_rejected": "정리 기록 거부: 아직 정리 대상이 아님(상태 {runStatus})",
    # manifest
    "manifest_registered": ("manifest 등록: 파티션 {partitions}개(0건 {emptyPartitions}개), "
                            "원천 {sourceCount}건, SCN {snapshotScn}"),
    "manifest_replayed": "manifest 재요청(이미 등록됨): 상태 {runStatus}",
    "manifest_conflict": "manifest 거부: run 상태가 {runStatus}",
    "manifest_invalid": "manifest 불변식 위반으로 run 실패: {violations}",
    "extract_validated": "추출 완료 판정: {extractedCount}건, 검증 호출 예약",
    # 파티션·chunk
    "partition_claimed": "파티션 claim: {partitionId} 시도 {attempt}회, worker {workerNode}",
    "partition_claim_replayed": "파티션 claim 재요청(같은 token): {partitionId}",
    "partition_claim_refused": "파티션 claim 거절: {partitionId} ({reason})",
    "chunk_recorded": "chunk 기록: {partitionId} #{chunkIndex}, 받은 chunk {receivedChunks}/{chunkCount}",
    "chunk_claim_mismatch": "chunk 보고 거부(409): {partitionId} claim token 불일치(이전 시도)",
    "chunk_path_outside_run": "chunk 보고 거부(422): run 경로 밖의 파일 {hdfsPath}",
    "chunk_conflict_after_success": "chunk 보고 거부(409): {partitionId}는 이미 다른 내용으로 성공",
    "chunk_after_run_end": "run이 끝난 뒤 도착한 chunk 보고: {partitionId}",
    "partition_success": "파티션 성공: {partitionId} {rows}건, 파일 {files}개",
    "partition_row_mismatch": "파티션 건수 불일치로 실패: {partitionId} 기대 {expectedRows}건, 실제 {rows}건",
    "partition_failed": "파티션 실패: {partitionId} 오류 {errorCode}",
    "partition_fail_replayed": "파티션 실패 보고 재요청(이미 반영됨): {partitionId}",
    "partition_fail_claim_mismatch": "파티션 실패 보고 거부(409): {partitionId} claim token 불일치",
    "partition_fail_rejected": "파티션 실패 보고 거부: {partitionId} 상태 {partitionStatus}",
    # 검증·게시
    "validation_started": "staging 검증 시작: dispatch {dispatchId}, 노드 {node}",
    "validation_start_duplicate": "staging 검증 중복 요청(무시): 상태 {runStatus}",
    "validation_start_dispatch_mismatch": "staging 검증 시작 거부(409): dispatch {dispatchId} 불일치",
    "validations_recorded": "{stage} 지표 {recorded}개 기록(FAIL {failCount}개)",
    "validations_rejected": "{stage} 지표 기록 거부: run 상태 {runStatus}",
    "validation_metrics_failed": "{stage} 지표 FAIL: {failed}",
    "stage_validated": "staging 검증 통과: {stageCount}건",
    "stage_validation_not_passed": "staging 검증 미통과: {reasons}",
    "target_validation_not_passed": "target 검증 미통과: {reasons}",
    "publish_claimed": "게시 시작(소유권 획득): {jobKey} 업무일자 {businessKey}",
    "publish_claim_replayed": "게시 claim 재요청(같은 token)",
    "publish_claim_refused": "게시 claim 거절: {reason}",
    "publish_result": "게시 결과 기록: {outcome}",
    "publish_result_replayed": "게시 결과 재요청(이미 반영됨): {outcome}",
    "publish_result_rejected": "게시 결과 거부: run 상태 {runStatus}",
    "publish_result_token_mismatch": "게시 결과 거부(409): publish token 불일치",
    "publish_unknown_resolved": "운영자가 게시 결과 불명을 {resolution}(으)로 확정",
    # 운영자
    "dispatch_resent_by_operator": "운영자가 dispatch {dispatchId} 재전송 요청(이전 상태 {previousStatus})",
    # worker: NiFi 호출(dispatch)
    "dispatch_sending": "NiFi 호출 시작: {type} POST {url} (시도 {attempt}회)",
    "dispatch_sent": "NiFi 호출 성공: {type} → {httpStatus} ({durationMs}ms)",
    "dispatch_retry": "NiFi 호출 실패, {delaySeconds}초 뒤 재시도: {type} → {httpStatus}",
    "dispatch_dead": "NiFi 호출 포기(DEAD): {type} → {httpStatus}, 시도 {attempt}회",
    "dispatch_loop_error": "dispatcher 처리 중 예외",
    "listen_disabled": "LISTEN 비활성: 폴링만 한다",
    "listen_connection_lost": "LISTEN 연결 끊김, 다시 연결한다",
    "listen_error": "LISTEN 처리 중 예외",
    # worker: sweeper
    "sweeper_actions": "sweeper 조치",
    "sweeper_error": "sweeper 처리 중 예외",
}
