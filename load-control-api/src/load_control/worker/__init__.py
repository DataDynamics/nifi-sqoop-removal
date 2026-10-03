"""worker 프로세스: outbox dispatcher와 sweeper. 실행: python -m load_control.worker

server가 요청을 처리하며 남긴 호출 요청(load_dispatch)을 NiFi PG-05로 보내고(dispatcher),
요청이 오지 않아도 멈춘 파티션·run, ACK 없는 dispatch, 정체된 검증·게시를 찾아 정리한다(sweeper).
server와는 직접 통신하지 않고 PostgreSQL(nifi_ops)만 공유한다.
"""
