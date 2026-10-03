"""운영 조회 도구: bin/oracle.sh, bin/hive.sh, bin/hdfs.sh (psql과 비슷한 대화형·일괄 실행 클라이언트).

적재 결과를 원천(Oracle)·HDFS chunk·Hive staging/target에서 직접 확인할 때 쓴다. API server·worker와는
독립된 명령행 도구이며, 접속 정보는 config.yaml의 clients 섹션에서 읽는다. 기본은 읽기 전용이고
쓰기(DML·DDL, HDFS 삭제 등)는 --write를 줘야 실행한다.
"""
