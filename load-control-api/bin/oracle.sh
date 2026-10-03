#!/usr/bin/env bash
# Oracle SQL 셸(psql과 비슷함): bin/oracle.sh [-c SQL]... [-f 파일] [-F table|vertical|csv|tsv|json] [--write]
#   접속 정보는 config/config.yaml의 clients.oracle(dsn, user, password). 기본은 읽기 전용(READ ONLY 트랜잭션).
#   예: bin/oracle.sh -c "SELECT COUNT(*) FROM APP.INSP_DTL AS OF SCN 2480802 WHERE BASE_DT = DATE '2026-09-28'"
#   대화형 메타 명령: \dt [패턴], \d 이름, \dn, \scn, \x, \format, \timing, \o 파일, \i 파일, \q. 전체는 \?
# 종료 코드: 0 성공, 1 실행 오류, 2 사용법·설정·접속 오류, 3 읽기 전용 거부, 130 Ctrl-C
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
lca_check_python || exit 1
exec "$LCA_PYTHON" -m load_control.query oracle --config "$LCA_CONFIG" "$@"
