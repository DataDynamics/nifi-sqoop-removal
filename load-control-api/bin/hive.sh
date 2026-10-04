#!/usr/bin/env bash
# Hive SQL 셸(HiveServer2, psql과 비슷함): bin/hive.sh [-d DB] [-c SQL]... [-f 파일] [-F 형식] [--write]
#   접속 정보는 config/config.yaml의 clients.hive(host, port, database, user, password, auth). 기본은 읽기 전용(SELECT·SHOW·DESCRIBE 등).
#   예: bin/hive.sh -c "SELECT COUNT(*) FROM dw.insp_dtl WHERE base_dt = '2026-09-28'"
#   대화형 메타 명령: \dt [db.패턴], \d 이름, \d+ 이름(FORMATTED), \dn, \x, \format, \timing, \q. 전체는 \?
# 종료 코드: 0 성공, 1 실행 오류, 2 사용법·설정·접속 오류, 3 읽기 전용 거부, 130 Ctrl-C
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
lca_check_python || exit 1
exec "$LCA_PYTHON" -m load_control.query hive --config "$LCA_CONFIG" "$@"
