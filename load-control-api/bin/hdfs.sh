#!/usr/bin/env bash
# HDFS 명령 셸(WebHDFS): bin/hdfs.sh [명령 [인자...]] | -c 명령... | -f 파일   (인자 없으면 대화형)
#   접속 정보는 config/config.yaml의 clients.hdfs(namenode_urls, user, home). 기본은 읽기 전용.
#   명령: ls, cd, pwd, stat, du, count, find, cat, head, tail, get / [--write] mkdir, rm, mv, put, chmod
#   예: bin/hdfs.sh du -s -h "/data/nifi/stage/*"   (glob은 로컬 셸이 펼치지 않게 따옴표로 감싼다)
# 종료 코드: 0 성공, 1 실행 오류, 2 사용법·설정·접속 오류, 3 읽기 전용 거부, 130 Ctrl-C
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
lca_check_python || exit 1
exec "$LCA_PYTHON" -m load_control.query hdfs --config "$LCA_CONFIG" "$@"
