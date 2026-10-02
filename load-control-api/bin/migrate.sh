#!/usr/bin/env bash
# 관리 DB migration: bin/migrate.sh [alembic 인자...]   (기본 upgrade head)
# DB URL은 config의 database.migration_url(없으면 database.url). 예: bin/migrate.sh current, bin/migrate.sh history
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
lca_check_python || exit 1
if (($# == 0)); then set -- upgrade head; fi
exec "$LCA_PYTHON" -m alembic -c "$LCA_HOME/config/alembic.ini" "$@"
