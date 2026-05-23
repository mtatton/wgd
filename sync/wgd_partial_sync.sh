#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-rollback}"
ROOT="${ROOT:-/mnt/d/dev/poviii}"
PARTIAL_ROLLBACK="${PARTIAL_ROLLBACK:-/tmp/wgd-sync-partial-rollback-test}"
PARTIAL_COMMIT="${PARTIAL_COMMIT:-/tmp/wgd-sync-partial-commit}"
PASSWORD_FILE="${PASSWORD_FILE:-/tmp/mssqlpass}"
SQLCMD_BIN="${SQLCMD_BIN:-sqlcmd}"
SQL_SERVER="${SQL_SERVER:-tcp:192.168.1.2,4001}"
ODBC_SERVER="${ODBC_SERVER:-192.168.1.2,4001}"
DATABASE="${DATABASE:-POVIID}"
SQL_USER="${SQL_USER:-sa}"
ODBC_DRIVER="${ODBC_DRIVER:-ODBC Driver 18 for SQL Server}"

progress() {
  local percent="$1"
  shift
  printf '[WGD partial-sync %5.1f%%] %s\n' "$percent" "$*" >&2
}

if [ ! -r "$PASSWORD_FILE" ]; then
  echo "password file is not readable: $PASSWORD_FILE" >&2
  exit 2
fi

export SQLCMDPASSWORD="${SQLCMDPASSWORD:-$(cat "$PASSWORD_FILE")}"
export MSSQL_CONNECTION_STRING="DRIVER={${ODBC_DRIVER}};SERVER=${ODBC_SERVER};DATABASE=${DATABASE};UID=${SQL_USER};PWD=${SQLCMDPASSWORD};Encrypt=no;TrustServerCertificate=yes;Connection Timeout=10"

SQLCMD_ARGS=(
  -S "$SQL_SERVER"
  -U "$SQL_USER"
  -C
  -d "$DATABASE"
  -b
)

cd "$ROOT"

case "$MODE" in
  rollback)
    progress 0 "starting rollback validation for metadata and cluster_vectors"
    python3 wgd/sync/wgd_sqlite_sync.py rollback-test \
      --table metadata \
      --table cluster_vectors \
      --skip-full \
      --execute \
      --verbose \
      --output-root "$PARTIAL_ROLLBACK"
    progress 100 "rollback validation finished"
    ;;

  commit)
    progress 0 "starting COMMIT apply for metadata and cluster_vectors on ${SQL_SERVER}/${DATABASE}"
    progress 10 "generating commit SQL package at ${PARTIAL_COMMIT}"
    python3 wgd/sync/wgd_sqlite_sync.py generate-sql \
      --table metadata \
      --table cluster_vectors \
      --output-dir "$PARTIAL_COMMIT" \
      --transaction-scope per-table \
      --rollback-mode commit \
      --package-id partial_commit \
      --format text

    cd "$PARTIAL_COMMIT"
    progress 30 "checking commit package uses @RollbackOnly = 0"
    grep -F "DECLARE @RollbackOnly bit = 0;" sync.sql >/dev/null
    progress 40 "loading staging CSVs into NODE010"
    SQLCMD_ARGS="${SQLCMD_ARGS[*]}" ./load_staging.sh
    progress 75 "running transactional MERGE commit SQL"
    "$SQLCMD_BIN" "${SQLCMD_ARGS[@]}" -i sync.sql
    progress 100 "commit phase finished"
    ;;

  *)
    echo "usage: $0 rollback|commit" >&2
    exit 2
    ;;
esac
