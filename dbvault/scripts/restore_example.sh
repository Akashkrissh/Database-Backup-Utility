#!/usr/bin/env bash
# Example: restore from a specific backup file
set -euo pipefail

BACKUP_FILE="${1:?Usage: $0 <backup-file>}"

dbvault restore \
  --db-type postgresql \
  --host "${DB_HOST:-localhost}" \
  --username "${DB_USER:-postgres}" \
  --database "${DB_NAME:-mydb}" \
  --backup-file "$BACKUP_FILE" \
  --log-level INFO
