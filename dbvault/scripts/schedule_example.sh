#!/usr/bin/env bash
# Example: run the scheduler (daily at 02:00 UTC)
# Designed to run as a systemd service or inside screen/tmux
set -euo pipefail

exec dbvault schedule \
  --db-type mysql \
  --host "${DB_HOST:-localhost}" \
  --username "${DB_USER:-root}" \
  --database "${DB_NAME:-mydb}" \
  --backup-type full \
  --compress \
  --storage local \
  --output-dir "${BACKUP_DIR:-/var/backups/dbvault}" \
  --cron "0 2 * * *" \
  --run-now \
  --slack-webhook "${DBVAULT_SLACK_WEBHOOK:-}" \
  --log-level INFO \
  --log-file "/var/log/dbvault/scheduler.log"
