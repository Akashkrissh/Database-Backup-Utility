#!/usr/bin/env bash
# Example: daily backup to S3 with Slack notification
set -euo pipefail

dbvault backup \
  --db-type postgresql \
  --host "${DB_HOST:-localhost}" \
  --port "${DB_PORT:-5432}" \
  --username "${DB_USER:-postgres}" \
  --database "${DB_NAME:-mydb}" \
  --backup-type full \
  --compress \
  --storage s3 \
  --s3-bucket "${S3_BUCKET:-my-dbvault-bucket}" \
  --s3-prefix "postgresql/" \
  --s3-region "${AWS_REGION:-us-east-1}" \
  --slack-webhook "${DBVAULT_SLACK_WEBHOOK:-}" \
  --log-level INFO \
  --log-file "/var/log/dbvault/backup.log"
