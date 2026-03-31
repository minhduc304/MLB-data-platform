#!/bin/bash
# Starters pipeline: refresh probable starters every 3 days.

set -euo pipefail

S3_BUCKET="${S3_BUCKET:?S3_BUCKET env var is required}"
S3_KEY="${S3_KEY:-mlb_stats.db}"
DB_PATH="data/mlb_stats.db"

log() { echo "[starters] $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }

mkdir -p data

log "Downloading database..."
set +e
python scripts/s3_sync.py download "$S3_BUCKET" "$S3_KEY" "$DB_PATH"
EXIT_CODE=$?
set -e
if [ $EXIT_CODE -eq 2 ]; then
    log "No existing database — initialising fresh"
    ./mlb collect init-db
elif [ $EXIT_CODE -ne 0 ]; then
    log "ERROR: Failed to download database"; exit 1
fi

log "Updating probable starters..."
./mlb collect update-starters

log "Uploading database..."
python scripts/s3_sync.py upload "$S3_BUCKET" "$S3_KEY" "$DB_PATH"

log "=== Starters pipeline complete ==="
