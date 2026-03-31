#!/bin/bash
# Daily MLB data collection pipeline.
# Runs inside ECS Fargate: downloads DB from S3, collects data, uploads DB back.

set -euo pipefail

S3_BUCKET="${S3_BUCKET:?S3_BUCKET env var is required}"
S3_KEY="${S3_KEY:-mlb_stats.db}"
DB_PATH="data/mlb_stats.db"
LOG_PREFIX="[pipeline]"

log() {
    echo "$LOG_PREFIX $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"
}

mkdir -p data

# ---------------------------------------------------------------------------
# 1. Download database from S3
# ---------------------------------------------------------------------------
log "Downloading database from s3://$S3_BUCKET/$S3_KEY"

python scripts/s3_sync.py download "$S3_BUCKET" "$S3_KEY" "$DB_PATH"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 2 ]; then
    log "No existing database — initialising fresh"
    ./mlb collect init-db
elif [ $EXIT_CODE -ne 0 ]; then
    log "ERROR: Failed to download database"
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Collection
# ---------------------------------------------------------------------------
log "=== Starting collection ==="

log "Refreshing probable starters..."
./mlb collect update-starters

log "Collecting injuries..."
./mlb collect injuries

log "Collecting lineups..."
./mlb collect lineups

log "Updating player season stats..."
./mlb player update-all

log "Collecting game logs (incremental)..."
./mlb player game-logs

log "Computing rolling stats..."
./mlb player rolling-stats

# ---------------------------------------------------------------------------
# 3. Props scraping
# ---------------------------------------------------------------------------
log "=== Scraping props ==="
./mlb scrape all || log "WARNING: prop scraping failed — continuing"

# ---------------------------------------------------------------------------
# 4. Outcome tracking (yesterday's props vs actual results)
# ---------------------------------------------------------------------------
log "Recording prop outcomes for yesterday..."
./mlb ml outcomes || log "WARNING: outcome tracking failed — continuing"

# ---------------------------------------------------------------------------
# 5. Upload database back to S3
# ---------------------------------------------------------------------------
log "Uploading database to s3://$S3_BUCKET/$S3_KEY"
python scripts/s3_sync.py upload "$S3_BUCKET" "$S3_KEY" "$DB_PATH"

log "=== Pipeline complete ==="
