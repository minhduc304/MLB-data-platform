#!/bin/bash
# Pre-game pipeline: collect injuries, lineups, weather, and props before first pitch.

set -euo pipefail

S3_BUCKET="${S3_BUCKET:?S3_BUCKET env var is required}"
S3_KEY="${S3_KEY:-mlb_stats.db}"
DB_PATH="data/mlb_stats.db"

log() { echo "[pregame] $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }

mkdir -p data

# ---------------------------------------------------------------------------
# 1. Download database
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# 2. Pre-game collection
# ---------------------------------------------------------------------------
log "Collecting injuries..."
./mlb collect injuries

log "Collecting lineups..."
./mlb collect lineups

log "Collecting weather..."
./mlb collect weather

log "Scraping props..."
./mlb scrape all || log "WARNING: prop scraping failed — continuing"

# ---------------------------------------------------------------------------
# 3. Upload database
# ---------------------------------------------------------------------------
log "Uploading database..."
python scripts/s3_sync.py upload "$S3_BUCKET" "$S3_KEY" "$DB_PATH"

log "=== Pre-game pipeline complete ==="
