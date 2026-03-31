#!/bin/bash
# Post-game pipeline: collect results from yesterday's games, then schedule today's pre-game runs.

set -euo pipefail

S3_BUCKET="${S3_BUCKET:?S3_BUCKET env var is required}"
S3_KEY="${S3_KEY:-mlb_stats.db}"
DB_PATH="data/mlb_stats.db"

log() { echo "[postgame] $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }

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
# 2. Post-game collection
# ---------------------------------------------------------------------------
log "Collecting game logs..."
./mlb player game-logs

log "Computing rolling stats..."
./mlb player rolling-stats

log "Recording prop outcomes..."
./mlb ml outcomes || log "WARNING: outcome tracking failed — continuing"

# ---------------------------------------------------------------------------
# 3. Upload database
# ---------------------------------------------------------------------------
log "Uploading database..."
python scripts/s3_sync.py upload "$S3_BUCKET" "$S3_KEY" "$DB_PATH"

# ---------------------------------------------------------------------------
# 4. Schedule today's pre-game runs
# ---------------------------------------------------------------------------
log "Setting pre-game schedule for today..."
python scripts/set_pregame_schedule.py || log "WARNING: failed to update pre-game schedule"

log "=== Post-game pipeline complete ==="
