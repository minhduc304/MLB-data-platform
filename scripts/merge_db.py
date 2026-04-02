"""
Merge the cloud SQLite database into the local one.

Downloads mlb_stats.db from S3 and inserts any rows not already present
locally using INSERT OR IGNORE. Safe to run at any time — never overwrites
existing local data.

Usage:
    python scripts/merge_db.py
    python scripts/merge_db.py --dry-run   # show counts without writing
"""

import argparse
import logging
import os
import sqlite3
import tempfile

import boto3

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

S3_BUCKET = "mlb-pipeline-data-143136004753"
S3_KEY = "mlb_stats.db"
LOCAL_DB = "data/mlb_stats.db"

# Tables to merge — order matters for FK constraints (parents before children)
TABLES = [
    "teams",
    "venues",
    "park_factors",
    "schedule",
    "batter_stats",
    "pitcher_stats",
    "player_name_aliases",
    "player_injuries",
    "starting_lineups",
    "game_weather",
    "batter_game_logs",
    "pitcher_game_logs",
    "batter_rolling_stats",
    "pitcher_rolling_stats",
    "all_props",
    "underdog_props",
    "prizepicks_props",
    "odds_api_props",
    "prop_outcomes",
]


def download_cloud_db(dest_path: str) -> None:
    logger.info(f"Downloading s3://{S3_BUCKET}/{S3_KEY}...")
    boto3.client("s3").download_file(S3_BUCKET, S3_KEY, dest_path)
    size_mb = os.path.getsize(dest_path) / 1024 / 1024
    logger.info(f"Downloaded ({size_mb:.1f} MB)")


def merge(local_path: str, cloud_path: str, dry_run: bool) -> None:
    conn = sqlite3.connect(local_path)
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(f"ATTACH DATABASE '{cloud_path}' AS cloud")

    total = 0
    for table in TABLES:
        # Check table exists in cloud DB
        exists = conn.execute(
            "SELECT name FROM cloud.sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if not exists:
            logger.info(f"  {table}: not in cloud DB, skipping")
            continue

        before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if not dry_run:
            conn.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM cloud.{table}")
            conn.commit()
        after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        added = after - before

        if added > 0 or dry_run:
            cloud_count = conn.execute(f"SELECT COUNT(*) FROM cloud.{table}").fetchone()[0]
            label = f"+{added}" if not dry_run else f"~{cloud_count - before} new"
            logger.info(f"  {table}: {label} rows (local: {after})")
        total += added

    conn.close()

    if dry_run:
        logger.info("Dry run — no changes written")
    else:
        logger.info(f"Merge complete — {total} total rows added")


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge cloud DB into local DB")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be merged without writing")
    parser.add_argument("--local-db", default=LOCAL_DB, help=f"Local DB path (default: {LOCAL_DB})")
    args = parser.parse_args()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        cloud_path = tmp.name

    try:
        download_cloud_db(cloud_path)
        merge(args.local_db, cloud_path, dry_run=args.dry_run)
    finally:
        os.unlink(cloud_path)


if __name__ == "__main__":
    main()
