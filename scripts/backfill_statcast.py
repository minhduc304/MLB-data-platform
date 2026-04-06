"""
Backfill Statcast pitch-level data for a historical season.

Usage:
    python scripts/backfill_statcast.py --season 2024
    python scripts/backfill_statcast.py --season 2024 --dry-run
"""

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.statcast import StatcastCollector
from src.config import get_db_path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%SZ',
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Backfill Statcast pitch data for a season')
    parser.add_argument('--season', default='2024', help='Season year to backfill (default: 2024)')
    parser.add_argument('--db', default=None, help='Database path (default: data/mlb_stats.db)')
    parser.add_argument('--dry-run', action='store_true', help='Print date ranges without fetching data')
    args = parser.parse_args()

    db_path = args.db or get_db_path()

    if args.season not in StatcastCollector.SEASON_DATES:
        supported = list(StatcastCollector.SEASON_DATES.keys())
        logger.error(f"Unknown season '{args.season}'. Supported seasons: {supported}")
        sys.exit(1)

    start_dt, end_dt = StatcastCollector.SEASON_DATES[args.season]

    if args.dry_run:
        logger.info(f"DRY RUN — would fetch: season={args.season}, {start_dt} → {end_dt}")
        logger.info(f"Database: {db_path}")
        return

    logger.info(f"Backfilling Statcast for {args.season} season ({start_dt} → {end_dt})")
    logger.info(f"Database: {db_path}")

    collector = StatcastCollector(db_path, season=args.season)
    p_count, b_count = collector.collect_date_range(start_dt, end_dt)

    logger.info(
        f"Backfill complete — {p_count} pitcher arsenal rows, {b_count} batter pitch-type rows updated"
    )


if __name__ == '__main__':
    main()
