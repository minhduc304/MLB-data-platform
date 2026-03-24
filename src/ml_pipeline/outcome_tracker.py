"""OutcomeTracker: match prop lines with actual game results to build training labels."""

import logging
import sqlite3
from typing import Optional

from src.ml_pipeline.config import BATTER_STATS, PITCHER_STATS, STAT_COLUMNS

logger = logging.getLogger(__name__)


class OutcomeTracker:
    """
    Join all_props with batter_game_logs / pitcher_game_logs to populate prop_outcomes.

    For each prop on a given game date:
      1. Resolve player_id from batter_stats / pitcher_stats by name
      2. Look up the actual stat value from the appropriate game log
      3. Write actual_value, hit_over, hit_under to prop_outcomes
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def process_date(self, game_date: str) -> int:
        """
        Process all props scheduled on game_date and record outcomes.

        Args:
            game_date: Date string in 'YYYY-MM-DD' format.

        Returns:
            Number of prop_outcomes rows inserted/updated.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        count = 0

        try:
            cursor.execute('''
                SELECT DISTINCT full_name, stat_name, stat_value,
                       american_odds, source
                FROM all_props
                WHERE DATE(scheduled_at) = ?
            ''', (game_date,))
            props = cursor.fetchall()

            for prop in props:
                try:
                    rows_written = self._process_prop(cursor, prop, game_date)
                    count += rows_written
                except Exception as e:
                    logger.debug(
                        f'Skipping {prop["full_name"]} {prop["stat_name"]}: {e}'
                    )

            conn.commit()
        finally:
            conn.close()

        logger.info(f'Outcome tracker: processed {count} outcomes for {game_date}')
        return count

    def _process_prop(
        self,
        cursor: sqlite3.Cursor,
        prop: sqlite3.Row,
        game_date: str,
    ) -> int:
        """Resolve actual value and write one outcome row. Returns 1 if inserted."""
        full_name = prop['full_name']
        stat_name = prop['stat_name']
        line = prop['stat_value']
        over_odds = prop['american_odds']
        sportsbook = prop['source']

        player_id = self._resolve_player_id(cursor, full_name, stat_name)
        if player_id is None:
            return 0

        actual_value = self._get_actual_value(cursor, player_id, stat_name, game_date)
        if actual_value is None:
            return 0

        hit_over = 1 if actual_value > line else 0
        hit_under = 1 if actual_value < line else 0

        # Simple edge: deviation normalised by line
        edge = (actual_value - line) / max(line, 0.5)

        cursor.execute('''
            INSERT OR REPLACE INTO prop_outcomes
                (player_name, player_id, game_date, stat_type, line,
                 sportsbook, over_odds, actual_value, hit_over, hit_under, edge)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            full_name, player_id, game_date, stat_name, line,
            sportsbook, over_odds, actual_value, hit_over, hit_under, edge,
        ))

        return cursor.rowcount

    def _resolve_player_id(
        self,
        cursor: sqlite3.Cursor,
        full_name: str,
        stat_name: str,
    ) -> Optional[int]:
        """Look up player_id from batter_stats or pitcher_stats by name."""
        table = 'pitcher_stats' if stat_name in PITCHER_STATS else 'batter_stats'

        cursor.execute(
            f'SELECT player_id FROM {table} WHERE player_name = ?',
            (full_name,),
        )
        row = cursor.fetchone()
        if row:
            return row['player_id']

        # Fuzzy fallback: try aliases table
        cursor.execute(
            'SELECT player_id FROM player_name_aliases WHERE alias = ?',
            (full_name,),
        )
        row = cursor.fetchone()
        return row['player_id'] if row else None

    def _get_actual_value(
        self,
        cursor: sqlite3.Cursor,
        player_id: int,
        stat_name: str,
        game_date: str,
    ) -> Optional[float]:
        """Retrieve the actual stat value from the appropriate game log table."""
        db_col = STAT_COLUMNS.get(stat_name)
        if db_col is None:
            logger.debug(f'No DB column mapping for stat: {stat_name}')
            return None

        if stat_name in PITCHER_STATS:
            table = 'pitcher_game_logs'
        else:
            table = 'batter_game_logs'

        cursor.execute(
            f'SELECT {db_col} FROM {table} WHERE player_id = ? AND game_date = ?',
            (player_id, game_date),
        )
        row = cursor.fetchone()
        if row and row[0] is not None:
            return float(row[0])
        return None

    def process_range(self, start_date: str, end_date: str) -> int:
        """
        Process outcomes for all dates in [start_date, end_date].

        Returns:
            Total outcomes written.
        """
        from datetime import date, timedelta

        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        total = 0
        current = start
        while current <= end:
            total += self.process_date(str(current))
            current += timedelta(days=1)
        return total
