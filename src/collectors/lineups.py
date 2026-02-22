"""Starting lineup data collector."""

import logging
import sqlite3
from datetime import date, datetime

from src.api.client import MLBAPIClient

logger = logging.getLogger(__name__)


class LineupCollector:
    """Collect starting lineups and batting order from boxscore data."""

    def __init__(self, db_path: str, client: MLBAPIClient = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()

    def collect(self, game_date: str = None) -> int:
        """
        Collect starting lineups for all games on a given date.

        Args:
            game_date: Date in MM/DD/YYYY format. Defaults to today.

        Returns:
            Number of lineup entries inserted/updated
        """
        if game_date is None:
            game_date = date.today().strftime("%m/%d/%Y")

        # Convert MM/DD/YYYY to YYYY-MM-DD for DB lookups
        game_date_iso = datetime.strptime(game_date, "%m/%d/%Y").strftime("%Y-%m-%d")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            # Look up game IDs from the schedule table
            cursor.execute(
                "SELECT game_id, home_team_id, away_team_id FROM schedule WHERE game_date = ?",
                (game_date_iso,)
            )
            games = cursor.fetchall()

            if not games:
                # Fallback: fetch from API
                api_games = self.client.get_schedule(game_date, game_date)
                games = [
                    (g.get("game_id"), g.get("home_id"), g.get("away_id"))
                    for g in api_games
                ]

            for game_id, home_team_id, away_team_id in games:
                try:
                    boxscore = self.client.get_boxscore_data(game_id)
                except Exception as e:
                    logger.warning(f"Failed to get boxscore for game {game_id}: {e}")
                    continue

                # Process both home and away batters
                for side, team_id in [("homeBatters", home_team_id), ("awayBatters", away_team_id)]:
                    batters = boxscore.get(side, [])
                    for batter in batters:
                        batting_order_raw = batter.get("battingOrder", "0")

                        try:
                            order_int = int(batting_order_raw)
                        except (ValueError, TypeError):
                            continue

                        # Starters have battingOrder divisible by 100 (100, 200, ..., 900)
                        if order_int == 0 or order_int % 100 != 0:
                            continue

                        # Skip substitutions
                        if batter.get("substitution", False):
                            continue

                        position = order_int // 100
                        player_id = batter.get("personId")
                        player_name = batter.get("name", "")
                        pos_abbrev = batter.get("position", "")

                        cursor.execute('''
                            INSERT OR REPLACE INTO starting_lineups
                            (game_id, game_date, team_id, player_id, player_name,
                             batting_order, position)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            game_id, game_date_iso, team_id,
                            player_id, player_name, position, pos_abbrev,
                        ))
                        count += 1

            conn.commit()
            logger.info(f"Collected {count} lineup entries for {game_date}")
        finally:
            conn.close()

        return count
