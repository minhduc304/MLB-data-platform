"""Schedule data collector."""

import logging
import sqlite3

import statsapi

from src.api.client import MLBAPIClient

logger = logging.getLogger(__name__)

STATUS_MAP = {
    "Final": "Final",
    "Game Over": "Final",
    "Completed Early": "Final",
    "Scheduled": "Scheduled",
    "Pre-Game": "Scheduled",
    "Warmup": "Scheduled",
    "Postponed": "Postponed",
    "Suspended": "Postponed",
    "Cancelled": "Postponed",
    "In Progress": "In Progress",
    "Manager Challenge": "In Progress",
    "Delayed": "In Progress",
    "Delayed Start": "Scheduled",
}


class ScheduleCollector:
    """Collect game schedule with probable pitchers and scores."""

    def __init__(self, db_path: str, client: MLBAPIClient = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()

    def collect(self, start_date: str, end_date: str) -> int:
        """
        Fetch games for a date range and insert into the schedule table.

        Args:
            start_date: Start date (MM/DD/YYYY)
            end_date: End date (MM/DD/YYYY)

        Returns:
            Number of games inserted/updated
        """
        games = self.client.get_schedule(start_date, end_date)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        team_abbrevs = self._get_team_abbreviations(cursor)

        try:
            for game in games:
                game_id = game.get("game_id")
                game_date = game.get("game_date", "")

                # Extract season from game_date (YYYY-MM-DD)
                season = game_date[:4] if game_date else ""

                home_id = game.get("home_id")
                away_id = game.get("away_id")
                home_abbr = team_abbrevs.get(home_id, "")
                away_abbr = team_abbrevs.get(away_id, "")

                venue_id = game.get("venue_id")

                home_score = game.get("home_score")
                away_score = game.get("away_score")

                raw_status = game.get("status", "")
                status = STATUS_MAP.get(raw_status, raw_status)

                # Probable pitchers
                home_pitcher_id = self._resolve_pitcher_id(game.get("home_probable_pitcher", ""))
                away_pitcher_id = self._resolve_pitcher_id(game.get("away_probable_pitcher", ""))

                cursor.execute('''
                    INSERT OR REPLACE INTO schedule
                    (game_id, game_date, season, home_team_id, away_team_id,
                     home_abbr, away_abbr, venue_id, home_score, away_score,
                     status, home_probable_pitcher_id, away_probable_pitcher_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (game_id, game_date, season, home_id, away_id,
                      home_abbr, away_abbr, venue_id, home_score, away_score,
                      status, home_pitcher_id, away_pitcher_id))
                count += 1

            conn.commit()
            logger.info(f"Collected {count} games from {start_date} to {end_date}")
        finally:
            conn.close()

        return count

    def update_scores(self, game_date: str) -> int:
        """
        Update final scores for completed games on a given date.

        Args:
            game_date: Date to update (MM/DD/YYYY)

        Returns:
            Number of games updated
        """
        games = self.client.get_schedule(game_date, game_date)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            for game in games:
                raw_status = game.get("status", "")
                status = STATUS_MAP.get(raw_status, raw_status)

                if status != "Final":
                    continue

                game_id = game.get("game_id")
                home_score = game.get("home_score")
                away_score = game.get("away_score")

                cursor.execute('''
                    UPDATE schedule
                    SET home_score = ?, away_score = ?, status = ?
                    WHERE game_id = ?
                ''', (home_score, away_score, status, game_id))

                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
            logger.info(f"Updated scores for {count} games on {game_date}")
        finally:
            conn.close()

        return count

    def _get_team_abbreviations(self, cursor) -> dict:
        """Build a mapping of team_id -> abbreviation from the teams table."""
        cursor.execute("SELECT team_id, abbreviation FROM teams")
        return {row[0]: row[1] for row in cursor.fetchall()}

    def _resolve_pitcher_id(self, pitcher_name: str):
        """
        Resolve a pitcher name to a player ID using statsapi.lookup_player.

        Returns:
            Player ID or None if not found
        """
        if not pitcher_name or pitcher_name == "":
            return None

        try:
            results = statsapi.lookup_player(pitcher_name)
            if results:
                return results[0].get("id")
        except Exception:
            logger.debug(f"Could not resolve pitcher: {pitcher_name}")

        return None
