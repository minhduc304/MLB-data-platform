"""Schedule data collector."""

import logging
import sqlite3
from datetime import datetime, timedelta

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

# Month chunks keep API responses small and give natural progress checkpoints
_CHUNK_DAYS = 30


class ScheduleCollector:
    """Collect game schedule with probable pitchers and scores."""

    def __init__(self, db_path: str, client: MLBAPIClient = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()
        # Cache pitcher name -> player_id across all games to avoid duplicate lookups
        self._pitcher_cache: dict[str, int | None] = {}

    def collect(self, start_date: str, end_date: str) -> int:
        """
        Fetch games for a date range and insert into the schedule table.
        Processes in monthly chunks with progress logging.
        Skips game_ids already present in the DB (incremental).

        Args:
            start_date: Start date (MM/DD/YYYY)
            end_date: End date (MM/DD/YYYY)

        Returns:
            Number of new games inserted
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        total_new = 0
        total_skipped = 0

        team_abbrevs = self._get_team_abbreviations(cursor)
        already_collected = self._get_existing_game_ids(cursor)
        logger.info(f"[schedule] {len(already_collected)} games already in DB — fetching {start_date} to {end_date}")

        try:
            for chunk_start, chunk_end in self._date_chunks(start_date, end_date):
                games = self.client.get_schedule(chunk_start, chunk_end)
                new_in_chunk = 0

                for game in games:
                    game_id = game.get("game_id")
                    if game_id in already_collected:
                        total_skipped += 1
                        continue

                    game_date = game.get("game_date", "")
                    season = game_date[:4] if game_date else ""
                    home_id = game.get("home_id")
                    away_id = game.get("away_id")
                    venue_id = game.get("venue_id")
                    raw_status = game.get("status", "")
                    status = STATUS_MAP.get(raw_status, raw_status)

                    home_pitcher_id = self._resolve_pitcher_id(game.get("home_probable_pitcher", ""))
                    away_pitcher_id = self._resolve_pitcher_id(game.get("away_probable_pitcher", ""))

                    cursor.execute('''
                        INSERT OR REPLACE INTO schedule
                        (game_id, game_date, season, home_team_id, away_team_id,
                         home_abbr, away_abbr, venue_id, home_score, away_score,
                         status, home_probable_pitcher_id, away_probable_pitcher_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (game_id, game_date, season, home_id, away_id,
                          team_abbrevs.get(home_id, ""), team_abbrevs.get(away_id, ""),
                          venue_id, game.get("home_score"), game.get("away_score"),
                          status, home_pitcher_id, away_pitcher_id))

                    already_collected.add(game_id)
                    new_in_chunk += 1
                    total_new += 1

                conn.commit()
                db_total = cursor.execute("SELECT COUNT(*) FROM schedule").fetchone()[0]
                logger.info(
                    f"[schedule] {chunk_start} → {chunk_end}: "
                    f"+{new_in_chunk} new, {total_skipped} skipped — {db_total} total in DB"
                )

        finally:
            conn.close()

        logger.info(f"[schedule] Done — {total_new} new games inserted")
        return total_new

    def update_starters(self, days_ahead: int = 7) -> int:
        """
        Refresh probable pitcher IDs for upcoming scheduled games.

        The MLB API only announces starters a few days out, so this should be
        run daily (or before scraping props) to keep the schedule table current.

        Args:
            days_ahead: How many days forward to refresh (default 7)

        Returns:
            Number of games updated with at least one starter
        """
        today = datetime.today()
        start_str = today.strftime("%m/%d/%Y")
        end_str = (today + timedelta(days=days_ahead)).strftime("%m/%d/%Y")

        games = self.client.get_schedule(start_str, end_str)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            for game in games:
                game_id = game.get("game_id")
                home_pitcher_id = self._resolve_pitcher_id(game.get("home_probable_pitcher", ""))
                away_pitcher_id = self._resolve_pitcher_id(game.get("away_probable_pitcher", ""))

                if home_pitcher_id is None and away_pitcher_id is None:
                    continue

                cursor.execute('''
                    UPDATE schedule
                    SET home_probable_pitcher_id = ?, away_probable_pitcher_id = ?
                    WHERE game_id = ?
                ''', (home_pitcher_id, away_pitcher_id, game_id))

                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
            logger.info(
                f"[schedule] Updated starters for {count} games "
                f"({start_str} to {end_str}), {len(self._pitcher_cache)} unique pitchers resolved"
            )
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
                cursor.execute('''
                    UPDATE schedule
                    SET home_score = ?, away_score = ?, status = ?
                    WHERE game_id = ?
                ''', (game.get("home_score"), game.get("away_score"), status, game_id))

                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
            logger.info(f"[schedule] Updated scores for {count} games on {game_date}")
        finally:
            conn.close()

        return count

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_team_abbreviations(self, cursor) -> dict:
        cursor.execute("SELECT team_id, abbreviation FROM teams")
        return {row[0]: row[1] for row in cursor.fetchall()}

    def _get_existing_game_ids(self, cursor) -> set:
        cursor.execute("SELECT game_id FROM schedule")
        return {row[0] for row in cursor.fetchall()}

    def _resolve_pitcher_id(self, pitcher_name: str):
        """
        Resolve a pitcher name to a player_id, with in-process caching
        to avoid duplicate API calls across games.
        """
        if not pitcher_name:
            return None

        if pitcher_name in self._pitcher_cache:
            return self._pitcher_cache[pitcher_name]

        try:
            results = statsapi.lookup_player(pitcher_name)
            player_id = results[0].get("id") if results else None
        except Exception:
            logger.debug(f"Could not resolve pitcher: {pitcher_name}")
            player_id = None

        self._pitcher_cache[pitcher_name] = player_id
        return player_id

    @staticmethod
    def _date_chunks(start_date: str, end_date: str):
        """
        Yield (chunk_start, chunk_end) pairs in MM/DD/YYYY format,
        splitting the range into ~30-day chunks.
        """
        fmt = "%m/%d/%Y"
        current = datetime.strptime(start_date, fmt)
        end = datetime.strptime(end_date, fmt)

        while current <= end:
            chunk_end = min(current + timedelta(days=_CHUNK_DAYS - 1), end)
            yield current.strftime(fmt), chunk_end.strftime(fmt)
            current = chunk_end + timedelta(days=1)
