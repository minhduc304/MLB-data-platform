"""Game weather data collector using the MLB Stats API."""

import logging
import re
import sqlite3

import statsapi

from src.api.client import MLBAPIClient

logger = logging.getLogger(__name__)

# Roof types that indicate a controlled indoor environment
DOME_ROOF_TYPES = {"Dome", "Indoor"}

# Wind direction strings from the MLB API → normalized category
# Used as a feature: 'out' helps offense, 'in' helps pitchers
WIND_DIRECTION_MAP = {
    "Out To CF":  "out",
    "Out To LF":  "out",
    "Out To RF":  "out",
    "In From CF": "in",
    "In From LF": "in",
    "In From RF": "in",
    "L To R":     "cross",
    "R To L":     "cross",
    "Calm":       "calm",
}

_WIND_RE = re.compile(r"(\d+)\s*mph,?\s*(.*)", re.IGNORECASE)


def _parse_wind(wind_str: str):
    """
    Parse MLB API wind string like '7 mph, Out To CF' into (speed_int, direction_str).
    Returns (None, None) if unparseable.
    """
    if not wind_str:
        return None, None
    m = _WIND_RE.match(wind_str.strip())
    if not m:
        return None, None
    speed = int(m.group(1))
    raw_dir = m.group(2).strip().title()
    direction = WIND_DIRECTION_MAP.get(raw_dir, raw_dir if raw_dir else None)
    return speed, direction


class WeatherCollector:
    """
    Collect weather conditions for MLB games from the MLB Stats API.

    For completed games the API returns actual recorded conditions
    (condition, temp, wind). For upcoming games the weather fields
    are typically empty — use `collect_upcoming` with a third-party
    weather API instead (not yet implemented).
    """

    def __init__(self, db_path: str, client: MLBAPIClient = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()

    def collect_season(self, season: str) -> int:
        """
        Backfill weather for all completed games in a season.
        Skips game_ids already in game_weather. Commits every 50 games.

        Args:
            season: Season year string, e.g. '2025'

        Returns:
            Number of rows inserted
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT game_id, game_date, venue_id FROM schedule "
            "WHERE season = ? AND status = 'Final' ORDER BY game_date",
            (season,)
        )
        all_games = cursor.fetchall()

        cursor.execute("SELECT game_id FROM game_weather")
        already_done = {row[0] for row in cursor.fetchall()}

        to_fetch = [(gid, gdate, vid) for gid, gdate, vid in all_games if gid not in already_done]
        total = len(to_fetch)
        logger.info(f"[weather] {season}: {len(already_done)} already collected, {total} to fetch")

        count = 0
        try:
            for i, (game_id, game_date, venue_id) in enumerate(to_fetch, 1):
                try:
                    row = self._fetch_game_weather(game_id, game_date, venue_id)
                except Exception as e:
                    logger.warning(f"[weather] game {game_id} failed: {e}")
                    continue

                cursor.execute('''
                    INSERT OR REPLACE INTO game_weather
                    (game_id, game_date, venue_id, condition, temp_f,
                     wind_speed, wind_direction, is_dome, roof_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
                count += 1

                if count % 50 == 0:
                    conn.commit()
                    logger.info(f"[weather] {season}: {count}/{total} collected")

            conn.commit()
            db_total = cursor.execute("SELECT COUNT(*) FROM game_weather").fetchone()[0]
            logger.info(f"[weather] {season}: done — +{count} new, {db_total} total in DB")
        finally:
            conn.close()

        return count

    def collect_date(self, game_date_iso: str) -> int:
        """
        Collect weather for all games on a specific date (YYYY-MM-DD).
        Overwrites existing rows for that date.

        Args:
            game_date_iso: Date in YYYY-MM-DD format

        Returns:
            Number of rows inserted/updated
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT game_id, game_date, venue_id FROM schedule WHERE game_date = ?",
            (game_date_iso,)
        )
        games = cursor.fetchall()
        logger.info(f"[weather] Collecting weather for {len(games)} games on {game_date_iso}...")

        count = 0
        try:
            for game_id, game_date, venue_id in games:
                try:
                    row = self._fetch_game_weather(game_id, game_date, venue_id)
                except Exception as e:
                    logger.warning(f"[weather] game {game_id} failed: {e}")
                    continue

                cursor.execute('''
                    INSERT OR REPLACE INTO game_weather
                    (game_id, game_date, venue_id, condition, temp_f,
                     wind_speed, wind_direction, is_dome, roof_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
                count += 1
                logger.info(
                    f"[weather] game {game_id}: "
                    f"{row[3]}, {row[4]}°F, wind {row[5]} mph {row[6]}"
                    + (" [dome]" if row[7] else "")
                )

            conn.commit()
        finally:
            conn.close()

        return count

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_game_weather(self, game_id: int, game_date: str, venue_id: int) -> tuple:
        """
        Fetch weather and venue roof info from the MLB game endpoint.

        Returns:
            Tuple matching game_weather INSERT column order.
        """
        data = self.client.get_game_weather(game_id)
        game_data = data.get("gameData", {})

        weather = game_data.get("weather", {})
        condition = weather.get("condition") or None
        temp_str = weather.get("temp")
        temp_f = int(temp_str) if temp_str and temp_str.isdigit() else None
        wind_speed, wind_direction = _parse_wind(weather.get("wind", ""))

        venue_info = game_data.get("venue", {})
        field_info = venue_info.get("fieldInfo", {})
        roof_type = field_info.get("roofType") or None
        is_dome = 1 if roof_type in DOME_ROOF_TYPES else 0

        return (
            game_id, game_date, venue_id,
            condition, temp_f, wind_speed, wind_direction,
            is_dome, roof_type,
        )
