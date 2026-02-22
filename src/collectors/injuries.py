"""Injury (IL) data collector."""

import logging
import sqlite3
from datetime import date

from src.api.client import MLBAPIClient
from src.config import CURRENT_SEASON

logger = logging.getLogger(__name__)

# Map MLB API status codes to human-readable IL designations
IL_STATUS_MAP = {
    "D7": "IL-7",
    "D10": "IL-10",
    "D15": "IL-15",
    "D60": "IL-60",
    "ILF": "IL-60",
}


class InjuriesCollector:
    """Collect current IL snapshot by scanning full rosters for all 30 teams."""

    def __init__(self, db_path: str, client: MLBAPIClient = None, season: str = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()
        self.season = season or CURRENT_SEASON

    def collect(self, collection_date: str = None) -> int:
        """
        Scan full rosters for IL-designated players and insert into player_injuries.

        Args:
            collection_date: Date string (YYYY-MM-DD). Defaults to today.

        Returns:
            Number of injury records inserted/updated
        """
        if collection_date is None:
            collection_date = date.today().isoformat()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            cursor.execute("SELECT team_id FROM teams")
            team_ids = [row[0] for row in cursor.fetchall()]

            for team_id in team_ids:
                try:
                    roster = self.client.get_team_full_roster(team_id, self.season)
                except Exception as e:
                    logger.warning(f"Failed to get full roster for team {team_id}: {e}")
                    continue

                for entry in roster:
                    person = entry.get("person", {})
                    status = person.get("status", {})
                    status_code = status.get("code", "")

                    if status_code not in IL_STATUS_MAP:
                        continue

                    player_id = person.get("id")
                    player_name = person.get("fullName", "")
                    injury_status = IL_STATUS_MAP[status_code]
                    injury_desc = status.get("description", "")

                    cursor.execute('''
                        INSERT OR REPLACE INTO player_injuries
                        (player_id, player_name, team_id, injury_status,
                         injury_description, collection_date)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        player_id, player_name, team_id,
                        injury_status, injury_desc, collection_date,
                    ))
                    count += 1

            conn.commit()
            logger.info(f"Collected {count} injury records for {collection_date}")
        finally:
            conn.close()

        return count
