"""Team and Venue data collectors."""

import logging
import sqlite3

from src.api.client import MLBAPIClient

logger = logging.getLogger(__name__)

# MLB league/division mapping from API division IDs
DIVISION_MAP = {
    200: ("AL", "East"),
    201: ("AL", "West"),
    202: ("AL", "Central"),
    203: ("NL", "West"),
    204: ("NL", "East"),
    205: ("NL", "Central"),
}


class TeamCollector:
    """Collect all 30 MLB teams from the API."""

    def __init__(self, db_path: str, client: MLBAPIClient = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()

    def collect(self) -> int:
        """
        Fetch all MLB teams and insert into the teams table.

        Returns:
            Number of teams inserted/updated
        """
        teams_data = self.client.get_teams()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            for team in teams_data:
                team_id = team.get("id")
                name = team.get("name", "")
                abbreviation = team.get("abbreviation", "")

                # Extract league and division
                division_id = team.get("division", {}).get("id")
                league, division = DIVISION_MAP.get(division_id, (None, None))

                venue = team.get("venue", {})
                venue_id = venue.get("id")
                venue_name = venue.get("name", "")

                cursor.execute('''
                    INSERT OR REPLACE INTO teams
                    (team_id, name, abbreviation, league, division, venue_name, venue_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (team_id, name, abbreviation, league, division, venue_name, venue_id))
                count += 1

            conn.commit()
            logger.info(f"Collected {count} teams")
        finally:
            conn.close()

        return count


class VenueCollector:
    """Collect venue data from team API responses."""

    def __init__(self, db_path: str, client: MLBAPIClient = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()

    def collect(self) -> int:
        """
        Extract unique venues from teams API and insert into venues table.

        Returns:
            Number of venues inserted/updated
        """
        teams_data = self.client.get_teams()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0
        seen_venues = set()

        try:
            for team in teams_data:
                venue = team.get("venue", {})
                venue_id = venue.get("id")

                if not venue_id or venue_id in seen_venues:
                    continue
                seen_venues.add(venue_id)

                name = venue.get("name", "")
                location = venue.get("location", {})
                city = location.get("city", "")
                state = location.get("stateProvince", "")

                cursor.execute('''
                    INSERT OR REPLACE INTO venues
                    (venue_id, name, city, state)
                    VALUES (?, ?, ?, ?)
                ''', (venue_id, name, city, state))
                count += 1

            conn.commit()
            logger.info(f"Collected {count} venues")
        finally:
            conn.close()

        return count
