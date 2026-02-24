"""Tests for InjuriesCollector."""

import sqlite3
from unittest.mock import MagicMock

import pytest

from src.collectors.injuries import InjuriesCollector, IL_STATUS_MAP


# ---- Fixtures ----

@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def seeded_db(test_db):
    """Test DB with teams seeded."""
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO teams (team_id, name, abbreviation, league, division)
        VALUES (147, 'New York Yankees', 'NYY', 'AL', 'East')
    ''')
    cursor.execute('''
        INSERT INTO teams (team_id, name, abbreviation, league, division)
        VALUES (111, 'Boston Red Sox', 'BOS', 'AL', 'East')
    ''')
    conn.commit()
    conn.close()
    return test_db


# ---- Tests ----

class TestInjuriesCollector:

    def test_inserts_il_players(self, seeded_db, mock_client):
        """IL players are inserted into player_injuries."""
        mock_client.get_team_full_roster.side_effect = [
            # NYY roster
            [{
                "person": {
                    "id": 543037,
                    "fullName": "Gerrit Cole",
                    "status": {"code": "D10", "description": "10-Day Injured List"},
                },
            }],
            # BOS roster
            [{
                "person": {
                    "id": 677951,
                    "fullName": "Chris Sale",
                    "status": {"code": "D15", "description": "15-Day Injured List"},
                },
            }],
        ]

        collector = InjuriesCollector(seeded_db, mock_client, season="2026")
        count = collector.collect(collection_date="2026-04-15")

        assert count == 2

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT player_name, injury_status, collection_date FROM player_injuries WHERE player_id = 543037")
        row = cursor.fetchone()
        conn.close()

        assert row[0] == "Gerrit Cole"
        assert row[1] == "IL-10"
        assert row[2] == "2026-04-15"

    def test_status_code_mapping(self, seeded_db, mock_client):
        """All IL status codes are mapped correctly."""
        roster_entries = [
            {
                "person": {
                    "id": 100 + i,
                    "fullName": f"Player {code}",
                    "status": {"code": code, "description": ""},
                },
            }
            for i, code in enumerate(IL_STATUS_MAP.keys())
        ]
        mock_client.get_team_full_roster.return_value = roster_entries

        collector = InjuriesCollector(seeded_db, mock_client, season="2026")
        count = collector.collect(collection_date="2026-04-15")

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT injury_status FROM player_injuries")
        statuses = {row[0] for row in cursor.fetchall()}
        conn.close()

        expected = set(IL_STATUS_MAP.values())
        assert statuses == expected

    def test_skips_active_players(self, seeded_db, mock_client):
        """Active players (status code 'A') are not inserted."""
        mock_client.get_team_full_roster.return_value = [
            {
                "person": {
                    "id": 660271,
                    "fullName": "Aaron Judge",
                    "status": {"code": "A", "description": "Active"},
                },
            },
        ]

        collector = InjuriesCollector(seeded_db, mock_client, season="2026")
        count = collector.collect(collection_date="2026-04-15")

        assert count == 0

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM player_injuries")
        total = cursor.fetchone()[0]
        conn.close()

        assert total == 0

    def test_same_day_replacement(self, seeded_db, mock_client):
        """Re-running on the same day replaces existing records."""
        mock_client.get_team_full_roster.side_effect = [
            # First team (NYY)
            [{
                "person": {
                    "id": 543037,
                    "fullName": "Gerrit Cole",
                    "status": {"code": "D10", "description": "10-Day IL"},
                },
            }],
            # Second team (BOS)
            [],
            # Second run: first team
            [{
                "person": {
                    "id": 543037,
                    "fullName": "Gerrit Cole",
                    "status": {"code": "D60", "description": "60-Day IL"},
                },
            }],
            # Second run: second team
            [],
        ]

        collector = InjuriesCollector(seeded_db, mock_client, season="2026")
        collector.collect(collection_date="2026-04-15")
        collector.collect(collection_date="2026-04-15")

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT injury_status FROM player_injuries WHERE player_id = 543037 AND collection_date = '2026-04-15'"
        )
        rows = cursor.fetchall()
        conn.close()

        # Should have exactly 1 record (replaced), with the updated status
        assert len(rows) == 1
        assert rows[0][0] == "IL-60"

    def test_api_failure_continues(self, seeded_db, mock_client):
        """API failure for one team doesn't stop collection for others."""
        mock_client.get_team_full_roster.side_effect = [
            Exception("API error"),
            [{
                "person": {
                    "id": 999,
                    "fullName": "Test Player",
                    "status": {"code": "D15", "description": "15-Day IL"},
                },
            }],
        ]

        collector = InjuriesCollector(seeded_db, mock_client, season="2026")
        count = collector.collect(collection_date="2026-04-15")

        assert count == 1

    def test_collection_date_defaults_to_today(self, seeded_db, mock_client):
        """When no date is provided, today's date is used."""
        from datetime import date

        mock_client.get_team_full_roster.return_value = [
            {
                "person": {
                    "id": 543037,
                    "fullName": "Gerrit Cole",
                    "status": {"code": "D10", "description": "10-Day IL"},
                },
            },
        ]

        collector = InjuriesCollector(seeded_db, mock_client, season="2026")
        collector.collect()

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT collection_date FROM player_injuries")
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert dates[0] == date.today().isoformat()
