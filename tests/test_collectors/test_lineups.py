"""Tests for LineupCollector."""

import sqlite3
from unittest.mock import MagicMock

import pytest

from src.collectors.lineups import LineupCollector


# ---- Fixtures ----

@pytest.fixture
def mock_client():
    return MagicMock()


def _make_batter(person_id, name, order, position="CF", substitution=False):
    """Helper to create a batter dict matching boxscore_data format."""
    return {
        "personId": person_id,
        "name": name,
        "battingOrder": str(order),
        "position": position,
        "substitution": substitution,
    }


@pytest.fixture
def seeded_db(test_db):
    """Test DB with teams and a scheduled game."""
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
    cursor.execute('''
        INSERT INTO schedule
        (game_id, game_date, season, home_team_id, away_team_id,
         home_abbr, away_abbr, venue_id, status)
        VALUES (717001, '2026-04-01', '2026', 147, 111, 'NYY', 'BOS', 3313, 'Final')
    ''')
    conn.commit()
    conn.close()
    return test_db


# ---- Tests ----

class TestLineupCollector:

    def test_inserts_starters(self, seeded_db, mock_client):
        """9 starters per team are inserted."""
        home_batters = [_make_batter(1000 + i, f"Home Player {i}", i * 100, "POS") for i in range(1, 10)]
        away_batters = [_make_batter(2000 + i, f"Away Player {i}", i * 100, "POS") for i in range(1, 10)]

        mock_client.get_boxscore_data.return_value = {
            "homeBatters": home_batters,
            "awayBatters": away_batters,
        }

        collector = LineupCollector(seeded_db, mock_client)
        count = collector.collect("04/01/2026")

        assert count == 18  # 9 per team

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM starting_lineups WHERE game_id = 717001")
        total = cursor.fetchone()[0]
        conn.close()

        assert total == 18

    def test_filters_substitutions(self, seeded_db, mock_client):
        """Substitution players are not inserted."""
        batters = [
            _make_batter(1001, "Starter", 100, "CF"),
            _make_batter(1002, "Sub Player", 100, "CF", substitution=True),
        ]

        mock_client.get_boxscore_data.return_value = {
            "homeBatters": batters,
            "awayBatters": [],
        }

        collector = LineupCollector(seeded_db, mock_client)
        count = collector.collect("04/01/2026")

        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT player_name FROM starting_lineups")
        row = cursor.fetchone()
        conn.close()

        assert row[0] == "Starter"

    def test_batting_order_parsing(self, seeded_db, mock_client):
        """battingOrder '300' -> position 3."""
        mock_client.get_boxscore_data.return_value = {
            "homeBatters": [_make_batter(1001, "Leadoff", 100, "CF"),
                            _make_batter(1003, "Third", 300, "1B")],
            "awayBatters": [],
        }

        collector = LineupCollector(seeded_db, mock_client)
        collector.collect("04/01/2026")

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT batting_order FROM starting_lineups WHERE player_id = 1003")
        row = cursor.fetchone()
        conn.close()

        assert row[0] == 3

    def test_both_teams_processed(self, seeded_db, mock_client):
        """Both home and away teams have their lineups recorded."""
        mock_client.get_boxscore_data.return_value = {
            "homeBatters": [_make_batter(1001, "Home Guy", 100, "CF")],
            "awayBatters": [_make_batter(2001, "Away Guy", 100, "CF")],
        }

        collector = LineupCollector(seeded_db, mock_client)
        collector.collect("04/01/2026")

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT team_id FROM starting_lineups")
        team_ids = {row[0] for row in cursor.fetchall()}
        conn.close()

        assert team_ids == {147, 111}

    def test_replace_on_rerun(self, seeded_db, mock_client):
        """Re-running replaces existing lineup entries."""
        mock_client.get_boxscore_data.return_value = {
            "homeBatters": [_make_batter(1001, "Original", 100, "CF")],
            "awayBatters": [],
        }

        collector = LineupCollector(seeded_db, mock_client)
        collector.collect("04/01/2026")

        # Second run with different player name
        mock_client.get_boxscore_data.return_value = {
            "homeBatters": [_make_batter(9999, "Replacement", 100, "LF")],
            "awayBatters": [],
        }
        collector.collect("04/01/2026")

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT player_name FROM starting_lineups WHERE game_id = 717001 AND team_id = 147 AND batting_order = 1"
        )
        row = cursor.fetchone()
        conn.close()

        assert row[0] == "Replacement"

    def test_game_not_found_uses_api_fallback(self, test_db, mock_client):
        """When schedule table is empty, collector falls back to API."""
        mock_client.get_schedule.return_value = [
            {"game_id": 800001, "home_id": 147, "away_id": 111},
        ]
        mock_client.get_boxscore_data.return_value = {
            "homeBatters": [_make_batter(1001, "API Player", 100, "CF")],
            "awayBatters": [],
        }

        collector = LineupCollector(test_db, mock_client)
        count = collector.collect("04/01/2026")

        assert count == 1
        mock_client.get_schedule.assert_called_once_with("04/01/2026", "04/01/2026")

    def test_non_starter_orders_filtered(self, seeded_db, mock_client):
        """Non-round battingOrder values (e.g. 101 for pinch-hitter) are skipped."""
        mock_client.get_boxscore_data.return_value = {
            "homeBatters": [
                _make_batter(1001, "Starter", 100, "CF"),
                _make_batter(1002, "Pinch Hitter", 101, "PH"),
            ],
            "awayBatters": [],
        }

        collector = LineupCollector(seeded_db, mock_client)
        count = collector.collect("04/01/2026")

        assert count == 1
