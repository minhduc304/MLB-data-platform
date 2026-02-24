"""Tests for batter stats and game log collectors."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.collectors.batter import BatterStatsCollector, BatterGameLogCollector


# ---- Fixtures ----

@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def seeded_db(test_db):
    """Test DB with teams and schedule data for game context lookups."""
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
    cursor.execute('''
        INSERT INTO schedule
        (game_id, game_date, season, home_team_id, away_team_id,
         home_abbr, away_abbr, venue_id, status)
        VALUES (717002, '2026-04-02', '2026', 111, 147, 'BOS', 'NYY', 3, 'Final')
    ''')

    conn.commit()
    conn.close()
    return test_db


# ---- BatterStatsCollector Tests ----

class TestBatterStatsCollector:

    def test_inserts_batter_stats(self, seeded_db, mock_client):
        """Verify batter stats are inserted into the database."""
        mock_client.get_roster_data.return_value = [
            {
                "person": {"id": 660271, "fullName": "Aaron Judge"},
                "position": {"abbreviation": "CF"},
            }
        ]
        mock_client.get_player_hitting_stats.return_value = {
            "stats": [{
                "type": {"displayName": "season"},
                "stats": {
                    "gamesPlayed": "100",
                    "plateAppearances": "450",
                    "atBats": "380",
                    "hits": "120",
                    "doubles": "25",
                    "triples": "1",
                    "homeRuns": "35",
                    "rbi": "80",
                    "runs": "90",
                    "stolenBases": "5",
                    "caughtStealing": "2",
                    "baseOnBalls": "65",
                    "strikeOuts": "110",
                    "avg": ".316",
                    "obp": ".420",
                    "slg": ".600",
                    "ops": "1.020",
                    "totalBases": "228",
                },
            }],
            "bat_side": "Right",
        }

        collector = BatterStatsCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT player_name, home_runs, bats FROM batter_stats WHERE player_id = 660271")
        row = cursor.fetchone()
        conn.close()

        assert row[0] == "Aaron Judge"
        assert row[1] == 35
        assert row[2] == "R"

    def test_skips_pitchers(self, seeded_db, mock_client):
        """Verify pitchers are skipped when collecting batter stats."""
        roster = [
            {
                "person": {"id": 543037, "fullName": "Gerrit Cole"},
                "position": {"abbreviation": "P"},
            },
            {
                "person": {"id": 660271, "fullName": "Aaron Judge"},
                "position": {"abbreviation": "CF"},
            },
        ]
        # Return roster only for NYY (147), empty for BOS (111)
        mock_client.get_roster_data.side_effect = lambda tid, s: roster if tid == 147 else []
        mock_client.get_player_hitting_stats.return_value = {
            "stats": [{
                "type": {"displayName": "season"},
                "stats": {
                    "gamesPlayed": "50", "plateAppearances": "200",
                    "atBats": "180", "hits": "50", "doubles": "10",
                    "triples": "0", "homeRuns": "10", "rbi": "30",
                    "runs": "25", "stolenBases": "2", "caughtStealing": "1",
                    "baseOnBalls": "15", "strikeOuts": "50",
                    "avg": ".278", "obp": ".350", "slg": ".450", "ops": ".800",
                    "totalBases": "81",
                },
            }],
            "bat_side": "Right",
        }

        collector = BatterStatsCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 1
        # get_player_hitting_stats should only be called for Judge, not Cole
        mock_client.get_player_hitting_stats.assert_called_once_with(660271, "2026")

    def test_incremental_update_skips_unchanged(self, seeded_db, mock_client):
        """Verify stats are not rewritten if games_played hasn't changed."""
        # Insert existing stats
        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO batter_stats (player_id, player_name, team_id, season, games_played)
            VALUES (660271, 'Aaron Judge', 147, '2026', 100)
        ''')
        conn.commit()
        conn.close()

        mock_client.get_roster_data.return_value = [
            {
                "person": {"id": 660271, "fullName": "Aaron Judge"},
                "position": {"abbreviation": "CF"},
            }
        ]
        mock_client.get_player_hitting_stats.return_value = {
            "stats": [{
                "type": {"displayName": "season"},
                "stats": {"gamesPlayed": "100"},
            }],
            "bat_side": "Right",
        }

        collector = BatterStatsCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 0


# ---- BatterGameLogCollector Tests ----

class TestBatterGameLogCollector:

    def test_inserts_game_logs(self, seeded_db, mock_client):
        """Verify game log entries are inserted."""
        # Seed a batter in batter_stats
        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO batter_stats (player_id, player_name, team_id, season, games_played)
            VALUES (660271, 'Aaron Judge', 147, '2026', 100)
        ''')
        conn.commit()
        conn.close()

        mock_client.get_hitting_game_log.return_value = {
            "stats": [{
                "type": {"displayName": "gameLog"},
                "stats": [
                    {
                        "date": "2026-04-01",
                        "game": {"gamePk": 717001},
                        "stat": {
                            "plateAppearances": "5", "atBats": "4",
                            "hits": "2", "doubles": "1", "triples": "0",
                            "homeRuns": "1", "rbi": "3", "runs": "2",
                            "stolenBases": "0", "baseOnBalls": "1",
                            "strikeOuts": "1", "totalBases": "6",
                        },
                    },
                ],
            }],
        }

        collector = BatterGameLogCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT hits, home_runs, is_home, opponent_abbr FROM batter_game_logs WHERE player_id = 660271")
        row = cursor.fetchone()
        conn.close()

        assert row[0] == 2  # hits
        assert row[1] == 1  # home_runs
        assert row[2] == 1  # is_home (NYY is home team in game 717001)
        assert row[3] == "BOS"  # opponent

    def test_incremental_skips_existing(self, seeded_db, mock_client):
        """Verify already-collected dates are skipped."""
        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO batter_stats (player_id, player_name, team_id, season, games_played)
            VALUES (660271, 'Aaron Judge', 147, '2026', 100)
        ''')
        # Insert existing game log for 04-01
        cursor.execute('''
            INSERT INTO batter_game_logs
            (player_id, game_id, game_date, season, team_id, hits)
            VALUES (660271, 717001, '2026-04-01', '2026', 147, 2)
        ''')
        conn.commit()
        conn.close()

        mock_client.get_hitting_game_log.return_value = {
            "stats": [{
                "type": {"displayName": "gameLog"},
                "stats": [
                    {
                        "date": "2026-04-01",
                        "game": {"gamePk": 717001},
                        "stat": {
                            "plateAppearances": "5", "atBats": "4",
                            "hits": "2", "doubles": "1", "triples": "0",
                            "homeRuns": "1", "rbi": "3", "runs": "2",
                            "stolenBases": "0", "baseOnBalls": "1",
                            "strikeOuts": "1", "totalBases": "6",
                        },
                    },
                    {
                        "date": "2026-04-02",
                        "game": {"gamePk": 717002},
                        "stat": {
                            "plateAppearances": "4", "atBats": "3",
                            "hits": "1", "doubles": "0", "triples": "0",
                            "homeRuns": "0", "rbi": "0", "runs": "1",
                            "stolenBases": "1", "baseOnBalls": "1",
                            "strikeOuts": "0", "totalBases": "1",
                        },
                    },
                ],
            }],
        }

        collector = BatterGameLogCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        # Only the 04-02 game should be inserted
        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM batter_game_logs WHERE player_id = 660271")
        total = cursor.fetchone()[0]
        conn.close()

        assert total == 2
