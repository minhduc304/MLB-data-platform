"""Tests for pitcher stats and game log collectors."""

import sqlite3
from unittest.mock import MagicMock

import pytest

from src.collectors.pitcher import PitcherStatsCollector, PitcherGameLogCollector, _ip_to_outs


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
        VALUES (717002, '2026-04-06', '2026', 111, 147, 'BOS', 'NYY', 3, 'Final')
    ''')

    conn.commit()
    conn.close()
    return test_db


# ---- _ip_to_outs Unit Tests ----

class TestIpToOuts:

    def test_whole_innings(self):
        assert _ip_to_outs(6.0) == 18

    def test_partial_innings(self):
        assert _ip_to_outs(6.2) == 20

    def test_one_third(self):
        assert _ip_to_outs(5.1) == 16

    def test_zero(self):
        assert _ip_to_outs(0.0) == 0

    def test_string_input(self):
        assert _ip_to_outs("7.1") == 22

    def test_nine_innings(self):
        assert _ip_to_outs(9.0) == 27


# ---- PitcherStatsCollector Tests ----

class TestPitcherStatsCollector:

    def test_inserts_pitcher_stats(self, seeded_db, mock_client):
        """Verify pitcher stats are inserted."""
        mock_client.get_roster_data.return_value = [
            {
                "person": {"id": 543037, "fullName": "Gerrit Cole"},
                "position": {"abbreviation": "P"},
            }
        ]
        mock_client.get_player_pitching_stats.return_value = {
            "stats": [{
                "type": {"displayName": "season"},
                "stats": {
                    "gamesPlayed": "25", "gamesStarted": "25",
                    "inningsPitched": "160.0", "wins": "12", "losses": "5",
                    "era": "3.10", "whip": "1.05",
                    "strikeOuts": "200", "baseOnBalls": "40",
                    "hits": "130", "homeRuns": "18", "earnedRuns": "55",
                    "runs": "60",
                },
            }],
            "pitch_hand": "Right",
        }

        collector = PitcherStatsCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT player_name, position, strikeouts, throws FROM pitcher_stats WHERE player_id = 543037"
        )
        row = cursor.fetchone()
        conn.close()

        assert row[0] == "Gerrit Cole"
        assert row[1] == "SP"  # 25/25 starts = 100% -> SP
        assert row[2] == 200
        assert row[3] == "R"

    def test_sp_rp_role_determination(self, seeded_db, mock_client):
        """Verify SP/RP role is determined correctly."""
        mock_client.get_roster_data.return_value = [
            {
                "person": {"id": 100001, "fullName": "Relief Ace"},
                "position": {"abbreviation": "P"},
            },
            {
                "person": {"id": 100002, "fullName": "Swingman"},
                "position": {"abbreviation": "P"},
            },
        ]

        def make_stats(gp, gs):
            return {
                "stats": [{
                    "type": {"displayName": "season"},
                    "stats": {
                        "gamesPlayed": str(gp), "gamesStarted": str(gs),
                        "inningsPitched": "50.0", "wins": "3", "losses": "2",
                        "era": "3.50", "whip": "1.20",
                        "strikeOuts": "50", "baseOnBalls": "20",
                        "hits": "45", "homeRuns": "5", "earnedRuns": "20",
                    },
                }],
                "pitch_hand": "Right",
            }

        # Relief Ace: 60 GP, 0 GS -> RP
        # Swingman: 30 GP, 15 GS -> SP (50% starts)
        mock_client.get_player_pitching_stats.side_effect = [
            make_stats(60, 0),
            make_stats(30, 15),
        ]

        collector = PitcherStatsCollector(seeded_db, mock_client, season="2026")
        collector.collect()

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT player_id, position FROM pitcher_stats ORDER BY player_id")
        rows = cursor.fetchall()
        conn.close()

        assert rows[0] == (100001, "RP")
        assert rows[1] == (100002, "SP")

    def test_skips_non_pitchers(self, seeded_db, mock_client):
        """Verify non-pitchers are skipped."""
        mock_client.get_roster_data.return_value = [
            {
                "person": {"id": 660271, "fullName": "Aaron Judge"},
                "position": {"abbreviation": "CF"},
            },
        ]

        collector = PitcherStatsCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 0
        mock_client.get_player_pitching_stats.assert_not_called()


# ---- PitcherGameLogCollector Tests ----

class TestPitcherGameLogCollector:

    def test_inserts_game_logs(self, seeded_db, mock_client):
        """Verify pitcher game log entries are inserted."""
        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO pitcher_stats (player_id, player_name, team_id, season, games_played)
            VALUES (543037, 'Gerrit Cole', 147, '2026', 25)
        ''')
        conn.commit()
        conn.close()

        mock_client.get_pitching_game_log.return_value = {
            "stats": [{
                "type": {"displayName": "gameLog"},
                "stats": [
                    {
                        "date": "2026-04-01",
                        "game": {"gamePk": 717001},
                        "stat": {
                            "gamesStarted": "1",
                            "inningsPitched": "7.0",
                            "hits": "5", "runs": "2", "earnedRuns": "2",
                            "baseOnBalls": "1", "strikeOuts": "10",
                            "homeRuns": "1", "numberOfPitches": "98",
                        },
                    },
                ],
            }],
        }

        collector = PitcherGameLogCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT is_start, outs_recorded, strikeouts, pitches_thrown, opponent_abbr "
            "FROM pitcher_game_logs WHERE player_id = 543037"
        )
        row = cursor.fetchone()
        conn.close()

        assert row[0] == 1     # is_start
        assert row[1] == 21    # 7.0 IP = 21 outs
        assert row[2] == 10    # strikeouts
        assert row[3] == 98    # pitches
        assert row[4] == "BOS"  # opponent

    def test_pitches_thrown_fallback(self, seeded_db, mock_client):
        """Verify pitchesThrown field is used if numberOfPitches is missing."""
        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO pitcher_stats (player_id, player_name, team_id, season, games_played)
            VALUES (543037, 'Gerrit Cole', 147, '2026', 25)
        ''')
        conn.commit()
        conn.close()

        mock_client.get_pitching_game_log.return_value = {
            "stats": [{
                "type": {"displayName": "gameLog"},
                "stats": [
                    {
                        "date": "2026-04-01",
                        "game": {"gamePk": 717001},
                        "stat": {
                            "gamesStarted": "1",
                            "inningsPitched": "6.2",
                            "hits": "4", "runs": "3", "earnedRuns": "3",
                            "baseOnBalls": "2", "strikeOuts": "8",
                            "homeRuns": "1", "pitchesThrown": "105",
                        },
                    },
                ],
            }],
        }

        collector = PitcherGameLogCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT outs_recorded, pitches_thrown FROM pitcher_game_logs WHERE player_id = 543037")
        row = cursor.fetchone()
        conn.close()

        assert row[0] == 20   # 6.2 IP = 20 outs
        assert row[1] == 105  # pitchesThrown fallback

    def test_incremental_skips_existing(self, seeded_db, mock_client):
        """Verify already-collected dates are skipped."""
        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO pitcher_stats (player_id, player_name, team_id, season, games_played)
            VALUES (543037, 'Gerrit Cole', 147, '2026', 25)
        ''')
        cursor.execute('''
            INSERT INTO pitcher_game_logs
            (player_id, game_id, game_date, season, team_id, strikeouts)
            VALUES (543037, 717001, '2026-04-01', '2026', 147, 10)
        ''')
        conn.commit()
        conn.close()

        mock_client.get_pitching_game_log.return_value = {
            "stats": [{
                "type": {"displayName": "gameLog"},
                "stats": [
                    {
                        "date": "2026-04-01",
                        "game": {"gamePk": 717001},
                        "stat": {
                            "gamesStarted": "1", "inningsPitched": "7.0",
                            "hits": "5", "runs": "2", "earnedRuns": "2",
                            "baseOnBalls": "1", "strikeOuts": "10",
                            "homeRuns": "1", "numberOfPitches": "98",
                        },
                    },
                    {
                        "date": "2026-04-06",
                        "game": {"gamePk": 717002},
                        "stat": {
                            "gamesStarted": "1", "inningsPitched": "6.0",
                            "hits": "7", "runs": "4", "earnedRuns": "4",
                            "baseOnBalls": "3", "strikeOuts": "7",
                            "homeRuns": "2", "numberOfPitches": "102",
                        },
                    },
                ],
            }],
        }

        collector = PitcherGameLogCollector(seeded_db, mock_client, season="2026")
        count = collector.collect()

        # Only 04-06 should be inserted
        assert count == 1

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pitcher_game_logs WHERE player_id = 543037")
        total = cursor.fetchone()[0]
        conn.close()

        assert total == 2
