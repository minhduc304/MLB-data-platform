"""Tests for rolling stats computation."""

import sqlite3

import pytest

from src.ml_pipeline.rolling_stats import compute_batter_rolling_stats, compute_pitcher_rolling_stats


# ---- Fixtures ----

@pytest.fixture
def batter_db(test_db):
    """DB seeded with batter game logs for two players."""
    conn = sqlite3.connect(test_db)

    # Player 1: 5 games with known values
    games = [
        (660271, 100001, '2025-04-01', '2025', 2, 1, 4, 1, 0, 0, 'R'),
        (660271, 100002, '2025-04-02', '2025', 0, 0, 3, 0, 0, 1, 'L'),
        (660271, 100003, '2025-04-04', '2025', 3, 1, 5, 1, 1, 0, 'R'),
        (660271, 100004, '2025-04-05', '2025', 1, 0, 4, 0, 0, 1, 'L'),
        (660271, 100005, '2025-04-07', '2025', 2, 0, 4, 1, 0, 0, 'R'),
    ]
    conn.executemany(
        "INSERT INTO batter_game_logs "
        "(player_id, game_id, game_date, season, hits, home_runs, plate_appearances, "
        "total_bases, stolen_bases, strikeouts, opposing_pitcher_hand) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        games
    )
    conn.commit()
    conn.close()
    return test_db


@pytest.fixture
def pitcher_db(test_db):
    """DB seeded with pitcher game logs (starts only for rolling windows)."""
    conn = sqlite3.connect(test_db)

    # Player: 5 starts
    starts = [
        (543037, 200001, '2025-04-03', '2025', 1, 8, 21, 2, 3, 90),
        (543037, 200002, '2025-04-08', '2025', 1, 6, 18, 3, 4, 85),
        (543037, 200003, '2025-04-13', '2025', 1, 10, 24, 1, 2, 95),
        (543037, 200004, '2025-04-18', '2025', 1, 7, 21, 2, 3, 88),
        (543037, 200005, '2025-04-23', '2025', 1, 9, 27, 0, 2, 100),
    ]
    conn.executemany(
        "INSERT INTO pitcher_game_logs "
        "(player_id, game_id, game_date, season, is_start, strikeouts, outs_recorded, "
        "earned_runs, walks_allowed, pitches_thrown) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        starts
    )
    conn.commit()
    conn.close()
    return test_db


# ---- Batter Rolling Stats Tests ----

class TestBatterRollingStats:

    def test_returns_count_equal_to_game_rows(self, batter_db):
        count = compute_batter_rolling_stats(batter_db)
        assert count == 5

    def test_first_game_has_null_rolling_values(self, batter_db):
        """First game has no prior data — all rolling averages should be NULL."""
        compute_batter_rolling_stats(batter_db)
        conn = sqlite3.connect(batter_db)
        row = conn.execute(
            "SELECT l10_hits, l20_hits, l30_hits FROM batter_rolling_stats "
            "WHERE player_id = 660271 ORDER BY game_date ASC LIMIT 1"
        ).fetchone()
        conn.close()
        assert row[0] is None
        assert row[1] is None
        assert row[2] is None

    def test_no_lookahead(self, batter_db):
        """Rolling stats for game N must not include game N's own result."""
        compute_batter_rolling_stats(batter_db)
        conn = sqlite3.connect(batter_db)
        # Second game: only 1 prior game (game 1: hits=2)
        row = conn.execute(
            "SELECT l10_hits, games_in_l10 FROM batter_rolling_stats "
            "WHERE player_id = 660271 ORDER BY game_date ASC LIMIT 1 OFFSET 1"
        ).fetchone()
        conn.close()
        assert row[0] == pytest.approx(2.0)
        assert row[1] == 1

    def test_l10_average_correctness(self, batter_db):
        """The 5th game's l10_hits should average the first 4 games: (2+0+3+1)/4=1.5."""
        compute_batter_rolling_stats(batter_db)
        conn = sqlite3.connect(batter_db)
        row = conn.execute(
            "SELECT l10_hits, games_in_l10 FROM batter_rolling_stats "
            "WHERE player_id = 660271 ORDER BY game_date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        assert row[0] == pytest.approx(1.5)
        assert row[1] == 4

    def test_platoon_splits_vs_rhp(self, batter_db):
        """
        Game 3 (vs RHP): prior RHP games = game1 (hits=2), no more.
        l10_hits_vs_rhp for game 3 should be avg of hits in prior RHP games = 2.0.
        """
        compute_batter_rolling_stats(batter_db)
        conn = sqlite3.connect(batter_db)
        row = conn.execute(
            "SELECT l10_hits_vs_rhp FROM batter_rolling_stats "
            "WHERE player_id = 660271 ORDER BY game_date ASC LIMIT 1 OFFSET 2"
        ).fetchone()
        conn.close()
        # game1=RHP(hits=2), game2=LHP(hits=0) — before game3, RHP hits = [2], avg=2.0
        assert row[0] == pytest.approx(2.0)

    def test_trend_is_l10_minus_l20(self, batter_db):
        """hits_trend = l10_hits - l20_hits. With <=10 games both windows are same → trend=0."""
        compute_batter_rolling_stats(batter_db)
        conn = sqlite3.connect(batter_db)
        row = conn.execute(
            "SELECT hits_trend, l10_hits, l20_hits FROM batter_rolling_stats "
            "WHERE player_id = 660271 ORDER BY game_date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        # With 4 prior games, l10 and l20 use the same games → trend = 0
        assert row[0] == pytest.approx(0.0)

    def test_idempotent_rerun(self, batter_db):
        """Running twice should not duplicate rows."""
        compute_batter_rolling_stats(batter_db)
        compute_batter_rolling_stats(batter_db)
        conn = sqlite3.connect(batter_db)
        total = conn.execute("SELECT COUNT(*) FROM batter_rolling_stats").fetchone()[0]
        conn.close()
        assert total == 5


# ---- Pitcher Rolling Stats Tests ----

class TestPitcherRollingStats:

    def test_returns_count_equal_to_game_rows(self, pitcher_db):
        count = compute_pitcher_rolling_stats(pitcher_db)
        assert count == 5

    def test_first_start_has_null_rolling_values(self, pitcher_db):
        """First start has no prior data."""
        compute_pitcher_rolling_stats(pitcher_db)
        conn = sqlite3.connect(pitcher_db)
        row = conn.execute(
            "SELECT l3_strikeouts, l5_strikeouts, l10_strikeouts FROM pitcher_rolling_stats "
            "WHERE player_id = 543037 ORDER BY game_date ASC LIMIT 1"
        ).fetchone()
        conn.close()
        assert row[0] is None
        assert row[1] is None
        assert row[2] is None

    def test_no_lookahead_for_starts(self, pitcher_db):
        """l3_strikeouts for the 2nd start uses only the 1st start (k=8)."""
        compute_pitcher_rolling_stats(pitcher_db)
        conn = sqlite3.connect(pitcher_db)
        row = conn.execute(
            "SELECT l3_strikeouts, starts_in_l3 FROM pitcher_rolling_stats "
            "WHERE player_id = 543037 ORDER BY game_date ASC LIMIT 1 OFFSET 1"
        ).fetchone()
        conn.close()
        assert row[0] == pytest.approx(8.0)
        assert row[1] == 1

    def test_l3_average_after_3_starts(self, pitcher_db):
        """4th start: l3 uses starts 1,2,3 (k=8,6,10 → avg=8.0)."""
        compute_pitcher_rolling_stats(pitcher_db)
        conn = sqlite3.connect(pitcher_db)
        row = conn.execute(
            "SELECT l3_strikeouts FROM pitcher_rolling_stats "
            "WHERE player_id = 543037 ORDER BY game_date ASC LIMIT 1 OFFSET 3"
        ).fetchone()
        conn.close()
        assert row[0] == pytest.approx(8.0)

    def test_l5_average_after_5_starts(self, pitcher_db):
        """5th start: l5 uses starts 1-4 (k=8,6,10,7 → avg=7.75) — only 4 prior."""
        compute_pitcher_rolling_stats(pitcher_db)
        conn = sqlite3.connect(pitcher_db)
        row = conn.execute(
            "SELECT l5_strikeouts, starts_in_l5 FROM pitcher_rolling_stats "
            "WHERE player_id = 543037 ORDER BY game_date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        assert row[0] == pytest.approx(7.75)
        assert row[1] == 4

    def test_idempotent_rerun(self, pitcher_db):
        """Running twice should not duplicate rows."""
        compute_pitcher_rolling_stats(pitcher_db)
        compute_pitcher_rolling_stats(pitcher_db)
        conn = sqlite3.connect(pitcher_db)
        total = conn.execute("SELECT COUNT(*) FROM pitcher_rolling_stats").fetchone()[0]
        conn.close()
        assert total == 5
