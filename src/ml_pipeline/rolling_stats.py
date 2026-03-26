"""Rolling stats computation for batters (L10/L20/L30) and pitchers (L3/L5/L10)."""

import logging
import sqlite3

import numpy as np

logger = logging.getLogger(__name__)


def _mean(values):
    return float(np.mean(values)) if values else None


def _std(values):
    return float(np.std(values, ddof=0)) if len(values) >= 2 else None


def _window(games, n):
    """Return up to last n games from a list (oldest-first)."""
    return games[-n:] if len(games) >= n else games


def compute_batter_rolling_stats(db_path: str, season: str = None) -> int:
    """
    Compute L10/L20/L30 rolling averages for all batters and insert into
    batter_rolling_stats.

    Windows are computed using games BEFORE the current game (no look-ahead).
    Platoon splits (vs LHP / vs RHP) use `opposing_pitcher_hand` from game logs.

    Args:
        db_path: Path to the SQLite database
        season: If set, only process game logs for this season

    Returns:
        Number of rows inserted/replaced
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    count = 0

    try:
        # Fetch all batter IDs to process
        cursor.execute("SELECT DISTINCT player_id FROM batter_game_logs ORDER BY player_id")
        player_ids = [row[0] for row in cursor.fetchall()]
        logger.info(f"Computing rolling stats for {len(player_ids)} batters...")

        for player_id in player_ids:
            # Load all games for this player ordered by date ascending
            season_filter = "AND season = ?" if season else ""
            params = (player_id, season) if season else (player_id,)
            cursor.execute(f"""
                SELECT game_id, game_date, hits, home_runs, rbi, runs, stolen_bases,
                       total_bases, walks, strikeouts, plate_appearances, at_bats,
                       opposing_pitcher_hand
                FROM batter_game_logs
                WHERE player_id = ?
                {season_filter}
                ORDER BY game_date ASC, game_id ASC
            """, params)
            games = [dict(row) for row in cursor.fetchall()]

            rows_for_player = []
            for i, game in enumerate(games):
                # All games BEFORE this one (no look-ahead)
                prior = games[:i]

                def col(g, k):
                    v = g[k]
                    return float(v) if v is not None else 0.0

                prior_vals = {
                    'hits': [col(g, 'hits') for g in prior],
                    'hr': [col(g, 'home_runs') for g in prior],
                    'rbi': [col(g, 'rbi') for g in prior],
                    'runs': [col(g, 'runs') for g in prior],
                    'sb': [col(g, 'stolen_bases') for g in prior],
                    'tb': [col(g, 'total_bases') for g in prior],
                    'bb': [col(g, 'walks') for g in prior],
                    'so': [col(g, 'strikeouts') for g in prior],
                    'pa': [col(g, 'plate_appearances') for g in prior],
                    'ab': [col(g, 'at_bats') for g in prior],
                }

                def w10(stat): return _window(prior_vals[stat], 10)
                def w20(stat): return _window(prior_vals[stat], 20)
                def w30(stat): return _window(prior_vals[stat], 30)

                # Platoon split games
                lhp_games = [g for g in prior if g.get('opposing_pitcher_hand') == 'L']
                rhp_games = [g for g in prior if g.get('opposing_pitcher_hand') == 'R']
                lhp10 = lhp_games[-10:]
                rhp10 = rhp_games[-10:]

                l10_hits = _mean(w10('hits'))
                l10_hr = _mean(w10('hr'))
                l10_rbi = _mean(w10('rbi'))
                l10_runs = _mean(w10('runs'))
                l10_sb = _mean(w10('sb'))
                l10_tb = _mean(w10('tb'))
                l10_bb = _mean(w10('bb'))
                l10_so = _mean(w10('so'))
                l10_pa = _mean(w10('pa'))
                l10_ab = _mean(w10('ab'))

                l20_hits = _mean(w20('hits'))
                l20_hr = _mean(w20('hr'))
                l20_rbi = _mean(w20('rbi'))
                l20_runs = _mean(w20('runs'))
                l20_sb = _mean(w20('sb'))
                l20_tb = _mean(w20('tb'))

                l30_hits = _mean(w30('hits'))
                l30_hr = _mean(w30('hr'))
                l30_rbi = _mean(w30('rbi'))
                l30_runs = _mean(w30('runs'))
                l30_sb = _mean(w30('sb'))
                l30_tb = _mean(w30('tb'))

                # Standard deviations
                l10_hits_std = _std(w10('hits'))
                l10_hr_std = _std(w10('hr'))
                l10_rbi_std = _std(w10('rbi'))
                l10_tb_std = _std(w10('tb'))
                l10_so_std = _std(w10('so'))

                # Trends: L10 - L20 (positive = trending up)
                def trend(l10_val, l20_val):
                    if l10_val is not None and l20_val is not None:
                        return round(l10_val - l20_val, 4)
                    return None

                hits_trend = trend(l10_hits, l20_hits)
                hr_trend = trend(l10_hr, l20_hr)
                rbi_trend = trend(l10_rbi, l20_rbi)
                tb_trend = trend(l10_tb, l20_tb)
                so_trend = trend(l10_so, _mean(w20('so')))

                # Platoon splits
                l10_hits_vs_lhp = _mean([col(g, 'hits') for g in lhp10])
                l10_hits_vs_rhp = _mean([col(g, 'hits') for g in rhp10])
                l10_tb_vs_lhp = _mean([col(g, 'total_bases') for g in lhp10])
                l10_tb_vs_rhp = _mean([col(g, 'total_bases') for g in rhp10])
                l10_so_vs_lhp = _mean([col(g, 'strikeouts') for g in lhp10])
                l10_so_vs_rhp = _mean([col(g, 'strikeouts') for g in rhp10])

                rows_for_player.append((
                    player_id, game['game_id'], game['game_date'],
                    l10_hits, l10_hr, l10_rbi, l10_runs,
                    l10_sb, l10_tb, l10_bb, l10_so, l10_pa, l10_ab,
                    l20_hits, l20_hr, l20_rbi, l20_runs, l20_sb, l20_tb,
                    l30_hits, l30_hr, l30_rbi, l30_runs, l30_sb, l30_tb,
                    l10_hits_std, l10_hr_std, l10_rbi_std, l10_tb_std, l10_so_std,
                    hits_trend, hr_trend, rbi_trend, tb_trend, so_trend,
                    l10_hits_vs_lhp, l10_hits_vs_rhp,
                    l10_tb_vs_lhp, l10_tb_vs_rhp,
                    l10_so_vs_lhp, l10_so_vs_rhp,
                    min(len(prior), 10), min(len(prior), 20), min(len(prior), 30),
                ))

            cursor.executemany('''
                INSERT OR REPLACE INTO batter_rolling_stats
                (player_id, game_id, game_date,
                 l10_hits, l10_hr, l10_rbi, l10_runs,
                 l10_sb, l10_tb, l10_bb, l10_so, l10_pa, l10_ab,
                 l20_hits, l20_hr, l20_rbi, l20_runs, l20_sb, l20_tb,
                 l30_hits, l30_hr, l30_rbi, l30_runs, l30_sb, l30_tb,
                 l10_hits_std, l10_hr_std, l10_rbi_std, l10_tb_std, l10_so_std,
                 hits_trend, hr_trend, rbi_trend, tb_trend, so_trend,
                 l10_hits_vs_lhp, l10_hits_vs_rhp,
                 l10_tb_vs_lhp, l10_tb_vs_rhp,
                 l10_so_vs_lhp, l10_so_vs_rhp,
                 games_in_l10, games_in_l20, games_in_l30)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', rows_for_player)
            count += len(rows_for_player)

        conn.commit()
        logger.info(f"Done — computed rolling stats for {count} batter game rows")
    finally:
        conn.close()

    return count


def compute_pitcher_rolling_stats(db_path: str, season: str = None) -> int:
    """
    Compute L3/L5/L10 rolling averages for pitchers (starts only) and insert
    into pitcher_rolling_stats.

    Windows are computed using starts BEFORE the current game (no look-ahead).

    Args:
        db_path: Path to the SQLite database
        season: If set, only process game logs for this season

    Returns:
        Number of rows inserted/replaced
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    count = 0

    try:
        cursor.execute("SELECT DISTINCT player_id FROM pitcher_game_logs ORDER BY player_id")
        player_ids = [row[0] for row in cursor.fetchall()]
        logger.info(f"Computing rolling stats for {len(player_ids)} pitchers...")

        for player_id in player_ids:
            season_filter = "AND season = ?" if season else ""
            params = (player_id, season) if season else (player_id,)
            cursor.execute(f"""
                SELECT game_id, game_date, is_start, strikeouts, outs_recorded,
                       earned_runs, hits_allowed, walks_allowed, pitches_thrown
                FROM pitcher_game_logs
                WHERE player_id = ?
                {season_filter}
                ORDER BY game_date ASC, game_id ASC
            """, params)
            all_games = [dict(row) for row in cursor.fetchall()]

            # For rolling windows, we only look at starts
            rows_for_player = []
            prior_starts = []  # ordered list of start game dicts

            for game in all_games:
                def col(g, k):
                    v = g[k]
                    return float(v) if v is not None else 0.0

                def w3(stat): return _window([col(g, stat) for g in prior_starts], 3)
                def w5(stat): return _window([col(g, stat) for g in prior_starts], 5)
                def w10(stat): return _window([col(g, stat) for g in prior_starts], 10)

                l3_k = _mean(w3('strikeouts'))
                l3_outs = _mean(w3('outs_recorded'))
                l3_er = _mean(w3('earned_runs'))
                l3_hits = _mean(w3('hits_allowed'))
                l3_walks = _mean(w3('walks_allowed'))
                l3_pitches = _mean(w3('pitches_thrown'))

                l5_k = _mean(w5('strikeouts'))
                l5_outs = _mean(w5('outs_recorded'))
                l5_er = _mean(w5('earned_runs'))
                l5_hits = _mean(w5('hits_allowed'))
                l5_walks = _mean(w5('walks_allowed'))
                l5_pitches = _mean(w5('pitches_thrown'))

                l10_k = _mean(w10('strikeouts'))
                l10_outs = _mean(w10('outs_recorded'))
                l10_er = _mean(w10('earned_runs'))
                l10_hits = _mean(w10('hits_allowed'))
                l10_walks = _mean(w10('walks_allowed'))
                l10_pitches = _mean(w10('pitches_thrown'))

                l5_k_std = _std(w5('strikeouts'))
                l5_outs_std = _std(w5('outs_recorded'))
                l5_er_std = _std(w5('earned_runs'))

                def trend(l3_val, l5_val):
                    if l3_val is not None and l5_val is not None:
                        return round(l3_val - l5_val, 4)
                    return None

                k_trend = trend(l3_k, l5_k)
                outs_trend = trend(l3_outs, l5_outs)
                er_trend = trend(l3_er, l5_er)

                n = len(prior_starts)
                rows_for_player.append((
                    player_id, game['game_id'], game['game_date'],
                    l3_k, l3_outs, l3_er, l3_hits, l3_walks, l3_pitches,
                    l5_k, l5_outs, l5_er, l5_hits, l5_walks, l5_pitches,
                    l10_k, l10_outs, l10_er, l10_hits, l10_walks, l10_pitches,
                    l5_k_std, l5_outs_std, l5_er_std,
                    k_trend, outs_trend, er_trend,
                    min(n, 3), min(n, 5), min(n, 10),
                ))

                # Track starts for future windows
                if game.get('is_start'):
                    prior_starts.append(game)

            cursor.executemany('''
                INSERT OR REPLACE INTO pitcher_rolling_stats
                (player_id, game_id, game_date,
                 l3_strikeouts, l3_outs, l3_er, l3_hits_allowed, l3_walks, l3_pitches,
                 l5_strikeouts, l5_outs, l5_er, l5_hits_allowed, l5_walks, l5_pitches,
                 l10_strikeouts, l10_outs, l10_er, l10_hits_allowed, l10_walks, l10_pitches,
                 l5_k_std, l5_outs_std, l5_er_std,
                 k_trend, outs_trend, er_trend,
                 starts_in_l3, starts_in_l5, starts_in_l10)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', rows_for_player)
            count += len(rows_for_player)

        conn.commit()
        logger.info(f"Done — computed rolling stats for {count} pitcher game rows")
    finally:
        conn.close()

    return count
