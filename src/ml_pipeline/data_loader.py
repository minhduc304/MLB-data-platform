"""Data loader: SQL queries that produce training-ready DataFrames."""

import sqlite3
from typing import Optional

import pandas as pd

from src.ml_pipeline.config import BATTER_STATS, PITCHER_STATS, STAT_COLUMNS


class PropDataLoader:
    """
    Loads data from the SQLite database into pandas DataFrames
    ready for feature engineering and model training.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Primary load methods
    # ------------------------------------------------------------------

    def load_historical_games(
        self,
        stat_type: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        season: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load historical game logs for regressor training.

        Joins game logs with rolling stats, park factors, weather, and
        pitcher matchup info. Returns one row per player-game.

        Args:
            stat_type: e.g. 'hits', 'pitcher_strikeouts'
            min_date: Inclusive lower bound (YYYY-MM-DD)
            max_date: Inclusive upper bound (YYYY-MM-DD)
            season: Filter to a specific season year

        Returns:
            DataFrame with target column = stat_type's DB column name
        """
        db_col = STAT_COLUMNS.get(stat_type)
        if not db_col:
            raise ValueError(f"Unknown stat_type: {stat_type}")

        is_pitcher = stat_type in PITCHER_STATS

        if is_pitcher:
            query, params = self._pitcher_game_query(
                db_col, min_date, max_date, season
            )
        else:
            query, params = self._batter_game_query(
                db_col, min_date, max_date, season
            )

        df = self._query(query, params)
        df['stat_type'] = stat_type
        df['target'] = df['actual_value']
        return df

    def load_training_data(
        self,
        stat_type: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load labeled prop outcomes for classifier training.

        Joins prop_outcomes with rolling stats and game context.
        Only returns rows where actual_value is known (game completed).

        Args:
            stat_type: e.g. 'hits', 'pitcher_strikeouts'
            min_date: Inclusive lower bound (YYYY-MM-DD)
            max_date: Inclusive upper bound (YYYY-MM-DD)

        Returns:
            DataFrame with 'target' = hit_over (1/0) and 'line' column
        """
        is_pitcher = stat_type in PITCHER_STATS
        rolling_table = 'pitcher_rolling_stats' if is_pitcher else 'batter_rolling_stats'
        log_table = 'pitcher_game_logs' if is_pitcher else 'batter_game_logs'

        filters = ["po.stat_type = ?", "po.actual_value IS NOT NULL"]
        params: list = [stat_type]

        if min_date:
            filters.append("po.game_date >= ?")
            params.append(min_date)
        if max_date:
            filters.append("po.game_date <= ?")
            params.append(max_date)

        where = " AND ".join(filters)

        # Batter-only columns (pitcher_game_logs lacks these)
        batter_cols = """
                gl.opposing_pitcher_id,
                gl.opposing_pitcher_hand,
                ps.era               AS opp_pitcher_era,
                ps.whip              AS opp_pitcher_whip,
                ps.k_per_9           AS opp_pitcher_k_per_9,""" if not is_pitcher else ""

        opp_pitcher_join = """
            LEFT JOIN pitcher_stats ps
                ON  ps.player_id = gl.opposing_pitcher_id
                AND ps.season    = substr(po.game_date, 1, 4)""" if not is_pitcher else ""

        query = f"""
            SELECT
                po.player_id,
                po.player_name      AS full_name,
                po.game_date,
                po.stat_type,
                po.line,
                po.sportsbook       AS source,
                po.over_odds,
                po.under_odds,
                po.actual_value,
                po.hit_over         AS target,
                po.edge,
                -- Rolling stats
                rs.*,
                -- Game context
                gl.team_id,
                gl.opponent_id,
                gl.is_home,
                gl.venue_id,
                {batter_cols}
                -- Park factors
                pf_overall.factor_value  AS park_factor_overall,
                pf_hr.factor_value       AS park_factor_hr,
                pf_h.factor_value        AS park_factor_h,
                -- Weather
                gw.temp_f,
                gw.wind_speed,
                gw.wind_direction,
                gw.is_dome
            FROM prop_outcomes po
            JOIN {rolling_table} rs
                ON  rs.player_id = po.player_id
                AND rs.game_date = po.game_date
            JOIN {log_table} gl
                ON  gl.player_id = po.player_id
                AND gl.game_date = po.game_date
            LEFT JOIN park_factors pf_overall
                ON  pf_overall.venue_id    = gl.venue_id
                AND pf_overall.season      = substr(po.game_date, 1, 4)
                AND pf_overall.factor_type = 'overall'
            LEFT JOIN park_factors pf_hr
                ON  pf_hr.venue_id    = gl.venue_id
                AND pf_hr.season      = substr(po.game_date, 1, 4)
                AND pf_hr.factor_type = 'hr'
            LEFT JOIN park_factors pf_h
                ON  pf_h.venue_id    = gl.venue_id
                AND pf_h.season      = substr(po.game_date, 1, 4)
                AND pf_h.factor_type = 'h'
            LEFT JOIN game_weather gw
                ON  gw.game_id = gl.game_id
            {opp_pitcher_join}
            WHERE {where}
            ORDER BY po.game_date
        """
        df = self._query(query, params)
        # Drop duplicate columns from rolling stats join
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    def load_upcoming_props(self, stat_type: str) -> pd.DataFrame:
        """
        Load today's props for prediction.

        Joins all_props with rolling stats, schedule context, lineup,
        park factors, and weather for upcoming games.

        Args:
            stat_type: e.g. 'hits', 'pitcher_strikeouts'

        Returns:
            DataFrame ready for FeatureEngineer + model inference
        """
        is_pitcher = stat_type in PITCHER_STATS
        rolling_table = 'pitcher_rolling_stats' if is_pitcher else 'batter_rolling_stats'

        query = f"""
            SELECT
                ap.id               AS prop_id,
                ap.source,
                ap.full_name,
                ap.stat_name        AS stat_type,
                ap.stat_value       AS line,
                ap.american_odds,
                ap.team_name,
                ap.opponent_name,
                ap.scheduled_at,
                -- Player identity
                bs.player_id,
                bs.team_id,
                bs.bats,
                -- Rolling stats (most recent game)
                rs.*,
                -- Schedule context
                s.home_team_id,
                s.away_team_id,
                s.venue_id,
                s.home_probable_pitcher_id,
                s.away_probable_pitcher_id,
                -- Lineup
                sl.batting_order,
                -- Park factors (current season)
                pf_overall.factor_value  AS park_factor_overall,
                pf_hr.factor_value       AS park_factor_hr,
                pf_h.factor_value        AS park_factor_h,
                -- Weather
                gw.temp_f,
                gw.wind_speed,
                gw.wind_direction,
                gw.is_dome,
                -- Opposing pitcher stats
                ps.era               AS opp_pitcher_era,
                ps.whip              AS opp_pitcher_whip,
                ps.k_per_9           AS opp_pitcher_k_per_9,
                ps.throws            AS opp_pitcher_hand_season
            FROM all_props ap
            -- Resolve player_id by matching name to batter_stats or pitcher_stats
            LEFT JOIN batter_stats bs
                ON  bs.player_name = ap.full_name
            LEFT JOIN pitcher_stats pit_id
                ON  pit_id.player_name = ap.full_name
            -- Most recent rolling stats row for this player
            LEFT JOIN {rolling_table} rs
                ON  rs.player_id = COALESCE(bs.player_id, pit_id.player_id)
                AND rs.game_date = (
                    SELECT MAX(game_date) FROM {rolling_table}
                    WHERE player_id = COALESCE(bs.player_id, pit_id.player_id)
                )
            -- Schedule: match by team name and scheduled date
            LEFT JOIN schedule s
                ON  s.game_date = substr(ap.scheduled_at, 1, 10)
                AND (s.home_abbr = ap.team_name OR s.away_abbr = ap.team_name)
            -- Lineup
            LEFT JOIN starting_lineups sl
                ON  sl.player_id  = COALESCE(bs.player_id, pit_id.player_id)
                AND sl.game_id    = s.game_id
            -- Park factors
            LEFT JOIN park_factors pf_overall
                ON  pf_overall.venue_id    = s.venue_id
                AND pf_overall.factor_type = 'overall'
            LEFT JOIN park_factors pf_hr
                ON  pf_hr.venue_id    = s.venue_id
                AND pf_hr.factor_type = 'hr'
            LEFT JOIN park_factors pf_h
                ON  pf_h.venue_id    = s.venue_id
                AND pf_h.factor_type = 'h'
            -- Weather (today's games)
            LEFT JOIN game_weather gw
                ON  gw.game_id = s.game_id
            -- Opposing pitcher
            LEFT JOIN pitcher_stats ps
                ON  ps.player_id = CASE
                        WHEN bs.team_id = s.home_team_id THEN s.away_probable_pitcher_id
                        ELSE s.home_probable_pitcher_id
                    END
            WHERE ap.stat_name = ?
            ORDER BY ap.scheduled_at
        """
        df = self._query(query, [stat_type])
        df = df.loc[:, ~df.columns.duplicated()]

        # Derive is_home
        if 'home_team_id' in df.columns and 'team_id' in df.columns:
            df['is_home'] = (df['team_id'] == df['home_team_id']).astype(int)

        return df

    # ------------------------------------------------------------------
    # Auxiliary data for matchup features
    # ------------------------------------------------------------------

    def get_batter_vs_team_stats(self, stat_type: str) -> pd.DataFrame:
        """Career and recent (L20 games) batter stats against each opponent team."""
        db_col = STAT_COLUMNS.get(stat_type, stat_type)
        query = f"""
            SELECT
                bgl.player_id,
                s.away_team_id  AS opponent_id,
                AVG(bgl.{db_col})                                    AS career_vs_team_stat,
                AVG(CASE WHEN bgl.game_date >= date('now', '-90 days')
                         THEN bgl.{db_col} END)                      AS recent_vs_team_stat,
                COUNT(*)                                             AS games_vs_team
            FROM batter_game_logs bgl
            JOIN schedule s ON s.game_id = bgl.game_id
            WHERE bgl.{db_col} IS NOT NULL
              AND bgl.team_id = s.home_team_id   -- batter is home → opponent is away
            GROUP BY bgl.player_id, s.away_team_id
            UNION ALL
            SELECT
                bgl.player_id,
                s.home_team_id  AS opponent_id,
                AVG(bgl.{db_col})                                    AS career_vs_team_stat,
                AVG(CASE WHEN bgl.game_date >= date('now', '-90 days')
                         THEN bgl.{db_col} END)                      AS recent_vs_team_stat,
                COUNT(*)                                             AS games_vs_team
            FROM batter_game_logs bgl
            JOIN schedule s ON s.game_id = bgl.game_id
            WHERE bgl.{db_col} IS NOT NULL
              AND bgl.team_id = s.away_team_id   -- batter is away → opponent is home
            GROUP BY bgl.player_id, s.home_team_id
        """
        return self._query(query)

    def get_pitcher_vs_team_stats(self, stat_type: str) -> pd.DataFrame:
        """Career and recent pitcher stats against each opponent team."""
        db_col = STAT_COLUMNS.get(stat_type, stat_type)
        query = f"""
            SELECT
                pgl.player_id,
                s.away_team_id  AS opponent_id,
                AVG(pgl.{db_col})   AS career_vs_team_stat,
                AVG(CASE WHEN pgl.game_date >= date('now', '-90 days')
                         THEN pgl.{db_col} END) AS recent_vs_team_stat,
                COUNT(*)            AS games_vs_team
            FROM pitcher_game_logs pgl
            JOIN schedule s ON s.game_id = pgl.game_id
            WHERE pgl.{db_col} IS NOT NULL
              AND pgl.is_start = 1
              AND pgl.team_id = s.home_team_id
            GROUP BY pgl.player_id, s.away_team_id
            UNION ALL
            SELECT
                pgl.player_id,
                s.home_team_id  AS opponent_id,
                AVG(pgl.{db_col})   AS career_vs_team_stat,
                AVG(CASE WHEN pgl.game_date >= date('now', '-90 days')
                         THEN pgl.{db_col} END) AS recent_vs_team_stat,
                COUNT(*)            AS games_vs_team
            FROM pitcher_game_logs pgl
            JOIN schedule s ON s.game_id = pgl.game_id
            WHERE pgl.{db_col} IS NOT NULL
              AND pgl.is_start = 1
              AND pgl.team_id = s.away_team_id
            GROUP BY pgl.player_id, s.home_team_id
        """
        return self._query(query)

    def get_arsenal_matchup_stats(
        self,
        batter_ids: list,
        pitcher_ids: list,
        season: str,
        min_pitches: int = 20,
    ) -> pd.DataFrame:
        """
        Compute pitch arsenal weighted matchup features for batter-pitcher pairs.

        Pitches are grouped by pitch_cluster (K-means on physical characteristics)
        rather than pitch_type labels, so two pitchers' "curveballs" with different
        movement profiles land in different clusters, and similar pitches from
        different pitch types land in the same cluster.

        Falls back to pitch_type-based matching if pitch_cluster is not yet populated
        (i.e., cluster_pitches.py has not been run yet).

        For each (batter_id, pitcher_id) pair:
          1. Aggregate pitcher's arsenal by cluster (summing n_pitches, recomputing usage_pct)
          2. Look up batter's performance vs each cluster (batter_pitch_type_stats)
          3. Weight batter stats by pitcher's cluster usage_pct
          4. Fall back to league averages when batter has < min_pitches for a cluster

        Returns:
            DataFrame with one row per (batter_id, pitcher_id) and columns:
            arsenal_weighted_ba, arsenal_weighted_whiff, arsenal_weighted_xba,
            arsenal_weighted_xwoba, arsenal_weighted_swstr,
            pitcher_avg_velocity, pitcher_arm_angle,
            pitcher_primary_movement_h, pitcher_primary_movement_v,
            batter_whiff_vs_primary_pitch, batter_xwoba_vs_primary_pitch,
            arsenal_sample_size
        """
        if not batter_ids or not pitcher_ids:
            return pd.DataFrame()

        # Try current season, fall back to prior season for early-season data gaps
        arsenal_df = pd.DataFrame()
        for s in [season, str(int(season) - 1)]:
            arsenal_df = self._fetch_pitcher_arsenal(pitcher_ids, s)
            if not arsenal_df.empty:
                totals = arsenal_df.groupby('pitcher_id')['n_pitches'].sum()
                if (totals >= 50).any():
                    break

        if arsenal_df.empty:
            return pd.DataFrame()

        # Determine join key: use pitch_cluster if populated, else fall back to pitch_type
        use_clusters = (
            'pitch_cluster' in arsenal_df.columns
            and arsenal_df['pitch_cluster'].notna().any()
        )
        join_key = 'pitch_cluster' if use_clusters else 'pitch_type'

        if use_clusters:
            # Re-aggregate arsenal by cluster per pitcher:
            # A pitcher may have multiple pitch types in the same cluster (e.g., FB + cutter).
            # Sum n_pitches, recompute usage_pct, take weighted averages of physical features.
            arsenal_df = arsenal_df[arsenal_df['pitch_cluster'].notna()].copy()
            arsenal_df['pitch_cluster'] = arsenal_df['pitch_cluster'].astype(int)

            physical_cols = ['avg_velocity', 'avg_pfx_x', 'avg_pfx_z', 'avg_arm_angle', 'avg_release_extension']
            for col in physical_cols:
                if col not in arsenal_df.columns:
                    arsenal_df[col] = float('nan')
                arsenal_df[f'_wt_{col}'] = arsenal_df['n_pitches'] * arsenal_df[col].fillna(0)

            cluster_agg = arsenal_df.groupby(['pitcher_id', 'pitch_cluster']).agg(
                n_pitches=('n_pitches', 'sum'),
                **{col: (f'_wt_{col}', 'sum') for col in physical_cols},
            ).reset_index()

            total_per_pitcher = cluster_agg.groupby('pitcher_id')['n_pitches'].transform('sum')
            cluster_agg['usage_pct'] = cluster_agg['n_pitches'] / total_per_pitcher.replace(0, 1)
            for col in physical_cols:
                cluster_agg[col] = cluster_agg[col] / cluster_agg['n_pitches'].replace(0, 1)

            arsenal_df = cluster_agg
        else:
            # No clusters yet — fall back to pitch_type grouping
            total_per_pitcher = arsenal_df.groupby('pitcher_id')['n_pitches'].transform('sum')
            arsenal_df = arsenal_df.copy()
            arsenal_df['usage_pct'] = arsenal_df['n_pitches'] / total_per_pitcher.replace(0, 1)

        batter_df = self._fetch_batter_pitch_stats(batter_ids, season, use_clusters=use_clusters)
        league_avg = self._compute_league_averages(season, use_clusters=use_clusters)

        # Cross-join batter_ids with pitcher arsenal rows
        batter_ids_df = pd.DataFrame({'batter_id': list(set(batter_ids))})
        cross = batter_ids_df.merge(arsenal_df, how='cross')

        # Join batter stats on (batter_id, join_key)
        stat_cols = ['ba', 'slg', 'whiff_rate', 'xba', 'xwoba', 'swstr_rate']
        batter_join_cols = ['batter_id', join_key, 'n_pitches'] + stat_cols
        if not use_clusters and 'p_throws' in batter_df.columns and 'p_throws' in cross.columns:
            batter_join_cols = ['batter_id', join_key, 'p_throws', 'n_pitches'] + stat_cols

        available_batter_cols = [c for c in batter_join_cols if c in batter_df.columns]
        cross = cross.merge(
            batter_df[available_batter_cols].rename(columns={'n_pitches': 'b_n_pitches'}),
            on=[c for c in [join_key, 'batter_id'] + (['p_throws'] if not use_clusters and 'p_throws' in cross.columns and 'p_throws' in batter_df.columns else [])],
            how='left',
        )

        # Apply league average fallback where batter data is insufficient
        needs_fallback = cross['b_n_pitches'].isna() | (cross['b_n_pitches'] < min_pitches)
        if needs_fallback.any() and not league_avg.empty:
            lg_join_cols = [join_key] + (['p_throws'] if not use_clusters and 'p_throws' in league_avg.columns and 'p_throws' in cross.columns else [])
            cross = cross.merge(
                league_avg[lg_join_cols + stat_cols].rename(
                    columns={c: f'{c}_lg' for c in stat_cols}
                ),
                on=lg_join_cols,
                how='left',
            )
            for col in stat_cols:
                cross.loc[needs_fallback, col] = cross.loc[needs_fallback, f'{col}_lg']
            cross = cross.drop(columns=[f'{col}_lg' for col in stat_cols])

        cross['b_n_pitches'] = cross['b_n_pitches'].fillna(0)

        # Compute weighted stats per row
        for stat, col in [
            ('arsenal_weighted_ba', 'ba'),
            ('arsenal_weighted_whiff', 'whiff_rate'),
            ('arsenal_weighted_xba', 'xba'),
            ('arsenal_weighted_xwoba', 'xwoba'),
            ('arsenal_weighted_swstr', 'swstr_rate'),
        ]:
            cross[col] = cross[col].fillna(0)
            cross[f'_w_{stat}'] = cross['usage_pct'] * cross[col]

        # Pitcher-level features (velocity weighted by usage)
        pitcher_feats = arsenal_df.copy()
        pitcher_feats['_wv'] = pitcher_feats['usage_pct'] * pitcher_feats['avg_velocity'].fillna(0)
        pitcher_summary = pitcher_feats.groupby('pitcher_id').agg(
            pitcher_avg_velocity=('_wv', 'sum'),
        ).reset_index()

        # Primary cluster/pitch per pitcher (highest usage)
        primary = arsenal_df.loc[arsenal_df.groupby('pitcher_id')['usage_pct'].idxmax()].copy()
        primary_rename = {
            'avg_arm_angle': 'pitcher_arm_angle',
            'avg_pfx_x': 'pitcher_primary_movement_h',
            'avg_pfx_z': 'pitcher_primary_movement_v',
            join_key: '_primary_key',
        }
        primary_cols = ['pitcher_id'] + [c for c in primary_rename if c in primary.columns]
        primary = primary[primary_cols].rename(columns=primary_rename)

        # Aggregate weighted stats per (batter_id, pitcher_id)
        grouped = cross.groupby(['batter_id', 'pitcher_id']).agg(
            arsenal_weighted_ba=('_w_arsenal_weighted_ba', 'sum'),
            arsenal_weighted_whiff=('_w_arsenal_weighted_whiff', 'sum'),
            arsenal_weighted_xba=('_w_arsenal_weighted_xba', 'sum'),
            arsenal_weighted_xwoba=('_w_arsenal_weighted_xwoba', 'sum'),
            arsenal_weighted_swstr=('_w_arsenal_weighted_swstr', 'sum'),
            arsenal_sample_size=('b_n_pitches', 'sum'),
        ).reset_index()

        grouped = grouped.merge(pitcher_summary, on='pitcher_id', how='left')
        grouped = grouped.merge(primary, on='pitcher_id', how='left')

        # Batter stats vs primary cluster/pitch
        cross_with_primary = cross.merge(
            primary[['pitcher_id', '_primary_key']],
            on='pitcher_id', how='left',
        )
        primary_rows = cross_with_primary[
            (cross_with_primary[join_key] == cross_with_primary['_primary_key']) &
            (cross_with_primary['b_n_pitches'] >= min_pitches)
        ][['batter_id', 'pitcher_id', 'whiff_rate', 'xwoba']].rename(columns={
            'whiff_rate': 'batter_whiff_vs_primary_pitch',
            'xwoba': 'batter_xwoba_vs_primary_pitch',
        })

        grouped = grouped.merge(primary_rows, on=['batter_id', 'pitcher_id'], how='left')
        grouped = grouped.drop(columns=['_primary_key'], errors='ignore')

        return grouped

    def _fetch_pitcher_arsenal(self, pitcher_ids: list, season: str) -> pd.DataFrame:
        if not pitcher_ids:
            return pd.DataFrame()
        placeholders = ','.join('?' * len(pitcher_ids))
        query = f"""
            SELECT pitcher_id, pitch_type, p_throws, n_pitches,
                   usage_pct, avg_velocity, avg_pfx_x, avg_pfx_z,
                   avg_arm_angle, avg_release_extension, pitch_cluster
            FROM pitcher_arsenal
            WHERE pitcher_id IN ({placeholders}) AND season = ?
        """
        return self._query(query, list(pitcher_ids) + [season])

    def _fetch_batter_pitch_stats(
        self, batter_ids: list, season: str, use_clusters: bool = False
    ) -> pd.DataFrame:
        if not batter_ids:
            return pd.DataFrame()
        placeholders = ','.join('?' * len(batter_ids))
        if use_clusters:
            query = f"""
                SELECT batter_id, pitch_cluster, SUM(n_pitches) AS n_pitches,
                       AVG(ba) AS ba, AVG(slg) AS slg, AVG(whiff_rate) AS whiff_rate,
                       AVG(xba) AS xba, AVG(xwoba) AS xwoba, AVG(swstr_rate) AS swstr_rate
                FROM batter_pitch_type_stats
                WHERE batter_id IN ({placeholders}) AND season = ?
                  AND pitch_cluster IS NOT NULL
                GROUP BY batter_id, pitch_cluster
            """
        else:
            query = f"""
                SELECT batter_id, pitch_type, p_throws, n_pitches,
                       ba, slg, whiff_rate, xba, xwoba, swstr_rate
                FROM batter_pitch_type_stats
                WHERE batter_id IN ({placeholders}) AND season = ?
            """
        return self._query(query, list(batter_ids) + [season])

    def _compute_league_averages(self, season: str, use_clusters: bool = False) -> pd.DataFrame:
        """Compute league-average batter stats per cluster (or pitch_type) for fallback."""
        if use_clusters:
            query = """
                SELECT pitch_cluster,
                       AVG(ba)         AS ba,
                       AVG(slg)        AS slg,
                       AVG(whiff_rate) AS whiff_rate,
                       AVG(xba)        AS xba,
                       AVG(xwoba)      AS xwoba,
                       AVG(swstr_rate) AS swstr_rate
                FROM batter_pitch_type_stats
                WHERE season = ? AND pitch_cluster IS NOT NULL
                GROUP BY pitch_cluster
            """
        else:
            query = """
                SELECT pitch_type, p_throws,
                       AVG(ba)         AS ba,
                       AVG(slg)        AS slg,
                       AVG(whiff_rate) AS whiff_rate,
                       AVG(xba)        AS xba,
                       AVG(xwoba)      AS xwoba,
                       AVG(swstr_rate) AS swstr_rate
                FROM batter_pitch_type_stats
                WHERE season = ?
                GROUP BY pitch_type, p_throws
            """
        return self._query(query, [season])

    def get_player_consistency_stats(self, stat_type: str) -> pd.DataFrame:
        """Per-player coefficient of variation (std/mean) as a consistency metric."""
        db_col = STAT_COLUMNS.get(stat_type, stat_type)
        table = 'pitcher_game_logs' if stat_type in PITCHER_STATS else 'batter_game_logs'
        query = f"""
            SELECT
                player_id,
                AVG({db_col})                        AS season_avg,
                AVG({db_col} * {db_col}) - AVG({db_col}) * AVG({db_col}) AS season_var,
                COUNT(*)                             AS games_played
            FROM {table}
            WHERE {db_col} IS NOT NULL
              AND season = (SELECT MAX(season) FROM {table})
            GROUP BY player_id
            HAVING COUNT(*) >= 5
        """
        df = self._query(query)
        df['consistency_cv'] = (df['season_var'] ** 0.5) / df['season_avg'].replace(0, float('nan'))
        return df[['player_id', 'season_avg', 'consistency_cv', 'games_played']]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _batter_game_query(self, db_col, min_date, max_date, season):
        filters = [f"bgl.{db_col} IS NOT NULL"]
        params = []

        if min_date:
            filters.append("bgl.game_date >= ?")
            params.append(min_date)
        if max_date:
            filters.append("bgl.game_date <= ?")
            params.append(max_date)
        if season:
            filters.append("bgl.season = ?")
            params.append(season)

        where = " AND ".join(filters)

        query = f"""
            SELECT
                bgl.player_id,
                bgl.game_id,
                bgl.game_date,
                bgl.season,
                bgl.team_id,
                bgl.opponent_id,
                bgl.is_home,
                bgl.opposing_pitcher_id,
                bgl.opposing_pitcher_hand,
                bgl.{db_col}             AS actual_value,
                bgl.venue_id,
                bs.bats,
                -- Rolling stats
                brs.*,
                -- Park factors
                pf_overall.factor_value  AS park_factor_overall,
                pf_hr.factor_value       AS park_factor_hr,
                pf_h.factor_value        AS park_factor_h,
                -- Weather
                gw.temp_f,
                gw.wind_speed,
                gw.wind_direction,
                gw.is_dome,
                -- Opposing pitcher stats
                ps.era               AS opp_pitcher_era,
                ps.whip              AS opp_pitcher_whip,
                ps.k_per_9           AS opp_pitcher_k_per_9,
                ps.throws            AS opp_pitcher_hand_season
            FROM batter_game_logs bgl
            JOIN batter_rolling_stats brs
                ON  brs.player_id = bgl.player_id
                AND brs.game_id   = bgl.game_id
            LEFT JOIN batter_stats bs
                ON  bs.player_id = bgl.player_id
            LEFT JOIN park_factors pf_overall
                ON  pf_overall.venue_id    = bgl.venue_id
                AND pf_overall.season      = bgl.season
                AND pf_overall.factor_type = 'overall'
            LEFT JOIN park_factors pf_hr
                ON  pf_hr.venue_id    = bgl.venue_id
                AND pf_hr.season      = bgl.season
                AND pf_hr.factor_type = 'hr'
            LEFT JOIN park_factors pf_h
                ON  pf_h.venue_id    = bgl.venue_id
                AND pf_h.season      = bgl.season
                AND pf_h.factor_type = 'h'
            LEFT JOIN game_weather gw
                ON  gw.game_id = bgl.game_id
            LEFT JOIN pitcher_stats ps
                ON  ps.player_id = bgl.opposing_pitcher_id
                AND ps.season    = bgl.season
            WHERE {where}
            ORDER BY bgl.game_date
        """
        return query, params

    def _pitcher_game_query(self, db_col, min_date, max_date, season):
        filters = [f"pgl.{db_col} IS NOT NULL", "pgl.is_start = 1"]
        params = []

        if min_date:
            filters.append("pgl.game_date >= ?")
            params.append(min_date)
        if max_date:
            filters.append("pgl.game_date <= ?")
            params.append(max_date)
        if season:
            filters.append("pgl.season = ?")
            params.append(season)

        where = " AND ".join(filters)

        query = f"""
            SELECT
                pgl.player_id,
                pgl.game_id,
                pgl.game_date,
                pgl.season,
                pgl.team_id,
                pgl.opponent_id,
                pgl.is_home,
                pgl.{db_col}             AS actual_value,
                pgl.venue_id,
                ps.throws               AS pitcher_hand,
                -- Rolling stats
                prs.*,
                -- Park factors
                pf_overall.factor_value  AS park_factor_overall,
                pf_hr.factor_value       AS park_factor_hr,
                pf_h.factor_value        AS park_factor_h,
                -- Weather
                gw.temp_f,
                gw.wind_speed,
                gw.wind_direction,
                gw.is_dome
            FROM pitcher_game_logs pgl
            JOIN pitcher_rolling_stats prs
                ON  prs.player_id = pgl.player_id
                AND prs.game_id   = pgl.game_id
            LEFT JOIN pitcher_stats ps
                ON  ps.player_id = pgl.player_id
            LEFT JOIN park_factors pf_overall
                ON  pf_overall.venue_id    = pgl.venue_id
                AND pf_overall.season      = pgl.season
                AND pf_overall.factor_type = 'overall'
            LEFT JOIN park_factors pf_hr
                ON  pf_hr.venue_id    = pgl.venue_id
                AND pf_hr.season      = pgl.season
                AND pf_hr.factor_type = 'hr'
            LEFT JOIN park_factors pf_h
                ON  pf_h.venue_id    = pgl.venue_id
                AND pf_h.season      = pgl.season
                AND pf_h.factor_type = 'h'
            LEFT JOIN game_weather gw
                ON  gw.game_id = pgl.game_id
            WHERE {where}
            ORDER BY pgl.game_date
        """
        return query, params

    def _query(self, sql: str, params: list = None) -> pd.DataFrame:
        conn = sqlite3.connect(self.db_path)
        try:
            df = pd.read_sql_query(sql, conn, params=params or [])
        finally:
            conn.close()
        return df.loc[:, ~df.columns.duplicated()]
