"""Feature engineering for MLB prop prediction."""

from typing import List

import numpy as np
import pandas as pd

from src.ml_pipeline.config import BATTER_STATS, PITCHER_STATS


# Expected PA multipliers by batting order position (based on ~4.3 PA/game avg)
BATTING_ORDER_PA_FACTOR = {
    1: 1.10, 2: 1.08, 3: 1.06, 4: 1.04, 5: 1.02,
    6: 0.98, 7: 0.96, 8: 0.94, 9: 0.92,
}


class FeatureEngineer:
    """
    Builds the full feature matrix for a given stat type.

    Call `engineer_features(df)` to add all feature columns to a DataFrame
    that already contains rolling stats, game context, and prop line data.
    """

    def __init__(self, stat_type: str, db_path: str = None):
        self.stat_type = stat_type
        self.db_path = db_path
        self.is_pitcher_stat = stat_type in PITCHER_STATS

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def engineer_features(
        self,
        df: pd.DataFrame,
        matchup_stats: pd.DataFrame = None,
        park_factors: pd.DataFrame = None,
        lineup_data: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """
        Apply all feature engineering steps in order.

        Args:
            df: Base DataFrame with rolling stats and game context columns
            matchup_stats: Optional per-player vs team history (player_id, team_id, stat cols)
            park_factors: Optional venue park factors (venue_id, factor_type, factor_value)
            lineup_data: Optional batting order data (player_id, game_id, batting_order)

        Returns:
            DataFrame with all engineered features added
        """
        df = df.copy()
        df = self._add_line_features(df)
        df = self._add_park_features(df, park_factors)
        if not self.is_pitcher_stat:
            df = self._add_pitcher_matchup_features(df)
            df = self._add_platoon_features(df)
            df = self._add_lineup_features(df, lineup_data)
        df = self._add_temporal_features(df)
        df = self._add_interaction_features(df)
        df = self._add_sportsbook_features(df)
        df = self._add_odds_features(df)
        if matchup_stats is not None:
            df = self._add_matchup_features(df, matchup_stats)
        df = self._handle_missing(df)
        return df

    # ------------------------------------------------------------------
    # Feature list getters
    # ------------------------------------------------------------------

    def get_regressor_features(self) -> List[str]:
        """Features for regressor (no line/odds features — predict raw value)."""
        return self.get_park_features() + self.get_temporal_features() + \
               self.get_pitcher_matchup_features() + self.get_platoon_features() + \
               self.get_lineup_features() + self.get_matchup_features()

    def get_classifier_features(self) -> List[str]:
        """Features for classifier (includes line and odds features)."""
        return self.get_regressor_features() + self.get_line_features() + \
               self.get_sportsbook_features() + self.get_odds_features() + \
               self.get_interaction_features()

    def get_line_features(self) -> List[str]:
        return ['line', 'line_vs_l10', 'line_vs_l20', 'line_vs_l30',
                'line_difficulty', 'line_pct_of_l10']

    def get_park_features(self) -> List[str]:
        return ['park_factor_overall', 'park_factor_hr', 'park_factor_stat',
                'is_hitter_park', 'is_home']

    def get_pitcher_matchup_features(self) -> List[str]:
        if self.is_pitcher_stat:
            return []
        return ['opp_pitcher_era', 'opp_pitcher_whip', 'opp_pitcher_k_per_9',
                'opp_pitcher_hand', 'opp_pitcher_l5_er']

    def get_platoon_features(self) -> List[str]:
        if self.is_pitcher_stat:
            return []
        return ['platoon_advantage', 'l10_stat_vs_hand', 'platoon_diff']

    def get_lineup_features(self) -> List[str]:
        if self.is_pitcher_stat:
            return []
        return ['batting_order', 'is_leadoff', 'is_cleanup', 'order_pa_factor']

    def get_temporal_features(self) -> List[str]:
        return ['day_of_week', 'month', 'day_of_season']

    def get_interaction_features(self) -> List[str]:
        return ['home_park_factor', 'trend_vs_line', 'park_adjusted_l10']

    def get_sportsbook_features(self) -> List[str]:
        return ['is_underdog', 'is_prizepicks', 'is_odds_api']

    def get_odds_features(self) -> List[str]:
        return ['over_prob', 'under_prob', 'vig', 'fair_over_prob']

    def get_matchup_features(self) -> List[str]:
        if self.is_pitcher_stat:
            return []
        return ['career_vs_team_stat', 'recent_vs_team_stat']

    # ------------------------------------------------------------------
    # Private feature methods
    # ------------------------------------------------------------------

    def _add_line_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Features comparing prop line to rolling averages."""
        l10 = df.get('l10_' + self._rolling_col(), pd.Series(np.nan, index=df.index))
        l20 = df.get('l20_' + self._rolling_col(), pd.Series(np.nan, index=df.index))
        l30 = df.get('l30_' + self._rolling_col(), pd.Series(np.nan, index=df.index))
        line = df.get('line', pd.Series(np.nan, index=df.index))

        df['line'] = line
        df['line_vs_l10'] = line - l10
        df['line_vs_l20'] = line - l20
        df['line_vs_l30'] = line - l30
        df['line_difficulty'] = (line - l10) / (l10.replace(0, np.nan))
        df['line_pct_of_l10'] = line / l10.replace(0, np.nan)
        return df

    def _add_park_features(self, df: pd.DataFrame, park_factors: pd.DataFrame = None) -> pd.DataFrame:
        """Add park factor columns."""
        if park_factors is not None and 'venue_id' in df.columns:
            # Pivot park_factors to wide format and merge on venue_id
            pf_wide = park_factors.pivot_table(
                index='venue_id', columns='factor_type', values='factor_value'
            ).reset_index()
            pf_wide.columns.name = None
            pf_wide = pf_wide.rename(columns={
                'overall': 'park_factor_overall',
                'hr': 'park_factor_hr',
            })
            df = df.merge(pf_wide[['venue_id', 'park_factor_overall', 'park_factor_hr']],
                          on='venue_id', how='left')
        else:
            df['park_factor_overall'] = df.get('park_factor_overall', 1.0)
            df['park_factor_hr'] = df.get('park_factor_hr', 1.0)

        stat_factor_map = {
            'hits': 'park_factor_h', 'total_bases': 'park_factor_overall',
            'home_runs': 'park_factor_hr', 'rbis': 'park_factor_overall',
            'runs': 'park_factor_overall', 'stolen_bases': 'park_factor_overall',
            'walks': 'park_factor_overall', 'batter_strikeouts': 'park_factor_overall',
            'pitcher_strikeouts': 'park_factor_overall', 'outs_recorded': 'park_factor_overall',
            'earned_runs_allowed': 'park_factor_overall', 'hits_allowed': 'park_factor_h',
        }
        src_col = stat_factor_map.get(self.stat_type, 'park_factor_overall')
        df['park_factor_stat'] = df.get(src_col, df.get('park_factor_overall', 1.0))
        df['is_hitter_park'] = (df['park_factor_overall'] > 1.05).astype(int)

        if 'is_home' not in df.columns:
            df['is_home'] = 0
        return df

    def _add_pitcher_matchup_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add opposing pitcher season stats (for batter props)."""
        for col in ['opp_pitcher_era', 'opp_pitcher_whip', 'opp_pitcher_k_per_9',
                    'opp_pitcher_l5_er']:
            if col not in df.columns:
                df[col] = np.nan

        # opp_pitcher_hand: encode as 0=R, 1=L, 0.5=unknown
        if 'opposing_pitcher_hand' in df.columns:
            df['opp_pitcher_hand'] = df['opposing_pitcher_hand'].map(
                {'R': 0, 'L': 1}
            ).fillna(0.5)
        elif 'opp_pitcher_hand' not in df.columns:
            df['opp_pitcher_hand'] = 0.5
        return df

    def _add_platoon_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add handedness advantage and platoon split features."""
        bats = df.get('bats', pd.Series('R', index=df.index))
        hand = df.get('opp_pitcher_hand', pd.Series(0.5, index=df.index))

        # platoon_advantage: 1 if batter has platoon edge, 0 otherwise
        # L vs R = advantage, R vs L = advantage, Switch = always advantage
        def _platoon_adv(row):
            b = row['bats'] if 'bats' in row.index else 'R'
            # opp_pitcher_hand is 0=R, 1=L, 0.5=unknown
            h = row.get('opp_pitcher_hand', 0.5)
            if b == 'S':
                return 1
            if b == 'L' and h == 0:    # L batter vs R pitcher
                return 1
            if b == 'R' and h == 1:    # R batter vs L pitcher
                return 1
            return 0

        df['platoon_advantage'] = df.apply(_platoon_adv, axis=1)

        rc = self._rolling_col()
        vs_lhp = 'l10_' + rc + '_vs_lhp'
        vs_rhp = 'l10_' + rc + '_vs_rhp'

        # Map specific rolling platoon columns by stat type
        platoon_col_map = {
            'hits': ('l10_hits_vs_lhp', 'l10_hits_vs_rhp'),
            'total_bases': ('l10_tb_vs_lhp', 'l10_tb_vs_rhp'),
            'batter_strikeouts': ('l10_so_vs_lhp', 'l10_so_vs_rhp'),
        }
        lhp_col, rhp_col = platoon_col_map.get(self.stat_type, (None, None))

        if lhp_col and lhp_col in df.columns:
            df['l10_stat_vs_lhp'] = df[lhp_col]
            df['l10_stat_vs_rhp'] = df[rhp_col]
            # Pick the stat vs current opponent hand
            df['l10_stat_vs_hand'] = np.where(
                df.get('opp_pitcher_hand', 0.5) == 1,
                df['l10_stat_vs_lhp'],
                df['l10_stat_vs_rhp'],
            )
            df['platoon_diff'] = df['l10_stat_vs_rhp'] - df['l10_stat_vs_lhp']
        else:
            df['l10_stat_vs_hand'] = df.get('l10_' + rc, np.nan)
            df['platoon_diff'] = 0.0

        return df

    def _add_lineup_features(self, df: pd.DataFrame, lineup_data: pd.DataFrame = None) -> pd.DataFrame:
        """Add batting order position features."""
        if lineup_data is not None and 'game_id' in df.columns:
            df = df.merge(
                lineup_data[['player_id', 'game_id', 'batting_order']],
                on=['player_id', 'game_id'], how='left'
            )

        if 'batting_order' not in df.columns:
            df['batting_order'] = np.nan

        df['is_leadoff'] = (df['batting_order'] == 1).astype(int)
        df['is_cleanup'] = df['batting_order'].between(3, 5).astype(int)
        df['order_pa_factor'] = df['batting_order'].map(BATTING_ORDER_PA_FACTOR).fillna(1.0)
        return df

    def _add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add day of week, month, and season day features."""
        if 'game_date' in df.columns:
            dates = pd.to_datetime(df['game_date'], errors='coerce')
            df['day_of_week'] = dates.dt.dayofweek
            df['month'] = dates.dt.month
            # Approximate day of season (season starts ~April 1)
            season_start = dates.dt.year.map(lambda y: pd.Timestamp(f'{y}-04-01'))
            df['day_of_season'] = (dates - season_start).dt.days.clip(lower=0)
        else:
            df['day_of_week'] = 0
            df['month'] = 4
            df['day_of_season'] = 0
        return df

    def _add_interaction_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add interaction terms between features."""
        is_home = df.get('is_home', pd.Series(0, index=df.index))
        pf = df.get('park_factor_overall', pd.Series(1.0, index=df.index))
        df['home_park_factor'] = is_home * pf

        l10_col = 'l10_' + self._rolling_col()
        l10 = df.get(l10_col, pd.Series(np.nan, index=df.index))
        trend_col = self._trend_col()
        trend = df.get(trend_col, pd.Series(0.0, index=df.index))
        line = df.get('line', pd.Series(np.nan, index=df.index))

        df['trend_vs_line'] = trend / line.replace(0, np.nan)
        df['park_adjusted_l10'] = l10 * pf
        return df

    def _add_sportsbook_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """One-hot encode the prop source (sportsbook)."""
        source = df.get('source', pd.Series('', index=df.index))
        df['is_underdog'] = (source == 'underdog').astype(int)
        df['is_prizepicks'] = (source == 'prizepicks').astype(int)
        df['is_odds_api'] = (source == 'odds_api').astype(int)
        return df

    def _add_odds_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Derive fair probability and vig from American odds."""
        over_odds = df.get('over_odds', pd.Series(np.nan, index=df.index))
        under_odds = df.get('under_odds', pd.Series(np.nan, index=df.index))

        def american_to_implied(odds):
            pos = odds > 0
            neg = odds <= -100
            prob = pd.Series(np.nan, index=odds.index)
            prob[pos] = 100 / (odds[pos] + 100)
            prob[neg] = (-odds[neg]) / (-odds[neg] + 100)
            return prob

        over_imp = american_to_implied(over_odds.fillna(-110))
        under_imp = american_to_implied(under_odds.fillna(-110))
        total_implied = over_imp + under_imp

        df['over_prob'] = over_imp
        df['under_prob'] = under_imp
        df['vig'] = (total_implied - 1.0).clip(lower=0)
        df['fair_over_prob'] = over_imp / total_implied.replace(0, np.nan)
        return df

    def _add_matchup_features(self, df: pd.DataFrame, matchup_stats: pd.DataFrame) -> pd.DataFrame:
        """Add batter/pitcher vs opponent team history."""
        rc = self._rolling_col()
        if matchup_stats is not None and not matchup_stats.empty:
            # Expects matchup_stats to have (player_id, opponent_id, career_stat, recent_stat)
            merge_cols = ['player_id', 'opponent_id']
            available = [c for c in merge_cols if c in df.columns and c in matchup_stats.columns]
            if available:
                matchup_sub = matchup_stats[available + ['career_vs_team_stat', 'recent_vs_team_stat']].copy()
                df = df.merge(matchup_sub, on=available, how='left')
        if 'career_vs_team_stat' not in df.columns:
            df['career_vs_team_stat'] = np.nan
        if 'recent_vs_team_stat' not in df.columns:
            df['recent_vs_team_stat'] = np.nan
        return df

    def _handle_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill remaining NaN values with safe defaults.
        Rolling stats: fill with column median (or 0 if no data).
        Binary/categorical: fill with 0 or neutral value.
        """
        rolling_prefix = ('l3_', 'l5_', 'l10_', 'l20_', 'l30_')
        for col in df.columns:
            if df[col].isna().any():
                if col.startswith(rolling_prefix) or col.endswith(('_std', '_trend')):
                    median = df[col].median()
                    df[col] = df[col].fillna(0.0 if pd.isna(median) else median)
                elif col in ('park_factor_overall', 'park_factor_hr', 'park_factor_stat',
                             'park_adjusted_l10'):
                    df[col] = df[col].fillna(1.0)
                elif col in ('opp_pitcher_hand',):
                    df[col] = df[col].fillna(0.5)
                elif col in ('batting_order', 'order_pa_factor'):
                    df[col] = df[col].fillna(5.0 if col == 'batting_order' else 1.0)
                else:
                    df[col] = df[col].fillna(0.0)
        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _rolling_col(self) -> str:
        """Map stat_type to the corresponding rolling stats column suffix."""
        col_map = {
            'hits': 'hits', 'home_runs': 'hr', 'rbis': 'rbi',
            'runs': 'runs', 'stolen_bases': 'sb', 'total_bases': 'tb',
            'walks': 'bb', 'batter_strikeouts': 'so',
            'pitcher_strikeouts': 'strikeouts', 'outs_recorded': 'outs',
            'earned_runs_allowed': 'er', 'hits_allowed': 'hits_allowed',
        }
        return col_map.get(self.stat_type, self.stat_type)

    def _trend_col(self) -> str:
        """Map stat_type to the trend column name."""
        trend_map = {
            'hits': 'hits_trend', 'home_runs': 'hr_trend', 'rbis': 'rbi_trend',
            'total_bases': 'tb_trend', 'batter_strikeouts': 'so_trend',
            'pitcher_strikeouts': 'k_trend', 'outs_recorded': 'outs_trend',
            'earned_runs_allowed': 'er_trend',
        }
        return trend_map.get(self.stat_type, 'hits_trend')
