"""Tests for FeatureEngineer."""

import numpy as np
import pandas as pd
import pytest

from src.ml_pipeline.features import FeatureEngineer


def _base_batter_df(**overrides):
    """Minimal batter DataFrame for feature engineering tests."""
    data = {
        'player_id': [660271],
        'game_id': [100001],
        'game_date': ['2025-06-15'],
        'line': [1.5],
        'source': ['underdog'],
        'is_home': [1],
        'venue_id': [3313],
        'bats': ['R'],
        'opposing_pitcher_hand': ['L'],
        'over_odds': [-115],
        'under_odds': [-105],
        'opponent_id': [111],
        # Rolling stats
        'l10_hits': [1.4], 'l10_hr': [0.3], 'l10_rbi': [1.1], 'l10_runs': [0.9],
        'l10_sb': [0.2], 'l10_tb': [2.1], 'l10_bb': [0.7], 'l10_so': [1.2],
        'l10_pa': [4.0], 'l10_ab': [3.5],
        'l20_hits': [1.2], 'l20_hr': [0.2], 'l20_rbi': [0.9], 'l20_runs': [0.8],
        'l20_sb': [0.1], 'l20_tb': [1.9],
        'l30_hits': [1.1], 'l30_hr': [0.2], 'l30_rbi': [0.8], 'l30_runs': [0.7],
        'l30_sb': [0.1], 'l30_tb': [1.8],
        'hits_trend': [0.2], 'hr_trend': [0.1], 'rbi_trend': [0.2],
        'tb_trend': [0.2], 'so_trend': [0.0],
        'l10_hits_vs_lhp': [1.6], 'l10_hits_vs_rhp': [1.3],
        'l10_tb_vs_lhp': [2.4], 'l10_tb_vs_rhp': [1.9],
        'l10_so_vs_lhp': [1.0], 'l10_so_vs_rhp': [1.3],
        # Pitcher matchup
        'opp_pitcher_era': [3.20], 'opp_pitcher_whip': [1.10],
        'opp_pitcher_k_per_9': [9.2], 'opp_pitcher_l5_er': [2.5],
        'park_factor_overall': [1.05], 'park_factor_hr': [1.10],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def _base_pitcher_df(**overrides):
    """Minimal pitcher DataFrame for feature engineering tests."""
    data = {
        'player_id': [543037],
        'game_id': [200001],
        'game_date': ['2025-06-15'],
        'line': [6.5],
        'source': ['prizepicks'],
        'is_home': [0],
        'venue_id': [3],
        'over_odds': [-110],
        'under_odds': [-110],
        # Rolling stats
        'l3_strikeouts': [7.3], 'l3_outs': [19.0], 'l3_er': [2.3],
        'l5_strikeouts': [7.0], 'l5_outs': [18.5], 'l5_er': [2.5],
        'l10_strikeouts': [6.8], 'l10_outs': [18.0], 'l10_er': [2.8],
        'k_trend': [0.3], 'outs_trend': [0.5], 'er_trend': [-0.2],
        'park_factor_overall': [0.95], 'park_factor_hr': [0.88],
    }
    data.update(overrides)
    return pd.DataFrame(data)


class TestLineFeatures:

    def test_line_vs_l10(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(line=[1.5], l10_hits=[1.4]))
        assert df['line_vs_l10'].iloc[0] == pytest.approx(0.1)

    def test_line_difficulty(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(line=[2.0], l10_hits=[1.4]))
        # (2.0 - 1.4) / 1.4 ≈ 0.4286
        assert df['line_difficulty'].iloc[0] == pytest.approx(0.6 / 1.4, rel=1e-4)

    def test_line_pct_of_l10(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(line=[1.5], l10_hits=[1.5]))
        assert df['line_pct_of_l10'].iloc[0] == pytest.approx(1.0)


class TestParkFeatures:

    def test_is_hitter_park_true(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(park_factor_overall=[1.10]))
        assert df['is_hitter_park'].iloc[0] == 1

    def test_is_hitter_park_false(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(park_factor_overall=[0.95]))
        assert df['is_hitter_park'].iloc[0] == 0

    def test_is_home_preserved(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(is_home=[1]))
        assert df['is_home'].iloc[0] == 1

    def test_park_factor_stat_for_hr(self):
        fe = FeatureEngineer('home_runs')
        df = fe.engineer_features(_base_batter_df(park_factor_hr=[1.20]))
        assert df['park_factor_stat'].iloc[0] == pytest.approx(1.20)


class TestPlatoonFeatures:

    def test_platoon_advantage_l_batter_vs_r_pitcher(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(bats=['L'], opposing_pitcher_hand=['R']))
        assert df['platoon_advantage'].iloc[0] == 1

    def test_platoon_advantage_r_batter_vs_r_pitcher(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(bats=['R'], opposing_pitcher_hand=['R']))
        assert df['platoon_advantage'].iloc[0] == 0

    def test_platoon_advantage_switch_hitter(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(bats=['S']))
        assert df['platoon_advantage'].iloc[0] == 1

    def test_l10_stat_vs_hand_for_lhp(self):
        """opp_pitcher_hand='L' → l10_stat_vs_hand should use l10_hits_vs_lhp."""
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(
            opposing_pitcher_hand=['L'],
            l10_hits_vs_lhp=[1.8], l10_hits_vs_rhp=[1.2],
        ))
        assert df['l10_stat_vs_hand'].iloc[0] == pytest.approx(1.8)

    def test_not_present_for_pitcher_stats(self):
        fe = FeatureEngineer('pitcher_strikeouts')
        df = fe.engineer_features(_base_pitcher_df())
        assert 'platoon_advantage' not in df.columns


class TestLineupFeatures:

    def test_is_leadoff(self):
        lineup = pd.DataFrame({'player_id': [660271], 'game_id': [100001], 'batting_order': [1]})
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(), lineup_data=lineup)
        assert df['is_leadoff'].iloc[0] == 1
        assert df['is_cleanup'].iloc[0] == 0

    def test_is_cleanup(self):
        lineup = pd.DataFrame({'player_id': [660271], 'game_id': [100001], 'batting_order': [4]})
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(), lineup_data=lineup)
        assert df['is_cleanup'].iloc[0] == 1

    def test_order_pa_factor_leadoff(self):
        lineup = pd.DataFrame({'player_id': [660271], 'game_id': [100001], 'batting_order': [1]})
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(), lineup_data=lineup)
        assert df['order_pa_factor'].iloc[0] == pytest.approx(1.10)

    def test_not_present_for_pitcher_stats(self):
        fe = FeatureEngineer('pitcher_strikeouts')
        df = fe.engineer_features(_base_pitcher_df())
        assert 'batting_order' not in df.columns


class TestTemporalFeatures:

    def test_day_of_week(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(game_date=['2025-06-15']))
        # 2025-06-15 is a Sunday → dayofweek=6
        assert df['day_of_week'].iloc[0] == 6

    def test_month(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(game_date=['2025-06-15']))
        assert df['month'].iloc[0] == 6

    def test_day_of_season_positive(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(game_date=['2025-06-15']))
        assert df['day_of_season'].iloc[0] > 0


class TestOddsFeatures:

    def test_fair_over_prob(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(over_odds=[-110], under_odds=[-110]))
        # Equal juice → fair prob ≈ 0.5
        assert df['fair_over_prob'].iloc[0] == pytest.approx(0.5, abs=0.01)

    def test_vig_positive(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(over_odds=[-115], under_odds=[-115]))
        assert df['vig'].iloc[0] > 0

    def test_sportsbook_flags(self):
        fe = FeatureEngineer('hits')
        df_ud = fe.engineer_features(_base_batter_df(source=['underdog']))
        assert df_ud['is_underdog'].iloc[0] == 1
        assert df_ud['is_prizepicks'].iloc[0] == 0

        df_pp = fe.engineer_features(_base_batter_df(source=['prizepicks']))
        assert df_pp['is_prizepicks'].iloc[0] == 1
        assert df_pp['is_underdog'].iloc[0] == 0


class TestInteractionFeatures:

    def test_home_park_factor(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(is_home=[1], park_factor_overall=[1.10]))
        assert df['home_park_factor'].iloc[0] == pytest.approx(1.10)

    def test_home_park_factor_away(self):
        fe = FeatureEngineer('hits')
        df = fe.engineer_features(_base_batter_df(is_home=[0], park_factor_overall=[1.10]))
        assert df['home_park_factor'].iloc[0] == pytest.approx(0.0)


class TestHandleMissing:

    def test_no_nan_after_handle_missing(self):
        fe = FeatureEngineer('hits')
        # Introduce NaN in rolling col
        df_in = _base_batter_df()
        df_in['l10_hits'] = float('nan')
        df = fe.engineer_features(df_in)
        numeric_cols = df.select_dtypes(include=[float, int]).columns
        assert not df[numeric_cols].isnull().any().any()

    def test_park_factor_default_to_1(self):
        fe = FeatureEngineer('hits')
        df_in = _base_batter_df()
        df_in['park_factor_overall'] = float('nan')
        df = fe.engineer_features(df_in)
        assert df['park_factor_overall'].iloc[0] == pytest.approx(1.0)
