"""Tests for arsenal pitch matchup feature engineering."""

import numpy as np
import pandas as pd
import pytest

from src.ml_pipeline.features import FeatureEngineer


def _base_batter_df(**overrides):
    """Minimal batter DataFrame with required columns for arsenal feature tests."""
    data = {
        'player_id': [660271, 673357],
        'game_id': [100001, 100002],
        'game_date': ['2025-06-15', '2025-06-15'],
        'opposing_pitcher_id': [592662, 592662],
        'season': ['2025', '2025'],
        'line': [1.5, 2.5],
        'source': ['underdog', 'underdog'],
        'is_home': [1, 0],
        'venue_id': [3313, 3313],
        'bats': ['R', 'L'],
        'opposing_pitcher_hand': ['R', 'R'],
        'over_odds': [-115, -110],
        'under_odds': [-105, -110],
        'opponent_id': [111, 111],
        'l10_hits': [1.4, 0.9], 'l10_hr': [0.3, 0.1], 'l10_rbi': [1.1, 0.7],
        'l10_runs': [0.9, 0.6], 'l10_sb': [0.2, 0.0], 'l10_tb': [2.1, 1.4],
        'l10_bb': [0.7, 0.5], 'l10_so': [1.2, 1.8], 'l10_pa': [4.0, 4.0], 'l10_ab': [3.5, 3.5],
        'l20_hits': [1.2, 0.8], 'l20_hr': [0.2, 0.1], 'l20_rbi': [0.9, 0.6],
        'l20_runs': [0.8, 0.5], 'l20_sb': [0.1, 0.0], 'l20_tb': [1.9, 1.3],
        'l30_hits': [1.1, 0.8], 'l30_hr': [0.2, 0.1], 'l30_rbi': [0.8, 0.6],
        'l30_runs': [0.7, 0.5], 'l30_sb': [0.1, 0.0], 'l30_tb': [1.8, 1.2],
        'hits_trend': [0.2, 0.1], 'hr_trend': [0.1, 0.0], 'rbi_trend': [0.2, 0.1],
        'tb_trend': [0.2, 0.1], 'so_trend': [0.0, 0.0],
        'l10_hits_vs_lhp': [1.6, 1.0], 'l10_hits_vs_rhp': [1.3, 0.8],
        'l10_tb_vs_lhp': [2.4, 1.5], 'l10_tb_vs_rhp': [1.9, 1.2],
        'l10_so_vs_lhp': [1.0, 2.0], 'l10_so_vs_rhp': [1.3, 1.6],
        'opp_pitcher_era': [3.20, 3.20], 'opp_pitcher_whip': [1.10, 1.10],
        'opp_pitcher_k_per_9': [9.2, 9.2], 'opp_pitcher_l5_er': [2.5, 2.5],
        'park_factor_overall': [1.05, 1.05], 'park_factor_hr': [1.10, 1.10],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def _make_arsenal_matchup(batter_id, pitcher_id):
    """Minimal arsenal matchup row with all 12 feature columns."""
    return pd.DataFrame([{
        'batter_id': batter_id,
        'pitcher_id': pitcher_id,
        'arsenal_weighted_ba': 0.280,
        'arsenal_weighted_whiff': 0.22,
        'arsenal_weighted_xba': 0.265,
        'arsenal_weighted_xwoba': 0.330,
        'arsenal_weighted_swstr': 0.11,
        'pitcher_avg_velocity': 93.5,
        'pitcher_arm_angle': 42.0,
        'pitcher_primary_movement_h': 8.5,
        'pitcher_primary_movement_v': 12.0,
        'batter_whiff_vs_primary_pitch': 0.25,
        'batter_xwoba_vs_primary_pitch': 0.310,
        'arsenal_sample_size': 150,
    }])


class TestArsenalMatchupFeatureGetter:
    def test_returns_12_features_for_batter_stat(self):
        fe = FeatureEngineer('hits')
        features = fe.get_arsenal_matchup_features()
        assert len(features) == 12
        assert 'arsenal_weighted_ba' in features
        assert 'pitcher_avg_velocity' in features
        assert 'arsenal_sample_size' in features

    def test_returns_empty_for_pitcher_stat(self):
        fe = FeatureEngineer('pitcher_strikeouts')
        assert fe.get_arsenal_matchup_features() == []

    def test_pitcher_stat_types_return_empty(self):
        for stat in ('pitcher_strikeouts', 'outs_recorded', 'earned_runs_allowed', 'hits_allowed'):
            fe = FeatureEngineer(stat)
            assert fe.get_arsenal_matchup_features() == []


class TestArsenalMatchupMerge:
    def test_features_are_nan_when_no_matchup_data(self):
        fe = FeatureEngineer('hits')
        df = _base_batter_df()
        result = fe.engineer_features(df, arsenal_matchup=None)

        # After _handle_missing(), NaN should be filled with 0.0
        assert 'arsenal_weighted_ba' in result.columns
        assert result['arsenal_weighted_ba'].notna().all()

    def test_features_merged_from_matchup_data(self):
        fe = FeatureEngineer('hits')
        df = _base_batter_df()
        matchup = _make_arsenal_matchup(batter_id=660271, pitcher_id=592662)

        result = fe.engineer_features(df, arsenal_matchup=matchup)

        # Row for batter 660271 should have actual values
        row = result[result['player_id'] == 660271].iloc[0]
        assert abs(row['arsenal_weighted_ba'] - 0.280) < 1e-6
        assert abs(row['pitcher_avg_velocity'] - 93.5) < 1e-6
        assert abs(row['arsenal_sample_size'] - 150) < 1e-6

    def test_missing_batter_gets_zero_fill(self):
        fe = FeatureEngineer('hits')
        df = _base_batter_df()
        # Only provide matchup for one batter (660271), not the other (673357)
        matchup = _make_arsenal_matchup(batter_id=660271, pitcher_id=592662)

        result = fe.engineer_features(df, arsenal_matchup=matchup)

        # batter 673357 has no matchup row → should be 0 after fillna
        row_missing = result[result['player_id'] == 673357].iloc[0]
        assert row_missing['arsenal_weighted_ba'] == 0.0

    def test_pitcher_stat_skips_arsenal_features(self):
        fe = FeatureEngineer('pitcher_strikeouts')
        # Pitcher df doesn't need arsenal columns
        pitcher_df = pd.DataFrame({
            'player_id': [543037],
            'game_id': [200001],
            'game_date': ['2025-06-15'],
            'line': [6.5],
            'source': ['prizepicks'],
            'is_home': [0],
            'venue_id': [3],
            'over_odds': [-110],
            'under_odds': [-110],
            'l3_strikeouts': [7.3], 'l3_outs': [19.0], 'l3_er': [2.3],
            'l3_hits_allowed': [5.0], 'l3_walks': [2.0], 'l3_pitches': [85.0],
            'l5_strikeouts': [6.8], 'l5_outs': [18.5], 'l5_er': [2.5],
            'l5_hits_allowed': [5.5], 'l5_walks': [2.2], 'l5_pitches': [88.0],
            'l10_strikeouts': [6.5], 'l10_outs': [18.0], 'l10_er': [2.8],
            'l10_hits_allowed': [6.0], 'l10_walks': [2.5], 'l10_pitches': [90.0],
            'l5_k_std': [1.2], 'l5_outs_std': [2.1], 'l5_er_std': [0.8],
            'k_trend': [0.5], 'outs_trend': [0.5], 'er_trend': [-0.3],
        })
        result = fe.engineer_features(pitcher_df, arsenal_matchup=None)
        # No arsenal columns should appear for pitcher stats
        for col in FeatureEngineer('hits').get_arsenal_matchup_features():
            assert col not in result.columns


class TestArsenalInRegressorFeatureList:
    def test_arsenal_features_in_regressor_list(self):
        fe = FeatureEngineer('hits')
        reg_features = fe.get_regressor_features()
        assert 'arsenal_weighted_ba' in reg_features
        assert 'arsenal_weighted_whiff' in reg_features

    def test_arsenal_features_in_classifier_list(self):
        fe = FeatureEngineer('total_bases')
        clf_features = fe.get_classifier_features()
        assert 'arsenal_weighted_xwoba' in clf_features

    def test_arsenal_not_in_pitcher_regressor_list(self):
        fe = FeatureEngineer('pitcher_strikeouts')
        reg_features = fe.get_regressor_features()
        assert 'arsenal_weighted_ba' not in reg_features
