"""Model trainer: chronological train/val/test splits + fit + evaluate."""

import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from src.ml_pipeline.config import BATTER_STATS, CLASSIFIER_PARAMS
from src.ml_pipeline.data_loader import PropDataLoader
from src.ml_pipeline.evaluator import evaluate_classifier
from src.ml_pipeline.models import PropClassifier

logger = logging.getLogger(__name__)

# Stat type → which columns are the feature set
# (exclude identifiers, targets, and leakage columns)
_EXCLUDE_COLS = {
    'id', 'player_id', 'player_name', 'game_id', 'game_date',
    'team_id', 'opponent_team_id', 'opponent_id', 'opposing_pitcher_id', 'season',
    'target', 'hit_over', 'hit_under', 'actual_value', 'edge',
    # raw stat columns that could leak
    'hits', 'home_runs', 'rbis', 'runs', 'stolen_bases',
    'total_bases', 'walks', 'batter_strikeouts',
    'pitcher_strikeouts', 'outs_recorded', 'earned_runs_allowed', 'hits_allowed',
}


def _split_chronological(
    df: pd.DataFrame,
    train_frac: float = 0.70,
    val_frac: float = 0.15,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split df chronologically into train / val / test.

    Sorts by game_date, then splits by fraction so that:
      - train = earliest 70%
      - val   = next 15%
      - test  = last 15%
    """
    df = df.sort_values('game_date').reset_index(drop=True)
    n = len(df)
    n_train = int(n * train_frac)
    n_val = int(n * val_frac)

    train = df.iloc[:n_train]
    val = df.iloc[n_train: n_train + n_val]
    test = df.iloc[n_train + n_val:]

    logger.info(
        f"[trainer] Split: train={len(train)} ({train['game_date'].min()} – {train['game_date'].max()}), "
        f"val={len(val)}, test={len(test)}"
    )
    return train, val, test


def _feature_cols(df: pd.DataFrame) -> list:
    """Return feature columns (exclude identifiers, targets, non-numeric)."""
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    return [c for c in numeric if c not in _EXCLUDE_COLS]


class ModelTrainer:
    """
    Trains a PropClassifier for a given stat type.

    Workflow:
      1. Load prop outcome data
      2. Chronological 70/15/15 split
      3. Fit classifier with early stopping on val
      4. Calibrate classifier on val set
      5. Evaluate on test set
      6. Save model to disk
    """

    def __init__(self, db_path: str, models_dir: str = 'models'):
        self.db_path = db_path
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        self.loader = PropDataLoader(db_path)

    def train(
        self,
        stat_type: str,
        min_date: str = None,
        max_date: str = None,
        classifier_params: dict = None,
        calibration_method: str = 'isotonic',
        ablate_statcast: bool = False,
    ) -> Dict[str, dict]:
        """
        Full training pipeline for one stat type.

        Args:
            stat_type: e.g. 'hits', 'pitcher_strikeouts'
            min_date: ISO date string to filter game data (e.g. '2024-04-01')
            max_date: ISO date string (inclusive)
            classifier_params: Override default XGBoost params
            calibration_method: 'isotonic' or 'sigmoid'

        Returns:
            Dict with 'classifier' evaluation metrics
        """
        logger.info(f"[trainer] Training models for stat_type={stat_type}")
        results = {}

        logger.info("[trainer] Loading prop outcome data for classifier...")
        clf_df = self.loader.load_training_data(
            stat_type=stat_type,
            min_date=min_date,
            max_date=max_date,
        )
        if stat_type in BATTER_STATS and not clf_df.empty:
            clf_df = self._merge_arsenal_features(clf_df, ablate_statcast)

        if len(clf_df) < 100:
            logger.warning(
                f"[trainer] Only {len(clf_df)} prop outcome rows — "
                "skipping (need at least 100)"
            )
        else:
            results['classifier'] = self._train_classifier(
                stat_type, clf_df, classifier_params, calibration_method, ablate_statcast
            )

        return results

    def _merge_arsenal_features(
        self,
        df: pd.DataFrame,
        ablate: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch arsenal matchup stats per season and merge into df.

        Handles multi-season training data by fetching per season and concatenating.
        When ablate=True, all arsenal feature columns are set to 0.
        """
        from src.ml_pipeline.features import FeatureEngineer
        arsenal_cols = FeatureEngineer('hits').get_arsenal_matchup_features()

        if 'opposing_pitcher_id' not in df.columns or 'player_id' not in df.columns:
            for col in arsenal_cols:
                df[col] = 0.0
            return df

        if ablate:
            logger.warning("[trainer] ABLATION: statcast arsenal features zeroed out")
            for col in arsenal_cols:
                df[col] = 0.0
            return df

        if 'season' in df.columns:
            seasons = df['season'].dropna().unique().tolist()
        elif 'game_date' in df.columns:
            df = df.copy()
            df['season'] = pd.to_datetime(df['game_date']).dt.year.astype(str)
            seasons = df['season'].dropna().unique().tolist()
        else:
            seasons = []

        if not seasons:
            for col in arsenal_cols:
                df[col] = 0.0
            return df

        all_matchups = []
        for season in seasons:
            season_df = df[df['season'] == season]
            batter_ids = season_df['player_id'].dropna().unique().tolist()
            pitcher_ids = season_df['opposing_pitcher_id'].dropna().unique().tolist()
            if not batter_ids or not pitcher_ids:
                continue
            try:
                matchup = self.loader.get_arsenal_matchup_stats(
                    batter_ids, pitcher_ids, str(season)
                )
                if not matchup.empty:
                    matchup['season'] = season
                    all_matchups.append(matchup)
            except Exception as e:
                logger.warning(f"[trainer] Arsenal stats failed for season {season}: {e}")

        if not all_matchups:
            logger.info("[trainer] No arsenal matchup data found — features will be 0")
            for col in arsenal_cols:
                df[col] = 0.0
            return df

        combined = pd.concat(all_matchups, ignore_index=True)
        combined = combined.rename(columns={
            'batter_id': 'player_id',
            'pitcher_id': 'opposing_pitcher_id',
        })
        merge_keys = ['player_id', 'opposing_pitcher_id', 'season']
        df = df.merge(combined, on=merge_keys, how='left')

        # Fill missing with 0 (players with no Statcast data yet)
        for col in arsenal_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)
            else:
                df[col] = 0.0

        covered = df[arsenal_cols[0]].ne(0).sum()
        logger.info(
            f"[trainer] Arsenal features merged — {covered}/{len(df)} rows have statcast data"
        )
        return df

    def _train_classifier(
        self,
        stat_type: str,
        df: pd.DataFrame,
        params: dict = None,
        calibration_method: str = 'isotonic',
        ablate_statcast: bool = False,
    ) -> dict:
        """Train, calibrate, and evaluate PropClassifier. Returns test metrics."""
        train, val, test = _split_chronological(df)

        feature_cols = _feature_cols(train)
        logger.info(f"[trainer] Classifier features: {len(feature_cols)}")

        X_train, y_train = train[feature_cols], train['target']
        X_val, y_val = val[feature_cols], val['target']
        X_test, y_test = test[feature_cols], test['target']

        # Check class balance and set scale_pos_weight to handle imbalance
        pos_rate = y_train.mean()
        logger.info(f"[trainer] Classifier class balance: {pos_rate:.1%} over")
        if pos_rate > 0:
            base = params if params is not None else CLASSIFIER_PARAMS
            params = {**base, 'scale_pos_weight': (1 - pos_rate) / pos_rate}

        model = PropClassifier(params=params)
        model.fit(X_train, y_train, X_val, y_val)

        # Calibrate on validation set
        model.calibrate(X_val, y_val, method=calibration_method)

        # Evaluate on test set
        proba = model.predict_proba(X_test)
        metrics = evaluate_classifier(y_test.values, proba, stat_type=stat_type)
        logger.info(f"[trainer] Classifier test metrics: {metrics}")

        # Ablated models use a different filename to avoid overwriting production
        suffix = '_ablated' if ablate_statcast else ''
        model_path = self.models_dir / f"classifier_{stat_type}{suffix}.xgb"
        cal_path = self.models_dir / f"calibrator_{stat_type}{suffix}.pkl"
        model.save(str(model_path), str(cal_path))

        # Log top features
        top_features = model.get_feature_importance().head(10)
        logger.info(f"[trainer] Top classifier features:\n{top_features.to_string()}")

        return {**metrics, 'n_train': len(train), 'n_test': len(test)}

    def train_all(
        self,
        stat_types: list = None,
        min_date: str = None,
        max_date: str = None,
        ablate_statcast: bool = False,
    ) -> Dict[str, Dict[str, dict]]:
        """
        Train models for multiple stat types.

        Args:
            stat_types: List of stat types to train. Defaults to all supported.
            min_date: ISO date string lower bound
            max_date: ISO date string upper bound

        Returns:
            Nested dict: {stat_type: {model_type: metrics}}
        """
        if stat_types is None:
            stat_types = [
                'hits', 'home_runs', 'rbis', 'total_bases',
                'pitcher_strikeouts', 'outs_recorded',
            ]

        all_results = {}
        for stat_type in stat_types:
            logger.info(f"\n{'='*60}")
            logger.info(f"[trainer] Training {stat_type}")
            logger.info(f"{'='*60}")
            try:
                all_results[stat_type] = self.train(
                    stat_type=stat_type,
                    min_date=min_date,
                    max_date=max_date,
                    ablate_statcast=ablate_statcast,
                )
            except Exception as e:
                logger.error(f"[trainer] Failed for {stat_type}: {e}", exc_info=True)
                all_results[stat_type] = {'error': str(e)}

        return all_results
