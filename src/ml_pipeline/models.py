"""ML models for prop prediction: XGBoost classifier."""

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class PropClassifier:
    """
    XGBoost binary classifier predicting hit_over (1 = outcome > line).

    Calibrated with isotonic regression on validation set to produce
    reliable probabilities for EV calculations.
    """

    def __init__(self, params: dict = None):
        import xgboost as xgb
        self._xgb = xgb

        self.params = {
            'objective': 'binary:logistic',
            'eval_metric': 'auc',
            'max_depth': 6,
            'learning_rate': 0.05,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 5,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'n_estimators': 500,
            'tree_method': 'hist',
            'verbosity': 0,
        }
        if params:
            self.params.update(params)

        self.model = None
        self.calibrator = None
        self.feature_names: List[str] = []

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame = None,
        y_val: pd.Series = None,
    ) -> 'PropClassifier':
        """
        Fit the classifier.

        Args:
            X_train: Training features
            y_train: Binary targets (1 = hit over, 0 = missed over)
            X_val: Validation features for early stopping
            y_val: Validation targets for early stopping

        Returns:
            self
        """
        self.feature_names = list(X_train.columns)

        xgb_params = {k: v for k, v in self.params.items() if k != 'n_estimators'}
        n_estimators = self.params.get('n_estimators', 500)

        dtrain = self._xgb.DMatrix(X_train, label=y_train, feature_names=self.feature_names)

        evals = [(dtrain, 'train')]
        callbacks = []

        if X_val is not None and y_val is not None:
            dval = self._xgb.DMatrix(X_val, label=y_val, feature_names=self.feature_names)
            evals.append((dval, 'val'))
            callbacks.append(self._xgb.callback.EarlyStopping(
                rounds=50, metric_name='auc', maximize=True, save_best=True
            ))

        self.model = self._xgb.train(
            xgb_params,
            dtrain,
            num_boost_round=n_estimators,
            evals=evals,
            callbacks=callbacks,
            verbose_eval=50,
        )

        logger.info(f"[classifier] Best iteration: {self.model.best_iteration}")
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return probability of hitting over (uncalibrated if no calibrator fitted)."""
        if self.model is None:
            raise RuntimeError("Model not fitted yet")
        dmat = self._xgb.DMatrix(X[self.feature_names], feature_names=self.feature_names)
        raw_proba = self.model.predict(dmat)

        if self.calibrator is not None:
            return self.calibrator.predict(raw_proba.reshape(-1, 1))

        return raw_proba

    def predict(self, X: pd.DataFrame, threshold: float = 0.5) -> np.ndarray:
        """Return binary predictions (1 = hit over)."""
        proba = self.predict_proba(X)
        return (proba >= threshold).astype(int)

    def calibrate(
        self,
        X_cal: pd.DataFrame,
        y_cal: pd.Series,
        method: str = 'isotonic',
    ) -> 'PropClassifier':
        """
        Fit a post-hoc calibrator on held-out calibration data.

        Args:
            X_cal: Calibration features (validation set, not training set)
            y_cal: True binary labels
            method: 'isotonic' (default) or 'sigmoid' (Platt scaling)

        Returns:
            self
        """
        from sklearn.isotonic import IsotonicRegression
        from sklearn.linear_model import LogisticRegression

        raw_proba = self._raw_predict(X_cal)

        if method == 'isotonic':
            self.calibrator = IsotonicRegression(out_of_bounds='clip')
            self.calibrator.fit(raw_proba, y_cal)
        elif method == 'sigmoid':
            self.calibrator = LogisticRegression()
            self.calibrator.fit(raw_proba.reshape(-1, 1), y_cal)
        else:
            raise ValueError(f"Unknown calibration method: {method}")

        # Measure calibration improvement
        cal_proba = self.calibrator.predict(raw_proba.reshape(-1, 1))
        brier_before = np.mean((raw_proba - y_cal) ** 2)
        brier_after = np.mean((cal_proba - y_cal) ** 2)
        logger.info(
            f"[classifier] Calibration ({method}): "
            f"Brier {brier_before:.4f} → {brier_after:.4f}"
        )

        return self

    def _raw_predict(self, X: pd.DataFrame) -> np.ndarray:
        """Return uncalibrated probabilities."""
        dmat = self._xgb.DMatrix(X[self.feature_names], feature_names=self.feature_names)
        return self.model.predict(dmat)

    def get_feature_importance(self, importance_type: str = 'gain') -> pd.Series:
        """Return feature importances sorted descending."""
        if self.model is None:
            raise RuntimeError("Model not fitted yet")
        scores = self.model.get_score(importance_type=importance_type)
        return pd.Series(scores).sort_values(ascending=False)

    def save(self, path: str, calibrator_path: str = None) -> None:
        """Save model (and optionally calibrator) to files."""
        import joblib
        if self.model is None:
            raise RuntimeError("Model not fitted yet")
        self.model.save_model(path)
        logger.info(f"[classifier] Model saved to {path}")

        if self.calibrator is not None and calibrator_path:
            joblib.dump(self.calibrator, calibrator_path)
            logger.info(f"[classifier] Calibrator saved to {calibrator_path}")

    @classmethod
    def load(cls, path: str, calibrator_path: str = None) -> 'PropClassifier':
        """Load model (and optionally calibrator) from files."""
        import joblib
        import xgboost as xgb

        obj = cls()
        obj.model = xgb.Booster()
        obj.model.load_model(path)
        obj.feature_names = obj.model.feature_names
        logger.info(f"[classifier] Model loaded from {path}")

        if calibrator_path:
            obj.calibrator = joblib.load(calibrator_path)
            logger.info(f"[classifier] Calibrator loaded from {calibrator_path}")

        return obj
