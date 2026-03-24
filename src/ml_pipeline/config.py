"""ML pipeline configuration: stat mappings, model params, feature settings."""

# ---------------------------------------------------------------------------
# Stat name mapping: prop stat name -> DB column name
# ---------------------------------------------------------------------------
STAT_COLUMNS = {
    # Batter props (use batter_game_logs)
    'hits': 'hits',
    'home_runs': 'home_runs',
    'rbis': 'rbi',
    'runs': 'runs',
    'stolen_bases': 'stolen_bases',
    'total_bases': 'total_bases',
    'walks': 'walks',
    'batter_strikeouts': 'strikeouts',
    # Pitcher props (use pitcher_game_logs)
    'pitcher_strikeouts': 'strikeouts',
    'outs_recorded': 'outs_recorded',
    'earned_runs_allowed': 'earned_runs',
    'hits_allowed': 'hits_allowed',
}

PRIORITY_STATS = [
    'pitcher_strikeouts',   # Most liquid MLB prop market
    'hits',
    'total_bases',
    'home_runs',
    'rbis',
    'outs_recorded',
    'earned_runs_allowed',
]

BATTER_STATS = {
    'hits', 'home_runs', 'rbis', 'runs',
    'stolen_bases', 'total_bases', 'walks', 'batter_strikeouts',
}

PITCHER_STATS = {
    'pitcher_strikeouts', 'outs_recorded',
    'earned_runs_allowed', 'hits_allowed',
}

# ---------------------------------------------------------------------------
# LightGBM regressor hyperparameters
# ---------------------------------------------------------------------------
REGRESSOR_PARAMS = {
    'objective': 'regression',
    'metric': 'mae',
    'num_leaves': 31,
    'learning_rate': 0.05,
    'n_estimators': 500,
    'min_child_samples': 20,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'reg_alpha': 0.1,
    'reg_lambda': 0.1,
    'random_state': 42,
    'verbose': -1,
}

# ---------------------------------------------------------------------------
# XGBoost classifier hyperparameters
# ---------------------------------------------------------------------------
CLASSIFIER_PARAMS = {
    'objective': 'binary:logistic',
    'eval_metric': 'auc',
    'max_depth': 6,
    'learning_rate': 0.05,
    'n_estimators': 500,
    'min_child_weight': 5,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'reg_alpha': 0.1,
    'reg_lambda': 1.0,
    'random_state': 42,
    'verbosity': 0,
}

# ---------------------------------------------------------------------------
# Training split defaults (more data than NBA → wider windows)
# ---------------------------------------------------------------------------
DEFAULT_VAL_DAYS = 5
DEFAULT_TEST_DAYS = 14

# Minimum samples required to train
MIN_TRAINING_SAMPLES = 100
MIN_TEST_SAMPLES = 20

# ---------------------------------------------------------------------------
# Model artifact naming
# ---------------------------------------------------------------------------
REGRESSOR_SUFFIX = '_regressor.joblib'
CLASSIFIER_SUFFIX = '_classifier.joblib'
