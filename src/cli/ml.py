"""Machine learning pipeline commands."""

import click

from src.config import CURRENT_SEASON


@click.group()
@click.pass_context
def ml(ctx):
    """Machine learning training and predictions."""
    pass


@ml.command('train')
@click.option(
    '--stat', 'stat_type', default=None,
    help='Stat type to train (e.g. hits, pitcher_strikeouts). Omit to train all.'
)
@click.option('--min-date', default=None, help='Min game date (YYYY-MM-DD)')
@click.option('--max-date', default=None, help='Max game date (YYYY-MM-DD)')
@click.option(
    '--models-dir', default='models',
    help='Directory to save trained models (default: models/)'
)
@click.option(
    '--calibration', default='isotonic',
    type=click.Choice(['isotonic', 'sigmoid']),
    help='Calibration method for classifier (default: isotonic)'
)
@click.option(
    '--ablate-statcast', is_flag=True, default=False,
    help='Zero out Statcast arsenal features for ablation comparison (saves model with _ablated suffix)'
)
@click.pass_context
def train(ctx, stat_type, min_date, max_date, models_dir, calibration, ablate_statcast):
    """Train classifier models for prop prediction."""
    from src.ml_pipeline.trainer import ModelTrainer
    from src.ml_pipeline.evaluator import print_evaluation_report

    db_path = ctx.obj['db']

    trainer = ModelTrainer(db_path=db_path, models_dir=models_dir)

    if stat_type:
        click.echo(f"Training models for stat_type={stat_type}...")
        results = {
            stat_type: trainer.train(
                stat_type=stat_type,
                min_date=min_date,
                max_date=max_date,
                calibration_method=calibration,
                ablate_statcast=ablate_statcast,
            )
        }
    else:
        click.echo("Training models for all stat types...")
        results = trainer.train_all(
            min_date=min_date,
            max_date=max_date,
            ablate_statcast=ablate_statcast,
        )

    print_evaluation_report(results)
    click.echo(click.style(f"Models saved to {models_dir}/", fg='green'))


@ml.command('outcomes')
@click.option('--date', 'game_date', default=None, help='Process a single date (YYYY-MM-DD). Defaults to yesterday.')
@click.option('--start', default=None, help='Start date for range processing (YYYY-MM-DD)')
@click.option('--end', default=None, help='End date for range processing (YYYY-MM-DD)')
@click.pass_context
def outcomes(ctx, game_date, start, end):
    """Match scraped prop lines with actual game results to build training labels."""
    from datetime import date, timedelta
    from src.ml_pipeline.outcome_tracker import OutcomeTracker

    db_path = ctx.obj['db']
    tracker = OutcomeTracker(db_path)

    if start and end:
        click.echo(f"Processing outcomes from {start} to {end}...")
        count = tracker.process_range(start, end)
        click.echo(click.style(f"Recorded {count} outcomes!", fg='green'))
    else:
        target = game_date or str(date.today() - timedelta(days=1))
        click.echo(f"Processing outcomes for {target}...")
        count = tracker.process_date(target)
        click.echo(click.style(f"Recorded {count} outcomes!", fg='green'))


@ml.command('predict')
@click.option(
    '--stat', 'stat_type', required=True,
    help='Stat type to generate predictions for (e.g. hits, pitcher_strikeouts)'
)
@click.option(
    '--models-dir', default='models',
    help='Directory containing trained models (default: models/)'
)
@click.option(
    '--min-edge', default=0.03, type=float,
    help='Minimum edge (predicted prob - implied prob) to flag as a bet (default: 0.03)'
)
@click.option('--output', default=None, help='CSV path to save predictions')
@click.pass_context
def predict(ctx, stat_type, models_dir, min_edge, output):
    """Generate predictions for today's props."""
    import os
    import pandas as pd
    from pathlib import Path

    from src.ml_pipeline.data_loader import PropDataLoader
    from src.ml_pipeline.models import PropClassifier

    db_path = ctx.obj['db']
    models_path = Path(models_dir)

    # Load upcoming props
    loader = PropDataLoader(db_path)
    click.echo(f"Loading upcoming props for {stat_type}...")
    df = loader.load_upcoming_props(stat_type)

    if df.empty:
        click.echo(click.style("No upcoming props found.", fg='yellow'))
        return

    click.echo(f"Found {len(df)} props")

    # Load classifier
    clf_path = models_path / f"classifier_{stat_type}.xgb"
    if not clf_path.exists():
        click.echo(click.style(f"Classifier model not found at {clf_path}", fg='red'))
        click.echo("Run: ./mlb ml train --stat " + stat_type)
        return

    cal_path = models_path / f"calibrator_{stat_type}.pkl"
    classifier = PropClassifier.load(
        str(clf_path),
        str(cal_path) if cal_path.exists() else None
    )
    clf_features = [c for c in classifier.feature_names if c in df.columns]
    X_clf = df[clf_features].fillna(0)
    df['p_over'] = classifier.predict_proba(X_clf)
    df['p_under'] = 1 - df['p_over']

    # Implied probability from American odds
    def american_to_implied(odds):
        if odds is None:
            return 0.5238  # -110 default
        if odds < 0:
            return abs(odds) / (abs(odds) + 100)
        return 100 / (odds + 100)

    if 'over_odds' in df.columns:
        df['implied_over'] = df['over_odds'].apply(american_to_implied)
        df['implied_under'] = df['under_odds'].apply(american_to_implied)
        df['edge_over'] = df['p_over'] - df['implied_over']
        df['edge_under'] = df['p_under'] - df['implied_under']
        df['bet_signal'] = df.apply(
            lambda r: 'OVER' if r['edge_over'] >= min_edge
            else ('UNDER' if r['edge_under'] >= min_edge else ''),
            axis=1
        )
    else:
        df['bet_signal'] = df.apply(
            lambda r: 'OVER' if r['p_over'] >= 0.55 else ('UNDER' if r['p_over'] <= 0.45 else ''),
            axis=1
        )

    # Display results
    display_cols = ['player_name', 'line', 'p_over', 'p_under', 'bet_signal']
    if 'sportsbook' in df.columns:
        display_cols.append('sportsbook')
    if 'game_date' in df.columns:
        display_cols.append('game_date')

    display_cols = [c for c in display_cols if c in df.columns]
    result_df = df[display_cols].copy()
    result_df = result_df.sort_values('p_over', ascending=False)

    click.echo(f"\nPredictions for {stat_type}:")
    click.echo(result_df.to_string(index=False))

    bets = df[df['bet_signal'].str.len() > 0] if 'bet_signal' in df.columns else pd.DataFrame()
    if len(bets) > 0:
        click.echo(click.style(f"\n{len(bets)} bet signals (edge >= {min_edge:.0%})", fg='green'))
    else:
        click.echo(click.style(f"\nNo strong bet signals (edge >= {min_edge:.0%})", fg='yellow'))

    if output:
        df.to_csv(output, index=False)
        click.echo(f"Predictions saved to {output}")


@ml.command('shap')
@click.option('--stat', 'stat_type', required=True, help='Stat type (e.g. hits, home_runs)')
@click.option('--models-dir', default='models', help='Directory containing trained models')
@click.option('--top', default=20, help='Number of top features to display (default: 20)')
@click.pass_context
def shap_cmd(ctx, stat_type, models_dir, top):
    """SHAP feature importance analysis for a trained classifier."""
    import shap
    import numpy as np
    import pandas as pd
    from pathlib import Path
    from src.ml_pipeline.models import PropClassifier
    from src.ml_pipeline.data_loader import PropDataLoader
    from src.ml_pipeline.trainer import ModelTrainer, _split_chronological, _feature_cols, _EXCLUDE_COLS
    from src.ml_pipeline.config import BATTER_STATS

    db_path = ctx.obj['db']
    models_path = Path(models_dir)

    clf_path = models_path / f"classifier_{stat_type}.xgb"
    if not clf_path.exists():
        click.echo(click.style(f"No model found at {clf_path}", fg='red'))
        return

    click.echo(f"Loading classifier for {stat_type}...")
    classifier = PropClassifier.load(str(clf_path))

    click.echo("Loading test data...")
    loader = PropDataLoader(db_path)
    df = loader.load_training_data(stat_type=stat_type)

    if stat_type in BATTER_STATS and not df.empty:
        trainer = ModelTrainer(db_path=db_path, models_dir=models_dir)
        df = trainer._merge_arsenal_features(df)

    _, _, test = _split_chronological(df)
    feature_cols = _feature_cols(test)
    # Use only features the model was trained on
    feature_cols = [c for c in classifier.feature_names if c in test.columns]
    X_test = test[feature_cols].fillna(0)

    click.echo(f"Computing SHAP values on {len(X_test)} test samples...")
    explainer = shap.TreeExplainer(classifier.model)
    shap_values = explainer.shap_values(X_test)

    # Mean absolute SHAP value per feature
    mean_abs = pd.Series(
        np.abs(shap_values).mean(axis=0),
        index=feature_cols,
    ).sort_values(ascending=False)

    click.echo(f"\nSHAP Feature Importance — {stat_type} (top {top})")
    click.echo("=" * 55)
    click.echo(f"{'Feature':<40} {'Mean |SHAP|':>12}")
    click.echo("-" * 55)
    for feat, val in mean_abs.head(top).items():
        click.echo(f"{feat:<40} {val:>12.4f}")
    click.echo("=" * 55)

    # Direction: positive SHAP = pushes toward over
    click.echo("\nTop 10 directional effects (positive = pushes toward OVER):")
    click.echo("-" * 55)
    mean_signed = pd.Series(shap_values.mean(axis=0), index=feature_cols).sort_values(key=abs, ascending=False)
    for feat, val in mean_signed.head(10).items():
        direction = "↑ OVER" if val > 0 else "↓ UNDER"
        click.echo(f"  {feat:<38} {val:+.4f}  {direction}")


@ml.command('error-analysis')
@click.option('--stat', 'stat_type', required=True, help='Stat type (e.g. hits, home_runs)')
@click.option('--models-dir', default='models', help='Directory containing trained models')
@click.pass_context
def error_analysis(ctx, stat_type, models_dir):
    """Analyse classifier failure modes on the test set."""
    import numpy as np
    import pandas as pd
    from pathlib import Path
    from src.ml_pipeline.models import PropClassifier
    from src.ml_pipeline.data_loader import PropDataLoader
    from src.ml_pipeline.trainer import ModelTrainer, _split_chronological, _feature_cols
    from src.ml_pipeline.config import BATTER_STATS

    db_path = ctx.obj['db']
    models_path = Path(models_dir)

    clf_path = models_path / f"classifier_{stat_type}.xgb"
    cal_path = models_path / f"calibrator_{stat_type}.pkl"
    if not clf_path.exists():
        click.echo(click.style(f"No model found at {clf_path}", fg='red'))
        return

    classifier = PropClassifier.load(
        str(clf_path),
        str(cal_path) if cal_path.exists() else None,
    )

    loader = PropDataLoader(db_path)
    df = loader.load_training_data(stat_type=stat_type)

    if stat_type in BATTER_STATS and not df.empty:
        trainer = ModelTrainer(db_path=db_path, models_dir=models_dir)
        df = trainer._merge_arsenal_features(df)

    _, _, test = _split_chronological(df)
    feature_cols = [c for c in classifier.feature_names if c in test.columns]
    X_test = test[feature_cols].fillna(0)

    test = test.copy()
    test['p_over'] = classifier.predict_proba(X_test)
    test['p_under'] = 1 - test['p_over']
    test['predicted'] = (test['p_over'] >= 0.5).astype(int)
    test['correct'] = (test['predicted'] == test['target']).astype(int)
    test['confidence'] = np.maximum(test['p_over'], test['p_under'])

    # Confidence tiers
    def tier(p):
        if p >= 0.60:
            return 'high (≥60%)'
        elif p >= 0.55:
            return 'mid (55-60%)'
        else:
            return 'low (<55%)'
    test['tier'] = test['confidence'].apply(tier)

    click.echo(f"\nError Analysis — {stat_type}")
    click.echo(f"Test set: {len(test)} samples  |  {test['target'].mean():.1%} actual over rate")

    # --- Confidence tier breakdown ---
    click.echo("\n── Confidence Tiers ──────────────────────────────────")
    click.echo(f"{'Tier':<18} {'Props':>6} {'Accuracy':>9} {'Over%':>7}")
    click.echo("-" * 45)
    for tier_name in ['high (≥60%)', 'mid (55-60%)', 'low (<55%)']:
        sub = test[test['tier'] == tier_name]
        if len(sub) == 0:
            continue
        acc = sub['correct'].mean()
        over_rate = sub['target'].mean()
        click.echo(f"{tier_name:<18} {len(sub):>6} {acc:>9.1%} {over_rate:>7.1%}")

    # --- Errors by line value ---
    click.echo("\n── Accuracy by Line ───────────────────────────────────")
    if 'line' in test.columns:
        test['line_bucket'] = pd.cut(test['line'], bins=5)
        by_line = test.groupby('line_bucket', observed=True).agg(
            props=('correct', 'count'),
            accuracy=('correct', 'mean'),
            over_rate=('target', 'mean'),
        ).reset_index()
        click.echo(f"{'Line range':<22} {'Props':>6} {'Accuracy':>9} {'Over%':>7}")
        click.echo("-" * 48)
        for _, row in by_line.iterrows():
            click.echo(f"{str(row['line_bucket']):<22} {int(row['props']):>6} {row['accuracy']:>9.1%} {row['over_rate']:>7.1%}")

    # --- False positives vs false negatives ---
    click.echo("\n── Error Types ────────────────────────────────────────")
    fp = test[(test['predicted'] == 1) & (test['target'] == 0)]
    fn = test[(test['predicted'] == 0) & (test['target'] == 1)]
    tn = test[(test['predicted'] == 0) & (test['target'] == 0)]
    tp = test[(test['predicted'] == 1) & (test['target'] == 1)]
    click.echo(f"  True Positives  (predicted over,  hit over):   {len(tp):>5}")
    click.echo(f"  True Negatives  (predicted under, hit under):  {len(tn):>5}")
    click.echo(f"  False Positives (predicted over,  hit under):  {len(fp):>5}")
    click.echo(f"  False Negatives (predicted under, hit over):   {len(fn):>5}")

    # --- FP/FN feature means vs correct predictions ---
    if len(fp) > 10 and len(fn) > 10:
        numeric_feats = [c for c in feature_cols if c not in ('line', 'over_odds', 'under_odds')][:15]
        click.echo("\n── Feature Means: Errors vs Correct ───────────────────")
        click.echo(f"{'Feature':<38} {'FP mean':>9} {'FN mean':>9} {'Correct':>9}")
        click.echo("-" * 68)
        correct = test[test['correct'] == 1]
        for feat in numeric_feats:
            if feat not in test.columns:
                continue
            fp_mean = fp[feat].mean()
            fn_mean = fn[feat].mean()
            c_mean = correct[feat].mean()
            if abs(fp_mean - c_mean) > 0.05 * abs(c_mean) or abs(fn_mean - c_mean) > 0.05 * abs(c_mean):
                click.echo(f"  {feat:<36} {fp_mean:>9.3f} {fn_mean:>9.3f} {c_mean:>9.3f}")
