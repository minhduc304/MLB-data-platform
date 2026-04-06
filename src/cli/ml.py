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
