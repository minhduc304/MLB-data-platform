"""Player stats commands."""

import click

from src.config import CURRENT_SEASON


@click.group()
@click.pass_context
def player(ctx):
    """Player stats collection and lookup."""
    pass


@player.command('update-all')
@click.option('--season', default=CURRENT_SEASON, help='Season year')
@click.pass_context
def update_all(ctx, season):
    """Update season stats for all batters and pitchers."""
    from src.collectors.batter import BatterStatsCollector
    from src.collectors.pitcher import PitcherStatsCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))

    click.echo(f"Collecting batter stats for {season}...")
    batter_collector = BatterStatsCollector(db_path, client, season)
    batter_count = batter_collector.collect()
    click.echo(f"  {batter_count} batters updated")

    click.echo(f"Collecting pitcher stats for {season}...")
    pitcher_collector = PitcherStatsCollector(db_path, client, season)
    pitcher_count = pitcher_collector.collect()
    click.echo(f"  {pitcher_count} pitchers updated")

    click.echo(click.style(
        f"Updated {batter_count} batters and {pitcher_count} pitchers!",
        fg='green'
    ))


@player.command('game-logs')
@click.option('--historical', default=None, help='Backfill a historical season (e.g. 2024)')
@click.option('--season', default=CURRENT_SEASON, help='Current season year')
@click.pass_context
def game_logs(ctx, historical, season):
    """Collect game-by-game logs for batters and pitchers."""
    from src.collectors.batter import BatterGameLogCollector
    from src.collectors.pitcher import PitcherGameLogCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))

    label = historical or season

    click.echo(f"Collecting batter game logs for {label}...")
    batter_collector = BatterGameLogCollector(db_path, client, season)
    batter_count = batter_collector.collect(historical_season=historical)
    click.echo(f"  {batter_count} batter game log entries")

    click.echo(f"Collecting pitcher game logs for {label}...")
    pitcher_collector = PitcherGameLogCollector(db_path, client, season)
    pitcher_count = pitcher_collector.collect(historical_season=historical)
    click.echo(f"  {pitcher_count} pitcher game log entries")

    click.echo(click.style(
        f"Collected {batter_count + pitcher_count} total game log entries!",
        fg='green'
    ))
