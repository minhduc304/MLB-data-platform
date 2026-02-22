"""Data collection commands."""

import click

from src.config import CURRENT_SEASON


@click.group()
@click.pass_context
def collect(ctx):
    """Data collection commands."""
    pass


@collect.command('init-db')
@click.pass_context
def init_db(ctx):
    """Initialize the database with all tables."""
    from src.db.init_db import init_database

    db_path = ctx.obj['db']
    click.echo(f"Initializing database at {db_path}...")
    init_database(db_path)
    click.echo(click.style("Database initialized successfully!", fg='green'))


@collect.command('teams')
@click.pass_context
def teams(ctx):
    """Collect all MLB teams and venues."""
    from src.collectors.team import TeamCollector, VenueCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))

    click.echo("Collecting teams...")
    team_collector = TeamCollector(db_path, client)
    team_count = team_collector.collect()
    click.echo(f"  {team_count} teams collected")

    click.echo("Collecting venues...")
    venue_collector = VenueCollector(db_path, client)
    venue_count = venue_collector.collect()
    click.echo(f"  {venue_count} venues collected")

    click.echo(click.style("Teams and venues collected successfully!", fg='green'))


@collect.command('schedule')
@click.option('--start', default=None, help='Start date (MM/DD/YYYY)')
@click.option('--end', default=None, help='End date (MM/DD/YYYY)')
@click.option('--season', default=CURRENT_SEASON, help='Season year (used for default date range)')
@click.pass_context
def schedule(ctx, start, end, season):
    """Collect game schedule with probable pitchers and scores."""
    from src.collectors.schedule import ScheduleCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))

    # Default to full season date range
    if start is None:
        start = f"03/20/{season}"
    if end is None:
        end = f"11/05/{season}"

    click.echo(f"Collecting schedule from {start} to {end}...")
    collector = ScheduleCollector(db_path, client)
    count = collector.collect(start, end)
    click.echo(click.style(f"Collected {count} games!", fg='green'))


@collect.command('injuries')
@click.option('--season', default=CURRENT_SEASON, help='Season year')
@click.pass_context
def injuries(ctx, season):
    """Collect current IL snapshot for all teams."""
    from src.collectors.injuries import InjuriesCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))

    click.echo("Collecting injury data...")
    collector = InjuriesCollector(db_path, client, season=season)
    count = collector.collect()
    click.echo(click.style(f"Collected {count} injury records!", fg='green'))


@collect.command('lineups')
@click.option('--date', 'game_date', default=None, help='Game date (MM/DD/YYYY)')
@click.pass_context
def lineups(ctx, game_date):
    """Collect starting lineups and batting order."""
    from src.collectors.lineups import LineupCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))

    click.echo("Collecting starting lineups...")
    collector = LineupCollector(db_path, client)
    count = collector.collect(game_date)
    click.echo(click.style(f"Collected {count} lineup entries!", fg='green'))


@collect.command('park-factors')
@click.option('--season', default=CURRENT_SEASON, help='Season year')
@click.pass_context
def park_factors(ctx, season):
    """Seed park factor data for all venues."""
    from src.collectors.park_factors import ParkFactorsCollector

    db_path = ctx.obj['db']

    click.echo(f"Seeding park factors for {season}...")
    collector = ParkFactorsCollector(db_path, season=season)
    count = collector.collect()
    click.echo(click.style(f"Seeded {count} park factor entries!", fg='green'))


@collect.command('all')
@click.option('--season', default=CURRENT_SEASON, help='Season year')
@click.pass_context
def collect_all(ctx, season):
    """Run all collection tasks."""
    ctx.invoke(teams)
    ctx.invoke(schedule, season=season)
    ctx.invoke(injuries, season=season)
    ctx.invoke(park_factors, season=season)
