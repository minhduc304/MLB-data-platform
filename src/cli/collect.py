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


@collect.command('update-starters')
@click.option('--days', default=7, help='Days ahead to refresh probable starters (default 7)')
@click.pass_context
def update_starters(ctx, days):
    """Refresh probable starting pitchers for upcoming scheduled games."""
    from src.collectors.schedule import ScheduleCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))

    click.echo(f"Refreshing probable starters for next {days} days...")
    collector = ScheduleCollector(db_path, client)
    count = collector.update_starters(days_ahead=days)
    click.echo(click.style(f"Updated starters for {count} games!", fg='green'))


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
@click.option('--season', default=None, help='Season year (omit to seed 2024, 2025, and 2026)')
@click.pass_context
def park_factors(ctx, season):
    """Seed park factor data for all venues."""
    from src.collectors.park_factors import ParkFactorsCollector

    db_path = ctx.obj['db']
    seasons = [season] if season else ['2024', '2025', '2026']
    total = 0

    for s in seasons:
        click.echo(f"Seeding park factors for {s}...")
        collector = ParkFactorsCollector(db_path, season=s)
        count = collector.collect()
        total += count
        click.echo(f"  {count} entries for {s}")

    click.echo(click.style(f"Seeded {total} total park factor entries!", fg='green'))


@collect.command('backfill-pitcher-hand')
@click.option('--delay', default=0.5, type=float, help='API delay between pitcher lookups (default 0.5s)')
@click.pass_context
def backfill_pitcher_hand(ctx, delay):
    """Backfill opposing pitcher hand in batter game logs, then recompute rolling stats."""
    from src.collectors.backfill_pitcher_hand import backfill_opposing_pitcher_hand

    db_path = ctx.obj['db']
    click.echo("Backfilling opposing pitcher hand in batter game logs...")
    count = backfill_opposing_pitcher_hand(db_path, delay=delay)
    click.echo(click.style(f"Updated {count} batter game log rows!", fg='green'))


@collect.command('weather')
@click.option('--season', default=None, help='Backfill weather for a full season (e.g. 2024)')
@click.option('--date', 'game_date', default=None, help='Collect weather for one date (YYYY-MM-DD)')
@click.pass_context
def weather(ctx, season, game_date):
    """Collect game weather conditions (temp, wind speed/direction, dome/outdoor)."""
    from src.collectors.weather import WeatherCollector
    from src.api.client import MLBAPIClient
    from src.config import APIConfig
    from datetime import date

    db_path = ctx.obj['db']
    client = MLBAPIClient(APIConfig(delay=ctx.obj['delay']))
    collector = WeatherCollector(db_path, client)

    if season:
        click.echo(f"Backfilling weather for {season} season...")
        count = collector.collect_season(season)
        click.echo(click.style(f"Collected weather for {count} games!", fg='green'))
    else:
        target_date = game_date or date.today().isoformat()
        click.echo(f"Collecting weather for {target_date}...")
        count = collector.collect_date(target_date)
        click.echo(click.style(f"Collected weather for {count} games!", fg='green'))


@collect.command('all')
@click.option('--season', default=CURRENT_SEASON, help='Season year')
@click.pass_context
def collect_all(ctx, season):
    """Run all collection tasks."""
    ctx.invoke(teams)
    ctx.invoke(schedule, season=season)
    ctx.invoke(injuries, season=season)
    ctx.invoke(park_factors, season=season)
