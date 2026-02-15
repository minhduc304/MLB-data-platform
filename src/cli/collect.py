"""Data collection commands."""

import click


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
