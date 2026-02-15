"""
MLB Prop Prediction System CLI

Unified command-line interface for data collection, ML training, and predictions.

Usage:
    mlb [OPTIONS] COMMAND [ARGS]...

Commands:
    collect   Data collection commands
    player    Player stats commands
    team      Team stats commands
    scrape    Props scraping commands
    ml        Machine learning pipeline
"""

import click
import logging
import os
import sys
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from src.config import APIConfig


def setup_logging(verbose: bool):
    """Configure logging to output to stdout."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


@click.group()
@click.option('--db', default='data/mlb_stats.db', help='Database path')
@click.option('--delay', default=APIConfig().delay, type=float, help='API delay in seconds')
@click.option('-v', '--verbose', is_flag=True, help='Verbose output (show DEBUG logs)')
@click.option('-q', '--quiet', is_flag=True, help='Quiet mode (only show warnings/errors)')
@click.pass_context
def cli(ctx, db, delay, verbose, quiet):
    """MLB Prop Prediction System - Data collection and ML predictions."""
    if quiet:
        logging.basicConfig(level=logging.WARNING, format='%(message)s')
    else:
        setup_logging(verbose)

    ctx.ensure_object(dict)
    ctx.obj['db'] = db
    ctx.obj['delay'] = delay
    ctx.obj['verbose'] = verbose


# Import and register command groups
from .collect import collect
from .player import player
from .team import team
from .scrape import scrape
from .ml import ml

cli.add_command(collect)
cli.add_command(player)
cli.add_command(team)
cli.add_command(scrape)
cli.add_command(ml)


if __name__ == '__main__':
    cli()
