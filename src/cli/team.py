"""Team stats commands."""

import click


@click.group()
@click.pass_context
def team(ctx):
    """Team stats collection and lookup."""
    pass
