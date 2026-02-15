"""Player stats commands."""

import click


@click.group()
@click.pass_context
def player(ctx):
    """Player stats collection and lookup."""
    pass
