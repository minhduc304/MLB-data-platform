"""Machine learning pipeline commands."""

import click


@click.group()
@click.pass_context
def ml(ctx):
    """Machine learning training and predictions."""
    pass
