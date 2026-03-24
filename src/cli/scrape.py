"""Props scraping commands."""

import click

from src.config import get_db_path


@click.group()
@click.pass_context
def scrape(ctx):
    """Props scraping from betting platforms."""
    pass


@scrape.command('underdog')
@click.pass_context
def scrape_underdog(ctx):
    """Scrape Underdog Fantasy MLB props."""
    from src.scrapers.underdog import UnderdogScraper

    db_path = ctx.obj.get('db_path', get_db_path()) if ctx.obj else get_db_path()
    scraper = UnderdogScraper(db_path=db_path)
    count = scraper.scrape()
    click.echo(f'Underdog: {count} props saved')


@scrape.command('prizepicks')
@click.pass_context
def scrape_prizepicks(ctx):
    """Scrape PrizePicks MLB props."""
    from src.scrapers.prizepicks import PrizePicksScraper

    db_path = ctx.obj.get('db_path', get_db_path()) if ctx.obj else get_db_path()
    scraper = PrizePicksScraper(db_path=db_path)
    count = scraper.scrape()
    click.echo(f'PrizePicks: {count} props saved')


@scrape.command('odds-api')
@click.option('--markets', default=None, help='Comma-separated market keys to fetch')
@click.pass_context
def scrape_odds_api(ctx, markets):
    """Scrape The Odds API MLB props."""
    from src.scrapers.odds_props import DEFAULT_MARKETS, OddsAPIScraper

    db_path = ctx.obj.get('db_path', get_db_path()) if ctx.obj else get_db_path()
    market_list = markets.split(',') if markets else DEFAULT_MARKETS
    scraper = OddsAPIScraper(db_path=db_path)
    count = scraper.scrape(markets=market_list)
    click.echo(f'Odds API: {count} props saved')


@scrape.command('no-odds')
@click.pass_context
def scrape_no_odds(ctx):
    """Scrape Underdog + PrizePicks (saves Odds API credits)."""
    from src.scrapers.prizepicks import PrizePicksScraper
    from src.scrapers.underdog import UnderdogScraper

    db_path = ctx.obj.get('db_path', get_db_path()) if ctx.obj else get_db_path()

    ud = UnderdogScraper(db_path=db_path).scrape()
    pp = PrizePicksScraper(db_path=db_path).scrape()
    click.echo(f'Underdog: {ud} | PrizePicks: {pp} props saved')


@scrape.command('all')
@click.pass_context
def scrape_all(ctx):
    """Scrape all three sources: Underdog, PrizePicks, Odds API."""
    from src.scrapers.odds_props import OddsAPIScraper
    from src.scrapers.prizepicks import PrizePicksScraper
    from src.scrapers.underdog import UnderdogScraper

    db_path = ctx.obj.get('db_path', get_db_path()) if ctx.obj else get_db_path()

    ud = UnderdogScraper(db_path=db_path).scrape()
    pp = PrizePicksScraper(db_path=db_path).scrape()
    oa = OddsAPIScraper(db_path=db_path).scrape()
    click.echo(f'Underdog: {ud} | PrizePicks: {pp} | Odds API: {oa} props saved')
