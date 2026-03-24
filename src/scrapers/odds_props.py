"""The Odds API MLB props scraper."""

import logging
import os
import sqlite3
import time
from typing import List

import requests

logger = logging.getLogger(__name__)

SPORT_KEY = 'baseball_mlb'

# Odds API market keys -> our canonical stat names
MARKET_STAT_MAP = {
    'batter_hits': 'hits',
    'batter_home_runs': 'home_runs',
    'batter_rbis': 'rbis',
    'batter_runs_scored': 'runs',
    'batter_stolen_bases': 'stolen_bases',
    'batter_total_bases': 'total_bases',
    'batter_walks': 'walks',
    'batter_strikeouts': 'batter_strikeouts',
    'pitcher_strikeouts': 'pitcher_strikeouts',
    'pitcher_outs': 'outs_recorded',
    'pitcher_hits_allowed': 'hits_allowed',
    'pitcher_earned_runs': 'earned_runs_allowed',
}

DEFAULT_MARKETS = [
    'batter_hits',
    'batter_home_runs',
    'batter_rbis',
    'batter_total_bases',
    'pitcher_strikeouts',
    'pitcher_outs',
]


class OddsAPIScraper:
    """Scrape MLB player props from The Odds API."""

    BASE_URL = 'https://api.the-odds-api.com/v4'

    def __init__(self, db_path: str, delay: float = 1.5):
        self.db_path = db_path
        self.delay = delay
        self.api_key = os.getenv('ODDS_API_KEY', '')

    def scrape(self, markets: List[str] = None) -> int:
        """
        Fetch MLB player props and store in odds_api_props and all_props.

        Args:
            markets: List of market keys to fetch. Defaults to DEFAULT_MARKETS.

        Returns:
            Number of props inserted.
        """
        if not self.api_key:
            logger.error('ODDS_API_KEY not set in environment')
            return 0

        markets = markets or DEFAULT_MARKETS
        events = self._fetch_events()
        if not events:
            logger.warning('No Odds API events found')
            return 0

        total = 0
        for event in events:
            props = self._fetch_event_props(event, markets)
            total += self._save_props(props)
            time.sleep(self.delay)

        logger.info(f'Odds API: saved {total} props')
        return total

    def _fetch_events(self) -> List[dict]:
        """Get upcoming MLB game events."""
        url = f'{self.BASE_URL}/sports/{SPORT_KEY}/events'
        params = {'apiKey': self.api_key}

        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f'Odds API events error: {e}')
            return []

    def _fetch_event_props(self, event: dict, markets: List[str]) -> List[dict]:
        """Fetch player prop lines for a single game event."""
        event_id = event.get('id', '')
        home_team = event.get('home_team', '')
        away_team = event.get('away_team', '')
        commence_time = event.get('commence_time', '')
        game_date = commence_time[:10] if commence_time else ''

        url = f'{self.BASE_URL}/sports/{SPORT_KEY}/events/{event_id}/odds'
        params = {
            'apiKey': self.api_key,
            'regions': 'us',
            'markets': ','.join(markets),
            'oddsFormat': 'american',
        }

        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.debug(f'Odds API props error for event {event_id}: {e}')
            return []

        props = []
        for bookmaker in data.get('bookmakers', []):
            sportsbook = bookmaker.get('key', '')
            for market in bookmaker.get('markets', []):
                market_key = market.get('key', '')
                stat_name = MARKET_STAT_MAP.get(market_key, market_key)

                # Group outcomes by player name to find over/under pair
                player_lines: dict = {}
                for outcome in market.get('outcomes', []):
                    player_name = outcome.get('description', '')
                    side = outcome.get('name', '').lower()  # 'Over'/'Under'
                    price = outcome.get('price')
                    line = outcome.get('point')

                    if player_name not in player_lines:
                        player_lines[player_name] = {'line': line, 'over_odds': None, 'under_odds': None}
                    if side == 'over':
                        player_lines[player_name]['over_odds'] = price
                        player_lines[player_name]['line'] = line
                    elif side == 'under':
                        player_lines[player_name]['under_odds'] = price

                for player_name, pl in player_lines.items():
                    if pl['line'] is None:
                        continue
                    props.append({
                        'event_id': event_id,
                        'player_name': player_name,
                        'stat_type': stat_name,
                        'sportsbook': sportsbook,
                        'line': pl['line'],
                        'over_odds': pl['over_odds'],
                        'under_odds': pl['under_odds'],
                        'game_date': game_date,
                        'home_team': home_team,
                        'away_team': away_team,
                    })

        return props

    def _save_props(self, props: List[dict]) -> int:
        """Upsert props into odds_api_props and all_props tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            for p in props:
                cursor.execute('''
                    INSERT OR IGNORE INTO odds_api_props
                        (event_id, player_name, stat_type, sportsbook, line,
                         over_odds, under_odds, game_date, home_team, away_team)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    p['event_id'], p['player_name'], p['stat_type'], p['sportsbook'],
                    p['line'], p['over_odds'], p['under_odds'],
                    p['game_date'], p['home_team'], p['away_team'],
                ))

                # Insert into all_props (over side)
                cursor.execute('''
                    INSERT OR IGNORE INTO all_props
                        (source, full_name, stat_name, stat_value, choice,
                         american_odds, opponent_name, scheduled_at)
                    VALUES ('odds_api', ?, ?, ?, 'over', ?, ?, ?)
                ''', (
                    p['player_name'], p['stat_type'], p['line'],
                    p['over_odds'], p['sportsbook'], p['game_date'],
                ))

                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
        finally:
            conn.close()

        return count
