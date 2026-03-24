"""PrizePicks MLB props scraper."""

import logging
import sqlite3
import time
from typing import List

import requests

logger = logging.getLogger(__name__)

# PrizePicks league ID for MLB
MLB_LEAGUE_ID = 2

# Map PrizePicks stat names to our canonical names
STAT_NAME_MAP = {
    'hits': 'hits',
    'home runs': 'home_runs',
    'homeruns': 'home_runs',
    'rbis': 'rbis',
    'rbi': 'rbis',
    'runs scored': 'runs',
    'runs': 'runs',
    'stolen bases': 'stolen_bases',
    'total bases': 'total_bases',
    'walks': 'walks',
    'strikeouts': 'batter_strikeouts',
    'hitter strikeouts': 'batter_strikeouts',
    'pitcher strikeouts': 'pitcher_strikeouts',
    'pitching outs': 'outs_recorded',
    'earned runs allowed': 'earned_runs_allowed',
    'hits allowed': 'hits_allowed',
}


class PrizePicksScraper:
    """Scrape MLB player props from PrizePicks."""

    BASE_URL = 'https://api.prizepicks.com'

    def __init__(self, db_path: str, delay: float = 1.0):
        self.db_path = db_path
        self.delay = delay

    def scrape(self) -> int:
        """
        Fetch current MLB props and store in prizepicks_props and all_props.

        Returns:
            Number of props inserted.
        """
        props = self._fetch_props()
        if not props:
            logger.warning('No PrizePicks props fetched')
            return 0

        count = self._save_props(props)
        logger.info(f'PrizePicks: saved {count} props')
        return count

    def _fetch_props(self) -> List[dict]:
        """Fetch all MLB projections from PrizePicks API."""
        headers = {
            'Content-Type': 'application/json',
            'X-Device-ID': 'mlb-data-platform',
        }
        url = f'{self.BASE_URL}/projections'
        params = {
            'league_id': MLB_LEAGUE_ID,
            'per_page': 250,
            'single_stat': True,
        }

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f'PrizePicks API error: {e}')
            return []

        time.sleep(self.delay)

        projections = data.get('data', [])
        included = data.get('included', [])

        players = {}
        games = {}
        for item in included:
            if item.get('type') == 'new_player':
                players[item['id']] = item.get('attributes', {})
            elif item.get('type') == 'game':
                games[item['id']] = item.get('attributes', {})

        props = []
        for proj in projections:
            try:
                prop = self._parse_projection(proj, players, games)
                if prop:
                    props.append(prop)
            except Exception as e:
                logger.debug(f'Failed to parse projection: {e}')

        return props

    def _parse_projection(
        self,
        proj: dict,
        players: dict,
        games: dict,
    ) -> dict | None:
        """Parse a single projection into our prop schema."""
        attrs = proj.get('attributes', {})
        relationships = proj.get('relationships', {})

        player_id = (
            relationships.get('new_player', {})
            .get('data', {})
            .get('id', '')
        )
        player = players.get(player_id, {})
        full_name = player.get('name', '')
        if not full_name:
            return None

        raw_stat = attrs.get('stat_type', '')
        stat_name = STAT_NAME_MAP.get(raw_stat.lower(), raw_stat.lower().replace(' ', '_'))

        stat_value = float(attrs.get('line_score', 0))
        prop_type = 'goblin' if attrs.get('projection_type') == 'diminished' else 'standard'
        scheduled_at = attrs.get('start_time', '')
        team_name = player.get('team', '')

        # PrizePicks returns one line per player (no explicit over/under split)
        # We store it once without a choice — treated as the line
        return {
            'full_name': full_name,
            'stat_name': stat_name,
            'stat_value': stat_value,
            'choice': 'over',  # Store as reference line
            'prop_type': prop_type,
            'scheduled_at': scheduled_at,
            'team_name': team_name,
        }

    def _save_props(self, props: List[dict]) -> int:
        """Upsert props into prizepicks_props and all_props tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            for p in props:
                cursor.execute('''
                    INSERT OR IGNORE INTO prizepicks_props
                        (full_name, stat_name, stat_value, choice, prop_type, scheduled_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    p['full_name'], p['stat_name'], p['stat_value'],
                    p['choice'], p['prop_type'], p['scheduled_at'],
                ))

                cursor.execute('''
                    INSERT OR IGNORE INTO all_props
                        (source, full_name, stat_name, stat_value, choice,
                         team_name, scheduled_at, prop_type)
                    VALUES ('prizepicks', ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    p['full_name'], p['stat_name'], p['stat_value'],
                    p['choice'], p['team_name'], p['scheduled_at'], p['prop_type'],
                ))

                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
        finally:
            conn.close()

        return count
