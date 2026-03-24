"""Underdog Fantasy MLB props scraper."""

import logging
import sqlite3
import time
from typing import List

import requests

from src.scrapers.underdog_auth import get_token

logger = logging.getLogger(__name__)

# Underdog league ID for MLB
MLB_LEAGUE_ID = 'MLB'

# Map Underdog stat names to our canonical names
STAT_NAME_MAP = {
    'hits': 'hits',
    'home_runs': 'home_runs',
    'rbis': 'rbis',
    'runs_scored': 'runs',
    'stolen_bases': 'stolen_bases',
    'total_bases': 'total_bases',
    'walks': 'walks',
    'strikeouts': 'batter_strikeouts',
    'pitcher_strikeouts': 'pitcher_strikeouts',
    'pitching_outs': 'outs_recorded',
    'earned_runs': 'earned_runs_allowed',
    'hits_allowed': 'hits_allowed',
}


class UnderdogScraper:
    """Scrape MLB player props from Underdog Fantasy."""

    BASE_URL = 'https://api.underdogfantasy.com/v1'

    def __init__(self, db_path: str, delay: float = 1.0):
        self.db_path = db_path
        self.delay = delay

    def scrape(self) -> int:
        """
        Fetch current MLB props and store in underdog_props and all_props.

        Returns:
            Number of props inserted.
        """
        try:
            token = get_token()
        except RuntimeError as e:
            logger.error(f'Underdog auth failed: {e}')
            return 0

        props = self._fetch_props(token)
        if not props:
            logger.warning('No Underdog props fetched')
            return 0

        count = self._save_props(props)
        logger.info(f'Underdog: saved {count} props')
        return count

    def _fetch_props(self, token: str) -> List[dict]:
        """Fetch all MLB over/under props from Underdog API."""
        headers = {'Authorization': f'Bearer {token}'}
        url = f'{self.BASE_URL}/over_under_lines'
        params = {'sport_id': MLB_LEAGUE_ID}

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f'Underdog API error: {e}')
            return []

        time.sleep(self.delay)

        # Underdog response structure: {over_under_lines: [...], appearances: [...], ...}
        lines = data.get('over_under_lines', [])
        appearances = {a['id']: a for a in data.get('appearances', [])}
        players = {p['id']: p for p in data.get('players', [])}
        matchups = {m['id']: m for m in data.get('matchups', [])}

        props = []
        for line in lines:
            try:
                prop = self._parse_line(line, appearances, players, matchups)
                if prop:
                    props.append(prop)
            except Exception as e:
                logger.debug(f'Failed to parse line: {e}')

        return props

    def _parse_line(
        self,
        line: dict,
        appearances: dict,
        players: dict,
        matchups: dict,
    ) -> dict | None:
        """Parse a single over_under_line into our prop schema."""
        appearance_id = line.get('over_under', {}).get('appearance_id')
        appearance = appearances.get(appearance_id, {})
        player_id = appearance.get('player_id')
        player = players.get(player_id, {})

        full_name = player.get('full_name', '')
        if not full_name:
            return None

        raw_stat = line.get('over_under', {}).get('appearance_stat', {}).get('display_stat', '')
        stat_name = STAT_NAME_MAP.get(raw_stat.lower().replace(' ', '_'), raw_stat.lower().replace(' ', '_'))

        stat_value = float(line.get('stat_value', 0))
        choice = line.get('choice', '').lower()  # 'higher'/'lower' → 'over'/'under'
        if choice == 'higher':
            choice = 'over'
        elif choice == 'lower':
            choice = 'under'

        american_odds = line.get('payout_multiplier')
        team_name = appearance.get('team_name', '')
        matchup_id = appearance.get('match_id', '')
        matchup = matchups.get(matchup_id, {})
        scheduled_at = matchup.get('scheduled_at', '')
        updated_at = line.get('updated_at', '')

        return {
            'full_name': full_name,
            'stat_name': stat_name,
            'stat_value': stat_value,
            'choice': choice,
            'american_odds': american_odds,
            'team_name': team_name,
            'opponent_name': '',
            'scheduled_at': scheduled_at,
            'updated_at': updated_at,
        }

    def _save_props(self, props: List[dict]) -> int:
        """Upsert props into underdog_props and all_props tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            for p in props:
                cursor.execute('''
                    INSERT OR IGNORE INTO underdog_props
                        (full_name, stat_name, stat_value, choice, american_odds,
                         team_name, opponent_name, scheduled_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    p['full_name'], p['stat_name'], p['stat_value'], p['choice'],
                    p['american_odds'], p['team_name'], p['opponent_name'],
                    p['scheduled_at'], p['updated_at'],
                ))

                cursor.execute('''
                    INSERT OR IGNORE INTO all_props
                        (source, full_name, stat_name, stat_value, choice, american_odds,
                         team_name, opponent_name, scheduled_at)
                    VALUES ('underdog', ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    p['full_name'], p['stat_name'], p['stat_value'], p['choice'],
                    p['american_odds'], p['team_name'], p['opponent_name'],
                    p['scheduled_at'],
                ))

                if cursor.rowcount > 0:
                    count += 1

            conn.commit()
        finally:
            conn.close()

        return count
