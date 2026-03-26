"""Underdog Fantasy MLB props scraper."""

import logging
import sqlite3
import time
from typing import List

import requests

logger = logging.getLogger(__name__)

MLB_SPORT_ID = 'MLB'
BASE_URL = 'https://api.underdogfantasy.com/v1'

# Map Underdog stat keys to our canonical stat names
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
    """Scrape MLB player props from Underdog Fantasy (no auth required)."""

    def __init__(self, db_path: str, delay: float = 1.0):
        self.db_path = db_path
        self.delay = delay

    def scrape(self) -> int:
        """
        Fetch current MLB props and store in underdog_props and all_props.

        Returns:
            Number of new props inserted.
        """
        props = self._fetch_props()
        if not props:
            logger.warning('No Underdog props fetched')
            return 0

        count = self._save_props(props)
        logger.info(f'Underdog: saved {count} props')
        return count

    def _fetch_props(self) -> List[dict]:
        """Fetch all active MLB over/under lines from Underdog API."""
        url = f'{BASE_URL}/over_under_lines'
        params = {'sport_id': MLB_SPORT_ID}

        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f'Underdog API error: {e}')
            return []

        time.sleep(self.delay)

        lines = data.get('over_under_lines', [])
        appearances = {a['id']: a for a in data.get('appearances', [])}
        players = {p['id']: p for p in data.get('players', [])}
        games = {g['id']: g for g in data.get('games', [])}

        props = []
        for line in lines:
            try:
                parsed = self._parse_line(line, appearances, players, games)
                props.extend(parsed)
            except Exception as e:
                logger.debug(f'Failed to parse line: {e}')

        return props

    def _parse_line(
        self,
        line: dict,
        appearances: dict,
        players: dict,
        games: dict,
    ) -> List[dict]:
        """
        Parse a single over_under_line into one prop dict per option (over + under).
        Returns empty list if the line should be skipped.
        """
        ou = line.get('over_under', {})
        appearance_stat = ou.get('appearance_stat', {})
        appearance_id = appearance_stat.get('appearance_id')
        appearance = appearances.get(appearance_id, {})

        # Resolve player name
        player_id = appearance.get('player_id')
        player = players.get(player_id, {})
        full_name = (
            f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            or None
        )
        if not full_name:
            return []

        # Stat name
        raw_stat = appearance_stat.get('stat', '')
        stat_name = STAT_NAME_MAP.get(raw_stat, raw_stat)

        # Skip combo stats (e.g. hits_runs_rbis) not in our model
        if stat_name not in STAT_NAME_MAP.values():
            return []

        stat_value = float(line.get('stat_value', 0))
        updated_at = line.get('updated_at', '')

        # Game context
        match_id = appearance.get('match_id')
        game = games.get(match_id, {})
        scheduled_at = game.get('scheduled_at', '')
        team_id = appearance.get('team_id', '')

        # Resolve team name from game title
        away_team_id = game.get('away_team_id', '')
        team_name = ''
        opponent_name = ''
        title = game.get('short_title', '')
        if title and '@' in title:
            parts = title.split('@')
            if team_id == away_team_id:
                team_name = parts[0].strip()
                opponent_name = parts[1].strip()
            else:
                team_name = parts[1].strip()
                opponent_name = parts[0].strip()

        # Each option is a separate over/under row
        props = []
        for option in line.get('options', []):
            raw_choice = option.get('choice', '').lower()
            choice = 'over' if raw_choice == 'higher' else 'under' if raw_choice == 'lower' else None
            if not choice:
                continue

            american_odds = option.get('american_price')
            try:
                american_odds = float(american_odds) if american_odds else None
            except (ValueError, TypeError):
                american_odds = None

            props.append({
                'full_name': full_name,
                'stat_name': stat_name,
                'stat_value': stat_value,
                'choice': choice,
                'american_odds': american_odds,
                'team_name': team_name,
                'opponent_name': opponent_name,
                'scheduled_at': scheduled_at,
                'updated_at': updated_at,
            })

        return props

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
