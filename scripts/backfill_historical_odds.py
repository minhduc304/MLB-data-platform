"""
Backfill historical MLB odds from The Odds API.

Uses the /v4/historical endpoints to fetch a pre-game snapshot (18:00 UTC)
for each game date in the given range. Skips dates already present in
odds_api_props. Writes to odds_api_props + all_props tables.

Usage:
    python scripts/backfill_historical_odds.py
    python scripts/backfill_historical_odds.py --start 2025-03-27 --end 2025-09-28
    python scripts/backfill_historical_odds.py --db data/mlb_stats.db --dry-run
"""

import argparse
import os
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = 'https://api.the-odds-api.com/v4'
SPORT = 'baseball_mlb'
SNAPSHOT_HOUR_UTC = 18  # 10am MST — lines are set, most games haven't started

MARKETS = [
    'batter_hits',
    'batter_home_runs',
    'batter_rbis',
    'pitcher_strikeouts',
]

MARKET_STAT_MAP = {
    'batter_hits': 'hits',
    'batter_home_runs': 'home_runs',
    'batter_rbis': 'rbis',
    'pitcher_strikeouts': 'pitcher_strikeouts',
}

# 2025 regular season defaults (excludes spring training and playoffs)
DEFAULT_START = '2025-03-27'
DEFAULT_END = '2025-09-28'


def get_api_key() -> str:
    raw = os.getenv('ODDS_API_KEY', '')
    keys = [k.strip() for k in raw.split(',') if k.strip()]
    if not keys:
        raise RuntimeError('ODDS_API_KEY not set in environment')
    return keys[0]  # First key is the paid key


def already_collected_dates(db_path: str) -> set:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute('SELECT DISTINCT game_date FROM odds_api_props')
        return {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()


def fetch_historical_events(api_key: str, snapshot: str) -> list:
    """GET /v4/historical/sports/{sport}/events?date=<snapshot> — costs 1 credit."""
    url = f'{BASE_URL}/historical/sports/{SPORT}/events'
    resp = requests.get(url, params={'apiKey': api_key, 'date': snapshot}, timeout=15)
    resp.raise_for_status()
    remaining = resp.headers.get('x-requests-remaining', '?')
    used = resp.headers.get('x-requests-used', '?')
    return resp.json().get('data', []), remaining, used


def fetch_historical_event_odds(api_key: str, event_id: str, snapshot: str) -> dict:
    """GET /v4/historical/.../events/{id}/odds — costs markets × regions credits."""
    url = f'{BASE_URL}/historical/sports/{SPORT}/events/{event_id}/odds'
    params = {
        'apiKey': api_key,
        'date': snapshot,
        'regions': 'us',
        'markets': ','.join(MARKETS),
        'oddsFormat': 'american',
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get('data', {})


def parse_props(event_data: dict, game_date: str) -> list:
    """Extract over/under props from event odds data."""
    props = []
    event_id = event_data.get('id', '')
    home_team = event_data.get('home_team', '')
    away_team = event_data.get('away_team', '')

    for bookmaker in event_data.get('bookmakers', []):
        sportsbook = bookmaker.get('key', '')
        for market in bookmaker.get('markets', []):
            market_key = market.get('key', '')
            stat_name = MARKET_STAT_MAP.get(market_key, market_key)

            player_lines: dict = {}
            for outcome in market.get('outcomes', []):
                player_name = outcome.get('description', '')
                side = outcome.get('name', '').lower()
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


def save_props(db_path: str, props: list) -> int:
    conn = sqlite3.connect(db_path)
    count = 0
    try:
        cursor = conn.cursor()
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


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description='Backfill historical MLB odds')
    parser.add_argument('--start', default=DEFAULT_START, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', default=DEFAULT_END, help='End date YYYY-MM-DD')
    parser.add_argument('--db', default='data/mlb_stats.db', help='SQLite DB path')
    parser.add_argument('--dry-run', action='store_true', help='Show plan without fetching')
    parser.add_argument('--delay', type=float, default=1.5, help='Seconds between requests')
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    api_key = get_api_key()

    collected = already_collected_dates(args.db)
    all_dates = list(date_range(start_date, end_date))
    pending = [d for d in all_dates if d.isoformat() not in collected]

    print(f'Date range:   {args.start} → {args.end} ({len(all_dates)} calendar days)')
    print(f'Already done: {len(all_dates) - len(pending)} days')
    print(f'To fetch:     {len(pending)} days')
    print(f'Est. credits: ~{len(pending) * 61} (assumes ~15 games/day, 4 markets)')
    print()

    if args.dry_run:
        print('Dry run — exiting without fetching.')
        return

    total_props = 0
    credits_remaining = '?'

    for i, game_date in enumerate(pending, 1):
        snapshot = datetime(
            game_date.year, game_date.month, game_date.day,
            SNAPSHOT_HOUR_UTC, 0, 0, tzinfo=timezone.utc
        ).strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            events, credits_remaining, credits_used = fetch_historical_events(api_key, snapshot)
        except requests.HTTPError as e:
            print(f'[{game_date}] ERROR fetching events: {e}')
            break

        if not events:
            print(f'[{game_date}] No games found — skipping')
            time.sleep(args.delay)
            continue

        day_props = 0
        for event in events:
            time.sleep(args.delay)
            try:
                event_data = fetch_historical_event_odds(api_key, event['id'], snapshot)
            except requests.HTTPError as e:
                print(f'[{game_date}] ERROR fetching odds for event {event["id"]}: {e}')
                continue

            if not event_data:
                continue

            props = parse_props(event_data, game_date.isoformat())
            day_props += save_props(args.db, props)

        total_props += day_props
        print(f'[{i}/{len(pending)}] {game_date}: {len(events)} games, {day_props} props saved  (credits remaining: {credits_remaining})')
        time.sleep(args.delay)

    print()
    print(f'Done — {total_props} total props inserted. Credits remaining: {credits_remaining}')


if __name__ == '__main__':
    main()
