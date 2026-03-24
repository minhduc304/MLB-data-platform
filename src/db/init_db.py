"""
MLB Stats Database Initialization

Creates all required database tables for the MLB Prop Prediction System.

Usage:
    ./mlb collect init-db
"""

import os
import sqlite3


def init_database(db_path: str = None) -> None:
    """
    Create database tables for the MLB Prop Prediction System.

    Args:
        db_path: Path to the SQLite database file
    """
    from src.config import get_db_path
    if db_path is None:
        db_path = get_db_path()

    # Create data directory if it doesn't exist
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # =========================================================================
    # TEAMS TABLE
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            abbreviation TEXT NOT NULL UNIQUE,
            league TEXT,
            division TEXT,
            venue_name TEXT,
            venue_id INTEGER
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_teams_abbreviation
        ON teams(abbreviation)
    ''')

    # =========================================================================
    # VENUES TABLE
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS venues (
            venue_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            city TEXT,
            state TEXT,
            park_factor_overall REAL DEFAULT 1.0,
            park_factor_hr REAL DEFAULT 1.0,
            park_factor_h REAL DEFAULT 1.0,
            last_updated TIMESTAMP
        )
    ''')

    # =========================================================================
    # BATTER STATS TABLE — Season-level batting
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS batter_stats (
            player_id INTEGER PRIMARY KEY,
            player_name TEXT NOT NULL,
            team_id INTEGER,
            position TEXT,
            season TEXT NOT NULL,
            games_played INTEGER,
            plate_appearances INTEGER,
            at_bats INTEGER,
            hits INTEGER,
            doubles INTEGER,
            triples INTEGER,
            home_runs INTEGER,
            rbi INTEGER,
            runs INTEGER,
            stolen_bases INTEGER,
            caught_stealing INTEGER,
            walks INTEGER,
            strikeouts INTEGER,
            batting_avg REAL,
            obp REAL,
            slg REAL,
            ops REAL,
            total_bases INTEGER,
            bats TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        )
    ''')

    # =========================================================================
    # PITCHER STATS TABLE — Season-level pitching
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pitcher_stats (
            player_id INTEGER PRIMARY KEY,
            player_name TEXT NOT NULL,
            team_id INTEGER,
            position TEXT,
            season TEXT NOT NULL,
            games_played INTEGER,
            games_started INTEGER,
            innings_pitched REAL,
            wins INTEGER,
            losses INTEGER,
            era REAL,
            whip REAL,
            strikeouts INTEGER,
            walks_allowed INTEGER,
            hits_allowed INTEGER,
            home_runs_allowed INTEGER,
            earned_runs INTEGER,
            k_per_9 REAL,
            bb_per_9 REAL,
            k_bb_ratio REAL,
            throws TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        )
    ''')

    # =========================================================================
    # BATTER GAME LOGS TABLE — Per-game batting
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS batter_game_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            game_id INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            season TEXT NOT NULL,
            team_id INTEGER,
            opponent_id INTEGER,
            opponent_abbr TEXT,
            is_home INTEGER,
            batting_order INTEGER,
            plate_appearances INTEGER,
            at_bats INTEGER,
            hits INTEGER,
            doubles INTEGER,
            triples INTEGER,
            home_runs INTEGER,
            rbi INTEGER,
            runs INTEGER,
            stolen_bases INTEGER,
            walks INTEGER,
            strikeouts INTEGER,
            total_bases INTEGER,
            opposing_pitcher_id INTEGER,
            opposing_pitcher_hand TEXT,
            venue_id INTEGER,
            UNIQUE(game_id, player_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_batter_logs_player_date
        ON batter_game_logs(player_id, game_date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_batter_logs_game
        ON batter_game_logs(game_id)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_batter_logs_season
        ON batter_game_logs(season)
    ''')

    # =========================================================================
    # PITCHER GAME LOGS TABLE — Per-game pitching
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pitcher_game_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            game_id INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            season TEXT NOT NULL,
            team_id INTEGER,
            opponent_id INTEGER,
            opponent_abbr TEXT,
            is_home INTEGER,
            is_start INTEGER,
            innings_pitched REAL,
            outs_recorded INTEGER,
            hits_allowed INTEGER,
            runs_allowed INTEGER,
            earned_runs INTEGER,
            walks_allowed INTEGER,
            strikeouts INTEGER,
            home_runs_allowed INTEGER,
            pitches_thrown INTEGER,
            venue_id INTEGER,
            UNIQUE(game_id, player_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_pitcher_logs_player_date
        ON pitcher_game_logs(player_id, game_date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_pitcher_logs_game
        ON pitcher_game_logs(game_id)
    ''')

    # =========================================================================
    # SCHEDULE TABLE — Games with probable pitchers, scores, status
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedule (
            game_id INTEGER PRIMARY KEY,
            game_date TEXT NOT NULL,
            season TEXT NOT NULL,
            home_team_id INTEGER,
            away_team_id INTEGER,
            home_abbr TEXT,
            away_abbr TEXT,
            venue_id INTEGER,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT,
            home_probable_pitcher_id INTEGER,
            away_probable_pitcher_id INTEGER,
            FOREIGN KEY (venue_id) REFERENCES venues(venue_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_schedule_date
        ON schedule(game_date)
    ''')

    # =========================================================================
    # PLAYER NAME ALIASES TABLE — For future prop name matching
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_name_aliases (
            player_id INTEGER NOT NULL,
            canonical_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            PRIMARY KEY (player_id, alias)
        )
    ''')

    # =========================================================================
    # PLAYER INJURIES TABLE — Daily IL snapshots
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_injuries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team_id INTEGER,
            injury_status TEXT,
            injury_description TEXT,
            collection_date TEXT NOT NULL,
            UNIQUE(player_id, collection_date),
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_injuries_player
        ON player_injuries(player_id)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_injuries_date
        ON player_injuries(collection_date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_injuries_status
        ON player_injuries(injury_status)
    ''')

    # =========================================================================
    # STARTING LINEUPS TABLE — Batting order per game
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS starting_lineups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            player_name TEXT,
            batting_order INTEGER NOT NULL,
            position TEXT,
            UNIQUE(game_id, team_id, batting_order),
            FOREIGN KEY (game_id) REFERENCES schedule(game_id),
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_lineups_game
        ON starting_lineups(game_id)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_lineups_date
        ON starting_lineups(game_date)
    ''')

    # =========================================================================
    # PARK FACTORS TABLE — Venue-level adjustments by season
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS park_factors (
            venue_id INTEGER NOT NULL,
            season TEXT NOT NULL,
            factor_type TEXT NOT NULL,
            factor_value REAL NOT NULL DEFAULT 1.0,
            PRIMARY KEY (venue_id, season, factor_type)
        )
    ''')

    # =========================================================================
    # UNDERDOG PROPS TABLE
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS underdog_props (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            stat_name TEXT NOT NULL,
            stat_value REAL NOT NULL,
            choice TEXT NOT NULL,
            american_odds REAL,
            team_name TEXT,
            opponent_name TEXT,
            scheduled_at TEXT,
            updated_at TEXT,
            UNIQUE(full_name, stat_name, stat_value, choice, updated_at)
        )
    ''')

    # =========================================================================
    # PRIZEPICKS PROPS TABLE
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prizepicks_props (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            stat_name TEXT NOT NULL,
            stat_value REAL NOT NULL,
            choice TEXT NOT NULL,
            prop_type TEXT DEFAULT 'standard',
            scheduled_at TEXT,
            UNIQUE(full_name, stat_name, stat_value, choice, scheduled_at)
        )
    ''')

    # =========================================================================
    # ODDS API PROPS TABLE
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS odds_api_props (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            player_name TEXT NOT NULL,
            stat_type TEXT NOT NULL,
            sportsbook TEXT NOT NULL,
            line REAL NOT NULL,
            over_odds REAL,
            under_odds REAL,
            game_date TEXT,
            home_team TEXT,
            away_team TEXT,
            UNIQUE(event_id, player_name, stat_type, sportsbook)
        )
    ''')

    # =========================================================================
    # ALL PROPS TABLE — Unified across all sources
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS all_props (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            full_name TEXT NOT NULL,
            stat_name TEXT NOT NULL,
            stat_value REAL NOT NULL,
            choice TEXT NOT NULL,
            american_odds REAL,
            team_name TEXT,
            opponent_name TEXT,
            scheduled_at TEXT,
            prop_type TEXT DEFAULT 'standard',
            UNIQUE(source, full_name, stat_name, stat_value, choice, scheduled_at)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_all_props_name_stat
        ON all_props(full_name, stat_name)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_all_props_scheduled
        ON all_props(scheduled_at)
    ''')

    # =========================================================================
    # PROP OUTCOMES TABLE — Labels for training
    # =========================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prop_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            player_id INTEGER,
            game_date TEXT NOT NULL,
            stat_type TEXT NOT NULL,
            line REAL NOT NULL,
            sportsbook TEXT,
            over_odds REAL,
            under_odds REAL,
            actual_value REAL,
            hit_over INTEGER,
            hit_under INTEGER,
            edge REAL,
            UNIQUE(player_name, game_date, stat_type, line, sportsbook)
        )
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_prop_outcomes_player_date
        ON prop_outcomes(player_name, game_date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_prop_outcomes_stat
        ON prop_outcomes(stat_type)
    ''')

    conn.commit()
    conn.close()


if __name__ == '__main__':
    init_database()
