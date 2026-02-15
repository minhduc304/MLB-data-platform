"""
MLB Stats Database Initialization

Creates all required database tables for the MLB Prop Prediction System.
Phase 1: teams and venues tables only. Later phases add player/game tables.

Usage:
    ./mlb collect init-db
"""

import os
import sqlite3


def init_database(db_path: str = None) -> None:
    """
    Create database tables for the MLB Prop Prediction System.

    Phase 1 tables:
        - teams: MLB team information
        - venues: Ballpark information with park factors

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

    conn.commit()
    conn.close()


if __name__ == '__main__':
    init_database()
