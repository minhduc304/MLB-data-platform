"""Tests for StatcastCollector."""

import sqlite3
import sys
from types import ModuleType
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.db.init_db import init_database


def _make_pybaseball_mock():
    """
    Build a minimal pybaseball mock module so tests can run without pybaseball installed.
    The actual statcast() call is patched per-test.
    """
    mock_mod = ModuleType('pybaseball')
    mock_mod.statcast = MagicMock(return_value=pd.DataFrame())
    mock_cache = ModuleType('pybaseball.cache')
    mock_cache.enable = MagicMock()
    mock_mod.cache = mock_cache
    sys.modules.setdefault('pybaseball', mock_mod)
    sys.modules.setdefault('pybaseball.cache', mock_cache)
    return mock_mod


# Register mock module before any collector import
_make_pybaseball_mock()


def _make_raw_statcast_df():
    """Minimal synthetic statcast pitch DataFrame matching pybaseball schema."""
    return pd.DataFrame({
        'pitcher': [592662, 592662, 592662, 621381, 621381],
        'batter': [660271, 660271, 673357, 660271, 673357],
        'p_throws': ['R', 'R', 'R', 'L', 'L'],
        'pitch_type': ['FF', 'SL', 'FF', 'CH', 'CH'],
        'release_speed': [95.1, 84.2, 94.8, 87.3, 86.9],
        'release_spin_rate': [2350.0, 2100.0, 2380.0, 1850.0, 1820.0],
        'pfx_x': [0.5, -0.8, 0.6, 0.4, 0.3],   # feet
        'pfx_z': [1.1, 0.2, 1.0, 0.8, 0.7],     # feet
        'release_extension': [6.2, 6.1, 6.3, 5.9, 6.0],
        'description': [
            'swinging_strike', 'hit_into_play', 'swinging_strike',
            'foul', 'hit_into_play',
        ],
        'events': [None, 'single', None, None, 'field_out'],
        'estimated_ba_using_speedangle': [None, 0.380, None, None, 0.210],
        'estimated_woba_using_speedangle': [None, 0.450, None, None, 0.280],
    })


@pytest.fixture
def test_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_database(db_path)
    return db_path


def test_collect_date_inserts_rows(test_db):
    """collect_date() should populate both tables for a given date."""
    from src.collectors.statcast import StatcastCollector

    raw_df = _make_raw_statcast_df()
    with patch.object(sys.modules['pybaseball'], 'statcast', return_value=raw_df), \
         patch('src.collectors.statcast._enable_pybaseball_cache'):
        collector = StatcastCollector(test_db, season='2025')
        p_count, b_count = collector.collect_date('2025-07-01')

    assert p_count > 0, "Expected pitcher arsenal rows"
    assert b_count > 0, "Expected batter pitch-type rows"

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pitcher_arsenal")
    assert cursor.fetchone()[0] > 0
    cursor.execute("SELECT COUNT(*) FROM batter_pitch_type_stats")
    assert cursor.fetchone()[0] > 0
    conn.close()


def test_collect_date_is_idempotent(test_db):
    """Re-running collect_date() for the same date should not duplicate rows."""
    from src.collectors.statcast import StatcastCollector

    raw_df = _make_raw_statcast_df()
    with patch.object(sys.modules['pybaseball'], 'statcast', return_value=raw_df), \
         patch('src.collectors.statcast._enable_pybaseball_cache'):
        collector = StatcastCollector(test_db, season='2025')
        collector.collect_date('2025-07-01')
        _, _ = collector.collect_date('2025-07-01')

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pitcher_arsenal")
    count_after_second_run = cursor.fetchone()[0]
    conn.close()

    # Count should be same as after first run — no duplicates
    raw_df2 = _make_raw_statcast_df()
    with patch('pybaseball.statcast', return_value=raw_df2), \
         patch('src.collectors.statcast._enable_pybaseball_cache'):
        collector2 = StatcastCollector(test_db, season='2025')
        collector2.collect_date('2025-07-01')

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pitcher_arsenal")
    count_after_third_run = cursor.fetchone()[0]
    conn.close()

    assert count_after_second_run == count_after_third_run


def test_excluded_pitch_types_are_filtered(test_db):
    """Pitchouts (PO) and unknown pitches (UN) should not appear in the DB."""
    from src.collectors.statcast import StatcastCollector

    raw_df = _make_raw_statcast_df()
    raw_df.loc[0, 'pitch_type'] = 'PO'   # pitchout
    raw_df.loc[1, 'pitch_type'] = 'UN'   # unknown

    with patch.object(sys.modules['pybaseball'], 'statcast', return_value=raw_df), \
         patch('src.collectors.statcast._enable_pybaseball_cache'):
        collector = StatcastCollector(test_db, season='2025')
        collector.collect_date('2025-07-01')

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT pitch_type FROM pitcher_arsenal")
    pitch_types = {row[0] for row in cursor.fetchall()}
    conn.close()

    assert 'PO' not in pitch_types
    assert 'UN' not in pitch_types


def test_missing_arm_angle_column_is_handled(test_db):
    """Statcast data without arm_angle column should not raise an error."""
    from src.collectors.statcast import StatcastCollector

    raw_df = _make_raw_statcast_df()
    # Simulate pre-2021 data: no arm_angle column
    assert 'arm_angle' not in raw_df.columns

    with patch.object(sys.modules['pybaseball'], 'statcast', return_value=raw_df), \
         patch('src.collectors.statcast._enable_pybaseball_cache'):
        collector = StatcastCollector(test_db, season='2021')
        p_count, b_count = collector.collect_date('2021-07-01')

    assert p_count > 0

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT avg_arm_angle FROM pitcher_arsenal LIMIT 1")
    arm_angle = cursor.fetchone()[0]
    conn.close()

    assert arm_angle is None  # Should be NULL, not an error


def test_pfx_converted_to_inches(test_db):
    """pfx_x and pfx_z from pybaseball (feet) should be stored as inches."""
    from src.collectors.statcast import StatcastCollector

    raw_df = _make_raw_statcast_df()
    # pitcher 592662 FF: pfx_x=0.5ft, pfx_z=1.1ft → should store ~6.0in, ~13.2in

    with patch.object(sys.modules['pybaseball'], 'statcast', return_value=raw_df), \
         patch('src.collectors.statcast._enable_pybaseball_cache'):
        collector = StatcastCollector(test_db, season='2025')
        collector.collect_date('2025-07-01')

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT avg_pfx_x FROM pitcher_arsenal WHERE pitcher_id=592662 AND pitch_type='FF'"
    )
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert abs(row[0]) > 1.0  # Should be in inches (> 1), not feet (~0.5)


def test_empty_statcast_response(test_db):
    """Empty pybaseball response should return (0, 0) without error."""
    from src.collectors.statcast import StatcastCollector

    with patch('pybaseball.statcast', return_value=pd.DataFrame()), \
         patch('src.collectors.statcast._enable_pybaseball_cache'):
        collector = StatcastCollector(test_db, season='2025')
        p_count, b_count = collector.collect_date('2025-07-01')

    assert p_count == 0
    assert b_count == 0
