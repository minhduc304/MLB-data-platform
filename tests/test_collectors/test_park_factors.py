"""Tests for ParkFactorsCollector."""

import sqlite3

import pytest

from src.collectors.park_factors import (
    ParkFactorsCollector,
    PARK_FACTORS,
    FACTOR_TYPES,
    DEFAULT_FACTORS,
)


# ---- Fixtures ----

@pytest.fixture
def seeded_db(test_db):
    """Test DB with a few venues seeded."""
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO venues (venue_id, name, city, state) VALUES (2399, 'Coors Field', 'Denver', 'CO')")
    cursor.execute("INSERT INTO venues (venue_id, name, city, state) VALUES (3, 'Fenway Park', 'Boston', 'MA')")
    cursor.execute("INSERT INTO venues (venue_id, name, city, state) VALUES (99999, 'Unknown Park', 'Nowhere', 'XX')")
    conn.commit()
    conn.close()
    return test_db


# ---- Tests ----

class TestParkFactorsCollector:

    def test_known_venue_factors(self, seeded_db):
        """Known venues get their specific park factors."""
        collector = ParkFactorsCollector(seeded_db, season="2026")
        collector.collect()

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT factor_value FROM park_factors WHERE venue_id = 2399 AND factor_type = 'hr' AND season = '2026'"
        )
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row[0] == PARK_FACTORS[2399]["hr"]

    def test_unknown_venue_defaults(self, seeded_db):
        """Unknown venues get default factor of 1.0 for all types."""
        collector = ParkFactorsCollector(seeded_db, season="2026")
        collector.collect()

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT factor_type, factor_value FROM park_factors WHERE venue_id = 99999"
        )
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == len(FACTOR_TYPES)
        for _, value in rows:
            assert value == 1.0

    def test_all_factor_types_present(self, seeded_db):
        """Each venue has all 5 factor types."""
        collector = ParkFactorsCollector(seeded_db, season="2026")
        collector.collect()

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT factor_type FROM park_factors WHERE venue_id = 3 AND season = '2026'"
        )
        types = {row[0] for row in cursor.fetchall()}
        conn.close()

        assert types == set(FACTOR_TYPES)

    def test_correct_total_count(self, seeded_db):
        """Total rows = number of venues * number of factor types."""
        collector = ParkFactorsCollector(seeded_db, season="2026")
        count = collector.collect()

        assert count == 3 * len(FACTOR_TYPES)  # 3 venues * 5 types

    def test_season_parameter(self, seeded_db):
        """Different seasons produce separate rows."""
        collector_2025 = ParkFactorsCollector(seeded_db, season="2025")
        collector_2026 = ParkFactorsCollector(seeded_db, season="2026")
        collector_2025.collect()
        collector_2026.collect()

        conn = sqlite3.connect(seeded_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM park_factors")
        total = cursor.fetchone()[0]
        conn.close()

        # 3 venues * 5 types * 2 seasons = 30
        assert total == 3 * len(FACTOR_TYPES) * 2
