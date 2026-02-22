"""Park factors collector with static seed data."""

import logging
import sqlite3

from src.config import CURRENT_SEASON

logger = logging.getLogger(__name__)

# Factor types seeded per venue
FACTOR_TYPES = ("overall", "hr", "h", "k", "bb")

# Static park factor data: venue_id -> {factor_type: value}
# Sources: ESPN Park Factors, FanGraphs Guts! page (approximate 3-year averages)
PARK_FACTORS = {
    # Coors Field (COL)
    2399: {"overall": 1.32, "hr": 1.27, "h": 1.36, "k": 0.92, "bb": 1.05},
    # Fenway Park (BOS)
    3: {"overall": 1.08, "hr": 1.10, "h": 1.10, "k": 0.97, "bb": 1.02},
    # Great American Ball Park (CIN)
    2602: {"overall": 1.10, "hr": 1.22, "h": 1.05, "k": 0.97, "bb": 0.99},
    # Globe Life Field (TEX)
    5325: {"overall": 0.96, "hr": 0.94, "h": 0.97, "k": 1.01, "bb": 1.00},
    # Wrigley Field (CHC)
    17: {"overall": 1.05, "hr": 1.13, "h": 1.02, "k": 0.97, "bb": 1.01},
    # Yankee Stadium (NYY)
    3313: {"overall": 1.05, "hr": 1.15, "h": 1.00, "k": 0.99, "bb": 1.01},
    # Citizens Bank Park (PHI)
    2681: {"overall": 1.06, "hr": 1.12, "h": 1.03, "k": 0.98, "bb": 1.00},
    # Guaranteed Rate Field (CWS)
    4: {"overall": 1.04, "hr": 1.11, "h": 1.01, "k": 0.99, "bb": 1.00},
    # Minute Maid Park (HOU)
    2392: {"overall": 1.02, "hr": 1.05, "h": 1.01, "k": 0.99, "bb": 1.00},
    # Target Field (MIN)
    3312: {"overall": 1.01, "hr": 1.03, "h": 1.00, "k": 0.99, "bb": 1.01},
    # Angel Stadium (LAA)
    1: {"overall": 0.97, "hr": 0.95, "h": 0.98, "k": 1.00, "bb": 1.00},
    # Dodger Stadium (LAD)
    22: {"overall": 0.97, "hr": 0.96, "h": 0.98, "k": 1.01, "bb": 0.99},
    # Oracle Park (SF)
    2395: {"overall": 0.93, "hr": 0.82, "h": 0.96, "k": 1.03, "bb": 1.00},
    # T-Mobile Park (SEA)
    680: {"overall": 0.94, "hr": 0.88, "h": 0.96, "k": 1.02, "bb": 1.00},
    # Petco Park (SD)
    2680: {"overall": 0.94, "hr": 0.88, "h": 0.96, "k": 1.02, "bb": 0.99},
    # Tropicana Field (TB)
    12: {"overall": 0.95, "hr": 0.90, "h": 0.97, "k": 1.01, "bb": 1.00},
    # Kauffman Stadium (KC)
    7: {"overall": 0.97, "hr": 0.90, "h": 1.00, "k": 1.01, "bb": 0.99},
    # Busch Stadium (STL)
    2889: {"overall": 0.98, "hr": 0.95, "h": 0.99, "k": 1.01, "bb": 1.00},
    # PNC Park (PIT)
    31: {"overall": 0.97, "hr": 0.92, "h": 0.99, "k": 1.01, "bb": 1.00},
    # Progressive Field (CLE)
    5: {"overall": 0.99, "hr": 0.98, "h": 1.00, "k": 1.00, "bb": 1.00},
    # Nationals Park (WSH)
    3309: {"overall": 1.00, "hr": 1.02, "h": 0.99, "k": 1.00, "bb": 1.00},
    # Citi Field (NYM)
    3289: {"overall": 0.96, "hr": 0.93, "h": 0.97, "k": 1.01, "bb": 1.00},
    # Truist Park (ATL)
    4705: {"overall": 1.01, "hr": 1.04, "h": 1.00, "k": 0.99, "bb": 1.00},
    # American Family Field (MIL)
    32: {"overall": 1.02, "hr": 1.07, "h": 1.00, "k": 0.99, "bb": 1.00},
    # Comerica Park (DET)
    2394: {"overall": 0.98, "hr": 0.93, "h": 0.99, "k": 1.01, "bb": 1.00},
    # Chase Field (ARI)
    15: {"overall": 1.04, "hr": 1.08, "h": 1.02, "k": 0.98, "bb": 1.00},
    # loanDepot Park (MIA)
    4169: {"overall": 0.93, "hr": 0.87, "h": 0.95, "k": 1.03, "bb": 1.00},
    # Rogers Centre (TOR)
    14: {"overall": 1.02, "hr": 1.06, "h": 1.00, "k": 0.99, "bb": 1.00},
    # Oakland Coliseum (OAK)
    10: {"overall": 0.94, "hr": 0.86, "h": 0.97, "k": 1.02, "bb": 1.00},
    # Camden Yards (BAL)
    2: {"overall": 1.03, "hr": 1.09, "h": 1.01, "k": 0.99, "bb": 1.00},
}

# Default factors for unknown venues
DEFAULT_FACTORS = {ft: 1.0 for ft in FACTOR_TYPES}


class ParkFactorsCollector:
    """Seed park factor data from static lookup. No API dependency."""

    def __init__(self, db_path: str, season: str = None):
        self.db_path = db_path
        self.season = season or CURRENT_SEASON

    def collect(self) -> int:
        """
        Insert park factors for all known venues.

        Returns:
            Number of factor rows inserted/updated
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            # Get all venue IDs from the venues table
            cursor.execute("SELECT venue_id FROM venues")
            venue_ids = [row[0] for row in cursor.fetchall()]

            for venue_id in venue_ids:
                factors = PARK_FACTORS.get(venue_id, DEFAULT_FACTORS)

                for factor_type in FACTOR_TYPES:
                    factor_value = factors.get(factor_type, 1.0)
                    cursor.execute('''
                        INSERT OR REPLACE INTO park_factors
                        (venue_id, season, factor_type, factor_value)
                        VALUES (?, ?, ?, ?)
                    ''', (venue_id, self.season, factor_type, factor_value))
                    count += 1

            conn.commit()
            logger.info(f"Seeded {count} park factor entries for season {self.season}")
        finally:
            conn.close()

        return count
