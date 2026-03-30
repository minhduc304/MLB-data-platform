"""
Backfill opposing_pitcher_id and opposing_pitcher_hand in batter_game_logs.

Strategy (no extra game-log API calls needed):
  1. Join batter_game_logs with pitcher_game_logs on same game_id
     where pitcher.team_id != batter.team_id and pitcher.is_start = 1
     → gives us the opposing starter's player_id for each batter game
  2. Fetch pitchHand for each unique starter via the person endpoint (~510 calls)
  3. UPDATE batter_game_logs with opposing_pitcher_id and opposing_pitcher_hand
  4. Recompute rolling stats so platoon splits are populated
"""

import logging
import sqlite3
import time

import statsapi

from src.ml_pipeline.rolling_stats import compute_batter_rolling_stats

logger = logging.getLogger(__name__)


def _fetch_pitcher_hands(pitcher_ids: list[int], delay: float = 0.5) -> dict[int, str]:
    """
    Fetch pitchHand code ('L' or 'R') for each pitcher_id via the person endpoint.
    Returns {player_id: hand}.
    """
    hands = {}
    total = len(pitcher_ids)
    for i, pid in enumerate(pitcher_ids, 1):
        try:
            data = statsapi.get("person", {"personId": pid})
            people = data.get("people", [{}])
            hand = people[0].get("pitchHand", {}).get("code") if people else None
            hands[pid] = hand
        except Exception as e:
            logger.warning(f"[backfill] Could not fetch hand for pitcher {pid}: {e}")
            hands[pid] = None

        if i % 50 == 0 or i == total:
            logger.info(f"[backfill] Fetched pitcher hands: {i}/{total}")

        time.sleep(delay)

    return hands


def backfill_opposing_pitcher_hand(db_path: str, delay: float = 0.5) -> int:
    """
    Populate opposing_pitcher_id and opposing_pitcher_hand in batter_game_logs,
    then recompute batter_rolling_stats so platoon splits are populated.

    Args:
        db_path: Path to SQLite database
        delay: API delay between pitcher hand lookups (seconds)

    Returns:
        Number of batter game log rows updated
    """
    conn = sqlite3.connect(db_path)

    # ----------------------------------------------------------------
    # Step 1: Find opposing starter for each batter game log row
    # ----------------------------------------------------------------
    logger.info("[backfill] Mapping batter game logs to opposing starters...")
    rows = conn.execute("""
        SELECT bgl.rowid, bgl.game_id, bgl.player_id, pgl.player_id AS pitcher_id
        FROM batter_game_logs bgl
        JOIN pitcher_game_logs pgl
            ON  pgl.game_id   = bgl.game_id
            AND pgl.is_start  = 1
            AND pgl.team_id  != bgl.team_id
        WHERE bgl.opposing_pitcher_id IS NULL
    """).fetchall()

    logger.info(f"[backfill] {len(rows)} batter rows need opposing_pitcher_id")

    if not rows:
        logger.info("[backfill] Nothing to update — already fully populated")
        conn.close()
        return 0

    # ----------------------------------------------------------------
    # Step 2: Collect unique pitcher IDs and fetch their hand
    # ----------------------------------------------------------------
    unique_pitcher_ids = list({r[3] for r in rows if r[3] is not None})
    logger.info(f"[backfill] Fetching pitchHand for {len(unique_pitcher_ids)} unique starters...")
    pitcher_hands = _fetch_pitcher_hands(unique_pitcher_ids, delay=delay)

    # ----------------------------------------------------------------
    # Step 3: Update batter_game_logs in bulk
    # ----------------------------------------------------------------
    logger.info("[backfill] Updating batter_game_logs...")
    update_rows = [
        (r[3], pitcher_hands.get(r[3]), r[0])   # (pitcher_id, hand, rowid)
        for r in rows
    ]

    conn.executemany("""
        UPDATE batter_game_logs
        SET opposing_pitcher_id = ?, opposing_pitcher_hand = ?
        WHERE rowid = ?
    """, update_rows)
    conn.commit()

    updated = conn.execute(
        "SELECT COUNT(*) FROM batter_game_logs WHERE opposing_pitcher_hand IS NOT NULL"
    ).fetchone()[0]
    logger.info(f"[backfill] Updated {len(update_rows)} rows — {updated} total now have pitcher hand")
    conn.close()

    # ----------------------------------------------------------------
    # Step 4: Recompute rolling stats with platoon splits
    # ----------------------------------------------------------------
    logger.info("[backfill] Recomputing batter rolling stats with platoon splits...")
    count = compute_batter_rolling_stats(db_path)
    logger.info(f"[backfill] Done — {count} rolling stat rows recomputed")

    return len(update_rows)
