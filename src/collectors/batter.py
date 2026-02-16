"""Batter stats and game log collectors."""

import logging
import sqlite3
from datetime import datetime

from src.api.client import MLBAPIClient
from src.config import CURRENT_SEASON

logger = logging.getLogger(__name__)

BAT_SIDE_MAP = {
    "Left": "L",
    "Right": "R",
    "Switch": "S",
}


class BatterStatsCollector:
    """Collect season-level batting stats for all rostered non-pitcher players."""

    def __init__(self, db_path: str, client: MLBAPIClient = None, season: str = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()
        self.season = season or CURRENT_SEASON

    def collect(self) -> int:
        """
        Iterate all teams, fetch active rosters, collect hitting stats for non-pitchers.

        Returns:
            Number of players inserted/updated
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            # Get all team IDs
            cursor.execute("SELECT team_id FROM teams")
            team_ids = [row[0] for row in cursor.fetchall()]

            for team_id in team_ids:
                try:
                    roster = self.client.get_roster_data(team_id, self.season)
                except Exception as e:
                    logger.warning(f"Failed to get roster for team {team_id}: {e}")
                    continue

                for entry in roster:
                    person = entry.get("person", {})
                    player_id = person.get("id")
                    player_name = person.get("fullName", "")

                    position = entry.get("position", {})
                    pos_abbrev = position.get("abbreviation", "")

                    # Skip pitchers
                    if pos_abbrev == "P":
                        continue

                    try:
                        stats_data = self.client.get_player_hitting_stats(player_id, self.season)
                    except Exception as e:
                        logger.debug(f"No hitting stats for {player_name} ({player_id}): {e}")
                        continue

                    stats_list = stats_data.get("stats", [])
                    if not stats_list:
                        continue

                    # Find season stats
                    stat = None
                    for s in stats_list:
                        if s.get("type", {}).get("displayName") == "season":
                            stat = s.get("stats", {})
                            break

                    if not stat:
                        continue

                    games_played = int(stat.get("gamesPlayed", 0))

                    # Check if update is needed
                    if not self._should_update(cursor, player_id, games_played):
                        continue

                    bat_side_raw = stats_data.get("bat_side", "")
                    bats = BAT_SIDE_MAP.get(bat_side_raw, bat_side_raw)

                    cursor.execute('''
                        INSERT OR REPLACE INTO batter_stats
                        (player_id, player_name, team_id, position, season,
                         games_played, plate_appearances, at_bats, hits, doubles,
                         triples, home_runs, rbi, runs, stolen_bases,
                         caught_stealing, walks, strikeouts, batting_avg, obp,
                         slg, ops, total_bases, bats, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        player_id, player_name, team_id, pos_abbrev, self.season,
                        games_played,
                        int(stat.get("plateAppearances", 0)),
                        int(stat.get("atBats", 0)),
                        int(stat.get("hits", 0)),
                        int(stat.get("doubles", 0)),
                        int(stat.get("triples", 0)),
                        int(stat.get("homeRuns", 0)),
                        int(stat.get("rbi", 0)),
                        int(stat.get("runs", 0)),
                        int(stat.get("stolenBases", 0)),
                        int(stat.get("caughtStealing", 0)),
                        int(stat.get("baseOnBalls", 0)),
                        int(stat.get("strikeOuts", 0)),
                        float(stat.get("avg", ".000").replace(".", "0.", 1)) if isinstance(stat.get("avg"), str) else float(stat.get("avg", 0)),
                        float(stat.get("obp", ".000").replace(".", "0.", 1)) if isinstance(stat.get("obp"), str) else float(stat.get("obp", 0)),
                        float(stat.get("slg", ".000").replace(".", "0.", 1)) if isinstance(stat.get("slg"), str) else float(stat.get("slg", 0)),
                        float(stat.get("ops", ".000").replace(".", "0.", 1)) if isinstance(stat.get("ops"), str) else float(stat.get("ops", 0)),
                        int(stat.get("totalBases", 0)),
                        bats,
                        datetime.now().isoformat(),
                    ))
                    count += 1

            conn.commit()
            logger.info(f"Collected stats for {count} batters")
        finally:
            conn.close()

        return count

    def _should_update(self, cursor, player_id: int, current_games: int) -> bool:
        """Check if player stats need updating (games_played changed)."""
        cursor.execute(
            "SELECT games_played FROM batter_stats WHERE player_id = ?",
            (player_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return True
        return row[0] != current_games


class BatterGameLogCollector:
    """Collect game-by-game batting logs."""

    def __init__(self, db_path: str, client: MLBAPIClient = None, season: str = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()
        self.season = season or CURRENT_SEASON

    def collect(self, historical_season: str = None) -> int:
        """
        Fetch game logs for all batters in batter_stats.

        Args:
            historical_season: If set, fetch logs for this past season instead

        Returns:
            Number of game log entries inserted
        """
        target_season = historical_season or self.season
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            cursor.execute("SELECT player_id, player_name, team_id FROM batter_stats")
            players = cursor.fetchall()

            for player_id, player_name, team_id in players:
                last_date = self._get_last_game_date(cursor, player_id, target_season)

                try:
                    if historical_season:
                        raw = self.client.get_player_game_log_by_season(player_id, "hitting", historical_season)
                        games = self._parse_raw_game_log(raw)
                    else:
                        data = self.client.get_hitting_game_log(player_id)
                        games = self._parse_player_stat_data(data)
                except Exception as e:
                    logger.debug(f"No game log for {player_name} ({player_id}): {e}")
                    continue

                for game in games:
                    game_date = game.get("date", "")

                    # Incremental: skip already-collected dates
                    if last_date and game_date <= last_date:
                        continue

                    game_id = game.get("game", {}).get("gamePk", 0)
                    stat = game.get("stat", {})

                    opponent_id, opponent_abbr, is_home, venue_id = self._get_game_context(
                        cursor, game_id, team_id
                    )

                    cursor.execute('''
                        INSERT OR IGNORE INTO batter_game_logs
                        (player_id, game_id, game_date, season, team_id,
                         opponent_id, opponent_abbr, is_home, batting_order,
                         plate_appearances, at_bats, hits, doubles, triples,
                         home_runs, rbi, runs, stolen_bases, walks,
                         strikeouts, total_bases,
                         opposing_pitcher_id, opposing_pitcher_hand, venue_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        player_id, game_id, game_date, target_season, team_id,
                        opponent_id, opponent_abbr, is_home, None,  # batting_order deferred
                        int(stat.get("plateAppearances", 0)),
                        int(stat.get("atBats", 0)),
                        int(stat.get("hits", 0)),
                        int(stat.get("doubles", 0)),
                        int(stat.get("triples", 0)),
                        int(stat.get("homeRuns", 0)),
                        int(stat.get("rbi", 0)),
                        int(stat.get("runs", 0)),
                        int(stat.get("stolenBases", 0)),
                        int(stat.get("baseOnBalls", 0)),
                        int(stat.get("strikeOuts", 0)),
                        int(stat.get("totalBases", 0)),
                        None,  # opposing_pitcher_id deferred
                        None,  # opposing_pitcher_hand deferred
                        venue_id,
                    ))

                    if cursor.rowcount > 0:
                        count += 1

            conn.commit()
            logger.info(f"Collected {count} batter game log entries")
        finally:
            conn.close()

        return count

    def _get_last_game_date(self, cursor, player_id: int, season: str):
        """Get the most recent game date already collected for a player."""
        cursor.execute(
            "SELECT MAX(game_date) FROM batter_game_logs WHERE player_id = ? AND season = ?",
            (player_id, season)
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] else None

    def _get_game_context(self, cursor, game_id: int, team_id: int):
        """Look up opponent, home/away, and venue from the schedule table."""
        cursor.execute(
            "SELECT home_team_id, away_team_id, home_abbr, away_abbr, venue_id FROM schedule WHERE game_id = ?",
            (game_id,)
        )
        row = cursor.fetchone()
        if not row:
            return None, None, None, None

        home_team_id, away_team_id, home_abbr, away_abbr, venue_id = row
        if team_id == home_team_id:
            return away_team_id, away_abbr, 1, venue_id
        else:
            return home_team_id, home_abbr, 0, venue_id

    def _parse_player_stat_data(self, data: dict) -> list:
        """Parse game log entries from player_stat_data response."""
        splits = []
        for stat_group in data.get("stats", []):
            if stat_group.get("type", {}).get("displayName") == "gameLog":
                splits = stat_group.get("stats", [])
                break
        return splits

    def _parse_raw_game_log(self, raw: dict) -> list:
        """Parse game log entries from raw statsapi.get('person') response."""
        people = raw.get("people", [])
        if not people:
            return []
        stats = people[0].get("stats", [])
        if not stats:
            return []
        return stats[0].get("splits", [])
