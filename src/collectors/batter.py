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


def _parse_rate_stat(value, default=0.0) -> float:
    """Parse a batting rate stat that the API returns as a string like '.333' or '1.167'."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("."):
            s = "0" + s
        try:
            return float(s)
        except ValueError:
            pass
    return default


class BatterStatsCollector:
    """Collect season-level batting stats for all rostered non-pitcher players."""

    def __init__(self, db_path: str, client: MLBAPIClient = None, season: str = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()
        self.season = season or CURRENT_SEASON

    def collect(self) -> int:
        """
        Collect hitting stats for players on teams that have played since the last update.

        Skips all API calls on off-days or when no new games have been completed.

        Returns:
            Number of players inserted/updated
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            active_team_ids = self._get_active_team_ids(cursor)
            if not active_team_ids:
                logger.info("[batters] No completed games since last collection — skipping")
                return 0

            placeholders = ",".join("?" * len(active_team_ids))
            cursor.execute(
                f"SELECT team_id, name FROM teams WHERE team_id IN ({placeholders})",
                list(active_team_ids),
            )
            teams = cursor.fetchall()
            logger.info(f"[batters] {len(teams)} teams played since last update — fetching stats")

            for team_id, team_name in teams:
                try:
                    roster = self.client.get_roster_data(team_id, self.season)
                except Exception as e:
                    logger.warning(f"Failed to get roster for {team_name}: {e}")
                    continue

                team_count = 0
                skipped = 0
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
                        if s.get("type") == "season":
                            stat = s.get("stats", {})
                            break

                    if not stat:
                        continue

                    games_played = int(stat.get("gamesPlayed", 0))

                    # Skip if games_played hasn't changed since last collection
                    if not self._should_update(cursor, player_id, games_played):
                        skipped += 1
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
                        _parse_rate_stat(stat.get("avg")),
                        _parse_rate_stat(stat.get("obp")),
                        _parse_rate_stat(stat.get("slg")),
                        _parse_rate_stat(stat.get("ops")),
                        int(stat.get("totalBases", 0)),
                        bats,
                        datetime.now().isoformat(),
                    ))
                    count += 1
                    team_count += 1

                conn.commit()
                msg = f"[batters] {team_name}: +{team_count} updated"
                if skipped:
                    msg += f", {skipped} unchanged"
                logger.info(msg)

            logger.info(f"Done — updated {count} batters for {self.season}")
        finally:
            conn.close()

        return count

    def _get_active_team_ids(self, cursor) -> set:
        """Return team IDs that played in completed games since the last stats collection."""
        cursor.execute(
            "SELECT MAX(last_updated) FROM batter_stats WHERE season = ?", (self.season,)
        )
        row = cursor.fetchone()
        last_updated = row[0][:10] if row and row[0] else "2000-01-01"

        cursor.execute("""
            SELECT DISTINCT home_team_id, away_team_id
            FROM schedule
            WHERE game_type = 'R'
              AND season = ?
              AND game_date > ?
              AND game_date <= date('now', '-1 day')
        """, (self.season, last_updated))

        team_ids = set()
        for home_id, away_id in cursor.fetchall():
            team_ids.add(home_id)
            team_ids.add(away_id)
        return team_ids

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
            if not historical_season:
                count = self._collect_incremental(cursor, conn, target_season)
            else:
                count = self._collect_historical(cursor, conn, historical_season)

            logger.info(f"Done — collected {count} batter game log entries for {target_season}")
        finally:
            conn.close()

        return count

    def _collect_incremental(self, cursor, conn, season: str) -> int:
        """
        Incremental collection using boxscores to find only players who appeared.

        1. Find regular season games not yet in batter_game_logs
        2. For each game fetch boxscore → actual batter IDs
        3. Deduplicate player IDs across all games
        4. Fetch each player's game log once, insert only rows for uncollected games
        """
        cursor.execute("""
            SELECT s.game_id, s.game_date
            FROM schedule s
            WHERE s.game_type = 'R'
              AND s.season = ?
              AND s.game_date <= date('now', '-1 day')
              AND s.game_id NOT IN (
                  SELECT DISTINCT game_id FROM batter_game_logs WHERE season = ?
              )
            ORDER BY s.game_date
        """, (season, season))
        uncollected = cursor.fetchall()

        if not uncollected:
            logger.info(f"[batters] No uncollected games for {season}")
            return 0

        logger.info(f"[batters] {len(uncollected)} uncollected games — fetching boxscores")

        # game_id -> game_date, for filtering game log responses
        game_date_map = {game_id: game_date for game_id, game_date in uncollected}

        # Build player_id -> team_id map from boxscore home/away sides
        player_team_map: dict[int, int] = {}
        for game_id, game_date in uncollected:
            try:
                boxscore = self.client.get_boxscore_data(game_id)
            except Exception as e:
                logger.warning(f"[batters] Could not fetch boxscore for game {game_id}: {e}")
                continue

            row = cursor.execute(
                "SELECT home_team_id, away_team_id FROM schedule WHERE game_id = ?", (game_id,)
            ).fetchone()
            if not row:
                continue
            home_team_id, away_team_id = row

            for entry in boxscore.get('homeBatters', []):
                pid = entry.get('personId', 0)
                if pid != 0:
                    player_team_map[pid] = home_team_id

            for entry in boxscore.get('awayBatters', []):
                pid = entry.get('personId', 0)
                if pid != 0:
                    player_team_map[pid] = away_team_id

        logger.info(f"[batters] {len(player_team_map)} batters appeared — fetching game logs")

        count = 0
        for i, player_id in enumerate(player_team_map, 1):
            try:
                data = self.client.get_hitting_game_log(player_id)
                games = self._parse_raw_game_log(data)
            except Exception as e:
                logger.debug(f"No game log for player {player_id}: {e}")
                continue

            player_count = 0
            for game in games:
                if not isinstance(game, dict):
                    continue
                gid = game.get("game", {}).get("gamePk", 0)
                if gid not in game_date_map:
                    continue

                game_date = game_date_map[gid]
                stat = game.get("stat", {})
                team_id = player_team_map.get(player_id)
                opponent_id, opponent_abbr, is_home, venue_id = self._get_game_context(
                    cursor, gid, team_id
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
                    player_id, gid, game_date, season, team_id,
                    opponent_id, opponent_abbr, is_home, None,
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
                    None, None,
                    venue_id,
                ))

                if cursor.rowcount > 0:
                    count += 1
                    player_count += 1

            conn.commit()
            if player_count > 0 or i % 25 == 0:
                logger.info(f"[batters] [{i}/{len(player_team_map)}] player {player_id}: +{player_count} games — {count} total inserted")

        return count

    def _collect_historical(self, cursor, conn, historical_season: str) -> int:
        """Historical backfill — loop all players, incremental by last collected date."""
        cursor.execute("SELECT player_id, player_name, team_id FROM batter_stats")
        players = cursor.fetchall()
        total_players = len(players)
        logger.info(f"Collecting {historical_season} batter game logs for {total_players} players...")

        count = 0
        for i, (player_id, player_name, team_id) in enumerate(players, 1):
            last_date = self._get_last_game_date(cursor, player_id, historical_season)

            try:
                raw = self.client.get_player_game_log_by_season(player_id, "hitting", historical_season)
                games = self._parse_raw_game_log(raw)
            except Exception as e:
                logger.debug(f"No game log for {player_name} ({player_id}): {e}")
                continue

            player_count = 0
            for game in games:
                if not isinstance(game, dict):
                    continue
                game_date = game.get("date", "")
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
                    player_id, game_id, game_date, historical_season, team_id,
                    opponent_id, opponent_abbr, is_home, None,
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
                    None, None,
                    venue_id,
                ))

                if cursor.rowcount > 0:
                    count += 1
                    player_count += 1

            conn.commit()
            if player_count > 0 or i % 50 == 0:
                logger.info(f"[{i}/{total_players}] {player_name}: +{player_count} games — {count} total")

        return count

    def _resolve_team_id(self, cursor, player_id: int, season: str):
        """Look up a player's team_id from batter_stats."""
        cursor.execute(
            "SELECT team_id FROM batter_stats WHERE player_id = ?", (player_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

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
            if stat_group.get("type") == "gameLog":
                splits = stat_group.get("splits", stat_group.get("stats", []))
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
