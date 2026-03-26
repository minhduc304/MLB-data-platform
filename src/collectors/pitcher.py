"""Pitcher stats and game log collectors."""

import logging
import sqlite3
from datetime import datetime

from src.api.client import MLBAPIClient
from src.config import CURRENT_SEASON

logger = logging.getLogger(__name__)


def _safe_float(value, default=0.0) -> float:
    """Convert a stat value to float, returning default for placeholders like '-.--'."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _ip_to_outs(ip) -> int:
    """
    Convert innings pitched to total outs.

    IP is represented as whole innings + fractional thirds:
    6.2 IP = 6 full innings + 2 outs = 20 outs (not 6.2 * 3).

    Args:
        ip: Innings pitched (float or string)

    Returns:
        Total outs as integer
    """
    ip_float = float(ip)
    whole = int(ip_float)
    fraction = round((ip_float - whole) * 10)
    return whole * 3 + fraction


class PitcherStatsCollector:
    """Collect season-level pitching stats for all rostered pitchers."""

    def __init__(self, db_path: str, client: MLBAPIClient = None, season: str = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()
        self.season = season or CURRENT_SEASON

    def collect(self) -> int:
        """
        Iterate all teams, fetch active rosters, collect pitching stats.

        Incremental: pitchers already collected this season are skipped (no API call).

        Returns:
            Number of pitchers inserted/updated
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        count = 0

        try:
            cursor.execute("SELECT team_id, name FROM teams")
            teams = cursor.fetchall()

            # Pre-load already-collected pitcher IDs to skip API calls on resume
            cursor.execute(
                "SELECT player_id FROM pitcher_stats WHERE season = ?", (self.season,)
            )
            already_collected = {row[0] for row in cursor.fetchall()}
            if already_collected:
                logger.info(f"Resuming: {len(already_collected)} pitchers already collected for {self.season}")

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

                    # Only pitchers
                    if pos_abbrev != "P":
                        continue

                    # Skip pitchers already in DB (resumable)
                    if player_id in already_collected:
                        skipped += 1
                        continue

                    try:
                        stats_data = self.client.get_player_pitching_stats(player_id, self.season)
                    except Exception as e:
                        logger.debug(f"No pitching stats for {player_name} ({player_id}): {e}")
                        continue

                    stats_list = stats_data.get("stats", [])
                    if not stats_list:
                        continue

                    stat = None
                    for s in stats_list:
                        if s.get("type") == "season":
                            stat = s.get("stats", {})
                            break

                    if not stat:
                        continue

                    games_played = int(stat.get("gamesPlayed", 0))

                    games_started = int(stat.get("gamesStarted", 0))

                    # Determine SP/RP role: SP if games_started >= 50% of games_played
                    if games_played > 0 and games_started >= (games_played * 0.5):
                        role = "SP"
                    else:
                        role = "RP"

                    throws = stats_data.get("pitch_hand", "")
                    if throws == "Left":
                        throws = "L"
                    elif throws == "Right":
                        throws = "R"

                    ip = _safe_float(stat.get("inningsPitched", 0))
                    k = int(stat.get("strikeOuts", 0))
                    bb = int(stat.get("baseOnBalls", 0))

                    # Calculate rate stats safely
                    k_per_9 = (k / ip * 9) if ip > 0 else 0.0
                    bb_per_9 = (bb / ip * 9) if ip > 0 else 0.0
                    k_bb_ratio = (k / bb) if bb > 0 else 0.0

                    cursor.execute('''
                        INSERT OR REPLACE INTO pitcher_stats
                        (player_id, player_name, team_id, position, season,
                         games_played, games_started, innings_pitched, wins, losses,
                         era, whip, strikeouts, walks_allowed, hits_allowed,
                         home_runs_allowed, earned_runs, k_per_9, bb_per_9,
                         k_bb_ratio, throws, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        player_id, player_name, team_id, role, self.season,
                        games_played, games_started, ip,
                        int(stat.get("wins", 0)),
                        int(stat.get("losses", 0)),
                        _safe_float(stat.get("era", 0)),
                        _safe_float(stat.get("whip", 0)),
                        k, bb,
                        int(stat.get("hits", 0)),
                        int(stat.get("homeRuns", 0)),
                        int(stat.get("earnedRuns", 0)),
                        k_per_9, bb_per_9, k_bb_ratio,
                        throws,
                        datetime.now().isoformat(),
                    ))
                    already_collected.add(player_id)
                    count += 1
                    team_count += 1

                conn.commit()
                msg = f"[pitchers] {team_name}: +{team_count} collected"
                if skipped:
                    msg += f", {skipped} skipped"
                msg += f" — {len(already_collected)} total in DB"
                logger.info(msg)

            logger.info(f"Done — collected {count} new pitchers for {self.season}")
        finally:
            conn.close()

        return count

    def _should_update(self, cursor, player_id: int, current_games: int) -> bool:
        """Check if pitcher stats need updating (games_played changed)."""
        cursor.execute(
            "SELECT games_played FROM pitcher_stats WHERE player_id = ?",
            (player_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return True
        return row[0] != current_games


class PitcherGameLogCollector:
    """Collect game-by-game pitching logs."""

    def __init__(self, db_path: str, client: MLBAPIClient = None, season: str = None):
        self.db_path = db_path
        self.client = client or MLBAPIClient()
        self.season = season or CURRENT_SEASON

    def collect(self, historical_season: str = None) -> int:
        """
        Fetch game logs for all pitchers in pitcher_stats.

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

            logger.info(f"Done — collected {count} pitcher game log entries for {target_season}")
        finally:
            conn.close()

        return count

    def _collect_incremental(self, cursor, conn, season: str) -> int:
        """Collect via boxscores — only pitchers who actually appeared."""
        cursor.execute("""
            SELECT s.game_id, s.game_date
            FROM schedule s
            WHERE s.game_type = 'R'
              AND s.season = ?
              AND s.game_date <= date('now', '-1 day')
              AND s.game_id NOT IN (
                  SELECT DISTINCT game_id FROM pitcher_game_logs WHERE season = ?
              )
            ORDER BY s.game_date
        """, (season, season))
        uncollected = cursor.fetchall()

        if not uncollected:
            logger.info(f"[pitchers] No uncollected games for {season}")
            return 0

        logger.info(f"[pitchers] {len(uncollected)} uncollected games — fetching boxscores")

        game_date_map = {game_id: game_date for game_id, game_date in uncollected}

        player_ids = set()
        for game_id, _ in uncollected:
            try:
                boxscore = self.client.get_boxscore_data(game_id)
            except Exception as e:
                logger.warning(f"[pitchers] Could not fetch boxscore for game {game_id}: {e}")
                continue

            for entry in boxscore.get('homePitchers', []) + boxscore.get('awayPitchers', []):
                pid = entry.get('personId', 0)
                if pid != 0:
                    player_ids.add(pid)

        logger.info(f"[pitchers] {len(player_ids)} pitchers appeared — fetching game logs")

        count = 0
        for i, player_id in enumerate(player_ids, 1):
            team_id = self._resolve_team_id(cursor, player_id)

            try:
                data = self.client.get_pitching_game_log(player_id)
                games = self._parse_player_stat_data(data)
            except Exception as e:
                logger.debug(f"No game log for pitcher {player_id}: {e}")
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
                opponent_id, opponent_abbr, is_home, venue_id = self._get_game_context(
                    cursor, gid, team_id
                )
                ip = _safe_float(stat.get("inningsPitched", 0))
                outs = _ip_to_outs(ip)
                is_start = 1 if int(stat.get("gamesStarted", 0)) > 0 else 0
                pitches = stat.get("numberOfPitches") or stat.get("pitchesThrown")
                if pitches is not None:
                    pitches = int(pitches)

                cursor.execute('''
                    INSERT OR IGNORE INTO pitcher_game_logs
                    (player_id, game_id, game_date, season, team_id,
                     opponent_id, opponent_abbr, is_home, is_start,
                     innings_pitched, outs_recorded, hits_allowed,
                     runs_allowed, earned_runs, walks_allowed,
                     strikeouts, home_runs_allowed, pitches_thrown, venue_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    player_id, gid, game_date, season, team_id,
                    opponent_id, opponent_abbr, is_home, is_start,
                    ip, outs,
                    int(stat.get("hits", 0)),
                    int(stat.get("runs", 0)),
                    int(stat.get("earnedRuns", 0)),
                    int(stat.get("baseOnBalls", 0)),
                    int(stat.get("strikeOuts", 0)),
                    int(stat.get("homeRuns", 0)),
                    pitches,
                    venue_id,
                ))

                if cursor.rowcount > 0:
                    count += 1
                    player_count += 1

            conn.commit()
            if player_count > 0:
                logger.info(f"[{i}/{len(player_ids)}] pitcher {player_id}: +{player_count} games — {count} total")

        return count

    def _collect_historical(self, cursor, conn, historical_season: str) -> int:
        """Historical backfill — loop all pitchers, incremental by last collected date."""
        cursor.execute("SELECT player_id, player_name, team_id FROM pitcher_stats")
        players = cursor.fetchall()
        total_players = len(players)
        logger.info(f"Collecting {historical_season} pitcher game logs for {total_players} players...")

        count = 0
        for i, (player_id, player_name, team_id) in enumerate(players, 1):
            last_date = self._get_last_game_date(cursor, player_id, historical_season)

            try:
                raw = self.client.get_player_game_log_by_season(player_id, "pitching", historical_season)
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
                ip = _safe_float(stat.get("inningsPitched", 0))
                outs = _ip_to_outs(ip)
                is_start = 1 if int(stat.get("gamesStarted", 0)) > 0 else 0
                pitches = stat.get("numberOfPitches") or stat.get("pitchesThrown")
                if pitches is not None:
                    pitches = int(pitches)

                cursor.execute('''
                    INSERT OR IGNORE INTO pitcher_game_logs
                    (player_id, game_id, game_date, season, team_id,
                     opponent_id, opponent_abbr, is_home, is_start,
                     innings_pitched, outs_recorded, hits_allowed,
                     runs_allowed, earned_runs, walks_allowed,
                     strikeouts, home_runs_allowed, pitches_thrown, venue_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    player_id, game_id, game_date, historical_season, team_id,
                    opponent_id, opponent_abbr, is_home, is_start,
                    ip, outs,
                    int(stat.get("hits", 0)),
                    int(stat.get("runs", 0)),
                    int(stat.get("earnedRuns", 0)),
                    int(stat.get("baseOnBalls", 0)),
                    int(stat.get("strikeOuts", 0)),
                    int(stat.get("homeRuns", 0)),
                    pitches,
                    venue_id,
                ))

                if cursor.rowcount > 0:
                    count += 1
                    player_count += 1

            conn.commit()
            if player_count > 0 or i % 50 == 0:
                logger.info(f"[{i}/{total_players}] {player_name}: +{player_count} games — {count} total")

        return count

    def _resolve_team_id(self, cursor, player_id: int):
        """Look up a player's team_id from pitcher_stats."""
        cursor.execute(
            "SELECT team_id FROM pitcher_stats WHERE player_id = ?", (player_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def _get_last_game_date(self, cursor, player_id: int, season: str):
        """Get the most recent game date already collected for a player."""
        cursor.execute(
            "SELECT MAX(game_date) FROM pitcher_game_logs WHERE player_id = ? AND season = ?",
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
