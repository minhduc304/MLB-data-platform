"""MLB Stats API Client - Wrapper around the statsapi library."""

import logging
import time

import statsapi

from src.config import APIConfig
from src.api.retry import RetryStrategy

logger = logging.getLogger(__name__)


class MLBAPIClient:
    """Client for the MLB Stats API using the statsapi library."""

    def __init__(self, config: APIConfig = None):
        if config is None:
            config = APIConfig()
        self.config = config
        self.retry_strategy = RetryStrategy(
            max_retries=config.max_retries,
            base_delay=config.delay,
            exponential_backoff=True,
        )

    def _rate_limit(self):
        """Sleep between API calls to respect rate limits."""
        time.sleep(self.config.delay)

    def get_player_stats(self, player_id: int, season: str) -> dict:
        """
        Get player season statistics.

        Args:
            player_id: MLB player ID
            season: Season year (e.g. '2025')

        Returns:
            Player stats dictionary
        """
        def _call():
            self._rate_limit()
            return statsapi.player_stat_data(player_id, group="hitting,pitching", type="season", sportId=1)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for player {player_id}: {e}"),
        )

    def get_game_log(self, player_id: int, season: str) -> list:
        """
        Get game-by-game logs for a player.

        Args:
            player_id: MLB player ID
            season: Season year (e.g. '2025')

        Returns:
            List of game log entries
        """
        def _call():
            self._rate_limit()
            return statsapi.player_stat_data(player_id, group="hitting,pitching", type="gameLog", sportId=1)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for game log {player_id}: {e}"),
        )

    def get_schedule(self, start_date: str, end_date: str) -> list:
        """
        Get schedule of games between dates.

        Args:
            start_date: Start date (MM/DD/YYYY)
            end_date: End date (MM/DD/YYYY)

        Returns:
            List of scheduled games
        """
        def _call():
            self._rate_limit()
            return statsapi.schedule(start_date=start_date, end_date=end_date)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for schedule: {e}"),
        )

    def get_roster(self, team_id: int) -> list:
        """
        Get team roster (formatted string).

        Args:
            team_id: MLB team ID

        Returns:
            Formatted roster string
        """
        def _call():
            self._rate_limit()
            return statsapi.roster(team_id)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for roster {team_id}: {e}"),
        )

    def get_teams(self) -> list:
        """
        Get all MLB teams.

        Returns:
            List of team dicts from the API
        """
        def _call():
            self._rate_limit()
            data = statsapi.get("teams", {"sportIds": 1})
            return data.get("teams", [])

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for teams: {e}"),
        )

    def get_roster_data(self, team_id: int, season: str) -> list:
        """
        Get raw roster data (not formatted string).

        Args:
            team_id: MLB team ID
            season: Season year

        Returns:
            List of roster entry dicts
        """
        def _call():
            self._rate_limit()
            data = statsapi.get("team_roster", {
                "teamId": team_id,
                "season": season,
                "rosterType": "active",
            })
            return data.get("roster", [])

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for roster data {team_id}: {e}"),
        )

    def get_team_full_roster(self, team_id: int, season: str) -> list:
        """
        Get full roster including IL players.

        Args:
            team_id: MLB team ID
            season: Season year

        Returns:
            List of roster entry dicts with hydrated person status
        """
        def _call():
            self._rate_limit()
            data = statsapi.get("team_roster", {
                "teamId": team_id,
                "rosterType": "fullRoster",
                "hydrate": "person",
                "season": season,
            })
            return data.get("roster", [])

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for full roster {team_id}: {e}"),
        )

    def get_boxscore_data(self, game_id: int) -> dict:
        """
        Get boxscore data for a game.

        Args:
            game_id: MLB game ID (gamePk)

        Returns:
            Parsed boxscore dict with homeBatters/awayBatters lists
        """
        def _call():
            self._rate_limit()
            return statsapi.boxscore_data(game_id)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for boxscore {game_id}: {e}"),
        )

    def get_player_hitting_stats(self, player_id: int, season: str) -> dict:
        """
        Get season hitting stats for a player.

        Args:
            player_id: MLB player ID
            season: Season year

        Returns:
            Player stat data dict
        """
        def _call():
            self._rate_limit()
            return statsapi.player_stat_data(player_id, group="[hitting]", type="season", sportId=1)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for hitting stats {player_id}: {e}"),
        )

    def get_player_pitching_stats(self, player_id: int, season: str) -> dict:
        """
        Get season pitching stats for a player.

        Args:
            player_id: MLB player ID
            season: Season year

        Returns:
            Player stat data dict
        """
        def _call():
            self._rate_limit()
            return statsapi.player_stat_data(player_id, group="[pitching]", type="season", sportId=1)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for pitching stats {player_id}: {e}"),
        )

    def get_hitting_game_log(self, player_id: int) -> dict:
        """
        Get current-season hitting game log.

        Args:
            player_id: MLB player ID

        Returns:
            Player stat data dict with game log entries
        """
        def _call():
            self._rate_limit()
            return statsapi.player_stat_data(player_id, group="[hitting]", type="gameLog", sportId=1)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for hitting game log {player_id}: {e}"),
        )

    def get_pitching_game_log(self, player_id: int) -> dict:
        """
        Get current-season pitching game log.

        Args:
            player_id: MLB player ID

        Returns:
            Player stat data dict with game log entries
        """
        def _call():
            self._rate_limit()
            return statsapi.player_stat_data(player_id, group="[pitching]", type="gameLog", sportId=1)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for pitching game log {player_id}: {e}"),
        )

    def get_player_game_log_by_season(self, player_id: int, group: str, season: str) -> dict:
        """
        Get game log for a specific historical season using raw API.

        The gameLog type via player_stat_data doesn't accept a season parameter,
        so we use the raw statsapi.get() endpoint.

        Args:
            player_id: MLB player ID
            group: Stat group ('hitting' or 'pitching')
            season: Season year

        Returns:
            Raw API response dict
        """
        def _call():
            self._rate_limit()
            return statsapi.get("person", {
                "personId": player_id,
                "hydrate": f"stats(group=[{group}],type=gameLog,season={season})",
            })

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for {group} game log {player_id} season {season}: {e}"),
        )
