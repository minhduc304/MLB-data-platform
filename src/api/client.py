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
        Get team roster.

        Args:
            team_id: MLB team ID

        Returns:
            List of roster entries
        """
        def _call():
            self._rate_limit()
            return statsapi.roster(team_id)

        return self.retry_strategy.execute(
            _call,
            on_retry=lambda attempt, e: logger.warning(f"Retry {attempt} for roster {team_id}: {e}"),
        )
