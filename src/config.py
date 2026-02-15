from dataclasses import dataclass
import os

# Default database path constant
DEFAULT_DB_PATH = 'data/mlb_stats.db'

# Current MLB season
CURRENT_SEASON = '2026'


def get_db_path() -> str:
    """
    Get the database path from environment variable or default.

    Uses DB_PATH environment variable if set, otherwise returns default path.
    This is the single source of truth for database path configuration.

    Returns:
        Path to the SQLite database file
    """
    return os.getenv('DB_PATH', DEFAULT_DB_PATH)


@dataclass
class APIConfig:
    timeout: int = 30
    delay: float = 1.0
    max_retries: int = 3

@dataclass
class Config:
    season: str = CURRENT_SEASON
    db_path: str = DEFAULT_DB_PATH
    api: APIConfig = None

    def __post_init__(self):
        if self.api is None:
            self.api = APIConfig()

    @classmethod
    def from_env(cls) -> 'Config':
        return cls(
            season=os.getenv('MLB_SEASON', CURRENT_SEASON),
            db_path=get_db_path(),
            api=APIConfig(
                timeout=int(os.getenv('API_TIMEOUT', 30)),
                delay=float(os.getenv('API_DELAY', 1.0)),
            )
        )
