"""Retry Strategy - Configurable retry logic for API calls."""

import time
from functools import wraps
from typing import Callable, TypeVar, Optional, List, Type

T = TypeVar('T')


class RetryStrategy:
    """Configurable retry logic with exponential backoff."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        exponential_backoff: bool = True,
        retryable_exceptions: Optional[List[Type[Exception]]] = None,
    ):
        """
        Initialize retry strategy.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            exponential_backoff: Whether to use exponential backoff
            retryable_exceptions: List of exception types to retry on (None = all)
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.exponential_backoff = exponential_backoff
        self.retryable_exceptions = retryable_exceptions or [Exception]

    def execute(self, func: Callable[[], T], on_retry: Optional[Callable[[int, Exception], None]] = None) -> T:
        """
        Execute function with retries.

        Args:
            func: Function to execute
            on_retry: Optional callback called on each retry with (attempt, exception)

        Returns:
            Result of the function

        Raises:
            Last exception if all retries fail
        """
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return func()
            except tuple(self.retryable_exceptions) as e:
                last_exception = e

                if attempt < self.max_retries - 1:
                    delay = self._calculate_delay(attempt)

                    if on_retry:
                        on_retry(attempt + 1, e)

                    time.sleep(delay)

        raise last_exception

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number."""
        if self.exponential_backoff:
            return self.base_delay * (2 ** attempt)
        return self.base_delay


def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    exponential_backoff: bool = True,
    retryable_exceptions: Optional[List[Type[Exception]]] = None,
):
    """
    Decorator to add retry logic to a function.

    Usage:
        @with_retry(max_retries=3, base_delay=1.0)
        def fetch_data():
            return api.get_data()
    """
    strategy = RetryStrategy(
        max_retries=max_retries,
        base_delay=base_delay,
        exponential_backoff=exponential_backoff,
        retryable_exceptions=retryable_exceptions,
    )

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return strategy.execute(lambda: func(*args, **kwargs))
        return wrapper
    return decorator
