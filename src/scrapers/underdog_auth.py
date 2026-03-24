"""Underdog Fantasy authentication and token management."""

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

_TOKEN_CACHE: dict = {}  # {token: str, expires_at: float}


def get_token() -> str:
    """
    Return a valid Underdog auth token, refreshing if expired.

    Reads UNDERDOG_EMAIL and UNDERDOG_PASSWORD from environment.
    Token is cached in-process until expiry.
    """
    now = time.time()
    if _TOKEN_CACHE.get('token') and _TOKEN_CACHE.get('expires_at', 0) > now + 60:
        return _TOKEN_CACHE['token']

    email = os.getenv('UNDERDOG_EMAIL', '')
    password = os.getenv('UNDERDOG_PASSWORD', '')

    if not email or not password:
        raise RuntimeError(
            'UNDERDOG_EMAIL and UNDERDOG_PASSWORD must be set in environment'
        )

    token = _login(email, password)
    _TOKEN_CACHE['token'] = token
    # Underdog tokens are valid for ~1 hour; cache for 50 minutes
    _TOKEN_CACHE['expires_at'] = now + 3000
    return token


def _login(email: str, password: str) -> str:
    """POST credentials to Underdog auth endpoint and return the bearer token."""
    url = 'https://api.underdogfantasy.com/v1/user/session'
    payload = {'email': email, 'password': password}

    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    token = (
        data.get('data', {}).get('attributes', {}).get('token')
        or data.get('token')
    )
    if not token:
        raise RuntimeError(f'No token in Underdog auth response: {data}')

    logger.debug('Obtained fresh Underdog auth token')
    return token


def clear_cache() -> None:
    """Clear the in-process token cache (useful for testing)."""
    _TOKEN_CACHE.clear()
