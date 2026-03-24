"""Tests for Underdog Fantasy scraper and auth token management."""

import sqlite3
import time
from unittest.mock import MagicMock, patch

import pytest

from src.scrapers import underdog_auth
from src.scrapers.underdog import UnderdogScraper


# ---------------------------------------------------------------------------
# underdog_auth tests
# ---------------------------------------------------------------------------

class TestUnderdogAuth:
    def setup_method(self):
        underdog_auth.clear_cache()

    def test_get_token_raises_without_credentials(self, monkeypatch):
        monkeypatch.delenv('UNDERDOG_EMAIL', raising=False)
        monkeypatch.delenv('UNDERDOG_PASSWORD', raising=False)
        with pytest.raises(RuntimeError, match='UNDERDOG_EMAIL'):
            underdog_auth.get_token()

    def test_get_token_returns_token_on_success(self, monkeypatch):
        monkeypatch.setenv('UNDERDOG_EMAIL', 'test@example.com')
        monkeypatch.setenv('UNDERDOG_PASSWORD', 'secret')

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            'data': {'attributes': {'token': 'test-token-123'}}
        }
        mock_resp.raise_for_status = MagicMock()

        with patch('src.scrapers.underdog_auth.requests.post', return_value=mock_resp):
            token = underdog_auth.get_token()

        assert token == 'test-token-123'

    def test_get_token_uses_cache_on_second_call(self, monkeypatch):
        monkeypatch.setenv('UNDERDOG_EMAIL', 'test@example.com')
        monkeypatch.setenv('UNDERDOG_PASSWORD', 'secret')

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            'data': {'attributes': {'token': 'cached-token'}}
        }
        mock_resp.raise_for_status = MagicMock()

        with patch('src.scrapers.underdog_auth.requests.post', return_value=mock_resp) as mock_post:
            underdog_auth.get_token()
            underdog_auth.get_token()

        assert mock_post.call_count == 1  # Only called once, second call uses cache

    def test_get_token_refreshes_when_expired(self, monkeypatch):
        monkeypatch.setenv('UNDERDOG_EMAIL', 'test@example.com')
        monkeypatch.setenv('UNDERDOG_PASSWORD', 'secret')

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            'data': {'attributes': {'token': 'new-token'}}
        }
        mock_resp.raise_for_status = MagicMock()

        # Manually set expired cache
        underdog_auth._TOKEN_CACHE['token'] = 'old-token'
        underdog_auth._TOKEN_CACHE['expires_at'] = time.time() - 100  # expired

        with patch('src.scrapers.underdog_auth.requests.post', return_value=mock_resp):
            token = underdog_auth.get_token()

        assert token == 'new-token'

    def test_clear_cache(self):
        underdog_auth._TOKEN_CACHE['token'] = 'some-token'
        underdog_auth._TOKEN_CACHE['expires_at'] = time.time() + 9999
        underdog_auth.clear_cache()
        assert underdog_auth._TOKEN_CACHE == {}


# ---------------------------------------------------------------------------
# UnderdogScraper tests
# ---------------------------------------------------------------------------

SAMPLE_API_RESPONSE = {
    'over_under_lines': [
        {
            'id': 'line-1',
            'stat_value': '1.5',
            'choice': 'higher',
            'payout_multiplier': -115,
            'updated_at': '2025-06-01T18:00:00Z',
            'over_under': {
                'appearance_id': 'app-1',
                'appearance_stat': {'display_stat': 'hits'},
            },
        },
        {
            'id': 'line-2',
            'stat_value': '6.5',
            'choice': 'lower',
            'payout_multiplier': -110,
            'updated_at': '2025-06-01T18:00:00Z',
            'over_under': {
                'appearance_id': 'app-2',
                'appearance_stat': {'display_stat': 'pitcher_strikeouts'},
            },
        },
    ],
    'appearances': [
        {'id': 'app-1', 'player_id': 'p-1', 'team_name': 'Yankees', 'match_id': 'm-1'},
        {'id': 'app-2', 'player_id': 'p-2', 'team_name': 'Mets', 'match_id': 'm-2'},
    ],
    'players': [
        {'id': 'p-1', 'full_name': 'Aaron Judge'},
        {'id': 'p-2', 'full_name': 'Justin Verlander'},
    ],
    'matchups': [
        {'id': 'm-1', 'scheduled_at': '2025-06-01T19:00:00Z'},
        {'id': 'm-2', 'scheduled_at': '2025-06-01T19:10:00Z'},
    ],
}


class TestUnderdogScraper:
    def test_scrape_saves_props_to_db(self, test_db):
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch('src.scrapers.underdog.get_token', return_value='fake-token'), \
             patch('src.scrapers.underdog.requests.get', return_value=mock_resp), \
             patch('src.scrapers.underdog.time.sleep'):

            scraper = UnderdogScraper(db_path=test_db, delay=0)
            count = scraper.scrape()

        assert count > 0

        conn = sqlite3.connect(test_db)
        rows = conn.execute('SELECT * FROM underdog_props').fetchall()
        conn.close()
        assert len(rows) > 0

    def test_scrape_normalises_choice(self, test_db):
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch('src.scrapers.underdog.get_token', return_value='fake-token'), \
             patch('src.scrapers.underdog.requests.get', return_value=mock_resp), \
             patch('src.scrapers.underdog.time.sleep'):

            scraper = UnderdogScraper(db_path=test_db, delay=0)
            scraper.scrape()

        conn = sqlite3.connect(test_db)
        choices = {row[0] for row in conn.execute('SELECT choice FROM underdog_props').fetchall()}
        conn.close()

        assert choices <= {'over', 'under'}

    def test_scrape_writes_to_all_props(self, test_db):
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch('src.scrapers.underdog.get_token', return_value='fake-token'), \
             patch('src.scrapers.underdog.requests.get', return_value=mock_resp), \
             patch('src.scrapers.underdog.time.sleep'):

            scraper = UnderdogScraper(db_path=test_db, delay=0)
            scraper.scrape()

        conn = sqlite3.connect(test_db)
        rows = conn.execute(
            "SELECT * FROM all_props WHERE source = 'underdog'"
        ).fetchall()
        conn.close()
        assert len(rows) > 0

    def test_scrape_returns_zero_on_auth_failure(self, test_db):
        with patch('src.scrapers.underdog.get_token', side_effect=RuntimeError('no creds')):
            scraper = UnderdogScraper(db_path=test_db)
            count = scraper.scrape()

        assert count == 0

    def test_scrape_deduplicates_on_rerun(self, test_db):
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch('src.scrapers.underdog.get_token', return_value='fake-token'), \
             patch('src.scrapers.underdog.requests.get', return_value=mock_resp), \
             patch('src.scrapers.underdog.time.sleep'):

            scraper = UnderdogScraper(db_path=test_db, delay=0)
            scraper.scrape()
            scraper.scrape()

        conn = sqlite3.connect(test_db)
        count = conn.execute('SELECT COUNT(*) FROM underdog_props').fetchone()[0]
        conn.close()
        # Second run should not duplicate rows
        assert count == len(SAMPLE_API_RESPONSE['over_under_lines'])

    def test_stat_name_mapped_correctly(self, test_db):
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch('src.scrapers.underdog.get_token', return_value='fake-token'), \
             patch('src.scrapers.underdog.requests.get', return_value=mock_resp), \
             patch('src.scrapers.underdog.time.sleep'):

            scraper = UnderdogScraper(db_path=test_db, delay=0)
            scraper.scrape()

        conn = sqlite3.connect(test_db)
        stat_names = {
            row[0] for row in conn.execute('SELECT stat_name FROM underdog_props').fetchall()
        }
        conn.close()

        assert 'hits' in stat_names
        assert 'pitcher_strikeouts' in stat_names
