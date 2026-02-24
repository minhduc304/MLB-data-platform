"""Shared pytest fixtures for MLB Prop Prediction System tests."""

import pytest


@pytest.fixture
def test_db(tmp_path):
    """
    Create a test database with all tables initialized.

    Uses tmp_path fixture to ensure isolation between tests.
    """
    db_path = str(tmp_path / "test.db")
    from src.db.init_db import init_database
    init_database(db_path)
    return db_path
