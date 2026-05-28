"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Add airflow/dags to sys.path so `libs.*` imports resolve
_dags_dir = str(Path(__file__).resolve().parent.parent / "airflow" / "dags")
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)


@pytest.fixture()
def mock_db_manager(mocker):
    """Return a mocked DBManager with a stubbed Redshift connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch("redshift_connector.connect", return_value=mock_conn)

    manager = MagicMock()
    manager.conn = mock_conn
    manager.cursor = mock_cursor
    manager.execute_query = MagicMock(return_value=None)
    manager.execute_sql_file = MagicMock(return_value=None)
    manager.query_sql_file = MagicMock(return_value=MagicMock())  # DataFrame-like
    return manager


@pytest.fixture()
def mock_sheets_manager(mocker):
    """Return a mocked GoogleSheetsManager."""
    mocker.patch("gspread.authorize", return_value=MagicMock())
    manager = MagicMock()
    manager.get_worksheet_data = MagicMock(return_value=[["h1", "h2"], ["a", "b"]])
    return manager
