"""Unit tests for libs/db.py — DBManager utilities."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from libs.db import DBManager, get_db_manager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db():
    return DBManager(
        host="localhost",
        port=5439,
        database="test_db",
        user="test_user",
        password="test_pw",
    )


# ---------------------------------------------------------------------------
# Tests: SQL helpers
# ---------------------------------------------------------------------------

class TestRemoveComments:
    def test_strips_line_comments(self):
        sql = "-- header\nSELECT 1;\n-- trailing"
        result = DBManager._remove_comments(sql)
        assert "SELECT 1" in result
        assert "header" not in result

    def test_strips_block_comments(self):
        sql = "/* block */ SELECT 1"
        result = DBManager._remove_comments(sql)
        assert "SELECT 1" in result
        assert "block" not in result

    def test_preserves_string_content(self):
        sql = "SELECT '--not-a-comment' AS val"
        result = DBManager._remove_comments(sql)
        assert "SELECT" in result


class TestSplitStatements:
    def test_single_statement(self):
        assert DBManager._split_statements("SELECT 1;") == ["SELECT 1"]

    def test_multiple_statements(self):
        result = DBManager._split_statements("BEGIN;\nDELETE FROM t;\nCOMMIT;")
        assert len(result) == 3

    def test_ignores_empty(self):
        assert DBManager._split_statements("") == []

    def test_respects_quoted_semicolons(self):
        sql = "SELECT 'a;b' FROM t;"
        result = DBManager._split_statements(sql)
        assert len(result) == 1
        assert "'a;b'" in result[0]


# ---------------------------------------------------------------------------
# Tests: SQL file execution
# ---------------------------------------------------------------------------

class TestExecuteSqlFile:
    @patch.object(DBManager, "_get_engine")
    def test_reads_and_executes(self, mock_get_engine, db, tmp_path):
        mock_conn = MagicMock()
        mock_get_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;\nSELECT 2;")
        db.execute_sql_file(sql_file)

        assert mock_conn.execute.call_count == 2


class TestQuerySqlFile:
    @patch("libs.db.pd.read_sql")
    @patch.object(DBManager, "_get_engine")
    def test_returns_dataframe(self, mock_get_engine, mock_read_sql, db, tmp_path):
        mock_conn = MagicMock()
        mock_get_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_read_sql.return_value = pd.DataFrame({"id": [1, 2]})

        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM t")
        result = db.query_sql_file(sql_file)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Tests: import_data
# ---------------------------------------------------------------------------

class TestImportData:
    @patch.object(DBManager, "_get_engine")
    def test_empty_df_is_noop(self, mock_get_engine, db):
        db.import_data(pd.DataFrame(), "test_table")
        mock_get_engine.assert_not_called()

    @patch.object(DBManager, "_get_engine")
    def test_chunks_large_dataframe(self, mock_get_engine, db):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        n_cols = 3
        chunk_size = 32_767 // n_cols
        n_rows = chunk_size + 10
        df = pd.DataFrame({"a": range(n_rows), "b": range(n_rows), "c": range(n_rows)})

        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            db.import_data(df, "analytics.test_table")
            assert mock_to_sql.call_count == 2


# ---------------------------------------------------------------------------
# Tests: factory
# ---------------------------------------------------------------------------

class TestGetDbManager:
    @patch.dict(os.environ, {
        "DB_HOST": "localhost", "DB_PORT": "5439",
        "DB_NAME": "dev", "DB_ID": "user", "DB_PW": "pass",
    })
    @patch("libs.db._get_airflow_connection", return_value=None)
    def test_env_var_path(self, _mock_af):
        mgr = get_db_manager()
        assert mgr.host == "localhost"
        assert mgr.database == "dev"

    @patch("libs.db._get_airflow_connection", return_value={
        "host": "rs.example.com", "port": 5439,
        "database": "analytics", "user": "airflow", "password": "secret",
    })
    def test_airflow_conn_path(self, _mock_af):
        mgr = get_db_manager()
        assert mgr.host == "rs.example.com"
        assert mgr.database == "analytics"
