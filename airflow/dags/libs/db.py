"""Database and Google Sheets managers for Airflow pipelines."""

from __future__ import annotations

import math
import os
import re
from pathlib import Path
from typing import Any

import gspread
import pandas as pd
import redshift_connector
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from libs.logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def get_env_var(key: str, default: str | None = None) -> str:
    """Return an environment variable or raise if missing and no default."""
    val = os.getenv(key, default)
    if val is None:
        raise EnvironmentError(f"Required env var '{key}' is not set")
    return val


def _get_airflow_connection(conn_id: str) -> dict[str, Any] | None:
    """Try to retrieve connection details from Airflow's Connection store."""
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(conn_id)
        return {
            "host": conn.host,
            "port": conn.port or 5439,
            "database": conn.schema or "sample_db",
            "user": conn.login,
            "password": conn.password,
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# DBManager
# ---------------------------------------------------------------------------

class DBManager:
    """Thin wrapper around redshift_connector + SQLAlchemy for Redshift."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._engine: Engine | None = None

    # -- connection helpers --------------------------------------------------

    def _get_raw_conn(self) -> redshift_connector.Connection:
        return redshift_connector.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def _get_engine(self) -> Engine:
        if self._engine is None:
            url = (
                f"redshift+redshift_connector://{self.user}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            self._engine = create_engine(url, pool_pre_ping=True)
        return self._engine

    # -- query execution -----------------------------------------------------

    def execute_query(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """Execute a single statement (DDL / DML) that returns no rows."""
        engine = self._get_engine()
        with engine.connect() as conn:
            conn.execute(text(sql), params or {})
            conn.commit()
        logger.info("Executed query (%d chars)", len(sql))

    def execute_sql_file(self, file_path: str | Path, params: dict[str, Any] | None = None) -> None:
        """Read a SQL file, split into statements, and execute each."""
        raw = Path(file_path).read_text(encoding="utf-8")
        cleaned = self._remove_comments(raw)
        statements = self._split_statements(cleaned)

        engine = self._get_engine()
        with engine.connect() as conn:
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                conn.execute(text(stmt), params or {})
            conn.commit()
        logger.info("Executed %d statements from %s", len(statements), file_path)

    def query_sql_file(self, file_path: str | Path, params: dict[str, Any] | None = None) -> pd.DataFrame:
        """Execute a SQL file and return the result of the *last* SELECT."""
        raw = Path(file_path).read_text(encoding="utf-8")
        cleaned = self._remove_comments(raw)
        statements = self._split_statements(cleaned)

        engine = self._get_engine()
        df = pd.DataFrame()
        with engine.connect() as conn:
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                if stmt.upper().lstrip().startswith("SELECT") or stmt.upper().lstrip().startswith("WITH"):
                    df = pd.read_sql(text(stmt), conn, params=params or {})
                else:
                    conn.execute(text(stmt), params or {})
            conn.commit()
        logger.info("Query returned %d rows from %s", len(df), file_path)
        return df

    # -- data import ---------------------------------------------------------

    def import_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
    ) -> None:
        """Import a DataFrame into Redshift, chunking to stay within the
        32 767 parameter limit of redshift_connector.

        Args:
            df: Data to import.
            table_name: Fully-qualified table name (e.g. ``analytics.plan_info``).
            if_exists: ``'append'``, ``'replace'``, or ``'fail'``.
        """
        if df.empty:
            logger.warning("import_data called with empty DataFrame — skipping")
            return

        max_params = 32_767
        cols = len(df.columns)
        chunk_size = max(1, math.floor(max_params / cols))

        engine = self._get_engine()
        total_rows = len(df)
        schema, tbl = self._parse_table_name(table_name)

        for start in range(0, total_rows, chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            # First chunk may need to replace; subsequent always append
            mode = if_exists if start == 0 else "append"
            chunk.to_sql(
                name=tbl,
                schema=schema,
                con=engine,
                index=False,
                if_exists=mode,
                method="multi",
            )
            logger.info(
                "Imported rows %d–%d / %d into %s",
                start,
                min(start + chunk_size, total_rows),
                total_rows,
                table_name,
            )

    def delete_table(self, table_name: str) -> None:
        """DROP TABLE IF EXISTS."""
        self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
        logger.info("Dropped table %s", table_name)

    # -- internal helpers ----------------------------------------------------

    @staticmethod
    def _remove_comments(sql: str) -> str:
        """Strip single-line (--) and block (/* */) comments."""
        sql = re.sub(r"--[^\n]*", "", sql)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        return sql

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        """Split SQL text on semicolons, respecting quoted strings."""
        stmts: list[str] = []
        current: list[str] = []
        in_single = False
        in_double = False

        for char in sql:
            if char == "'" and not in_double:
                in_single = not in_single
            elif char == '"' and not in_single:
                in_double = not in_double
            elif char == ";" and not in_single and not in_double:
                stmts.append("".join(current))
                current = []
                continue
            current.append(char)

        trailing = "".join(current).strip()
        if trailing:
            stmts.append(trailing)
        return stmts

    @staticmethod
    def _parse_table_name(table_name: str) -> tuple[str | None, str]:
        """Split 'schema.table' into (schema, table)."""
        parts = table_name.split(".", maxsplit=1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, parts[0]

    def close(self) -> None:
        """Dispose of the SQLAlchemy engine."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


# ---------------------------------------------------------------------------
# GoogleSheetsManager
# ---------------------------------------------------------------------------

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSheetsManager:
    """Read Google Sheets into DataFrames using a service-account key."""

    def __init__(self, credentials_path: str) -> None:
        creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        self.client = gspread.authorize(creds)
        logger.info("Google Sheets client initialised")

    def get_sheet(self, spreadsheet_key: str, worksheet_name: str) -> pd.DataFrame:
        """Fetch a worksheet as a DataFrame (first row = header)."""
        sh = self.client.open_by_key(spreadsheet_key)
        ws = sh.worksheet(worksheet_name)
        rows = ws.get_all_values()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows[1:], columns=rows[0])
        logger.info("Fetched %d rows from sheet '%s'", len(df), worksheet_name)
        return df


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def get_db_manager() -> DBManager:
    """Create a DBManager, preferring the Airflow connection store."""
    airflow_params = _get_airflow_connection("redshift_default")
    if airflow_params:
        return DBManager(**airflow_params)

    return DBManager(
        host=get_env_var("DB_HOST"),
        port=int(get_env_var("DB_PORT", "5439")),
        database=get_env_var("DB_NAME", "sample_db"),
        user=get_env_var("DB_ID"),
        password=get_env_var("DB_PW"),
    )


def get_sheets_manager() -> GoogleSheetsManager:
    """Create a GoogleSheetsManager from env-var credential path."""
    creds_path = get_env_var(
        "GOOGLE_CREDENTIALS_FILE",
        "/opt/airflow/google-credentials.json",
    )
    return GoogleSheetsManager(creds_path)
