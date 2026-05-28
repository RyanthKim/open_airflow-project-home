"""Shared utilities: data validation, memory management."""

from __future__ import annotations

import ctypes
import ctypes.util
import gc
import platform
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from libs.db import DBManager
from libs.logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data-quality exception
# ---------------------------------------------------------------------------

class DataQualityError(Exception):
    """Raised when a data-validation rule fails."""


# ---------------------------------------------------------------------------
# DataValidator
# ---------------------------------------------------------------------------

class DataValidator:
    """Run declarative quality checks against a Redshift table.

    Each rule is a dict with at least ``{"type": "<rule_type>"}`` plus
    rule-specific keys.  Supported types:

    * ``not_empty``          – table has >= 1 row.
    * ``row_count_range``    – row count in ``[min, max]``.
    * ``no_nulls``           – ``column`` has no NULL values.
    * ``unique``             – ``column`` has no duplicate values.
    * ``freshness``          – ``column`` has a value within ``max_hours`` of now.
    """

    def __init__(self, db: DBManager) -> None:
        self.db = db

    def validate(
        self,
        table_name: str,
        rules: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Run *rules* against *table_name*. Returns a list of results.

        Raises ``DataQualityError`` on the first failing rule.
        """
        results: list[dict[str, Any]] = []

        for rule in rules:
            rule_type = rule["type"]
            handler = getattr(self, f"_check_{rule_type}", None)
            if handler is None:
                raise ValueError(f"Unknown rule type: {rule_type}")

            passed, detail = handler(table_name, rule)
            results.append({"table": table_name, "rule": rule_type, "passed": passed, "detail": detail})

            if not passed:
                msg = f"Validation failed on {table_name} — {rule_type}: {detail}"
                logger.error(msg)
                raise DataQualityError(msg)

            logger.info("PASS  %s  %s  %s", table_name, rule_type, detail)

        return results

    # -- rule implementations -----------------------------------------------

    def _check_not_empty(self, table: str, _rule: dict) -> tuple[bool, str]:
        df = self._count(table)
        cnt = int(df.iloc[0, 0])
        return cnt > 0, f"row_count={cnt}"

    def _check_row_count_range(self, table: str, rule: dict) -> tuple[bool, str]:
        df = self._count(table)
        cnt = int(df.iloc[0, 0])
        lo = rule.get("min", 0)
        hi = rule.get("max", float("inf"))
        return lo <= cnt <= hi, f"row_count={cnt} expected=[{lo}, {hi}]"

    def _check_no_nulls(self, table: str, rule: dict) -> tuple[bool, str]:
        col = rule["column"]
        sql = f'SELECT COUNT(*) AS n FROM {table} WHERE "{col}" IS NULL'
        df = pd.read_sql(sql, self.db._get_engine())
        null_cnt = int(df.iloc[0, 0])
        return null_cnt == 0, f"null_count({col})={null_cnt}"

    def _check_unique(self, table: str, rule: dict) -> tuple[bool, str]:
        col = rule["column"]
        sql = (
            f'SELECT COUNT(*) - COUNT(DISTINCT "{col}") AS dupes FROM {table}'
        )
        df = pd.read_sql(sql, self.db._get_engine())
        dupes = int(df.iloc[0, 0])
        return dupes == 0, f"duplicates({col})={dupes}"

    def _check_freshness(self, table: str, rule: dict) -> tuple[bool, str]:
        col = rule["column"]
        max_hours = rule.get("max_hours", 48)
        sql = f'SELECT MAX("{col}") AS latest FROM {table}'
        df = pd.read_sql(sql, self.db._get_engine())
        latest = pd.Timestamp(df.iloc[0, 0])
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_hours)
        ok = latest >= pd.Timestamp(cutoff)
        return ok, f"latest={latest}, cutoff={cutoff}"

    # -- helpers -------------------------------------------------------------

    def _count(self, table: str) -> pd.DataFrame:
        return pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {table}", self.db._get_engine())


# ---------------------------------------------------------------------------
# Memory management
# ---------------------------------------------------------------------------

def force_memory_cleanup() -> None:
    """Aggressive memory cleanup for long-running Airflow workers.

    Runs ``gc.collect()`` three times (to clear cyclic refs across
    generations) and calls ``malloc_trim`` on Linux to release pages
    back to the OS.
    """
    for _ in range(3):
        gc.collect()

    if platform.system() == "Linux":
        try:
            libc_name = ctypes.util.find_library("c")
            if libc_name:
                libc = ctypes.CDLL(libc_name)
                libc.malloc_trim(0)
                logger.info("malloc_trim(0) succeeded")
        except Exception as exc:
            logger.debug("malloc_trim unavailable: %s", exc)

    logger.info("Memory cleanup complete")
