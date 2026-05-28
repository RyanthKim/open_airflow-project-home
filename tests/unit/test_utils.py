"""Unit tests for libs/utils.py — DataValidator and memory utilities."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from libs.utils import DataQualityError, DataValidator, force_memory_cleanup


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_engine():
    return MagicMock()


@pytest.fixture()
def db(mock_engine):
    manager = MagicMock()
    manager._get_engine.return_value = mock_engine
    return manager


@pytest.fixture()
def validator(db):
    return DataValidator(db)


# ---------------------------------------------------------------------------
# TestNotEmpty
# ---------------------------------------------------------------------------

class TestNotEmpty:
    def test_success(self, validator, db):
        db._get_engine.return_value = MagicMock()
        with patch("pandas.read_sql", return_value=pd.DataFrame({"cnt": [5]})):
            results = validator.validate("t", [{"type": "not_empty"}])
        assert results[0]["passed"]

    def test_failure(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"cnt": [0]})):
            with pytest.raises(DataQualityError, match="not_empty"):
                validator.validate("t", [{"type": "not_empty"}])


# ---------------------------------------------------------------------------
# TestRowCountRange
# ---------------------------------------------------------------------------

class TestRowCountRange:
    def test_within_range(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"cnt": [50]})):
            results = validator.validate("t", [{"type": "row_count_range", "min": 10, "max": 100}])
        assert results[0]["passed"]

    def test_below_min(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"cnt": [5]})):
            with pytest.raises(DataQualityError):
                validator.validate("t", [{"type": "row_count_range", "min": 10, "max": 100}])

    def test_above_max(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"cnt": [200]})):
            with pytest.raises(DataQualityError):
                validator.validate("t", [{"type": "row_count_range", "min": 10, "max": 100}])


# ---------------------------------------------------------------------------
# TestNoNulls
# ---------------------------------------------------------------------------

class TestNoNulls:
    def test_no_nulls_found(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"n": [0]})):
            results = validator.validate("t", [{"type": "no_nulls", "column": "id"}])
        assert results[0]["passed"]

    def test_nulls_found(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"n": [3]})):
            with pytest.raises(DataQualityError, match="no_nulls"):
                validator.validate("t", [{"type": "no_nulls", "column": "id"}])


# ---------------------------------------------------------------------------
# TestUnique
# ---------------------------------------------------------------------------

class TestUnique:
    def test_all_unique(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"dupes": [0]})):
            results = validator.validate("t", [{"type": "unique", "column": "id"}])
        assert results[0]["passed"]

    def test_duplicates_found(self, validator):
        with patch("pandas.read_sql", return_value=pd.DataFrame({"dupes": [5]})):
            with pytest.raises(DataQualityError, match="unique"):
                validator.validate("t", [{"type": "unique", "column": "id"}])


# ---------------------------------------------------------------------------
# TestFreshness
# ---------------------------------------------------------------------------

class TestFreshness:
    def test_fresh_data(self, validator):
        now = datetime.utcnow()
        with patch("pandas.read_sql", return_value=pd.DataFrame({"latest": [now]})):
            results = validator.validate("t", [{"type": "freshness", "column": "ts", "max_hours": 24}])
        assert results[0]["passed"]

    def test_stale_data(self, validator):
        old = datetime.utcnow() - timedelta(hours=72)
        with patch("pandas.read_sql", return_value=pd.DataFrame({"latest": [old]})):
            with pytest.raises(DataQualityError, match="freshness"):
                validator.validate("t", [{"type": "freshness", "column": "ts", "max_hours": 24}])


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

class TestForceMemoryCleanup:
    def test_runs_without_error(self):
        force_memory_cleanup()
