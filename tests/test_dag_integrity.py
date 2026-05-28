"""DAG integrity tests -- verify all DAGs parse without errors."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Ensure libs package is importable
_dags_dir = str(Path(__file__).resolve().parent.parent / "airflow" / "dags")
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)

# Minimal Airflow config so DagBag doesn't need a running DB
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow_test")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:////tmp/airflow_test.db")

DAGS_FOLDER = _dags_dir
EXPECTED_DAGS = {"dag_daily_metrics", "dag_subscription_analysis"}


@pytest.fixture()
def dagbag():
    """Parse all DAG files in the dags folder."""
    from airflow.models import DagBag

    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    """Every DAG file should import without errors."""
    assert dagbag.import_errors == {}, (
        f"DAG import errors: {dagbag.import_errors}"
    )


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_expected_dags_exist(dagbag, dag_id):
    """Portfolio DAGs should be discovered by the DagBag."""
    if not dagbag.dags:
        pytest.skip("No DAG files in dags folder (private DAGs not committed)")
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found"


def test_task_counts_are_reasonable(dagbag):
    """Each DAG should have at least 1 task and no more than 200."""
    if not dagbag.dags:
        pytest.skip("No DAG files in dags folder")
    for dag_id, dag in dagbag.dags.items():
        task_count = len(dag.tasks)
        assert 1 <= task_count <= 200, (
            f"DAG '{dag_id}' has {task_count} tasks -- outside [1, 200]"
        )
