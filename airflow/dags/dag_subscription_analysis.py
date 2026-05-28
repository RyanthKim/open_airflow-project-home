"""Subscription analysis pipeline — triggered by BILLING_ASSET updates.

Runs automatically when the daily_metrics DAG publishes the billing Asset.
Follows Bronze temp → Silver base → Silver final → Cleanup.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from airflow.sdk import dag, task, Asset

from libs.alerts import on_failure_callback, on_success_callback
from libs.db import get_db_manager
from libs.logging_config import get_logger

logger = get_logger(__name__)

SQL_DIR = Path(__file__).parent / "sql"

BILLING_ASSET = Asset("analytics://billing")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": on_failure_callback,
    "pool": "redshift_pool",
    "pool_slots": 2,
}


@dag(
    dag_id="subscription_analysis",
    schedule=(BILLING_ASSET,),
    start_date="2024-01-01",
    catchup=False,
    max_active_tasks=4,
    default_args=DEFAULT_ARGS,
    on_success_callback=on_success_callback,
    tags=["subscription", "asset-triggered"],
)
def subscription_analysis():

    # -- Phase 1: Bronze temp tables ----------------------------------------

    @task()
    def bronze_billing_temp():
        """Create temp table with raw billing events for the current period."""
        db = get_db_manager()
        try:
            db.execute_sql_file(SQL_DIR / "bronze" / "billing_events_temp.sql")
            logger.info("Bronze billing temp table created")
        finally:
            db.close()

    @task()
    def bronze_subscription_events_temp():
        """Create temp table joining subscriptions with plan metadata."""
        db = get_db_manager()
        try:
            db.execute_sql_file(SQL_DIR / "bronze" / "subscription_events_temp.sql")
            logger.info("Bronze subscription events temp table created")
        finally:
            db.close()

    @task()
    def bronze_period_calc_temp():
        """Calculate subscription periods using LEAD/LAG window functions.

        Computes: period_start, period_end, days_active, is_churned.
        """
        db = get_db_manager()
        try:
            db.execute_sql_file(SQL_DIR / "bronze" / "period_calc_temp.sql")
            logger.info("Bronze period calculation temp table created")
        finally:
            db.close()

    # -- Phase 2: Silver base -----------------------------------------------

    @task()
    def silver_subscription_periods():
        """Deduplicate and merge overlapping subscription periods."""
        db = get_db_manager()
        try:
            db.execute_sql_file(SQL_DIR / "silver" / "subscription_periods.sql")
            logger.info("Silver subscription periods written")
        finally:
            db.close()

    @task()
    def silver_cohort_base():
        """Build cohort base table from subscription periods."""
        db = get_db_manager()
        try:
            db.execute_sql_file(SQL_DIR / "silver" / "cohort_base.sql")
            logger.info("Silver cohort base written")
        finally:
            db.close()

    # -- Phase 3: Silver final ----------------------------------------------

    @task()
    def silver_retention_matrix():
        """Produce month-over-month retention matrix."""
        db = get_db_manager()
        try:
            db.execute_sql_file(SQL_DIR / "silver" / "retention_matrix.sql")
            logger.info("Silver retention matrix written")
        finally:
            db.close()

    @task()
    def silver_revenue_by_cohort():
        """Aggregate revenue by signup cohort and subscription month."""
        db = get_db_manager()
        try:
            db.execute_sql_file(SQL_DIR / "silver" / "revenue_by_cohort.sql")
            logger.info("Silver revenue by cohort written")
        finally:
            db.close()

    # -- Cleanup (always runs) ----------------------------------------------

    @task(trigger_rule="all_done")
    def cleanup_temp_tables():
        """Drop all temp tables regardless of upstream success/failure."""
        db = get_db_manager()
        try:
            temp_tables = [
                "analytics.tmp_billing_events",
                "analytics.tmp_subscription_events",
                "analytics.tmp_period_calc",
            ]
            for tbl in temp_tables:
                db.delete_table(tbl)
            logger.info("Temp tables cleaned up")
        finally:
            db.close()

    # -- Dependencies -------------------------------------------------------
    t_billing = bronze_billing_temp()
    t_sub_events = bronze_subscription_events_temp()
    t_period = bronze_period_calc_temp()

    t_periods = silver_subscription_periods()
    t_cohort = silver_cohort_base()

    t_retention = silver_retention_matrix()
    t_revenue = silver_revenue_by_cohort()

    t_cleanup = cleanup_temp_tables()

    # Phase 1 → Phase 2 → Phase 3 → Cleanup
    [t_billing, t_sub_events] >> t_period >> [t_periods, t_cohort]
    [t_periods, t_cohort] >> [t_retention, t_revenue] >> t_cleanup


subscription_analysis()
