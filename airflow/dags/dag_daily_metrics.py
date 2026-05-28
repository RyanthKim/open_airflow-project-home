"""Daily metrics pipeline — medallion architecture with text mining.

Schedule: daily at 23:55 UTC
Architecture: Sensor → Silver base → Gold base → Silver derived → Gold derived → Text mining → Validation
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from airflow.sdk import dag, task, Asset, TaskGroup

from libs.alerts import on_failure_callback, on_success_callback
from libs.db import get_db_manager, get_sheets_manager
from libs.logging_config import get_logger
from libs.sensors import RedshiftConnectionSensor
from libs.text_mining import TextMiningAnalyzer
from libs.utils import DataValidator, force_memory_cleanup

logger = get_logger(__name__)

SQL_DIR = Path(__file__).parent / "sql"

# ---------------------------------------------------------------------------
# Assets (dataset-triggered downstream DAGs listen on these)
# ---------------------------------------------------------------------------
PLAN_INFO_ASSET = Asset("analytics://plan_info")
BILLING_ASSET = Asset("analytics://billing")

# ---------------------------------------------------------------------------
# Text-mining configs
# ---------------------------------------------------------------------------
TEXT_MINING_CONFIGS = [
    {
        "sql_file": str(SQL_DIR / "bronze" / "text_product_review.sql"),
        "table_name": "analytics.product_review_categories",
        "category_paths": [str(SQL_DIR.parent / "dicts" / "review.json")],
        "category_type": "product_review",
        "delete_flag": True,
    },
    {
        "sql_file": str(SQL_DIR / "bronze" / "text_support_ticket.sql"),
        "table_name": "analytics.support_ticket_categories",
        "category_paths": [str(SQL_DIR.parent / "dicts" / "support.json")],
        "category_type": "support_ticket",
        "delete_flag": True,
    },
    {
        "sql_file": str(SQL_DIR / "bronze" / "text_faq_article.sql"),
        "table_name": "analytics.faq_article_categories",
        "category_paths": [str(SQL_DIR.parent / "dicts" / "faq.json")],
        "category_type": "faq_article",
        "delete_flag": False,
    },
    {
        "sql_file": str(SQL_DIR / "bronze" / "text_user_feedback.sql"),
        "table_name": "analytics.user_feedback_categories",
        "category_paths": [str(SQL_DIR.parent / "dicts" / "feedback.json")],
        "category_type": "user_feedback",
        "delete_flag": True,
    },
]

# ---------------------------------------------------------------------------
# Validation rules
# ---------------------------------------------------------------------------
VALIDATION_RULES: dict[str, list[dict]] = {
    "analytics.plan_info": [
        {"type": "not_empty"},
        {"type": "no_nulls", "column": "plan_id"},
        {"type": "unique", "column": "plan_id"},
    ],
    "analytics.daily_user_counts": [
        {"type": "not_empty"},
        {"type": "row_count_range", "min": 1, "max": 500_000},
        {"type": "freshness", "column": "created_at", "max_hours": 48},
    ],
    "analytics.billing": [
        {"type": "not_empty"},
        {"type": "no_nulls", "column": "billing_id"},
    ],
    "analytics.subscription_summary": [
        {"type": "not_empty"},
    ],
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
    "pool": "redshift_pool",
    "pool_slots": 2,
}


@dag(
    dag_id="daily_metrics",
    schedule="55 23 * * *",
    start_date="2024-01-01",
    catchup=False,
    max_active_tasks=6,
    default_args=DEFAULT_ARGS,
    on_success_callback=on_success_callback,
    tags=["daily", "metrics", "medallion"],
)
def daily_metrics():

    # -- sensor --------------------------------------------------------------
    check_redshift = RedshiftConnectionSensor(
        task_id="check_redshift_connection",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
        pool="redshift_pool",
        pool_slots=1,
    )

    # -- sheets ingestion ----------------------------------------------------
    @task()
    def update_sheets_data():
        """Pull reference data from Google Sheets into Redshift."""
        db = get_db_manager()
        sheets = get_sheets_manager()
        try:
            df = sheets.get_sheet(
                spreadsheet_key="1A2B3C4D5E6F_sample_key",
                worksheet_name="plan_master",
            )
            db.import_data(df, "analytics.plan_info", if_exists="replace")
            logger.info("Sheets → analytics.plan_info (%d rows)", len(df))
        finally:
            db.close()

    sheets_update = update_sheets_data()

    # -- silver base ---------------------------------------------------------
    with TaskGroup("silver_base_group") as silver_base:

        @task(outlets=[PLAN_INFO_ASSET])
        def silver_plan_info():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "silver" / "plan_info.sql")
            finally:
                db.close()

        @task()
        def silver_user_accounts():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "silver" / "user_accounts.sql")
            finally:
                db.close()

        @task()
        def silver_daily_signups():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "silver" / "daily_signups.sql")
            finally:
                db.close()

        silver_plan_info()
        silver_user_accounts()
        silver_daily_signups()

    # -- gold base -----------------------------------------------------------
    with TaskGroup("gold_base_group") as gold_base:

        @task(outlets=[BILLING_ASSET])
        def gold_billing():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "gold" / "billing.sql")
            finally:
                db.close()

        @task()
        def gold_daily_user_counts():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "gold" / "daily_user_counts.sql")
            finally:
                db.close()

        gold_billing()
        gold_daily_user_counts()

    # -- silver derived ------------------------------------------------------
    with TaskGroup("silver_derived_group") as silver_derived:

        @task()
        def silver_subscription_events():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "silver" / "subscription_events.sql")
            finally:
                db.close()

        @task()
        def silver_payment_history():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "silver" / "payment_history.sql")
            finally:
                db.close()

        silver_subscription_events()
        silver_payment_history()

    # -- gold derived --------------------------------------------------------
    with TaskGroup("gold_derived_group") as gold_derived:

        @task()
        def gold_subscription_summary():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "gold" / "subscription_summary.sql")
            finally:
                db.close()

        @task()
        def gold_viral_metrics():
            db = get_db_manager()
            try:
                db.execute_sql_file(SQL_DIR / "gold" / "viral_metrics.sql")
            finally:
                db.close()

        gold_subscription_summary()
        gold_viral_metrics()

    # -- text mining ---------------------------------------------------------
    with TaskGroup("text_mining_group") as text_mining:

        @task()
        def run_text_mining(config: dict):
            db = get_db_manager()
            try:
                analyzer = TextMiningAnalyzer(db)
                analyzer.analyze(config)
            finally:
                db.close()

        for cfg in TEXT_MINING_CONFIGS:
            run_text_mining.override(
                task_id=f"mine_{cfg['category_type']}",
            )(config=cfg)

    @task()
    def cleanup_memory():
        """Release memory after text-mining tasks."""
        force_memory_cleanup()

    memory_cleanup = cleanup_memory()

    # -- validation ----------------------------------------------------------
    with TaskGroup("validation_group") as validation:

        @task()
        def validate_tables():
            db = get_db_manager()
            try:
                validator = DataValidator(db)
                for table, rules in VALIDATION_RULES.items():
                    validator.validate(table, rules)
                logger.info("All validation rules passed")
            finally:
                db.close()

        validate_tables()

    # -- dependencies --------------------------------------------------------
    check_redshift >> [silver_base, sheets_update]
    sheets_update >> gold_base
    silver_base >> gold_base >> silver_derived >> gold_derived >> text_mining
    text_mining >> memory_cleanup >> validation


daily_metrics()
