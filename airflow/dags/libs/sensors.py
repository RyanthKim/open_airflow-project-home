"""Custom Airflow sensors."""

from __future__ import annotations

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from libs.db import get_db_manager
from libs.logging_config import get_logger

logger = get_logger(__name__)


class RedshiftConnectionSensor(BaseSensorOperator):
    """Verify Redshift connectivity before pipeline tasks run.

    Useful when the Airflow worker needs VPN / network access to
    Redshift and you want the DAG to wait rather than fail immediately.
    """

    def __init__(self, *, conn_id: str = "redshift_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def poke(self, context: Context) -> bool:
        try:
            db = get_db_manager()
            db.execute_query("SELECT 1")
            logger.info("Redshift connection OK")
            return True
        except Exception as exc:
            logger.warning("Redshift not reachable: %s", exc)
            return False
