"""Slack alert callbacks for Airflow DAGs."""

from __future__ import annotations

import json
import os
from typing import Any

import requests

from libs.logging_config import get_logger

logger = get_logger(__name__)


def send_slack(payload: dict[str, Any]) -> None:
    """POST a payload to the Slack webhook URL."""
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping notification")
        return
    resp = requests.post(url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=10)
    resp.raise_for_status()


def on_success_callback(context: dict[str, Any]) -> None:
    """Send a Slack message when the DAG succeeds."""
    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"].isoformat()
    send_slack({
        "text": f":white_check_mark: DAG *{dag_id}* succeeded\n`{execution_date}`",
    })


def on_failure_callback(context: dict[str, Any]) -> None:
    """Send a Slack message when a task fails."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"].isoformat()
    exception = context.get("exception", "")

    # Sensor timeouts are expected during network outages — downgrade severity
    is_sensor = task_id.startswith("check_") or "sensor" in task_id.lower()
    emoji = ":warning:" if is_sensor else ":x:"

    send_slack({
        "text": (
            f"{emoji} Task *{task_id}* failed in DAG *{dag_id}*\n"
            f"`{execution_date}`\n"
            f"```{exception}```"
        ),
    })
