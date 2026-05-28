"""Logging configuration for Airflow and local environments."""

from __future__ import annotations

import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a logger that works in both Airflow and local environments.

    In Airflow: propagates to the task handler without adding duplicate handlers.
    In local/test: adds a StreamHandler with a readable format.
    """
    logger = logging.getLogger(name)

    if _is_airflow_context():
        logger.propagate = True
        logger.setLevel(logging.INFO)
        return logger

    # Local / test environment — add handler only once
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger


def _is_airflow_context() -> bool:
    """Detect whether we are running inside an Airflow worker."""
    return os.getenv("AIRFLOW_HOME") is not None or "airflow" in sys.modules
