"""Text mining with Kiwi (kiwipiepy) tokenizer for Korean NLP."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import pandas as pd

from libs.db import DBManager
from libs.logging_config import get_logger

logger = get_logger(__name__)

BATCH_SIZE = 50_000


class TextMiningAnalyzer:
    """Tokenise Korean text, match nouns to category dictionaries, and
    write results back to Redshift.

    Each *config* dict must contain::

        {
            "sql_file":        "sql/bronze/text_source.sql",
            "table_name":      "analytics.review_categories",
            "category_paths":  ["dicts/review.json", ...],
            "category_type":   "product_review",
            "delete_flag":     True,
        }
    """

    def __init__(self, db: DBManager) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def analyze(self, config: dict[str, Any]) -> None:
        """Run the full text-mining pipeline for a single config."""
        sql_file = config["sql_file"]
        table_name = config["table_name"]
        category_paths = config["category_paths"]
        category_type = config["category_type"]
        delete_flag = config.get("delete_flag", True)

        categories = self._load_categories(category_paths)

        if delete_flag:
            self.db.delete_table(table_name)
            logger.info("Cleared target table %s", table_name)

        with self._kiwi_context() as kiwi:
            offset = 0
            total_processed = 0

            while True:
                batch = self._get_batch_data(sql_file, offset)
                if batch.empty:
                    break

                results: list[dict[str, Any]] = []
                for _, row in batch.iterrows():
                    converted = self._convert_row(row, category_type)
                    processed = self._process_text(
                        kiwi, converted, categories, category_type,
                    )
                    if processed:
                        results.append(processed)

                if results:
                    result_df = pd.DataFrame(results)
                    self.db.import_data(result_df, table_name, if_exists="append")

                total_processed += len(batch)
                offset += BATCH_SIZE
                logger.info(
                    "[%s] Processed %d rows so far (batch=%d)",
                    category_type,
                    total_processed,
                    len(batch),
                )

        logger.info(
            "[%s] Analysis complete — %d rows processed",
            category_type,
            total_processed,
        )

    # ------------------------------------------------------------------
    # text processing
    # ------------------------------------------------------------------

    def _process_text(
        self,
        kiwi: Any,
        row_data: dict[str, Any],
        categories: dict[str, list[str]],
        category_type: str,
    ) -> dict[str, Any] | None:
        """Tokenise a single text value, extract nouns, match categories."""
        text_value = row_data.get("title", "")
        if not text_value or not isinstance(text_value, str):
            return None

        # Kiwi tokenization — extract nouns (NNG, NNP)
        tokens = kiwi.tokenize(text_value)
        nouns = [
            t.form for t in tokens
            if t.tag in ("NNG", "NNP") and len(t.form) >= 2
        ]

        if not nouns:
            return None

        matched_categories: list[str] = []
        for category_name, keywords in categories.items():
            if any(noun in keywords for noun in nouns):
                matched_categories.append(category_name)

        return {
            "id": row_data.get("id"),
            "title": text_value,
            "nouns": ",".join(nouns),
            "category": ",".join(matched_categories) if matched_categories else "uncategorized",
            "category_type": category_type,
        }

    # ------------------------------------------------------------------
    # data access helpers
    # ------------------------------------------------------------------

    def _get_batch_data(self, sql_file: str, offset: int) -> pd.DataFrame:
        """Fetch a batch of rows using OFFSET / LIMIT."""
        raw_sql = Path(sql_file).read_text(encoding="utf-8")
        paginated = f"{raw_sql}\nOFFSET {offset}\nLIMIT {BATCH_SIZE}"
        try:
            return pd.read_sql(paginated, self.db._get_engine())
        except Exception as exc:
            logger.error("Batch query failed at offset %d: %s", offset, exc)
            return pd.DataFrame()

    @staticmethod
    def _convert_row(row: pd.Series, category_type: str) -> dict[str, Any]:
        """Transform a DataFrame row into a dict for processing."""
        return {
            "id": row.get("id"),
            "title": str(row.get("title", "")),
            "category_type": category_type,
        }

    # ------------------------------------------------------------------
    # category dictionary
    # ------------------------------------------------------------------

    @staticmethod
    def _load_categories(paths: list[str]) -> dict[str, list[str]]:
        """Merge multiple JSON category files into one dict."""
        merged: dict[str, list[str]] = {}
        for p in paths:
            data = TextMiningAnalyzer._load_json(p)
            merged.update(data)
        logger.info("Loaded %d categories from %d files", len(merged), len(paths))
        return merged

    @staticmethod
    def _load_json(path: str) -> dict[str, list[str]]:
        """Read a JSON file into a dict."""
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # ------------------------------------------------------------------
    # Kiwi lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    @contextmanager
    def _kiwi_context() -> Generator[Any, None, None]:
        """Create and tear down a Kiwi instance to free native memory."""
        from kiwipiepy import Kiwi  # type: ignore[import-untyped]

        kiwi = Kiwi()
        try:
            yield kiwi
        finally:
            del kiwi
            logger.info("Kiwi instance released")
