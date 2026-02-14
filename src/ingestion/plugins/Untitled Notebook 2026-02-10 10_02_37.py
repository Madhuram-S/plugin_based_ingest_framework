# Databricks notebook source
from __future__ import annotations

from typing import Any, Dict
from pyspark.sql import functions as F

from src.ingestion.runner.plugin_contract import IngestResult

# COMMAND ----------



class FilePlugin:
    """
    Extract-only plugin:
    - Batch read by default (CSV/Parquet/JSON)
    - Auto Loader streaming is best handled as a separate job/task; MVP can do batch for Excel/CSV
    """
    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def extract(self, run_ctx, obj_cfg: Dict[str, Any], prior_state: Dict[str, Any]) -> IngestResult:
        landing = obj_cfg.get("landing", {}) or {}
        path = landing.get("path")
        fmt = (landing.get("format") or "csv").lower()

        if not path:
            raise ValueError("File requires landing.path")

        # Excel note: prefer landing Excel->CSV externally or use a library.
        # MVP: treat Excel as already converted to CSV in landing.
        if fmt in {"xlsx", "xls"}:
            raise ValueError("MVP: Excel should be converted to CSV/Parquet before runner ingestion (or implement spark-excel).")

        reader = self.spark.read.format(fmt)
        options = landing.get("options", {}) or {}
        for k, v in options.items():
            reader = reader.option(k, v)

        df = reader.load(path)

        return IngestResult(
            status="SUCCESS",
            ingest_mode="FILE_BATCH",
            df=df,
            row_count_source=None,
            next_state={"last_modified_date":'2026-02-10'},
            artifacts={"source": "file", "path": path, "format": fmt},
            warnings=[],
        )

