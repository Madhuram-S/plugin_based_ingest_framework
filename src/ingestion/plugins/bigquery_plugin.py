# bigquery_plugin.py
from __future__ import annotations

from typing import Any, Dict
from pyspark.sql import functions as F

from src.ingestion.runner.plugin_contract import IngestResult

class BigQueryPlugin:
    """
    Extract-only plugin:
    - Reads from BigQuery via Spark BigQuery connector.
    - Applies incremental watermark filtering if configured.
    - Returns df + next_state.
    """
    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def extract(self, run_ctx, obj_cfg: Dict[str, Any], prior_state: Dict[str, Any]) -> IngestResult:
        object_name = obj_cfg["object_name"]  # expected dataset.table
        load = obj_cfg.get("load", {}) or {}

        mode = (load.get("mode") or "full").lower()
        strategy = (load.get("strategy") or "full").lower()

        watermark_col = load.get("watermark_column")
        initial_value = load.get("initial_value")

        # Read table
        df = self.spark.read.format("bigquery").option("table", object_name).load()

        ingest_mode = "FULL"
        next_state = {}

        if mode == "incremental" and strategy == "watermark":
            if not watermark_col:
                raise ValueError("BigQuery incremental watermark requires load.watermark_column")

            last = prior_state.get("last_watermark") or initial_value
            if last is None:
                # First run behavior: treat as full or set a floor.
                ingest_mode = "FULL"
            else:
                ingest_mode = "INCR"
                df = df.filter(F.col(watermark_col) > F.lit(last))

                # Compute new watermark from extracted batch (safe: only advance if data present)
                max_row = df.select(F.max(F.col(watermark_col)).alias("m")).collect()
                max_wm = max_row[0]["m"]
                if max_wm is not None:
                    next_state["last_watermark"] = str(max_wm)

        # Avoid expensive counts in MVP (leave None); ops logs can omit
        return IngestResult(
            status="SUCCESS",
            ingest_mode=ingest_mode,
            df=df,
            row_count_source=None,
            next_state=next_state,
            artifacts={"source": "bigquery", "table": object_name},
            warnings=[],
        )
