# sql_plugin.py
from __future__ import annotations

from typing import Any, Dict
from pyspark.sql import functions as F

from src.ingestion.runner.plugin_contract import IngestResult

class SqlPlugin:
    """
    Optional: JDBC extract plugin.
    Expects connection_options to include jdbc_url + properties and object_name to be table or query.
    """
    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def extract(self, run_ctx, obj_cfg: Dict[str, Any], prior_state: Dict[str, Any]) -> IngestResult:
        conn = obj_cfg.get("connection_options", {}) or {}
        jdbc_url = conn.get("jdbc_url")
        if not jdbc_url:
            raise ValueError("SQL requires connection_options.jdbc_url")

        props = conn.get("properties", {}) or {}
        # props may include user/password injected via secrets_runtime

        load = obj_cfg.get("load", {}) or {}
        mode = (load.get("mode") or "full").lower()
        strategy = (load.get("strategy") or "full").lower()

        table_or_query = obj_cfg["object_name"]
        dbtable = table_or_query

        # MVP: watermark strategy implemented as query wrapping
        next_state = {}
        ingest_mode = "FULL"

        if mode == "incremental" and strategy == "watermark":
            wc = load.get("watermark_column")
            init = load.get("initial_value")
            last = prior_state.get("last_watermark") or init
            if not wc:
                raise ValueError("SQL watermark requires load.watermark_column")
            if last is not None:
                ingest_mode = "INCR"
                dbtable = f"(SELECT * FROM {table_or_query} WHERE {wc} > '{last}') t"

        df = self.spark.read.jdbc(url=jdbc_url, table=dbtable, properties=props)

        if ingest_mode == "INCR":
            wc = load["watermark_column"]
            max_row = df.select(F.max(F.col(wc)).alias("m")).collect()
            max_wm = max_row[0]["m"]
            if max_wm is not None:
                next_state["last_watermark"] = str(max_wm)

        return IngestResult(
            status="SUCCESS",
            ingest_mode=ingest_mode,
            df=df,
            row_count_source=None,
            next_state=next_state,
            artifacts={"source": "sql", "dbtable": dbtable},
            warnings=[],
        )
