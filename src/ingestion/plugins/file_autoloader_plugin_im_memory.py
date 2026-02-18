# file_autoloader_plugin.py
"""
Auto Loader–based file ingestion for CSV, JSON, Parquet, and Avro.
Uses Spark Structured Streaming with cloudFiles source and trigger(once=True)
to read only new files since last run (checkpoint). Fits the runner framework:
extract(run_ctx, obj_cfg, prior_state) -> IngestResult with batch DataFrame.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from src.ingestion.runner.plugin_contract import IngestResult

SUPPORTED_FORMATS = ("csv", "json", "parquet", "avro")


class FileAutoloaderPlugin:
    """
    Extract plugin using Databricks Auto Loader (cloudFiles).
    Reads new files since last checkpoint and returns a single batch DataFrame per run.
    """

    def __init__(self, spark: SparkSession, dbutils: Any):
        self.spark = spark
        self.dbutils = dbutils

    def extract(
        self,
        run_ctx: Any,
        obj_cfg: Dict[str, Any],
        prior_state: Dict[str, Any],
    ) -> IngestResult:
        landing = obj_cfg.get("landing", {}) or {}
        path = landing.get("path")
        checkpoint = landing.get("checkpoint")
        schema_location = landing.get("schema_location")
        fmt = (landing.get("format") or "csv").lower()

        if not path:
            return IngestResult(
                status="SKIPPED",
                ingest_mode="FILE_AUTOLOADER",
                df=None,
                row_count_source=0,
                warnings=["landing.path is required for file_autoloader"],
            )
        if not checkpoint:
            return IngestResult(
                status="SKIPPED",
                ingest_mode="FILE_AUTOLOADER",
                df=None,
                row_count_source=0,
                warnings=["landing.checkpoint is required for file_autoloader (use incremental.mode: file_autoloader or set landing.checkpoint)"],
            )
        if fmt not in SUPPORTED_FORMATS:
            return IngestResult(
                status="SKIPPED",
                ingest_mode="FILE_AUTOLOADER",
                df=None,
                row_count_source=0,
                warnings=[f"landing.format must be one of {SUPPORTED_FORMATS}; got {fmt}"],
            )

        # Schema location required for cloudFiles (schema evolution)
        if not schema_location:
            schema_location = path.rstrip("/") + "/_schema"

        options = dict(landing.get("options", {}) or {})
        infer_cols = options.pop("inferColumnTypes", "true")
        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.inferColumnTypes", infer_cols)
        )

        # Format-specific options (e.g. CSV: header, sep, escape)
        for k, v in options.items():
            reader = reader.option(k, str(v))

        stream_df = reader.load(path)

        # Single batch: trigger once, write to memory sink, then read back.
        # Use a short query name to avoid truncation; memory sink may register in spark_catalog.default (Unity Catalog).
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", (run_ctx.run_id + "_" + (obj_cfg.get("object_id") or "obj")))[:48]
        query_name = f"_al_{safe_name}"

        try:
            query = (
                stream_df.writeStream
                .option("checkpointLocation", checkpoint)
                .trigger(once=True)
                .format("memory")
                .queryName(query_name)
                .start()
            )
            query.awaitTermination()
        except Exception as e:
            return IngestResult(
                status="SKIPPED",
                ingest_mode="FILE_AUTOLOADER",
                df=None,
                row_count_source=0,
                warnings=[f"Auto Loader stream failed: {e}"],
            )

        df = None
        candidates = [
            query_name,
            f"spark_catalog.default.{query_name}",
            f"hive_metastore.default.{query_name}",
            f"default.{query_name}",
        ]
        for table_ref in candidates:
            try:
                df = self.spark.table(table_ref)
                break
            except Exception:
                continue
        if df is None:
            return IngestResult(
                status="SKIPPED",
                ingest_mode="FILE_AUTOLOADER",
                df=None,
                row_count_source=0,
                warnings=[f"Auto Loader batch table not found (tried: {', '.join(candidates)}). Set spark.sql.legacy.createHiveTableByDefault or use a catalog that has the memory sink table."],
            )
        row_count = df.count()
        try:
            self.spark.catalog.dropTempView(query_name)
        except Exception:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS spark_catalog.default.{query_name}")
            except Exception:
                pass

        return IngestResult(
            status="SUCCESS",
            ingest_mode="FILE_AUTOLOADER",
            df=df,
            row_count_source=row_count,
            next_state={},
            artifacts={
                "source": "file_autoloader",
                "path": path,
                "format": fmt,
                "checkpoint": checkpoint,
                "schema_location": schema_location,
            },
            warnings=[],
        )
