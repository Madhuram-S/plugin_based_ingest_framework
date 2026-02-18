# file_autoloader_plugin.py
"""
Auto Loader–based file ingestion for CSV, JSON, Parquet, and Avro.
Uses Spark Structured Streaming with cloudFiles source, trigger(once=True),
and a Delta micro-batch sink (foreachBatch). Each micro-batch is written with
run_id and batch_id; we read back all rows for the current run_id and return
them (all micro-batches in this run). No in-memory sink.
"""
from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.ingestion.runner.plugin_contract import IngestResult

SUPPORTED_FORMATS = ("csv", "json", "parquet", "avro")

# Columns added in foreachBatch; dropped before return
RUN_ID_COL = "_autoloader_run_id"
BATCH_ID_COL = "_autoloader_batch_id"


class FileAutoloaderPlugin:
    """
    Extract plugin using Databricks Auto Loader (cloudFiles).
    Micro-batch: trigger(once=True) + foreachBatch to Delta; each batch tagged with run_id.
    Read back all rows for this run_id (all micro-batches in the current run) and return.
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
        for k, v in options.items():
            reader = reader.option(k, str(v))

        stream_df = reader.load(path)

        batch_output_path = checkpoint.rstrip("/") + "/_batch_output"
        current_run_id = run_ctx.run_id

        def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            (
                batch_df
                .withColumn(RUN_ID_COL, F.lit(current_run_id))
                .withColumn(BATCH_ID_COL, F.lit(batch_id))
                .write.mode("append")
                .format("delta")
                .save(batch_output_path)
            )

        try:
            query = (
                stream_df.writeStream
                .option("checkpointLocation", checkpoint)
                .trigger(once=True)
                .foreachBatch(write_micro_batch)
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

        try:
            batch_table = self.spark.read.format("delta").load(batch_output_path)
            if batch_table.isEmpty():
                schema = batch_table.schema
                for drop_col in (RUN_ID_COL, BATCH_ID_COL):
                    if drop_col in schema.names:
                        schema = StructType([f for f in schema.fields if f.name != drop_col])
                df = self.spark.createDataFrame([], schema)
                row_count = 0
            else:
                if RUN_ID_COL in batch_table.schema.names:
                    df = batch_table.filter(F.col(RUN_ID_COL) == current_run_id).drop(RUN_ID_COL, BATCH_ID_COL)
                else:
                    max_batch = batch_table.agg(F.max(BATCH_ID_COL)).first()[0]
                    if max_batch is not None:
                        df = batch_table.filter(F.col(BATCH_ID_COL) == max_batch).drop(BATCH_ID_COL)
                    else:
                        df = batch_table.drop(BATCH_ID_COL) if BATCH_ID_COL in batch_table.schema.names else batch_table
                row_count = df.count()
        except Exception as e:
            err_msg = str(e).lower()
            if "path does not exist" in err_msg or "cannot find" in err_msg or "no such file" in err_msg:
                df = self.spark.createDataFrame([], StructType([]))
                row_count = 0
            else:
                return IngestResult(
                    status="SKIPPED",
                    ingest_mode="FILE_AUTOLOADER",
                    df=None,
                    row_count_source=0,
                    warnings=[f"Failed to read Auto Loader batch from {batch_output_path}: {e}"],
                )

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
