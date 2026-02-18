# ops_logger.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from pyspark.sql import functions as F

# Event types for append-only run log (no UPDATEs → no Delta write conflicts from concurrent tasks).
EVENT_START = "START"
EVENT_END = "END"
EVENT_SKIP = "SKIP"

# Example view for "one row per run/object with final status":
#   SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY run_id, object_id ORDER BY event_ts DESC) AS rn
#                   FROM run_log ) WHERE rn = 1
# Run metrics at end of pipeline: see docs/RUN_METRICS_FROM_EVENTS.md (aggregate by run_id or job_run_id).


class OpsLogger:
    """
    Append-only ops logging. Run log is an event stream: START, END, SKIP.
    No UPDATEs on run_log → safe under many concurrent tasks (avoids Delta write conflicts).
    For "one row per run/object with final status" use a view (see module-level comment).
    """
    def __init__(self, spark, run_log_table: str, dq_log_table: str):
        self.spark = spark
        self.run_log = run_log_table
        self.dq_log = dq_log_table
        self._ensure_tables()

    def _ensure_tables(self):
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.run_log} (
          event_type STRING,
          event_ts TIMESTAMP,
          run_id STRING,
          object_id STRING,
          layer STRING,
          env STRING,
          config_version STRING,
          registry_sha256 STRING,
          source_system STRING,
          source_type STRING,
          object_name STRING,
          target_table STRING,
          bronze_table STRING,
          schedule_group STRING,
          shard_id INT,
          shard_count INT,
          status STRING,
          start_ts TIMESTAMP,
          end_ts TIMESTAMP,
          rows_read LONG,
          rows_written LONG,
          warnings_json STRING,
          error STRING,
          extra_json STRING
        )
        USING DELTA
        """)

        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.dq_log} (
          run_id STRING,
          object_id STRING,
          check_name STRING,
          severity STRING,
          status STRING,
          details STRING,
          ts TIMESTAMP
        )
        USING DELTA
        """)
        self._add_run_log_columns_if_missing()

    def _add_run_log_columns_if_missing(self):
        """Add event_type, event_ts, layer, target_table to existing run_log tables (migration)."""
        existing = {f.name.lower() for f in self.spark.table(self.run_log).schema.fields}
        for col_name, col_type in [
            ("event_type", "STRING"),
            ("event_ts", "TIMESTAMP"),
            ("layer", "STRING"),
            ("target_table", "STRING"),
        ]:
            if col_name.lower() not in existing:
                self.spark.sql(f"ALTER TABLE {self.run_log} ADD COLUMN ({col_name} {col_type})")

    def log_run_start(self, **kwargs):
        row = dict(kwargs)
        row.setdefault("layer", "bronze")
        row.setdefault("shard_id", None)
        row.setdefault("shard_count", None)
        row.setdefault("target_table", None)
        row.setdefault("bronze_table", None)
        row["event_type"] = EVENT_START
        row["event_ts"] = None  # set below
        row["status"] = "RUNNING"
        row["start_ts"] = None  # set below
        row["end_ts"] = None
        row["rows_read"] = None
        row["rows_written"] = None
        row["warnings_json"] = None
        row["error"] = None
        row["extra_json"] = None

        temp_df = self.spark.table(self.run_log)
        run_log_schema = temp_df.schema
        df = (
            self.spark.createDataFrame([row], run_log_schema)
            .withColumn("event_ts", F.from_utc_timestamp(F.current_timestamp(), "America/New_York"))
            .withColumn("start_ts", F.col("event_ts"))
        )
        df.write.mode("append").saveAsTable(self.run_log)

    # Metadata keys we accept on END/SKIP so rows are self-contained (no join to START needed).
    _RUN_LOG_META_KEYS = frozenset({
        "env", "config_version", "registry_sha256", "source_system", "source_type",
        "object_name", "target_table", "bronze_table", "schedule_group", "shard_id", "shard_count", "layer",
        "start_ts",  # so END row has run start time without joining to START event
    })

    def _append_end_event(
        self,
        run_id: str,
        object_id: str,
        status: str,
        event_type: str = EVENT_END,
        rows_read: Optional[int] = None,
        rows_written: Optional[int] = None,
        error: Optional[str] = None,
        extra_json: Optional[str] = None,
        warnings_json: Optional[str] = None,
        **metadata,
    ):
        """Append one END/SKIP event row (no UPDATE). Include metadata so END rows are self-contained."""
        temp_df = self.spark.table(self.run_log)
        schema = temp_df.schema
        row = {
            "event_type": event_type,
            "event_ts": None,
            "run_id": run_id,
            "object_id": object_id,
            "layer": None,
            "env": None,
            "config_version": None,
            "registry_sha256": None,
            "source_system": None,
            "source_type": None,
            "object_name": None,
            "target_table": None,
            "bronze_table": None,
            "schedule_group": None,
            "shard_id": None,
            "shard_count": None,
            "status": status,
            "start_ts": None,
            "end_ts": None,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "warnings_json": warnings_json,
            "error": error,
            "extra_json": extra_json,
        }
        for k, v in metadata.items():
            if k in self._RUN_LOG_META_KEYS and k in row:                
                row[k] = v
        
        df = (
            self.spark.createDataFrame([row], schema)
            .withColumn("event_ts", F.from_utc_timestamp(F.current_timestamp(), "America/New_York"))
            .withColumn("end_ts", F.col("event_ts"))
        )
        df.write.mode("append").saveAsTable(self.run_log)

    def log_run_success(
        self,
        run_id: str,
        object_id: str,
        rows_read: Optional[int] = None,
        rows_written: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
        warnings: Optional[List[str]] = None,
        **metadata,
    ):
        print(metadata['start_ts'])
        self._append_end_event(
            run_id=run_id,
            object_id=object_id,
            status="SUCCESS",
            rows_read=rows_read,
            rows_written=rows_written,
            extra_json=json.dumps(extra or {}),
            warnings_json=json.dumps(warnings or []),
            **{k: v for k, v in metadata.items() if k in self._RUN_LOG_META_KEYS},
        )

    def log_run_failure(self, run_id: str, object_id: str, error: str, **metadata):
        self._append_end_event(
            run_id=run_id,
            object_id=object_id,
            status="FAILED",
            error=error or "",
            **{k: v for k, v in metadata.items() if k in self._RUN_LOG_META_KEYS},
        )

    def log_run_skipped(self, run_id: str, object_id: str, reason: str, **metadata):
        self._append_end_event(
            run_id=run_id,
            object_id=object_id,
            status="SKIPPED",
            event_type=EVENT_SKIP,
            error=reason or "",
            **{k: v for k, v in metadata.items() if k in self._RUN_LOG_META_KEYS},
        )

    @staticmethod
    def canonical_run_log_view_sql(run_log_table: str, view_name: str = "run_log_canonical") -> str:
        """
        Returns SQL to create a view that coalesces START metadata onto END/SKIP rows.
        One row per (run_id, object_id) with final status and full context (env, schedule_group, etc.).
        Use when END rows were written without metadata (legacy) or to guarantee a single canonical row.
        """
        return f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT
          e.run_id,
          e.object_id,
          COALESCE(e.layer, s.layer) AS layer,
          e.event_type,
          e.event_ts,
          e.status,
          COALESCE(e.env,           s.env)           AS env,
          COALESCE(e.config_version, s.config_version) AS config_version,
          COALESCE(e.registry_sha256, s.registry_sha256) AS registry_sha256,
          COALESCE(e.source_system, s.source_system) AS source_system,
          COALESCE(e.source_type,   s.source_type)   AS source_type,
          COALESCE(e.object_name,   s.object_name)   AS object_name,
          COALESCE(e.target_table,  s.target_table,  e.bronze_table, s.bronze_table) AS target_table,
          COALESCE(e.bronze_table,  s.bronze_table)  AS bronze_table,
          COALESCE(e.schedule_group, s.schedule_group) AS schedule_group,
          COALESCE(e.shard_id,      s.shard_id)      AS shard_id,
          COALESCE(e.shard_count,   s.shard_count)   AS shard_count,
          s.start_ts,
          e.end_ts,
          e.rows_read,
          e.rows_written,
          e.warnings_json,
          e.error,
          e.extra_json
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (PARTITION BY run_id, object_id ORDER BY event_ts DESC) AS rn
          FROM {run_log_table}
          WHERE event_type IN ('END', 'SKIP')
        ) e
        LEFT JOIN (
          SELECT run_id, object_id, layer, env, config_version, registry_sha256,
                 source_system, source_type, object_name, target_table, bronze_table, schedule_group,
                 shard_id, shard_count, start_ts
          FROM {run_log_table}
          WHERE event_type = 'START'
        ) s ON e.run_id = s.run_id AND e.object_id = s.object_id
        WHERE e.rn = 1
        """

    def log_dq(self, run_id: str, object_id: str, check_name: str, severity: str, status: str, details: str):
        df = self.spark.createDataFrame([{
            "run_id": run_id,
            "object_id": object_id,
            "check_name": check_name,
            "severity": severity,
            "status": status,
            "details": details,
        }]).withColumn("ts", F.from_utc_timestamp(F.current_timestamp(), "America/New_York"))
        df.write.mode("append").saveAsTable(self.dq_log)
