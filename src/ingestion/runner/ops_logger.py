# ops_logger.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from pyspark.sql import functions as F

class OpsLogger:
    def __init__(self, spark, run_log_table: str, dq_log_table: str):
        self.spark = spark
        self.run_log = run_log_table
        self.dq_log = dq_log_table
        self._ensure_tables()

    def _ensure_tables(self):
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.run_log} (
          run_id STRING,
          object_id STRING,
          env STRING,
          config_version STRING,
          registry_sha256 STRING,
          source_system STRING,
          source_type STRING,
          object_name STRING,
          bronze_table STRING,
          schedule_group STRING,
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

    def log_run_start(self, **kwargs):
        row = dict(kwargs)
        row.update({
            "status": "RUNNING",
            "start_ts": None,
            "end_ts": None,
            "rows_read": None,
            "rows_written": None,
            "warnings_json": None,
            "error": None,
            "extra_json": None,
        })
        
        
        #Get schema of run_log table
        temp_df = self.spark.table(self.run_log)
        run_log_schema = temp_df.schema
        
        #Create dataframe with schema of run_log table
        df = self.spark.createDataFrame([row], run_log_schema).withColumn("start_ts", F.current_timestamp())
        
        #Write to run_log table
        df.write.mode("append").saveAsTable(self.run_log)

    def log_run_success(self, run_id: str, object_id: str, rows_read: Optional[int], rows_written: Optional[int],
                        extra: Optional[Dict[str, Any]] = None, warnings: Optional[List[str]] = None):
        extra_json = json.dumps(extra or {})
        warnings_json = json.dumps(warnings or [])
        self.spark.sql(f"""
        UPDATE {self.run_log}
        SET status='SUCCESS',
            end_ts=current_timestamp(),
            rows_read={ 'NULL' if rows_read is None else int(rows_read) },
            rows_written={ 'NULL' if rows_written is None else int(rows_written) },
            extra_json='{extra_json.replace("'", "''")}',
            warnings_json='{warnings_json.replace("'", "''")}'
        WHERE run_id='{run_id}' AND object_id='{object_id}'
        """)

    def log_run_failure(self, run_id: str, object_id: str, error: str):
        esc = (error or "").replace("'", "''")
        self.spark.sql(f"""
        UPDATE {self.run_log}
        SET status='FAILED', end_ts=current_timestamp(), error='{esc}'
        WHERE run_id='{run_id}' AND object_id='{object_id}'
        """)

    def log_run_skipped(self, run_id: str, object_id: str, reason: str):
        esc = (reason or "").replace("'", "''")
        self.spark.sql(f"""
        UPDATE {self.run_log}
        SET status='SKIPPED', end_ts=current_timestamp(), error='{esc}'
        WHERE run_id='{run_id}' AND object_id='{object_id}'
        """)

    def log_dq(self, run_id: str, object_id: str, check_name: str, severity: str, status: str, details: str):
        df = self.spark.createDataFrame([{
            "run_id": run_id,
            "object_id": object_id,
            "check_name": check_name,
            "severity": severity,
            "status": status,
            "details": details,
        }]).withColumn("ts", F.current_timestamp())
        df.write.mode("append").saveAsTable(self.dq_log)
