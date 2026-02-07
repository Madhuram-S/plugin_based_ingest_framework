# lock.py
from __future__ import annotations

from typing import Optional
from pyspark.sql import functions as F

class ObjectLock:
    """
    Simple Delta-table lock with TTL.
    Prevents concurrent runs for the same object_id.

    Table schema:
      object_id STRING
      run_id STRING
      acquired_ts TIMESTAMP
      expires_ts TIMESTAMP
    """
    def __init__(self, spark, table_name: str):
        self.spark = spark
        self.table = table_name
        self._ensure_table()

    def _ensure_table(self):
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
          object_id STRING,
          run_id STRING,
          acquired_ts TIMESTAMP,
          expires_ts TIMESTAMP
        )
        USING DELTA
        """)

    def try_acquire(self, object_id: str, run_id: str, ttl_minutes: int = 120) -> bool:
        # Clean expired locks
        self.spark.sql(f"DELETE FROM {self.table} WHERE expires_ts < current_timestamp()")

        # Attempt to insert lock if no existing lock
        self.spark.createDataFrame([{
            "object_id": object_id,
            "run_id": run_id,
        }]).createOrReplaceTempView("__lock_req")

        # Insert only if object_id absent (atomic-ish via MERGE)
        self.spark.sql(f"""
        MERGE INTO {self.table} t
        USING __lock_req s
        ON t.object_id = s.object_id
        WHEN NOT MATCHED THEN INSERT (
          object_id, run_id, acquired_ts, expires_ts
        ) VALUES (
          s.object_id, s.run_id, current_timestamp(), timestampadd(MINUTE, {ttl_minutes}, current_timestamp())
        )
        """)

        # Verify ownership
        df = self.spark.table(self.table).where((F.col("object_id") == object_id) & (F.col("run_id") == run_id)).limit(1)
        return df.count() == 1

    def release(self, object_id: str, run_id: str) -> None:
        self.spark.sql(f"""
        DELETE FROM {self.table}
        WHERE object_id = '{object_id.replace("'", "''")}'
          AND run_id = '{run_id.replace("'", "''")}'
        """)
