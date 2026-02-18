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
      layer STRING
      run_id STRING
      acquired_ts TIMESTAMP
      expires_ts TIMESTAMP
    Key: (object_id, layer).
    """
    def __init__(self, spark, table_name: str):
        self.spark = spark
        self.table = table_name
        self._ensure_table()

    def _ensure_table(self):
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
          object_id STRING,
          layer STRING,
          run_id STRING,
          acquired_ts TIMESTAMP,
          expires_ts TIMESTAMP
        )
        USING DELTA
        """)
        self._add_layer_column_if_missing()

    def _add_layer_column_if_missing(self):
        """Migration: add layer column to existing lock tables."""
        try:
            existing = {f.name.lower() for f in self.spark.table(self.table).schema.fields}
            if "layer" not in existing:
                self.spark.sql(f"ALTER TABLE {self.table} ADD COLUMN (layer STRING)")
        except Exception:
            pass

    def try_acquire(self, object_id: str, run_id: str, layer: str = "bronze", ttl_minutes: int = 120) -> bool:
        # Clean expired locks
        self.spark.sql(f"DELETE FROM {self.table} WHERE expires_ts < current_timestamp()")

        layer_esc = (layer or "bronze").replace("'", "''")
        oid_esc = object_id.replace("'", "''")
        rid_esc = run_id.replace("'", "''")

        # Insert only if (object_id, layer) absent; Delta MERGE is atomic so only one caller wins.
        self.spark.sql(f"""
        MERGE INTO {self.table} t
        USING (SELECT '{oid_esc}' AS object_id, '{layer_esc}' AS layer, '{rid_esc}' AS run_id) s
        ON t.object_id = s.object_id AND t.layer = s.layer
        WHEN NOT MATCHED THEN INSERT (
          object_id, layer, run_id, acquired_ts, expires_ts
        ) VALUES (
          s.object_id, s.layer, s.run_id, current_timestamp(), timestampadd(MINUTE, {ttl_minutes}, current_timestamp())
        )
        """)

        # Verify ownership
        df = (
            self.spark.table(self.table)
            .where((F.col("object_id") == object_id) & (F.col("layer") == layer) & (F.col("run_id") == run_id))
            .limit(1)
        )
        return df.count() == 1

    def release(self, object_id: str, run_id: str, layer: str = "bronze") -> None:
        layer_esc = (layer or "bronze").replace("'", "''")
        self.spark.sql(f"""
        DELETE FROM {self.table}
        WHERE object_id = '{object_id.replace("'", "''")}'
          AND layer = '{layer_esc}'
          AND run_id = '{run_id.replace("'", "''")}'
        """)
