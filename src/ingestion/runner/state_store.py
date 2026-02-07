# state_store.py
from __future__ import annotations

from typing import Any, Dict, Optional
from pyspark.sql import functions as F

class StateStore:
    """
    Stores incremental state per object.
    IMPORTANT: Only update state on success.
    """
    def __init__(self, spark, table_name: str):
        self.spark = spark
        self.table = table_name
        self._ensure_table()

    def _ensure_table(self):
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
          object_id STRING,
          last_watermark STRING,
          last_cursor STRING,
          last_success_ts TIMESTAMP,
          last_run_id STRING
        )
        USING DELTA
        """)

    def get_state(self, object_id: str) -> Dict[str, Any]:
        df = self.spark.table(self.table).where(F.col("object_id") == object_id).limit(1)
        rows = df.collect()
        if not rows:
            return {"last_watermark": None, "last_cursor": None, "last_success_ts": None, "last_run_id": None}
        r = rows[0].asDict()
        return {
            "last_watermark": r.get("last_watermark"),
            "last_cursor": r.get("last_cursor"),
            "last_success_ts": r.get("last_success_ts"),
            "last_run_id": r.get("last_run_id"),
        }

    def update_on_success(
        self,
        object_id: str,
        run_id: str,
        last_watermark: Optional[str] = None,
        last_cursor: Optional[str] = None,
    ) -> None:
        
        #Get schema of state store table
        temp_df = self.spark.table(self.table)
        state_store_schema = temp_df.schema

        self.spark.createDataFrame([{
            "object_id": object_id,
            "last_watermark": last_watermark,
            "last_cursor": last_cursor,
            "last_run_id": run_id,
        }], state_store_schema).createOrReplaceTempView("__state_upd")

        self.spark.sql(f"""
        MERGE INTO {self.table} t
        USING __state_upd s
        ON t.object_id = s.object_id
        WHEN MATCHED THEN UPDATE SET
          t.last_watermark = COALESCE(s.last_watermark, t.last_watermark),
          t.last_cursor    = COALESCE(s.last_cursor, t.last_cursor),
          t.last_success_ts = current_timestamp(),
          t.last_run_id = s.last_run_id
        WHEN NOT MATCHED THEN INSERT (
          object_id, last_watermark, last_cursor, last_success_ts, last_run_id
        ) VALUES (
          s.object_id, s.last_watermark, s.last_cursor, current_timestamp(), s.last_run_id
        )
        """)
