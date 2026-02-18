# state_store.py
from __future__ import annotations

from typing import Any, Dict, Optional
from pyspark.sql import functions as F

class StateStore:
    """
    Stores incremental state per object.
    IMPORTANT: Only update state on success.
    Uses optimistic concurrency: pass expected_last_run_id (from get_state) so we
    only update if no other task updated the row in between (avoids watermark corruption).
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
          last_watermark STRING,
          last_cursor STRING,
          last_success_ts TIMESTAMP,
          last_run_id STRING
        )
        USING DELTA
        """)
        self._add_layer_column_if_missing()

    def _add_layer_column_if_missing(self):
        """Migration: add layer column to existing state tables (default bronze)."""
        try:
            existing = {f.name.lower() for f in self.spark.table(self.table).schema.fields}
            if "layer" not in existing:
                self.spark.sql(f"ALTER TABLE {self.table} ADD COLUMN (layer STRING)")
        except Exception:
            pass  # table may not exist yet or not readable

    def get_state(self, object_id: str, layer: str = "bronze") -> Dict[str, Any]:
        df = (
            self.spark.table(self.table)
            .where((F.col("object_id") == object_id) & (F.col("layer") == layer))
            .limit(1)
        )
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
        layer: str = "bronze",
        last_watermark: Optional[str] = None,
        last_cursor: Optional[str] = None,
        expected_last_run_id: Optional[str] = None,
    ) -> None:
        """
        Update state after a successful run. If expected_last_run_id is provided (from
        get_state at start of processing), the update is applied only when the row's
        last_run_id still matches (optimistic concurrency); otherwise a concurrent
        update would overwrite our state and we skip to avoid corrupting watermarks.
        """
        # Escape for SQL; NULLs as SQL NULL
        def esc(s: Optional[str]) -> str:
            if s is None:
                return "NULL"
            return "'" + str(s).replace("'", "''") + "'"

        layer_esc = (layer or "bronze").replace("'", "''")
        oid_esc = object_id.replace("'", "''")
        rid_esc = run_id.replace("'", "''")
        wm_sql = esc(last_watermark)
        cur_sql = esc(last_cursor)

        # Optimistic concurrency: only update if row still has the run_id we saw at get_state time
        if expected_last_run_id is None or expected_last_run_id == "":
            match_condition = "1=1"
        else:
            exp_esc = expected_last_run_id.replace("'", "''")
            match_condition = f"t.last_run_id = '{exp_esc}'"

        # Use literal subquery (no temp view) to avoid shared-view race and injection. Key by (object_id, layer).
        self.spark.sql(f"""
        MERGE INTO {self.table} t
        USING (SELECT '{oid_esc}' AS object_id, '{layer_esc}' AS layer, {wm_sql} AS last_watermark, {cur_sql} AS last_cursor, '{rid_esc}' AS last_run_id) s
        ON t.object_id = s.object_id AND t.layer = s.layer
        WHEN MATCHED AND {match_condition} THEN UPDATE SET
          t.last_watermark = COALESCE(s.last_watermark, t.last_watermark),
          t.last_cursor    = COALESCE(s.last_cursor, t.last_cursor),
          t.last_success_ts = current_timestamp(),
          t.last_run_id = s.last_run_id
        WHEN NOT MATCHED THEN INSERT (
          object_id, layer, last_watermark, last_cursor, last_success_ts, last_run_id
        ) VALUES (
          s.object_id, s.layer, s.last_watermark, s.last_cursor, current_timestamp(), s.last_run_id
        )
        """)
