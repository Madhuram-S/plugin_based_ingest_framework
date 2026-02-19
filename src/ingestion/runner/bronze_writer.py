# bronze_writer.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame, functions as F

# Default audit columns when registry does not define conventions.audit_columns (same as registry.yml)
DEFAULT_AUDIT_COLUMNS = [
    {"name": "ingest_dt", "type": "date"},
    {"name": "ingest_ts", "type": "timestamp"},
    {"name": "run_id", "type": "string"},
    {"name": "source_system", "type": "string"},
    {"name": "source_object", "type": "string"},
    {"name": "ingest_mode", "type": "string"},
]


class BronzeWriter:
    def __init__(self, spark, audit_columns: Optional[List[Dict[str, Any]]] = None):
        self.spark = spark
        self.audit_columns = audit_columns if audit_columns else DEFAULT_AUDIT_COLUMNS

    def with_audit_cols(self, df: DataFrame, run_id: str, source_system: str, source_object: str, ingest_mode: str) -> DataFrame:
        """Add audit columns from registry conventions.audit_columns (name + type)."""
        # Well-known names -> expression; registry controls which columns and order
        value_by_name = {
            "ingest_dt": F.current_date(),
            "ingest_ts": F.current_timestamp(),
            "run_id": F.lit(run_id),
            "source_system": F.lit(source_system),
            "source_object": F.lit(source_object),
            "ingest_mode": F.lit(ingest_mode),
        }
        for col_def in self.audit_columns:
            if not isinstance(col_def, dict):
                continue
            name = (col_def.get("name") or "").strip()
            if not name:
                continue
            if name in value_by_name:
                df = df.withColumn(name, value_by_name[name])
            # else: unknown name from registry is skipped (no hardcoded list)
        return df

    def write(self, df: DataFrame, target_table: str, mode: str, primary_key: List[str]) -> int:
        """
        Write to target_table (bronze, silver, or gold). Returns rows_written if cheap to estimate; otherwise -1.
        For streaming, your plugin should handle writes separately.
        """
        mode = (mode or "append").lower()

        if mode == "overwrite":
            df.write.format("delta").mode("overwrite").saveAsTable(target_table)
            return df.count()

        if mode == "append" or not primary_key:
            df.write.format("delta").mode("append").saveAsTable(target_table)
            return df.count()

        # Merge (upsert)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA AS SELECT * FROM (SELECT 1 as __dummy) WHERE 1=0")

        staging_view = "__staging_layer_write"
        df.createOrReplaceTempView(staging_view)

        on_clause = " AND ".join([f"t.`{c}` = s.`{c}`" for c in primary_key])
        set_clause = ", ".join([f"t.`{c}` = s.`{c}`" for c in df.columns])
        insert_cols = ", ".join([f"`{c}`" for c in df.columns])
        insert_vals = ", ".join([f"s.`{c}`" for c in df.columns])

        self.spark.sql(f"""
        MERGE INTO {target_table} t
        USING {staging_view} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

        return df.count()
