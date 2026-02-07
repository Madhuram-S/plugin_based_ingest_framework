# bronze_writer.py
from __future__ import annotations

from typing import List, Optional
from pyspark.sql import DataFrame, functions as F

class BronzeWriter:
    def __init__(self, spark):
        self.spark = spark

    def with_audit_cols(self, df: DataFrame, run_id: str, source_system: str, source_object: str, ingest_mode: str) -> DataFrame:
        return (
            df
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
            .withColumn("source_system", F.lit(source_system))
            .withColumn("source_object", F.lit(source_object))
            .withColumn("ingest_mode", F.lit(ingest_mode))
        )

    def write(self, df: DataFrame, bronze_table: str, mode: str, primary_key: List[str]) -> int:
        """
        Returns rows_written if cheap to estimate; otherwise returns -1.
        For streaming, your plugin should handle writes separately.
        """
        mode = (mode or "append").lower()

        if mode == "overwrite":
            df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
            return df.count()

        if mode == "append" or not primary_key:
            df.write.format("delta").mode("append").saveAsTable(bronze_table)
            return df.count()

        # Merge (upsert)
        # Ensure target exists with schema. Using a safe create-if-not-exists pattern:
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table} USING DELTA AS SELECT * FROM (SELECT 1 as __dummy) WHERE 1=0")

        staging_view = "__staging_bronze_write"
        df.createOrReplaceTempView(staging_view)

        on_clause = " AND ".join([f"t.`{c}` = s.`{c}`" for c in primary_key])
        set_clause = ", ".join([f"t.`{c}` = s.`{c}`" for c in df.columns])
        insert_cols = ", ".join([f"`{c}`" for c in df.columns])
        insert_vals = ", ".join([f"s.`{c}`" for c in df.columns])

        self.spark.sql(f"""
        MERGE INTO {bronze_table} t
        USING {staging_view} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

        return df.count()
