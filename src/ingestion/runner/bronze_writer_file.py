# src/common/bronze_writer.py (additions)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class BronzeWriter:
    def __init__(self, spark):
        self.spark = spark

    def with_audit_cols(self, df: DataFrame, run_ctx: dict, obj_cfg: dict) -> DataFrame:
        return (df
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("run_id", F.lit(run_ctx["run_id"]))
            .withColumn("object_id", F.lit(obj_cfg["object_id"]))
            .withColumn("source_system", F.lit(obj_cfg.get("source_system")))
            .withColumn("source_object", F.lit(obj_cfg.get("object_name")))
            .withColumn("config_version", F.lit(run_ctx.get("config_version")))
        )

    def delete_by_file_paths(self, table: str, file_path_col: str, file_paths: list[str]) -> None:
        if not file_paths:
            return
        # Safe chunking to avoid huge IN clauses
        chunk = 200
        for i in range(0, len(file_paths), chunk):
            fp = [p.replace("'", "''") for p in file_paths[i:i+chunk]]
            in_list = ",".join([f"'{p}'" for p in fp])
            self.spark.sql(f"DELETE FROM {table} WHERE {file_path_col} IN ({in_list})")

    def append(self, df: DataFrame, table: str) -> None:
        df.write.mode("append").format("delta").saveAsTable(table)
