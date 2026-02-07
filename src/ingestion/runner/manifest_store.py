# src/manifest/manifest_store.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dataclass(frozen=True)
class ManifestStoreConfig:
    manifest_table: str  # e.g. "main.ops.ctl_file_manifest"
    run_items_table: str # e.g. "main.ops.ctl_file_run_items"

class ManifestStore:
    def __init__(self, spark, cfg: ManifestStoreConfig):
        self.spark = spark
        self.cfg = cfg

    def upsert_discovered(self, object_id: str, inv_df: DataFrame, batch_id: str) -> None:
        """
        Upsert discovery records. If file_path exists with a different hash, mark as DISCOVERED again.
        """
        src = (inv_df
            .withColumn("object_id", F.lit(object_id))
            .withColumn("discovered_ts", F.current_timestamp())
            .withColumn("last_seen_ts", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id))
        )

        src.createOrReplaceTempView("src_discovery")

        self.spark.sql(f"""
        MERGE INTO {self.cfg.manifest_table} t
        USING src_discovery s
          ON t.object_id = s.object_id AND t.file_path = s.file_path
        WHEN MATCHED AND (t.content_hash IS NULL OR t.content_hash <> s.content_hash) THEN
          UPDATE SET
            t.file_name = s.file_name,
            t.file_ext = s.file_ext,
            t.file_size = s.file_size,
            t.file_mod_time = s.file_mod_time,
            t.content_hash = s.content_hash,
            t.last_seen_ts = s.last_seen_ts,
            t.status = 'DISCOVERED',
            t.batch_id = s.batch_id,
            t.error_class = NULL,
            t.error_message = NULL
        WHEN MATCHED AND t.content_hash = s.content_hash THEN
          UPDATE SET
            t.last_seen_ts = s.last_seen_ts
        WHEN NOT MATCHED THEN
          INSERT (
            object_id, file_path, file_name, file_ext, file_size, file_mod_time, content_hash,
            discovered_ts, last_seen_ts, status, run_id, batch_id, error_class, error_message
          )
          VALUES (
            s.object_id, s.file_path, s.file_name, s.file_ext, s.file_size, s.file_mod_time, s.content_hash,
            s.discovered_ts, s.last_seen_ts, 'DISCOVERED', NULL, s.batch_id, NULL, NULL
          )
        """)

    def claim_files(self, object_id: str, run_id: str, batch_id: str, max_files: int) -> DataFrame:
        """
        Atomically claim up to max_files by moving DISCOVERED/FAILED/REPLAY_REQUESTED → IN_PROGRESS.
        Returns claimed rows as a DataFrame.
        """
        # Claim candidate set
        candidates = (self.spark.table(self.cfg.manifest_table)
                      .where(F.col("object_id") == object_id)
                      .where(F.col("status").isin(["DISCOVERED", "FAILED", "REPLAY_REQUESTED"]))
                      .orderBy(F.col("file_mod_time").asc_nulls_last(), F.col("file_path").asc())
                      .limit(max_files))

        candidates.createOrReplaceTempView("src_candidates")

        self.spark.sql(f"""
        MERGE INTO {self.cfg.manifest_table} t
        USING src_candidates s
          ON t.object_id = s.object_id AND t.file_path = s.file_path AND t.content_hash = s.content_hash
        WHEN MATCHED AND t.status IN ('DISCOVERED','FAILED','REPLAY_REQUESTED') THEN
          UPDATE SET
            t.status = 'IN_PROGRESS',
            t.run_id = '{run_id}',
            t.batch_id = '{batch_id}',
            t.error_class = NULL,
            t.error_message = NULL
        """)

        # Return claimed set for this run
        claimed = (self.spark.table(self.cfg.manifest_table)
                   .where(F.col("object_id") == object_id)
                   .where(F.col("run_id") == run_id)
                   .where(F.col("status") == "IN_PROGRESS"))

        return claimed

    def mark_success(self, object_id: str, run_id: str, file_paths: list[str]) -> None:
        df = self.spark.createDataFrame([(object_id, p, run_id) for p in file_paths], "object_id STRING, file_path STRING, run_id STRING")
        df.createOrReplaceTempView("src_done")

        self.spark.sql(f"""
        MERGE INTO {self.cfg.manifest_table} t
        USING src_done s
          ON t.object_id = s.object_id AND t.file_path = s.file_path AND t.run_id = s.run_id
        WHEN MATCHED AND t.status='IN_PROGRESS' THEN
          UPDATE SET
            t.status='SUCCESS',
            t.last_seen_ts=current_timestamp()
        """)

    def mark_failed(self, object_id: str, run_id: str, file_path: str, error_class: str, error_message: str) -> None:
        # Avoid huge error payloads
        msg = (error_message or "")[:1000].replace("'", "''")
        cls = (error_class or "UNKNOWN")[:200].replace("'", "''")

        self.spark.sql(f"""
        UPDATE {self.cfg.manifest_table}
           SET status='FAILED',
               error_class='{cls}',
               error_message='{msg}',
               last_seen_ts=current_timestamp()
         WHERE object_id='{object_id}' AND run_id='{run_id}' AND file_path='{file_path}' AND status='IN_PROGRESS'
        """)

    def write_run_items(self, run_id: str, object_id: str, file_paths: list[str], status: str, error_message: str | None = None) -> None:
        rows = [(run_id, object_id, p, None, status, (error_message or "")[:1000]) for p in file_paths]
        df = self.spark.createDataFrame(rows, "run_id STRING, object_id STRING, file_path STRING, attempt_ts TIMESTAMP, status STRING, error_message STRING") \
                       .withColumn("attempt_ts", F.current_timestamp())
        df.write.mode("append").format("delta").saveAsTable(self.cfg.run_items_table)
