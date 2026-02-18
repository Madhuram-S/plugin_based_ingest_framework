# bigquery_plugin.py
from __future__ import annotations

import time
from py4j.protocol import Py4JJavaError
from typing import Any, Dict, Tuple
from pyspark.sql import functions as F

from src.ingestion.runner.plugin_contract import IngestResult

NON_RETRYABLE_EXCEPTIONS = (
    ModuleNotFoundError, 
    ValueError,
    KeyError,
    Py4JJavaError
)

class BigQueryPlugin:
    """
    Extract-only plugin:
    - Reads from BigQuery via Spark BigQuery connector.
    - Applies incremental watermark filtering if configured.
    - Returns df + next_state.
    """
    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def check_bigquery_connector(self) -> bool:
        """Checks if the BigQuery connector is available at runtime."""
        try:
            self.spark.read.format("bigquery")
            return True
        except Exception as e:
            raise ModuleNotFoundError("BigQuery connector is not available on the compute.")
        
    def parse_config(self, obj_cfg: Dict[str, Any]) -> Any:
        """
        Validates the object configuration in the ff order:
        1. Connection
        2. Source
        3. Load
        """
        conn = obj_cfg.get("connection_options", {}) or {}
        auth = conn.get("auth", {}) or {}
        sec_ref = auth.get("secret_ref", {}) or {}
        conn_mode = auth.get("mode", {}) or {}

        path = sec_ref.get("path", {}) or {}
        sec_scope = sec_ref.get("scope", {}) or {}
        sec_key = sec_ref.get("key", {}) or {}

        if (conn_mode == "service_account_json") and (not path):
            raise ValueError("BigQuery service account json file path is required for service_account_json auth mode.")
        elif (conn_mode == "service_account_base64") and (not sec_scope or not sec_key):
            raise ValueError("Secret scope and key are required for service_account_base64 auth mode for BigQuery.")
        elif conn_mode not in ["service_account_json", "service_account_base64"]:
            raise ValueError("Supported BigQuery authentication is either service_account_json or service_account_base64.")

        src = obj_cfg.get("source", {}) or {}
        project_id = src.get("project_id", {}) or {}
        dataset = src.get("dataset", {}) or {}
        schema = src.get("schema", {}) or {}
        table = src.get("table", {}) or obj_cfg.get("name", {})
        query = src.get("query", {}) or {}
        tmp_vw = src.get("tmp_vw", {}) or "temp_" + obj_cfg.get("generated_at", "")

        if (not project_id) and (not dataset):
            raise ValueError("Project ID and dataset are required for BigQuery source.")
        

        load = obj_cfg.get("load", {}) or {}
        ingest_mode = load.get("ingest_mode", {}) or {}
        watermark_col = load.get("watermark_column", {}) or {}
        initial_value = load.get("initial_value", {}) or {}

        if ingest_mode not in ["full", "incremental"]:
            raise ValueError("Load ingest_mode must be either full or incremental.")
        
        if (ingest_mode == "incremental") and (not watermark_col):
            raise ValueError("Watermark column is required for incremental load.")
        
        print("BigQuery source configuration is valid.")
        return (
            conn, auth, 
            sec_ref, conn_mode,
            path, sec_scope, 
            sec_key, src, 
            project_id, dataset, 
            schema, table, 
            query, tmp_vw, 
            load, ingest_mode, 
            watermark_col, initial_value
        )

    def read_bigquery_table(self, obj_cfg: Dict[str, Any], prior_state: Dict[str, Any]) -> IngestResult:
        """Reads a BigQuery table or view and returns it as a DataFrame."""
        object_name = obj_cfg.get("name")
        (
            conn, auth, 
            sec_ref, conn_mode,
            path, sec_scope, 
            sec_key, src, 
            project_id, dataset, 
            schema, table, 
            query, tmp_vw, 
            load, ingest_mode, 
            watermark_col, initial_value
        ) = self.parse_config(obj_cfg)

        local_path = None
        if conn_mode == "service_account_json":
            if path.startswith("abfss://"):
                # Temporarily store the JSON file to local
                local_path =f'tmp/bigquery/{path.split("/")[-1]}'
                self.dbutils.fs.cp(path, f'dbfs:/{local_path}')
                path = f'/dbfs/{local_path}'
            self.spark.conf.set("credentialsFile", path)
        elif conn_mode == "service_account_base64":
            service_account_key = dbutils.secrets.get(scope=sec_scope, key=sec_key)
            self.spark.conf.set("credentials", service_account_key)
        
        table_ref = None if query else f"{dataset}.{schema}.{table}"
        options = {"parentProject": project_id}
        if query:
            options.update({
                "viewsEnabled": "true",
                "materializationProject": project_id,
                "materializationDataset": tmp_vw,
                "query": query,
            })
        elif ingest_mode == "incremental":
            filter_val = initial_value or prior_state.get("last_watermark")
            if filter_val is None:
                raise ValueError("Incremental mode requires initial_value or prior_state.last_watermark")
            options["filter"] = f'{watermark_col} > {filter_val}'

        reader = self.spark.read.format("bigquery")
        for k, v in options.items():
            reader = reader.option(k, v)
        df = reader.load() if query else reader.load(table_ref)

        next_state = {}
        if ingest_mode == "incremental":
            # Compute new watermark from extracted batch (safe: only advance if data present)
            max_row = df.select(F.max(F.col(watermark_col)).alias("m")).collect()
            max_wm = max_row[0]["m"]
            if max_wm is not None:
                next_state["last_watermark"] = str(max_wm)
        
        print("Ingestion is successfull")
        # Delete the credentials file on local drive.
        if local_path:
            self.dbutils.fs.rm(local_path, recurse=True)

        return IngestResult(
            status="SUCCESS",
            ingest_mode=ingest_mode,
            df=df,
            row_count_source=None,
            next_state=next_state,
            artifacts={"source": "bigquery", "table": object_name},
            warnings=[],
        )
    
    def extract(self, run_ctx, obj_cfg: Dict[str, Any], prior_state: Dict[str, Any]) -> IngestResult:
        """
        Verifies if the object configuration is valid 
        and reads a table or view from BigQuery.
        Returns a IngestResult instance.
        """
        prior_state = {}
        ingestion = obj_cfg.get("ingestion", {}) or {}
        max_retries = ingestion.get("max_retries", 3)
        retry_backoff_seconds = ingestion.get("retry_backoff_seconds", 5)
        MAX_SLEEP_SECONDS = 60
        retry = 0

        while True:
            try:
                if self.check_bigquery_connector():
                    result = self.read_bigquery_table(obj_cfg, prior_state)
                    return result
                    break
            except NON_RETRYABLE_EXCEPTIONS as e:
                raise e
            except Exception as e:
                print(f'{type(e).__name__} :{e}')
                if retry >= max_retries:
                    raise RuntimeError(f"Max retries exceeded for object {obj_cfg.get('name')}") from e
                else:
                    backoff_seconds = retry_backoff_seconds * (2 ** retry)
                    sleep_seconds = min(backoff_seconds, MAX_SLEEP_SECONDS)

                    retry += 1
                    time.sleep(sleep_seconds)