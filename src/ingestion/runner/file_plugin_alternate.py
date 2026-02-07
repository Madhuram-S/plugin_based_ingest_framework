# src/plugins/file_plugin.py
from __future__ import annotations
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class FilePlugin:
    def __init__(self, spark):
        self.spark = spark

    def read_files(self, file_paths: List[str], obj_cfg: Dict[str, Any]) -> DataFrame:
        fmt = obj_cfg["landing"]["format"].lower()

        if fmt in ("csv", "json", "parquet"):
            reader = self.spark.read.format(fmt)
            for k, v in (obj_cfg["landing"].get("options") or {}).items():
                reader = reader.option(k, v)
            df = reader.load(file_paths)

        elif fmt in ("excel", "xlsx", "xls"):
            # Requires spark-excel library (com.crealytics.spark.excel).
            # If you cannot add libs, enforce upstream conversion to CSV.
            excel_cfg = obj_cfg["landing"].get("excel") or {}
            sheet = excel_cfg.get("sheet_name", "Sheet1")
            header = bool(excel_cfg.get("header", True))
            df = (self.spark.read.format("com.crealytics.spark.excel")
                  .option("sheetName", sheet)
                  .option("useHeader", str(header).lower())
                  .option("inferSchema", "true")
                  .load(file_paths))
        else:
            raise ValueError(f"Unsupported landing.format: {fmt}")

        # Add file_path lineage column (crucial for replace-by-file and audit)
        df = df.withColumn("_file_path", F.input_file_name())
        return df
