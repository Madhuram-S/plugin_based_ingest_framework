from typing import Dict, Any
from src.ingestion.plugins.bigquery_plugin import BigQueryPlugin
from src.ingestion.plugins.api_plugin import ApiPlugin
from src.ingestion.plugins.file_plugin import FilePlugin

def build_plugin_registry(spark, dbutils, writer, state_store) -> Dict[str, Any]:
    return {
        "bigquery": BigQueryPlugin(spark, dbutils),
        "api": ApiPlugin(spark, dbutils),
        "file": FilePlugin(spark, dbutils),
        # "sql": SqlPlugin(spark, dbutils),  # add later
        # "excel": ExcelPlugin()
    }
