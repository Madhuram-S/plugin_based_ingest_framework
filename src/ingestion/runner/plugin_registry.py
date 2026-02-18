from typing import Dict, Any
from src.ingestion.plugins.bigquery_plugin import BigQueryPlugin
from src.ingestion.plugins.api_plugin import ApiPlugin
from src.ingestion.plugins.file_plugin import FilePlugin
from src.ingestion.plugins.file_autoloader_plugin import FileAutoloaderPlugin

def build_plugin_registry(spark, dbutils, writer, state_store, layer: str = "bronze") -> Dict[str, Any]:
    """
    Build plugin registry for the given layer.
    Bronze: extract plugins (bigquery, api, file). Silver/gold: same for now;
    add silver_plugin / gold_plugin (transform-from-Delta) when implemented.
    """
    layer = (layer or "bronze").lower()
    if layer == "bronze":
        return {
            "bigquery": BigQueryPlugin(spark, dbutils),
            "api": ApiPlugin(spark, dbutils),
            "file": FilePlugin(spark, dbutils),
            "file_autoloader": FileAutoloaderPlugin(spark, dbutils),
        }
    # Silver/gold: reuse extract plugins for objects that read from upstream Delta; add transform plugins later
    return {
        "bigquery": BigQueryPlugin(spark, dbutils),
        "api": ApiPlugin(spark, dbutils),
        "file": FilePlugin(spark, dbutils),
    }
