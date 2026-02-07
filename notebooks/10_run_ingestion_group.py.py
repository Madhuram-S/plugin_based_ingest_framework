# Databricks notebook source
# Create Databricks notebook widgets for parameterization
dbutils.widgets.text("TARGET_ENV", "dev")  # Target environment (e.g., dev, prod)
dbutils.widgets.text("SCHEDULE_GROUP", "P0_bigquery")  # Ingestion schedule group
dbutils.widgets.text(
    "REGISTRY_PATH",
    "abfss://config@<storage>.dfs.core.windows.net/compiled/dev/users/<alias>/latest/compiled_registry.json"  # Path to registry config
)
dbutils.widgets.text("CONFIG_VERSION", "unknown")     # Config version (e.g., git SHA or bundle version)
dbutils.widgets.text("STRICT_MODE", "false")          # Strict mode flag (recommended true for prod P0)

# Retrieve widget values for use in the notebook
TARGET_ENV = dbutils.widgets.get("TARGET_ENV")
SCHEDULE_GROUP = dbutils.widgets.get("SCHEDULE_GROUP")
REGISTRY_PATH = dbutils.widgets.get("REGISTRY_PATH")
CONFIG_VERSION = dbutils.widgets.get("CONFIG_VERSION")
STRICT_MODE = dbutils.widgets.get("STRICT_MODE").lower() == "true"  # Convert to boolean

# COMMAND ----------

# Import the run_ingestion_group function from the ingestion runner module.
from src.ingestion.runner.run_group import run_ingestion_group

# Execute the ingestion group runner with the specified parameters.
# This function is responsible for orchestrating the ingestion process for a group of data sources,
# using the provided Spark session and Databricks utilities.
# Parameters:
#   spark:        The active SparkSession object for distributed data processing.
#   dbutils:      Databricks utility object for interacting with the workspace and data.
#   target_env:   The target environment (e.g., 'dev', 'prod') to determine config and data paths.
#   schedule_group: The group of ingestion jobs to run, typically mapped to a schedule or priority.
#   registry_path: Path to the compiled registry configuration file (JSON) containing source definitions.
#   config_version: Version identifier for the configuration (e.g., git SHA or bundle version).
#   strict_mode:  Boolean flag to enable strict validation and error handling (recommended for production).

run_ingestion_group(
    spark=spark,
    dbutils=dbutils,
    target_env=TARGET_ENV,
    schedule_group=SCHEDULE_GROUP,
    registry_path=REGISTRY_PATH,
    config_version=CONFIG_VERSION,
    strict_mode=STRICT_MODE
)

# COMMAND ----------


