# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %reload_ext autoreload

# COMMAND ----------

dbutils.widgets.text("TARGET_ENV", "dev")  # Target environment (e.g., dev, prod)
dbutils.widgets.text("LAYER", "bronze")    # Layer: bronze | silver | gold
dbutils.widgets.text("SCHEDULE_GROUP", "P0_file_autoloader")  # Ingestion schedule group
dbutils.widgets.text(
    "REGISTRY_PATH",
    "abfss://config@<storage>.dfs.core.windows.net/compiled/dev/users/<alias>/latest/compiled_registry.json"  # Path to registry config
)
dbutils.widgets.text("CONFIG_VERSION", "unknown")     # Config version (e.g., git SHA or bundle version)
dbutils.widgets.text("STRICT_MODE", "false")          # Strict mode flag (recommended true for prod P0)
dbutils.widgets.text("SHARD_COUNT", "")               # Optional: total shards (e.g., 10)
dbutils.widgets.text("SHARD_ID", "")                  # Optional: this shard id (0..SHARD_COUNT-1)
dbutils.widgets.text("OBJECT_ID", "")                 # Optional: run single object (for For-each over object_ids = full parallelism) <env>|<schema>|<table>

# Retrieve widget values for use in the notebook
TARGET_ENV = dbutils.widgets.get("TARGET_ENV")
LAYER = dbutils.widgets.get("LAYER").strip() or "bronze"
SCHEDULE_GROUP = dbutils.widgets.get("SCHEDULE_GROUP")
REGISTRY_PATH = dbutils.widgets.get("REGISTRY_PATH")
CONFIG_VERSION = dbutils.widgets.get("CONFIG_VERSION")
STRICT_MODE = dbutils.widgets.get("STRICT_MODE").lower() == "true"  # Convert to boolean

SHARD_COUNT_RAW = dbutils.widgets.get("SHARD_COUNT").strip()
SHARD_ID_RAW = dbutils.widgets.get("SHARD_ID").strip()
SHARD_COUNT = int(SHARD_COUNT_RAW) if SHARD_COUNT_RAW else None
SHARD_ID = int(SHARD_ID_RAW) if SHARD_ID_RAW else None

OBJECT_ID_RAW = dbutils.widgets.get("OBJECT_ID").strip()
OBJECT_IDS = [OBJECT_ID_RAW] if OBJECT_ID_RAW else None  # One task per object when set

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
    strict_mode=STRICT_MODE,
    layer=LAYER,
    shard_id=SHARD_ID,
    shard_count=SHARD_COUNT,
    object_ids=OBJECT_IDS,
)


# COMMAND ----------


