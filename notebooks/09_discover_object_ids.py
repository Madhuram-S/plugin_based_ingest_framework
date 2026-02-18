# Databricks notebook source
# Databricks notebook source
# Discover object_ids for a schedule group and set as task value for For-each (one task per object = full parallelism).
# This notebook runs as the first task; the job's For-each task uses {{tasks.discover_objects.values.object_ids}}.

dbutils.widgets.text("TARGET_ENV", "dev")
dbutils.widgets.text("LAYER", "bronze")  # bronze | silver | gold
dbutils.widgets.text("SCHEDULE_GROUP", "P0_bigquery")
dbutils.widgets.text(
    "REGISTRY_PATH",
    "abfss://config@<storage>.dfs.core.windows.net/compiled/dev/users/<alias>/latest/compiled_registry.json"
)

TARGET_ENV = dbutils.widgets.get("TARGET_ENV")
LAYER = dbutils.widgets.get("LAYER").strip() or "bronze"
SCHEDULE_GROUP = dbutils.widgets.get("SCHEDULE_GROUP")
REGISTRY_PATH = dbutils.widgets.get("REGISTRY_PATH")

# COMMAND ----------

from src.ingestion.runner.registry_reader import get_object_ids_for_schedule_group

object_ids = get_object_ids_for_schedule_group(
    dbutils=dbutils,
    registry_path=REGISTRY_PATH,
    schedule_group=SCHEDULE_GROUP,
    target_env=TARGET_ENV,
    layer=LAYER,
)
dbutils.jobs.taskValues.set("object_ids", object_ids)
print(f"Set object_ids: {len(object_ids)} objects for schedule_group={SCHEDULE_GROUP} layer={LAYER}")

