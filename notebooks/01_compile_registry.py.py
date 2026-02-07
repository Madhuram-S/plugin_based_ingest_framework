# Databricks notebook source
import json
import hashlib
from datetime import datetime, timezone
import pytz

# COMMAND ----------

# Import the config_loader class from the ingestion runner module.
from src.ingestion.runner.config_loader import ConfigLoader

# COMMAND ----------

dbutils.widgets.text("TARGET_ENV", "dev")
dbutils.widgets.text("OUT_BASE_PATH", "abfss://landing_madhu@dlsdbxtraining002.dfs.core.windows.net/config/compiled")
dbutils.widgets.text("VERSION_STAMP", "git:unknown|bundle:unknown")
dbutils.widgets.text("DEVELOPER_ALIAS", "users/madhu")  # use "users/<alias>" in dev if desired

TARGET_ENV = dbutils.widgets.get("TARGET_ENV")
OUT_BASE_PATH = dbutils.widgets.get("OUT_BASE_PATH").rstrip("/")
VERSION_STAMP = dbutils.widgets.get("VERSION_STAMP")
DEVELOPER_ALIAS = dbutils.widgets.get("DEVELOPER_ALIAS")

# COMMAND ----------

generated_at = datetime.now(pytz.timezone("America/New_York")).isoformat()

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root_path = "/Workspace" + "/".join(notebook_path.split("/")[:-2])
repo_root_path

# COMMAND ----------

# TODO: Replace this with your real loader:
registry = ConfigLoader(f"{repo_root_path}/configs").compile_registry(target_env=TARGET_ENV, overlay_yml_path=f"env/{TARGET_ENV}.yml")
registry['config_version'] = VERSION_STAMP
registry['generated_at'] = generated_at


# COMMAND ----------

content = json.dumps(registry, indent=2, sort_keys=True)
sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
registry["content_sha256"] = sha
content = json.dumps(registry, indent=2, sort_keys=True)


# COMMAND ----------

# Choose output paths
# versioned path (immutable)
version_folder = f"{OUT_BASE_PATH}/{TARGET_ENV}/{DEVELOPER_ALIAS}/git-{sha[:12]}"
version_path = f"{version_folder}/compiled_registry.json"

# latest path (mutable pointer)
latest_path = f"{OUT_BASE_PATH}/{TARGET_ENV}/{DEVELOPER_ALIAS}/latest/compiled_registry.json"



# COMMAND ----------

latest_path

# COMMAND ----------

# Write versioned (fail-if-exists)
try:
    dbutils.fs.ls(version_folder)
    raise RuntimeError(f"Refusing to overwrite existing versioned folder: {version_folder}")
except Exception:
    pass

dbutils.fs.mkdirs(version_folder)
dbutils.fs.put(version_path, content, overwrite=False)

# COMMAND ----------

# Update latest (overwrite OK)
latest_folder = f"{OUT_BASE_PATH}/{TARGET_ENV}/{DEVELOPER_ALIAS}/latest"
dbutils.fs.mkdirs(latest_folder)
dbutils.fs.put(latest_path, content, overwrite=True)

print(f"Wrote versioned: {version_path}")
print(f"Updated latest:  {latest_path}")
print(f"content_sha256:  {sha}")
