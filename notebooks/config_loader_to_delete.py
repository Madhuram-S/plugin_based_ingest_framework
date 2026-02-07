# Databricks notebook source
# MAGIC %md
# MAGIC ### Key Notes:
# MAGIC TARGET_ENV to set in init scripts
# MAGIC

# COMMAND ----------

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

# COMMAND ----------

_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")  # ${env.catalog} style

TOKEN_PATTERN = re.compile(r"\{\{([^}]+)\}\}") # {{primary_key}}

# COMMAND ----------

class ConfigError(Exception):
    pass


# COMMAND ----------

def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"YAML file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"YAML root must be a mapping/dict: {path}")
    return data

# COMMAND ----------

def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge dict b into dict a and return new dict.
    Lists are replaced (not concatenated) by default—MVP-safe behavior.
    """
    out = dict(a)
    for k, v in (b or {}).items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


# COMMAND ----------


def get_by_path(obj: Dict[str, Any], path: str) -> Any:
    """
    Resolve dotted path like "env.catalog" from a nested dict.
    """
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise ConfigError(f"Missing variable path: {path}")
        cur = cur[part]
    return cur

# COMMAND ----------

def expand_vars(value: Any, context: Dict[str, Any], max_passes: int = 5) -> Any:
    """
    Expand ${...} placeholders using the provided context dict.
    Works on strings, and recursively on dicts/lists.
    """
    if isinstance(value, str):
        s = value
        for _ in range(max_passes):
            changed = False

            def _repl(m: re.Match) -> str:
                nonlocal changed
                var_path = m.group(1).strip()
                resolved = get_by_path(context, var_path)
                if isinstance(resolved, (dict, list)):
                    raise ConfigError(
                        f"Cannot inject non-scalar into string for {var_path}: {resolved}"
                    )
                changed = True
                return str(resolved)

            new_s = _VAR_PATTERN.sub(_repl, s)
            s = new_s
            if not changed:
                break
        return s

    if isinstance(value, dict):
        return {k: expand_vars(v, context, max_passes=max_passes) for k, v in value.items()}

    if isinstance(value, list):
        return [expand_vars(v, context, max_passes=max_passes) for v in value]

    return value


# COMMAND ----------

def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


# COMMAND ----------

def _normalize_pk(pk: Any) -> List[str]:
    if pk is None:
        return []
    if isinstance(pk, str):
        return [pk]
    if isinstance(pk, list) and all(isinstance(i, str) for i in pk):
        return pk
    raise ConfigError(f"primary_key must be string or list[str], got: {pk}")

# COMMAND ----------


def _safe_name(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", s).strip("_").lower()

# COMMAND ----------

def derive_bronze_table(obj_cfg: Dict[str, Any]) -> str:
    """
    Derive bronze table name from target defaults.
    Supports:
      - explicit: obj_cfg['bronze_table']
      - derived: target.catalog + target.schema + prefix + object short name
    """
    if obj_cfg.get("bronze_table"):
        return obj_cfg["bronze_table"]

    tgt = obj_cfg.get("target", {}) or {}
    catalog = tgt.get("catalog")
    schema = tgt.get("schema")
    prefix = tgt.get("table_prefix", "")

    if not (catalog and schema):
        raise ConfigError(f"Missing target.catalog or target.schema for object: {obj_cfg.get('name')}")

    # object_name can be like "dataset.table" or "dbo.Table"
    object_name = obj_cfg.get("name") or obj_cfg.get("object_name")
    if not object_name:
        raise ConfigError("Object missing 'name' (or 'object_name').")

    short = object_name.split(".")[-1]
    tbl = f"{prefix}{_safe_name(short)}"
    return f"{catalog}.{schema}.{tbl}"

# COMMAND ----------

def derive_landing_paths(env_cfg: Dict[str, Any], source_system: str, object_name: str) -> Tuple[str, str]:
    """
    Standard landing + checkpoint layout:
      landing:    {landing_base}/{source_system}/{object_name}/
      checkpoint: {checkpoint_base}/{source_system}/{object_name}/
    """
    landing_base = env_cfg.get("landing_base_path")
    checkpoint_base = env_cfg.get("checkpoint_base_path")
    if not landing_base or not checkpoint_base:
        raise ConfigError("env.landing_base_path and env.checkpoint_base_path are required in overlay.")

    ss = _safe_name(source_system)
    on = _safe_name(object_name.replace(".", "_"))
    landing = f"{landing_base}/{ss}/{on}"
    checkpoint = f"{checkpoint_base}/{ss}/{on}"
    return landing, checkpoint


# COMMAND ----------

@dataclass
class LoadedObject:
    """
    Final expanded object config ready for UC registry table / runners.
    """
    env: str
    source_system: str
    source_type: str
    object_name: str
    object_type: str
    connection: str
    bronze_table: str
    schedule_group: str
    enabled: bool
    raw: Dict[str, Any]

    def to_row(self) -> Dict[str, Any]:
        return {
            "env": self.env,
            "source_system": self.source_system,
            "source_type": self.source_type,
            "object_name": self.object_name,
            "object_type": self.object_type,
            "connection": self.connection,
            "bronze_table": self.bronze_table,
            "schedule_group": self.schedule_group,
            "enabled": self.enabled,
            "raw_config_json": json.dumps(self.raw, sort_keys=True),
        }


# COMMAND ----------

def resolve_runtime_tokens(value, context):
    if isinstance(value, str):
        def repl(match):
            path = match.group(1).strip()
            cur = context
            for part in path.split("."):
                cur = cur.get(part)
                if cur is None:
                    raise ValueError(f"Missing runtime token: {path}")
            return str(cur)
        return TOKEN_PATTERN.sub(repl, value)

    if isinstance(value, list):
        return [resolve_runtime_tokens(v, context) for v in value]

    if isinstance(value, dict):
        return {k: resolve_runtime_tokens(v, context) for k, v in value.items()}

    return value

# COMMAND ----------

class ConfigLoader:
    """
    Loads and expands YAML config for ingestion framework.
    """

    def __init__(self, repo_root: str | Path):
        self.repo_root = Path(repo_root)

    def load(
        self,
        target_env: str,
        base_dir: str = "configs/base",
        env_dir: str = "configs/env",
        sources_glob: str = "sources_*.yml",
    ) -> Dict[str, Any]:
        """
        Returns expanded full config dict (registry+connections+rulesets+sources merged with env overlay).
        """
        base_path = self.repo_root / base_dir
        env_path = self.repo_root / env_dir / f"{target_env}.yml"

        registry = _read_yaml(base_path / "registry.yml")
        connections = _read_yaml(base_path / "connections.yml")
        dq_rulesets = _read_yaml(base_path / "dq_rulesets.yml")

        # Merge core docs
        merged = deep_merge(registry, connections)
        merged = deep_merge(merged, dq_rulesets)

        # Merge env overlay
        env_overlay = _read_yaml(env_path)
        merged = deep_merge(merged, env_overlay)

        # Load and merge all sources_*.yml
        sources_files = sorted(base_path.glob(sources_glob))
        if not sources_files:
            raise ConfigError(f"No sources files found in {base_path} matching {sources_glob}")

        all_sources: List[Dict[str, Any]] = []
        for f in sources_files:
            doc = _read_yaml(f)
            if "sources" not in doc:
                raise ConfigError(f"Sources file missing 'sources' root key: {f}")
            all_sources.extend(_ensure_list(doc.get("sources")))

        merged["sources"] = all_sources

        # Expand ${...} variables using merged context
        merged = expand_vars(merged, merged)

        return merged

    def expand_objects(self, cfg: Dict[str, Any]) -> List[LoadedObject]:
        """
        Expand dataset defaults to object-level configs and compute derived fields.
        """
        env_cfg = cfg.get("env", {}) or {}
        env_name = env_cfg.get("name")
        if not env_name:
            raise ConfigError("env.name must be set in env overlay.")

        sources = _ensure_list(cfg.get("sources"))
        if not sources:
            raise ConfigError("No sources found after merge.")

        loaded: List[LoadedObject] = []

        for s in sources:
            source_system = s.get("source_system")
            source_type = s.get("source_type")
            connection = s.get("connection")
            enabled_source = bool(s.get("enabled", True))

            if not (source_system and source_type and connection):
                raise ConfigError(f"Each source must have source_system, source_type, connection. Got: {s}")

            defaults = s.get("defaults", {}) or {}
            schedule_default = defaults.get("schedule", {}) or s.get("schedule", {}) or {}
            default_group = schedule_default.get("group", cfg.get("defaults", {}).get("scheduling", {}).get("group", "P1"))

            objects = _ensure_list(s.get("objects"))
            if not objects:
                raise ConfigError(f"Source has no objects: {source_system}")

            for o in objects:
                obj_name = o.get("name") or o.get("object_name")
                obj_type = o.get("object_type", "table")
                enabled_obj = bool(o.get("enabled", True))

                if not obj_name:
                    raise ConfigError(f"Object missing name under source {source_system}: {o}")

                # Apply defaults to object (deep merge defaults -> object)
                obj_cfg = deep_merge(defaults, o)

                # Add base fields to object config
                obj_cfg["source_system"] = source_system
                obj_cfg["source_type"] = source_type
                obj_cfg["connection"] = connection
                obj_cfg["object_name"] = obj_name
                obj_cfg["object_type"] = obj_type

                # Normalize primary key
                write_cfg = obj_cfg.get("write", {}) or {}
                write_cfg["primary_key"] = _normalize_pk(write_cfg.get("primary_key"))
                obj_cfg["write"] = write_cfg

                # Derive bronze_table
                bronze_table = derive_bronze_table(obj_cfg)
                obj_cfg["bronze_table"] = bronze_table

                # Derive landing/checkpoint if landing enabled or file ingestion
                landing_cfg = obj_cfg.get("landing", {}) or {}
                if landing_cfg.get("enabled") or source_type in ("api", "file") or obj_cfg.get("load", {}).get("from_landing"):
                    landing_path, checkpoint_path = derive_landing_paths(env_cfg, source_system, obj_name)
                    landing_cfg.setdefault("path", landing_path)
                    landing_cfg.setdefault("checkpoint", checkpoint_path)
                    obj_cfg["landing"] = landing_cfg

                # Schedule group resolution
                sched_cfg = obj_cfg.get("schedule", {}) or schedule_default
                schedule_group = sched_cfg.get("group", default_group)
                obj_cfg["schedule"] = deep_merge(schedule_default, sched_cfg)

                loaded.append(
                    LoadedObject(
                        env=env_name,
                        source_system=source_system,
                        source_type=source_type,
                        object_name=obj_name,
                        object_type=obj_type,
                        connection=connection,
                        bronze_table=bronze_table,
                        schedule_group=schedule_group,
                        enabled=enabled_source and enabled_obj and bool(obj_cfg.get("schedule", {}).get("enabled", True)),
                        raw=obj_cfg,
                    )
                )

        return loaded

# COMMAND ----------

def _get_notebook_path():
    """
    Returns the root path of the repository containing the current notebook in Databricks.
    """
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root_path = "/Workspace" + "/".join(notebook_path.split("/")[:-3])
    return repo_root_path

# COMMAND ----------

loader = ConfigLoader(repo_root= _get_notebook_path())  # assumes src/common/config_loader.py

cfg = loader.load(target_env=os.environ.get("TARGET_ENV", "dev"))
objs = loader.expand_objects(cfg)
print(f"Loaded objects: {len(objs)}")
print(objs[0].to_row())


# COMMAND ----------


