# src/ingestion/config_loader.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")  # compile-time: ${env.catalog}


class ConfigError(Exception):
    pass


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"YAML file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"Top-level YAML must be a mapping: {path}")
    return data


def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-merge b into a (dicts merge, lists overwrite)."""
    out = dict(a)
    for k, v in (b or {}).items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _get_path(ctx: Dict[str, Any], path: str) -> Any:
    cur: Any = ctx
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise ConfigError(f"Missing compile-time variable: {path}")
        cur = cur[part]
    return cur


def expand_vars(obj: Any, ctx: Dict[str, Any]) -> Any:
    """Recursively expand ${...} tokens."""
    if isinstance(obj, str):
        def repl(m):
            key = m.group(1).strip()
            return str(_get_path(ctx, key))
        return _VAR_PATTERN.sub(repl, obj)

    if isinstance(obj, list):
        return [expand_vars(x, ctx) for x in obj]

    if isinstance(obj, dict):
        return {k: expand_vars(v, ctx) for k, v in obj.items()}

    return obj


def _safe_name(name: str) -> str:
    return name.replace(".", "_").replace("/", "_").replace(" ", "_")


@dataclass
class LoadedObject:
    env: str
    schedule_group: str
    enabled: bool
    source_system: str
    source_type: str
    object_name: str
    connection: str
    bronze_table: str
    raw: Dict[str, Any]


class ConfigLoader:
    """
    Clean, importable config loader.

    Expected config files in config_dir:
      - registry.yml
      - connections.yml
      - sources*.yml (any number)
      - optional env overlays (env/dev.yml, env/prod.yml etc) handled by caller if desired
    """

    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)

    def load_base(self) -> Dict[str, Any]:
        registry = _read_yaml(self.config_dir / "registry.yml")
        connections = _read_yaml(self.config_dir / "connections.yml")

        merged = deep_merge(registry, connections)

        # Load all sources*.yml
        for p in sorted(self.config_dir.glob("sources*.yml")):
            src = _read_yaml(p)
            merged = deep_merge(merged, src)

        return merged

    # def load(self, target_env: str, overlay: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    def load(self, target_env: str, overlay_yml_path: str = None) -> Dict[str, Any]:
        cfg = self.load_base()

        # Apply overlay (env-specific), if provided
        if overlay_yml_path:
            # read overlay YAML
            overlay = _read_yaml(self.config_dir / overlay_yml_path)
            cfg = deep_merge(cfg, overlay)

        # Ensure env.name exists
        env_block = cfg.get("env", {}) or {}
        env_block.setdefault("name", target_env)
        cfg["env"] = env_block

        # Expand compile-time ${...}
        cfg = expand_vars(cfg, cfg)

        return cfg

    def expand_objects(self, cfg: Dict[str, Any]) -> List[LoadedObject]:
        env_name = (cfg.get("env", {}) or {}).get("name")
        if not env_name:
            raise ConfigError("env.name missing after load()")

        registry_defaults = (cfg.get("defaults", {}) or {})
        connections = (cfg.get("connections", {}) or {})

        sources = cfg.get("sources", [])
        if not isinstance(sources, list):
            raise ConfigError("sources must be a list (from sources*.yml)")

        loaded: List[LoadedObject] = []

        for s in sources:
            if not isinstance(s, dict):
                raise ConfigError("Each source must be a mapping")

            source_enabled = bool(s.get("enabled", True))
            source_system = s.get("source_system")
            source_type = s.get("source_type")
            connection = s.get("connection")

            if not source_system or not source_type or not connection:
                raise ConfigError(f"source missing required fields: {s}")

            if connection not in connections:
                raise ConfigError(f"Missing connection '{connection}' for source_system={source_system}")

            # Merge defaults: registry.defaults -> source.defaults -> object overrides
            source_defaults = deep_merge(registry_defaults, (s.get("defaults", {}) or {}))

            objects = s.get("objects", [])
            if not isinstance(objects, list):
                raise ConfigError(f"objects must be a list for source_system={source_system}")

            for o in objects:
                if not isinstance(o, dict):
                    raise ConfigError(f"Each object must be a mapping under source_system={source_system}")

                obj_name = o.get("name")
                if not obj_name:
                    raise ConfigError(f"Object missing 'name' under source_system={source_system}")

                obj_enabled = bool(o.get("enabled", True))
                enabled = source_enabled and obj_enabled and bool((source_defaults.get("schedule", {}) or {}).get("enabled", True))

                # Build merged object config
                obj_cfg = deep_merge(source_defaults, o)

                # Normalize fields expected by runner/plugins
                obj_cfg["source_system"] = source_system
                obj_cfg["source_type"] = source_type
                obj_cfg["object_name"] = obj_name
                obj_cfg["connection"] = connection
                obj_cfg["enabled"] = enabled

                # schedule_group
                sched = obj_cfg.get("schedule", {}) or {}
                schedule_group = sched.get("group") or "default"
                obj_cfg["schedule_group"] = schedule_group

                # Target -> bronze_table
                tgt = obj_cfg.get("target", {}) or {}
                catalog = tgt.get("catalog")
                schema = tgt.get("schema")
                prefix = tgt.get("table_prefix", "")

                if not catalog or not schema:
                    raise ConfigError(f"target.catalog/target.schema required for object={obj_name}")

                bronze_table = f"{catalog}.{schema}.{prefix}{_safe_name(obj_name)}"
                obj_cfg["bronze_table"] = bronze_table

                # Derive landing paths for file sources (if not explicitly set)
                if source_type == "file":
                    conn_opts = connections[connection]
                    root_path = conn_opts.get("root_path")
                    landing = obj_cfg.get("landing", {}) or {}
                    if not landing.get("path"):
                        if not root_path:
                            raise ConfigError(f"connections.{connection}.root_path required to derive landing.path")
                        landing["path"] = f"{root_path.rstrip('/')}/{_safe_name(obj_name)}/"
                    # checkpoint/schema_location derived if using autoloader
                    inc = obj_cfg.get("incremental", {}) or {}
                    if (inc.get("mode") or "").lower() == "file_autoloader":
                        landing.setdefault("checkpoint", f"{landing['path'].rstrip('/')}/_checkpoint")
                        landing.setdefault("schema_location", f"{landing['path'].rstrip('/')}/_schema")
                    obj_cfg["landing"] = landing

                # Inject connection_options (fully expanded, compile-time)
                obj_cfg["connection_options"] = connections[connection]

                # object_id
                object_id = f"{env_name}|{source_system}|{obj_name}"
                obj_cfg["object_id"] = object_id

                loaded.append(
                    LoadedObject(
                        env=env_name,
                        schedule_group=schedule_group,
                        enabled=enabled,
                        source_system=source_system,
                        source_type=source_type,
                        object_name=obj_name,
                        connection=connection,
                        bronze_table=bronze_table,
                        raw=obj_cfg,
                    )
                )

        return loaded


    def compile_registry(self, target_env: str, overlay_yml_path: str = None) -> Dict[str, Any]:
        cfg = self.load(target_env=target_env, overlay_yml_path=overlay_yml_path)
        loaded = self.expand_objects(cfg)
        return {
            "env": (cfg.get("env", {}) or {}).get("name"),
            "objects": [lo.raw for lo in loaded],
        }