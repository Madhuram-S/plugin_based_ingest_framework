# registry_reader.py
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple


def load_compiled_registry(dbutils, registry_path: str, max_bytes: int = 50_000_000) -> Tuple[Dict[str, Any], str]:
    """
    Loads compiled_registry.json as text and returns (registry_dict, sha256_of_text).
    Uses dbutils.fs.head for simplicity. If your registry can exceed max_bytes, store it chunked or as a Delta table.
    """
    text = dbutils.fs.head(registry_path, max_bytes)
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    registry = json.loads(text)
    return registry, sha


def get_object_ids_for_schedule_group(
    dbutils,
    registry_path: str,
    schedule_group: str,
    target_env: str = "dev",
    layer: str = "bronze",
    max_bytes: int = 50_000_000,
) -> List[str]:
    """
    Returns the list of object_ids for the given schedule_group and layer (enabled only).
    Used by the discover_objects notebook to drive For-each over objects (one task per object).
    """
    registry, _ = load_compiled_registry(dbutils, registry_path, max_bytes)
    objects: List[Dict[str, Any]] = registry.get("objects", [])
    layer_n = (layer or "bronze").lower()
    selected = [
        o for o in objects
        if bool(o.get("enabled", True))
        and o.get("schedule_group") == schedule_group
        and (o.get("layer") or "bronze") == layer_n
    ]
    out: List[str] = []
    for o in selected:
        oid = o.get("object_id") or f"{target_env}|{o.get('source_system')}|{o.get('object_name')}"
        out.append(oid)
    return out
