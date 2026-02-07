# registry_reader.py
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Tuple


def load_compiled_registry(dbutils, registry_path: str, max_bytes: int = 50_000_000) -> Tuple[Dict[str, Any], str]:
    """
    Loads compiled_registry.json as text and returns (registry_dict, sha256_of_text).
    Uses dbutils.fs.head for simplicity. If your registry can exceed max_bytes, store it chunked or as a Delta table.
    """
    text = dbutils.fs.head(registry_path, max_bytes)
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    registry = json.loads(text)
    return registry, sha
