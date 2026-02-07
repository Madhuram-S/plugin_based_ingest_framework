# secrets_runtime.py
from __future__ import annotations

from typing import Any, Dict

def resolve_secrets_in_config(cfg: Any, dbutils) -> Any:
    """
    Replaces dicts of the form:
      { "secret_ref": { "scope": "<scope>", "key": "<key>" } }
    with the secret string (runtime only, never persisted).

    Safe by design:
    - only recognizes 'secret_ref' key explicitly
    - everything else passes through unchanged
    """

    def walk(x: Any) -> Any:
        if isinstance(x, dict):
            if "secret_ref" in x and isinstance(x["secret_ref"], dict):
                ref = x["secret_ref"]
                scope = ref.get("scope")
                key = ref.get("key")
                if not scope or not key:
                    raise ValueError("secret_ref must include scope and key")
                return dbutils.secrets.get(scope=scope, key=key)
            return {k: walk(v) for k, v in x.items()}

        if isinstance(x, list):
            return [walk(v) for v in x]

        return x

    return walk(cfg)
