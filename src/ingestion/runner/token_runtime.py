import re
from typing import Any, Dict

_TOKEN_PATTERN = re.compile(r"\{\{([^}]+)\}\}")

def _get_path(ctx: Dict[str, Any], path: str) -> Any:
    cur: Any = ctx
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(f"Missing token path: {path}")
        cur = cur[part]
    return cur

def resolve_runtime_tokens(value: Any, ctx: Dict[str, Any]) -> Any:
    """
    Resolve {{token.paths}} recursively. Safe for config values, not secrets materialization.
    """
    if isinstance(value, str):
        def repl(m):
            token = m.group(1).strip()
            v = _get_path(ctx, token)
            # if token resolves to list/dict and this string is ONLY the token, return raw (not str)
            if value.strip() == m.group(0) and isinstance(v, (list, dict)):
                return v
            return str(v)
        # If token-only returns list/dict, handle special-case:
        if value.strip().startswith("{{") and value.strip().endswith("}}") and _TOKEN_PATTERN.fullmatch(value.strip()):
            token = value.strip()[2:-2].strip()
            return _get_path(ctx, token)
        return _TOKEN_PATTERN.sub(lambda m: str(_get_path(ctx, m.group(1).strip())), value)

    if isinstance(value, list):
        return [resolve_runtime_tokens(v, ctx) for v in value]

    if isinstance(value, dict):
        return {k: resolve_runtime_tokens(v, ctx) for k, v in value.items()}

    return value
