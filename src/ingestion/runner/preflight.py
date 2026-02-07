# preflight.py
# The preflight.py module validates configuration objects that define how data should be ingested from various sources (BigQuery, APIs, files, SQL) into a data lakehouse. It acts as a quality gate, scanning configurations for missing fields, invalid values, unexpanded variables, and potential security issues before any actual data movement occurs.

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

_VAR_PATTERN = re.compile(r"\$\{[^}]+\}")  # lingering ${...} must NOT exist in compiled config

SUPPORTED_SOURCE_TYPES = {"bigquery", "api", "file", "sql"}
SUPPORTED_WRITE_MODES = {"append", "merge", "overwrite"}
SUPPORTED_LOAD_MODES = {"full", "incremental"}

@dataclass
class PreflightIssue:
    severity: str     # "ERROR" | "WARN"
    object_id: str
    message: str

def _find_var_tokens(obj: Any, path: str = "") -> List[str]:
    found = []
    if isinstance(obj, str):
        if _VAR_PATTERN.search(obj):
            found.append(path or "<root>")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            found.extend(_find_var_tokens(v, f"{path}.{k}" if path else str(k)))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found.extend(_find_var_tokens(v, f"{path}[{i}]"))
    return found

def validate_object_config(obj: Dict[str, Any]) -> List[PreflightIssue]:
    issues: List[PreflightIssue] = []

    oid = obj.get("object_id") or f"{obj.get('source_system')}|{obj.get('object_name')}"
    obj["object_id"] = oid

    enabled = bool(obj.get("enabled", True))
    if not enabled:
        issues.append(PreflightIssue("WARN", oid, "Object disabled; skipping deep validation."))
        return issues

    # Required fields
    for f in ["source_type", "source_system", "object_name", "bronze_table", "schedule_group"]:
        if not obj.get(f):
            issues.append(PreflightIssue("ERROR", oid, f"Missing required field: {f}"))

    st = obj.get("source_type")
    if st and st not in SUPPORTED_SOURCE_TYPES:
        issues.append(PreflightIssue("ERROR", oid, f"Unsupported source_type={st}. Supported={sorted(SUPPORTED_SOURCE_TYPES)}"))

    # Compiled registry must not contain ${...}
    lingering = _find_var_tokens(obj)
    if lingering:
        issues.append(PreflightIssue("ERROR", oid, f"Found unexpanded ${'{...}'} tokens at: {lingering[:10]}{'...' if len(lingering)>10 else ''}"))

    # Write config
    write_cfg = obj.get("write", {}) or {}
    wmode = write_cfg.get("mode", "append")
    if wmode not in SUPPORTED_WRITE_MODES:
        issues.append(PreflightIssue("ERROR", oid, f"Invalid write.mode={wmode}"))

    pk = write_cfg.get("primary_key", []) or []
    if isinstance(pk, str):
        pk = [pk]
        write_cfg["primary_key"] = pk

    if wmode == "merge":
        if not pk or not all(isinstance(x, str) and x for x in pk):
            issues.append(PreflightIssue("ERROR", oid, "write.mode=merge requires write.primary_key as non-empty list[str]."))

    # Load config
    load_cfg = obj.get("load", {}) or {}
    lmode = load_cfg.get("mode", "full")
    if lmode not in SUPPORTED_LOAD_MODES:
        issues.append(PreflightIssue("ERROR", oid, f"Invalid load.mode={lmode}"))

    if lmode == "incremental":
        strategy = load_cfg.get("strategy")
        if not strategy:
            issues.append(PreflightIssue("ERROR", oid, "incremental requires load.strategy"))
        if strategy == "watermark":
            if not load_cfg.get("watermark_column"):
                issues.append(PreflightIssue("ERROR", oid, "watermark incremental requires load.watermark_column"))
            if load_cfg.get("initial_value") is None:
                issues.append(PreflightIssue("WARN", oid, "watermark incremental missing load.initial_value (first run behavior must be defined)"))

    # Source-specific checks
    if st == "bigquery":
        on = obj.get("object_name", "")
        if "." not in on:
            issues.append(PreflightIssue("WARN", oid, "BigQuery object_name should look like 'dataset.table'"))

    if st == "api":
        req = obj.get("request", {}) or {}
        if not req.get("path"):
            issues.append(PreflightIssue("ERROR", oid, "API requires request.path"))
        if not (obj.get("auth") or obj.get("headers") or req.get("headers")):
            issues.append(PreflightIssue("WARN", oid, "API has no obvious auth/headers config. Ensure auth is present via connection_options or secrets."))
        if not (obj.get("landing", {}) or {}).get("enabled", False):
            issues.append(PreflightIssue("WARN", oid, "API landing.enabled=false; consider landing raw for replay/debug."))

    if st == "file":
        landing = obj.get("landing", {}) or {}
        if not landing.get("path"):
            issues.append(PreflightIssue("ERROR", oid, "File requires landing.path"))
        inc_mode = (obj.get("incremental", {}) or {}).get("mode", "file_autoloader")
        if inc_mode == "file_autoloader" and not landing.get("checkpoint"):
            issues.append(PreflightIssue("ERROR", oid, "Auto Loader requires landing.checkpoint"))

    # lightweight “raw secret smell” (warn only)
    raw = str(obj).lower()
    if "password=" in raw or "apikey=" in raw or "secret=" in raw:
        issues.append(PreflightIssue("WARN", oid, "Config looks like it may contain raw credentials; use secret_ref pointers only."))

    return issues

def preflight_validate_objects(objects: List[Dict[str, Any]], schedule_group: Optional[str] = None) -> Tuple[List[Dict[str, Any]], List[PreflightIssue]]:
    selected = objects
    if schedule_group:
        selected = [o for o in objects if o.get("schedule_group") == schedule_group and bool(o.get("enabled", True))]

    issues: List[PreflightIssue] = []
    approved: List[Dict[str, Any]] = []

    for obj in selected:
        obj_issues = validate_object_config(obj)
        issues.extend(obj_issues)
        if not any(i.severity == "ERROR" and i.object_id == obj["object_id"] for i in obj_issues):
            approved.append(obj)

    return approved, issues
