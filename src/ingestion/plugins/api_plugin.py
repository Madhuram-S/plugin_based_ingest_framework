# api_plugin.py
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import Row

from src.ingestion.runner.plugin_contract import IngestResult

class ApiPlugin:
    """
    Extract-only plugin (MVP):
    - Calls API and returns a DataFrame with a single column '_raw_payload' (JSON string per record).
    - Handles basic pagination via page_number and basic 429 retry.
    - State handling: optional watermark param (runner/state store persists next_state).
    """
    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def extract(self, run_ctx, obj_cfg: Dict[str, Any], prior_state: Dict[str, Any]) -> IngestResult:
        req = obj_cfg.get("request", {}) or {}
        conn = obj_cfg.get("connection_options", {}) or {}

        base_url = conn.get("base_url") or req.get("base_url")
        if not base_url:
            raise ValueError("API requires connection_options.base_url or request.base_url")

        path = req.get("path")
        if not path:
            raise ValueError("API requires request.path")

        method = (req.get("method") or "GET").upper()
        headers = (req.get("headers") or conn.get("headers") or {})  # can be injected via secrets_runtime
        params = dict(req.get("params") or {})

        # Optional watermark parameter
        incr = obj_cfg.get("incremental", {}) or {}
        if (incr.get("strategy") or "").lower() == "watermark":
            param_name = incr.get("param_name")
            initial = incr.get("initial_value")
            last = prior_state.get("last_watermark") or initial
            if param_name and last is not None:
                params[param_name] = last

        pagination = obj_cfg.get("pagination", {}) or {}
        ptype = (pagination.get("type") or "page_number").lower()
        per_page = int(pagination.get("per_page", 200))
        page = int(pagination.get("start_page", 1))

        records: List[Dict[str, Any]] = []
        url = base_url.rstrip("/") + path

        while True:
            call_params = dict(params)
            if ptype == "page_number":
                call_params["page"] = page
                call_params["per_page"] = per_page

            resp = requests.request(method, url, headers=headers, params=call_params, timeout=60)

            if resp.status_code == 429:
                ra = int(resp.headers.get("Retry-After", "5"))
                time.sleep(ra)
                continue

            resp.raise_for_status()
            payload = resp.json()

            # Extract records
            resp_cfg = obj_cfg.get("response", {}) or {}
            key = resp_cfg.get("records_key")  # e.g., "data"
            if key and isinstance(payload, dict):
                batch = payload.get(key, [])
            elif isinstance(payload, list):
                batch = payload
            else:
                # fallback: treat dict as single record
                batch = [payload] if isinstance(payload, dict) else []

            if not isinstance(batch, list):
                batch = [batch]

            for r in batch:
                records.append({"_raw_payload": json.dumps(r)})

            if ptype == "page_number":
                if len(batch) < per_page:
                    break
                page += 1
            else:
                break

        df = self.spark.createDataFrame([Row(**r) for r in records])

        # Next watermark (optional): if API returns an updated_at field, compute it; else do not advance.
        next_state = {}
        wm_field = (incr.get("watermark_field") or "").strip()
        if wm_field and "_raw_payload" in df.columns:
            # MVP: skip parsing; do not advance watermark unless you parse into a column safely.
            pass

        return IngestResult(
            status="SUCCESS",
            ingest_mode="API",
            df=df,
            row_count_source=None,
            next_state=next_state,
            artifacts={"source": "api", "url": url, "records": len(records)},
            warnings=[],
        )
