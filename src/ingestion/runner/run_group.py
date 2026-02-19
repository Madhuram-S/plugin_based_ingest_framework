from __future__ import annotations

import hashlib
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

from src.ingestion.runner.plugin_contract import RunContext, IngestResult
from src.ingestion.runner.registry_reader import load_compiled_registry
from src.ingestion.runner.preflight import preflight_validate_objects, PreflightIssue
from src.ingestion.runner.plugin_registry import build_plugin_registry
from src.ingestion.runner.token_runtime import resolve_runtime_tokens
from src.ingestion.runner.secrets_runtime import resolve_secrets_in_config
from src.ingestion.runner.state_store import StateStore
from src.ingestion.runner.ops_logger import OpsLogger
from src.ingestion.runner.bronze_writer import BronzeWriter
from src.ingestion.runner.dq_executor import DQExecutor
from src.ingestion.runner.lock import ObjectLock


def shard_for_object(object_id: str, shard_count: int) -> int:
    """
    Deterministically assign object_id to a shard (0..shard_count-1).
    Must be stable across all job runs and all tasks so the same object always
    lands in the same shard. Uses SHA-256 for deterministic, portable assignment.
    """
    h = hashlib.sha256(object_id.encode("utf-8")).hexdigest()
    return int(h, 16) % shard_count


def run_ingestion_group(
    spark,
    dbutils,
    target_env: str,
    schedule_group: str,
    registry_path: str,
    config_version: str,
    strict_mode: bool = False,
    max_objects: Optional[int] = None,
    shard_id: Optional[int] = None,
    shard_count: Optional[int] = None,
    object_ids: Optional[List[str]] = None,
    layer: str = "bronze",
) -> None:
    """
    Entry function called by notebook/job.
    Runs all enabled objects in a schedule group from compiled_registry.json.
    - layer: "bronze" | "silver" | "gold"; only objects with matching layer run.
    - When shard_id and shard_count are set: only objects assigned to this shard run (parallel by shard).
    - When object_ids is set: only those objects run (use for one task per object = full parallelism).
    """
    layer = (layer or "bronze").lower()
    if layer not in ("bronze", "silver", "gold"):
        raise ValueError(f"layer must be bronze, silver, or gold; got {layer}")

    # 1) Load compiled registry + hash
    registry, registry_sha256 = load_compiled_registry(dbutils, registry_path)

    # 2) Build run context
    run_ctx = RunContext(
        run_id=str(uuid.uuid4()),
        env=target_env,
        layer=layer,
        config_version=config_version,
        registry_path=registry_path,
        registry_sha256=registry_sha256,
        started_epoch=time.time(),
        shard_id=shard_id,
        shard_count=shard_count,
    )

    # 3) Standard services (centralized)
    # catalog = "prod_bronze" if target_env == "prod" else f"{target_env}_bronze"
    catalog = "dbx_genie_sandbox"
    ops_schema = "ops"  # keep constant; map via UC grants
    state = StateStore(spark, f"{catalog}.{ops_schema}.ctl_ingestion_state")
    ops = OpsLogger(spark, f"{catalog}.{ops_schema}.ops_run_log", f"{catalog}.{ops_schema}.ops_dq_log")
    audit_columns = (registry.get("conventions") or {}).get("audit_columns")
    writer = BronzeWriter(spark, audit_columns=audit_columns)
    dq = DQExecutor(spark, ops)

    # Optional but recommended for P0: object-level lock
    lock = ObjectLock(spark, f"{catalog}.{ops_schema}.ops_object_lock")

    # 4) Plugin registry (layer-specific; bronze = extract plugins, silver/gold = transform later)
    plugins = build_plugin_registry(spark, dbutils, writer, state, layer=layer)

    # 5) Select objects (by schedule_group and layer)
    objects: List[Dict[str, Any]] = registry.get("objects", [])
    selected = [
        o for o in objects
        if bool(o.get("enabled", True))
        and o.get("schedule_group") == schedule_group
        and (o.get("layer") or "bronze") == layer
    ]

    if max_objects:
        selected = selected[:max_objects]

    # Normalize object_id on all selected
    for o in selected:
        o["object_id"] = o.get("object_id") or f"{target_env}|{o.get('source_system')}|{o.get('object_name')}"

    # Optional: run only these object_ids (one task per object = full parallelism)
    if object_ids is not None:
        id_set = set(object_ids)
        selected = [o for o in selected if o["object_id"] in id_set]

    # Sharding filter (when not using object_ids): split objects across parallel workflow tasks
    if shard_count is not None and object_ids is None:
        if shard_id is None:
            raise ValueError("shard_id must be provided when shard_count is set")
        if shard_id < 0 or shard_id >= shard_count:
            raise ValueError(f"Invalid shard_id={shard_id}; must be 0..{shard_count - 1}")

        sharded: List[Dict[str, Any]] = []
        for o in selected:
            object_id = o.get("object_id") or f"{target_env}|{o.get('source_system')}|{o.get('object_name')}"
            o["object_id"] = object_id
            if shard_for_object(object_id, shard_count) == shard_id:
                sharded.append(o)
        selected = sharded

    if not selected:
        msg = f"[Runner] No enabled objects for schedule_group={schedule_group}"
        if shard_count is not None:
            msg += f" (shard_id={shard_id}, shard_count={shard_count})"
        if object_ids:
            msg += f" for object_ids={object_ids}"
        print(msg)
        return

    shard_info = f" shard={shard_id}/{shard_count}" if shard_count is not None else ""
    object_info = f" object_ids={object_ids}" if object_ids else ""
    print(f"[Runner] run_id={run_ctx.run_id} env={target_env} layer={layer} group={schedule_group} objects={len(selected)}{shard_info}{object_info}")

    # 6) Preflight validation (fast fail)
    approved, issues = preflight_validate_objects(selected, schedule_group=schedule_group)
    
    for iss in issues:
        print(f"[Preflight {iss.severity}] {iss.object_id}: {iss.message}")

    if strict_mode and any(i.severity == "ERROR" for i in issues):
        raise RuntimeError("Preflight failed in STRICT_MODE. Fix config before running.")

    if not approved:
        print("[Runner] No approved objects after preflight.")
        return
    
    # 7) Execute each object
    for obj in approved:
        _run_one_object(
            spark=spark,
            dbutils=dbutils,
            run_ctx=run_ctx,
            obj=obj,
            plugins=plugins,
            state=state,
            ops=ops,
            writer=writer,
            dq=dq,
            lock=lock,
            strict_mode=strict_mode,
        )


def _run_one_object(
    spark,
    dbutils,
    run_ctx: RunContext,
    obj: Dict[str, Any],
    plugins: Dict[str, Any],
    state: StateStore,
    ops: OpsLogger,
    writer: BronzeWriter,
    dq: DQExecutor,
    lock: ObjectLock,
    strict_mode: bool,
) -> None:
    source_type = obj.get("source_type")
    object_id = obj.get("object_id") or f"{run_ctx.env}|{obj.get('source_system')}|{obj.get('object_name')}"
    obj["object_id"] = object_id

    if source_type not in plugins:
        msg = f"Unsupported source_type={source_type}"
        if strict_mode:
            raise ValueError(msg)
        print(f"[Runner WARN] {msg} object_id={object_id}")
        return
    
    target_table = obj.get("target_table") or obj.get("bronze_table")
    obj_layer = obj.get("layer") or "bronze"

    # Capture start time so END rows can store it (self-contained without joining to START)
    start_ts = datetime.now(ZoneInfo("America/New_York"))

    # Log start
    ops.log_run_start(
        run_id=run_ctx.run_id,
        object_id=object_id,
        layer=obj_layer,
        env=run_ctx.env,
        config_version=run_ctx.config_version,
        registry_sha256=run_ctx.registry_sha256,
        source_system=obj.get("source_system"),
        source_type=source_type,
        object_name=obj.get("object_name"),
        target_table=target_table,
        bronze_table=obj.get("bronze_table"),
        schedule_group=obj.get("schedule_group"),
        shard_id=run_ctx.shard_id,
        shard_count=run_ctx.shard_count,
    )

    # Acquire lock (avoid concurrent reruns / schedule overlap); keyed by (object_id, layer)
    got_lock = lock.try_acquire(object_id, run_ctx.run_id, layer=obj_layer, ttl_minutes=120)
    if not got_lock:
        ops.log_run_skipped(
            run_ctx.run_id, object_id, "Lock not acquired (another run in progress).",
            start_ts=start_ts, layer=obj_layer, env=run_ctx.env, config_version=run_ctx.config_version, registry_sha256=run_ctx.registry_sha256,
            source_system=obj.get("source_system"), source_type=obj.get("source_type"),
            object_name=obj.get("object_name"), target_table=target_table, bronze_table=obj.get("bronze_table"),
            schedule_group=obj.get("schedule_group"), shard_id=run_ctx.shard_id, shard_count=run_ctx.shard_count,
        )
        return

    try:
        # Load prior state (read-only); keyed by (object_id, layer)
        prior_state = state.get_state(object_id, layer=obj_layer)

        # Runtime token context: keep tight & predictable
        runtime_ctx = {
            "env": {"name": run_ctx.env},
            "state": prior_state,
            "primary_key": (obj.get("write", {}) or {}).get("primary_key", []),
            "incremental": {
                "watermark_column": (obj.get("load", {}) or {}).get("watermark_column")
            },
        }

        # Resolve {{ }} tokens first (non-secret)
        obj_resolved = resolve_runtime_tokens(obj, runtime_ctx)
        
        # Resolve secret_ref pointers last-mile (in memory only)
        obj_resolved = resolve_secrets_in_config(obj_resolved, dbutils)
        
        # Execute plugin extract
        plugin = plugins[source_type]
        result: IngestResult = plugin.extract(run_ctx, obj_resolved, prior_state)
        
        if result.status == "SKIPPED":
            ops.log_run_skipped(
                run_ctx.run_id, object_id, result.warnings[0] if result.warnings else "Skipped by plugin.",
                start_ts=start_ts, layer=obj_layer, env=run_ctx.env, config_version=run_ctx.config_version, registry_sha256=run_ctx.registry_sha256,
                source_system=obj_resolved.get("source_system"), source_type=obj_resolved.get("source_type"),
                object_name=obj_resolved.get("object_name"), target_table=obj_resolved.get("target_table"), bronze_table=obj_resolved.get("bronze_table"),
                schedule_group=obj_resolved.get("schedule_group"), shard_id=run_ctx.shard_id, shard_count=run_ctx.shard_count,
            )
            return

        # Centralized write (NO plugin writes Bronze)
        df = result.df
        if df is None:
            raise RuntimeError("Plugin returned SUCCESS but no df. Return df or mark SKIPPED.")
            
        # Add audit columns centrally
        df = writer.with_audit_cols(
            df=df,
            run_id=run_ctx.run_id,
            source_system=obj_resolved["source_system"],
            source_object=obj_resolved["object_name"],
            ingest_mode=result.ingest_mode,
        )

        # Write
        write_cfg = obj_resolved.get("write", {}) or {}
        write_mode = write_cfg.get("mode", "append")
        pk = write_cfg.get("primary_key", []) or []

        target_table_resolved = obj_resolved.get("target_table") or obj_resolved.get("bronze_table")
        rows_read = result.row_count_source or None
        rows_written = writer.write(df, target_table_resolved, write_mode, pk)

        # Run DQ (layer-agnostic; runs against target_table)
        dq.run(run_ctx, object_id, obj_resolved)

        # Persist state only after successful write + DQ (optimistic: only if no concurrent update)
        next_state = result.next_state or {}
        state.update_on_success(
            object_id=object_id,
            run_id=run_ctx.run_id,
            layer=obj_layer,
            last_watermark=next_state.get("last_watermark"),
            last_cursor=next_state.get("last_cursor"),
            expected_last_run_id=prior_state.get("last_run_id"),
        )

        # Success log (include metadata so END row is self-contained, including start_ts)
        ops.log_run_success(
            run_id=run_ctx.run_id,
            object_id=object_id,
            rows_read=rows_read,
            rows_written=rows_written,
            extra=result.artifacts,
            warnings=result.warnings,
            start_ts=start_ts, layer=obj_layer, env=run_ctx.env, config_version=run_ctx.config_version, registry_sha256=run_ctx.registry_sha256,
            source_system=obj_resolved.get("source_system"), source_type=obj_resolved.get("source_type"),
            object_name=obj_resolved.get("object_name"), target_table=target_table_resolved, bronze_table=obj_resolved.get("bronze_table"),
            schedule_group=obj_resolved.get("schedule_group"), shard_id=run_ctx.shard_id, shard_count=run_ctx.shard_count,
        )

    except Exception as e:
        ops.log_run_failure(
            run_ctx.run_id, object_id, str(e),
            start_ts=start_ts, layer=obj_layer, env=run_ctx.env, config_version=run_ctx.config_version, registry_sha256=run_ctx.registry_sha256,
            source_system=obj.get("source_system"), source_type=obj.get("source_type"),
            object_name=obj.get("object_name"), target_table=target_table, bronze_table=obj.get("bronze_table"),
            schedule_group=obj.get("schedule_group"), shard_id=run_ctx.shard_id, shard_count=run_ctx.shard_count,
        )
        if strict_mode:
            raise
        print(f"[Runner] FAILED object_id={object_id} error={e}")

    finally:
        lock.release(object_id, run_ctx.run_id, layer=obj_layer)
