from __future__ import annotations

import time
import uuid
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


def run_ingestion_group(
    spark,
    dbutils,
    target_env: str,
    schedule_group: str,
    registry_path: str,
    config_version: str,
    strict_mode: bool = False,
    max_objects: Optional[int] = None,
) -> None:
    """
    Entry function called by notebook/job.
    Runs all enabled objects in a schedule group from compiled_registry.json.
    """

    # 1) Load compiled registry + hash
    registry, registry_sha256 = load_compiled_registry(dbutils, registry_path)

    # 2) Build run context
    run_ctx = RunContext(
        run_id=str(uuid.uuid4()),
        env=target_env,
        config_version=config_version,
        registry_path=registry_path,
        registry_sha256=registry_sha256,
        started_epoch=time.time(),
    )

    # 3) Standard services (centralized)
    # catalog = "prod_bronze" if target_env == "prod" else f"{target_env}_bronze"
    catalog = "dbx_genie_sandbox"
    ops_schema = "ops"  # keep constant; map via UC grants
    state = StateStore(spark, f"{catalog}.{ops_schema}.ctl_ingestion_state")
    ops = OpsLogger(spark, f"{catalog}.{ops_schema}.ops_run_log", f"{catalog}.{ops_schema}.ops_dq_log")
    writer = BronzeWriter(spark)
    dq = DQExecutor(spark, ops)

    # Optional but recommended for P0: object-level lock
    lock = ObjectLock(spark, f"{catalog}.{ops_schema}.ops_object_lock")

    # 4) Plugin registry
    plugins = build_plugin_registry(spark, dbutils, writer, state)

    # 5) Select objects
    objects: List[Dict[str, Any]] = registry.get("objects", [])
    selected = [
        o for o in objects
        if bool(o.get("enabled", True))
        and o.get("schedule_group") == schedule_group
    ]

    if max_objects:
        selected = selected[:max_objects]

    if not selected:
        print(f"[Runner] No enabled objects for schedule_group={schedule_group}")
        return

    print(f"[Runner] run_id={run_ctx.run_id} env={target_env} group={schedule_group} objects={len(selected)}")

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
    
    # Log start
    ops.log_run_start(
        run_id=run_ctx.run_id,
        object_id=object_id,
        env=run_ctx.env,
        config_version=run_ctx.config_version,
        registry_sha256=run_ctx.registry_sha256,
        source_system=obj.get("source_system"),
        source_type=source_type,
        object_name=obj.get("object_name"),
        bronze_table=obj.get("bronze_table"),
        schedule_group=obj.get("schedule_group"),
    )

    

    # Acquire lock (avoid concurrent reruns / schedule overlap)
    got_lock = lock.try_acquire(object_id, run_ctx.run_id, ttl_minutes=120)
    if not got_lock:
        ops.log_run_skipped(run_ctx.run_id, object_id, "Lock not acquired (another run in progress).")
        return

    try:
        # Load prior state (read-only)
        prior_state = state.get_state(object_id)

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
            ops.log_run_skipped(run_ctx.run_id, object_id, result.warnings[0] if result.warnings else "Skipped by plugin.")
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

        rows_read = result.row_count_source or None
        rows_written = writer.write(df, obj_resolved["bronze_table"], write_mode, pk)
        
        # Run Bronze-grade DQ (can be warn/quarantine policy)
        dq.run(run_ctx, object_id, obj_resolved)
        
        # Persist state only after successful write + DQ policy
        next_state = result.next_state or {}
        state.update_on_success(
            object_id=object_id,
            run_id=run_ctx.run_id,
            last_watermark=next_state.get("last_watermark"),
            last_cursor=next_state.get("last_cursor"),
        )


        # Success log
        ops.log_run_success(
            run_id=run_ctx.run_id,
            object_id=object_id,
            rows_read=rows_read,
            rows_written=rows_written,
            extra=result.artifacts,
            warnings=result.warnings,
        )

    except Exception as e:
        ops.log_run_failure(run_ctx.run_id, object_id, str(e))
        if strict_mode:
            raise
        print(f"[Runner] FAILED object_id={object_id} error={e}")

    finally:
        lock.release(object_id, run_ctx.run_id)
