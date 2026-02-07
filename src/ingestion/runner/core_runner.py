# run_group.py
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from src.ingestion.runner.plugin_contract import RunContext, IngestResult
from src.ingestion.runner.registry_reader import load_compiled_registry
from src.ingestion.runner.preflight import preflight_validate_objects
from src.ingestion.runner.token_runtime import resolve_runtime_tokens
from src.ingestion.runner.secrets_runtime import resolve_secrets_in_config
from src.ingestion.runner.state_store import StateStore
from src.ingestion.runner.ops_logger import OpsLogger
from src.ingestion.runner.bronze_writer import BronzeWriter
from src.ingestion.runner.dq_executor import DQExecutor
from src.ingestion.runner.lock import ObjectLock

from src.ingestion.plugins.bigquery_plugin import BigQueryPlugin
from src.ingestion.plugins.api_plugin import ApiPlugin
from src.ingestion.plugins.file_plugin import FilePlugin
# from src.ingestion.plugins.sql_plugin import SqlPlugin


def run_ingestion_group(
    spark,
    dbutils,
    target_env: str,
    schedule_group: str,
    registry_path: str,
    config_version: str,
    strict_mode: bool = False,
) -> None:
    registry, registry_sha = load_compiled_registry(dbutils, registry_path)

    run_ctx = RunContext(
        run_id=str(uuid.uuid4()),
        env=target_env,
        config_version=config_version,
        registry_path=registry_path,
        registry_sha256=registry_sha,
        started_epoch=time.time(),
    )

    catalog = "main" if target_env == "prod" else f"main_{target_env}"
    ops_schema = "ops"

    state = StateStore(spark, f"{catalog}.{ops_schema}.ctl_ingestion_state")
    ops = OpsLogger(spark, f"{catalog}.{ops_schema}.ops_run_log", f"{catalog}.{ops_schema}.ops_dq_log")
    writer = BronzeWriter(spark)
    dq = DQExecutor(spark, ops)
    lock = ObjectLock(spark, f"{catalog}.{ops_schema}.ops_object_lock")

    plugins = {
        "bigquery": BigQueryPlugin(spark, dbutils),
        "api": ApiPlugin(spark, dbutils),
        "file": FilePlugin(spark, dbutils),
        # "sql": SqlPlugin(spark, dbutils),
    }

    objects: List[Dict[str, Any]] = registry.get("objects", [])
    selected = [o for o in objects if bool(o.get("enabled", True)) and o.get("schedule_group") == schedule_group]

    if not selected:
        print(f"[Runner] No objects for schedule_group={schedule_group}")
        return

    approved, issues = preflight_validate_objects(selected, schedule_group=schedule_group)
    for i in issues:
        print(f"[Preflight {i.severity}] {i.object_id}: {i.message}")

    if strict_mode and any(i.severity == "ERROR" for i in issues):
        raise RuntimeError("Preflight failed in STRICT_MODE.")

    for obj in approved:
        _run_one(spark, dbutils, run_ctx, obj, plugins, state, ops, writer, dq, lock, strict_mode)


def _run_one(
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
        print(f"[Runner WARN] {msg}")
        return

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

    if not lock.try_acquire(object_id, run_ctx.run_id, ttl_minutes=120):
        ops.log_run_skipped(run_ctx.run_id, object_id, "Lock not acquired.")
        return

    try:
        prior = state.get_state(object_id)

        runtime_ctx = {
            "env": {"name": run_ctx.env},
            "state": prior,
            "primary_key": (obj.get("write", {}) or {}).get("primary_key", []),
            "incremental": {"watermark_column": (obj.get("load", {}) or {}).get("watermark_column")},
        }

        obj_rt = resolve_runtime_tokens(obj, runtime_ctx)
        obj_rt = resolve_secrets_in_config(obj_rt, dbutils)

        plugin = plugins[source_type]
        result: IngestResult = plugin.extract(run_ctx, obj_rt, prior)

        if result.status == "SKIPPED":
            ops.log_run_skipped(run_ctx.run_id, object_id, (result.warnings[0] if result.warnings else "Skipped"))
            return

        if result.df is None:
            raise RuntimeError("Plugin returned SUCCESS but df is None.")

        df = writer.with_audit_cols(
            result.df,
            run_id=run_ctx.run_id,
            source_system=obj_rt["source_system"],
            source_object=obj_rt["object_name"],
            ingest_mode=result.ingest_mode,
        )

        write_cfg = obj_rt.get("write", {}) or {}
        rows_written = writer.write(
            df=df,
            bronze_table=obj_rt["bronze_table"],
            mode=write_cfg.get("mode", "append"),
            primary_key=write_cfg.get("primary_key", []) or [],
        )

        dq.run(run_ctx, object_id, obj_rt)

        # Only update state after success
        ns = result.next_state or {}
        state.update_on_success(
            object_id=object_id,
            run_id=run_ctx.run_id,
            last_watermark=ns.get("last_watermark"),
            last_cursor=ns.get("last_cursor"),
        )

        ops.log_run_success(
            run_id=run_ctx.run_id,
            object_id=object_id,
            rows_read=result.row_count_source,
            rows_written=rows_written,
            extra=result.artifacts,
            warnings=result.warnings,
        )

    except Exception as e:
        ops.log_run_failure(run_ctx.run_id, object_id, str(e))
        if strict_mode:
            raise
        print(f"[Runner] FAILED object_id={object_id}: {e}")

    finally:
        lock.release(object_id, run_ctx.run_id)
