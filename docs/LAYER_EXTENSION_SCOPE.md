# Extending the runner + plugin framework for Silver and Gold (layer scope)

This document lists the **extent of changes** needed to introduce a **layer** (`bronze` | `silver` | `gold`) across the framework so the same runner can drive bronze, silver, and gold pipelines.

**Status: Implemented.** Layer is wired through contract, config, runner, state/lock/ops (shared tables with `layer` column), writer (`target_table`), DQ, registry reader, notebooks, workflow, and generator. Silver/gold transform plugins can be added later; the registry and runner already filter and run by layer.

---

## 1. Contract and data model

### 1.1 Object config and identity

| Current | With layer |
|--------|------------|
| `object_id` = `{env}|{source_system}|{object_name}` | `object_id` = `{env}|{layer}|{source_system}|{object_name}` (or keep as-is and add `layer` as separate field; recommend **adding** `layer` so one logical object can differ by layer) |
| `bronze_table` (single target table) | **`target_table`** (or keep `bronze_table` as alias when `layer=bronze`) so silver/gold use `silver_table` / `gold_table` or a generic **`target_table`** |
| No `layer` in object | Every object has **`layer`**: `bronze` | `silver` | `gold` |

**Files to change**

- **`config_loader.py`**
  - Add `layer` to source/object (default `bronze`). Require or default from source.
  - Derive **`target_table`** from target config (catalog, schema, prefix) and optionally keep `bronze_table` as alias for `layer=bronze`.
  - Add `layer` to `LoadedObject` dataclass and to `obj_cfg`.
  - `object_id`: either include layer, e.g. `f"{env_name}|{layer}|{source_system}|{obj_name}"`, or leave as-is and store `layer` only in config (simpler; recommend this so object_id stays unique per “source object” and layer is a filter).

- **`registry.yml` / sources YAML**
  - Add optional `layer` at source or object level (default `bronze`).
  - Target block: today uses `bronze_schema`; add **`silver_schema`**, **`gold_schema`** in env and/or target so `target_table` is derived per layer.

- **`preflight.py`**
  - Require **`layer`** in required-fields check (or allow default).
  - Validate `layer` in `{"bronze", "silver", "gold"}`.
  - If you rename to `target_table`: require **`target_table`** (or `bronze_table` when layer=bronze) so one required “target table” field exists per layer.

---

## 2. Runner and services (table names, writer selection)

### 2.1 Ops / state / lock tables: per-layer or shared with layer column

Two options:

- **Option A – Shared tables with `layer` column**  
  One `ops_run_log`, one `ctl_ingestion_state`, one `ops_object_lock`; add column **`layer`** and filter/partition by it. All runner code that writes/reads these tables must pass/filter `layer`.

- **Option B – Separate table names per layer**  
  e.g. `ops_run_log_bronze`, `ops_run_log_silver`, `ops_run_log_gold` (and same for state and lock). Runner gets table name from `layer` (e.g. `f"{catalog}.{ops_schema}.ctl_ingestion_state_{layer}"`).

Recommendation: **Option A** (shared tables + `layer` column) so one schema, one place to query, and run metrics can slice by layer.

**Files to change**

- **`run_group.py`**
  - Accept **`layer`** (default `bronze`) and pass into RunContext and services.
  - StateStore: instantiate with same table but **pass `layer` into get_state/update_on_success** if state is keyed by (object_id, layer) or object_id already embeds layer. If object_id does not embed layer, state store must be keyed by (object_id, layer) – then add **`layer`** to state table schema and to get/update calls.
  - OpsLogger: add **`layer`** to run_log schema and to every log_run_* call (or derive from run_ctx).
  - ObjectLock: key by (object_id, layer) or keep object_id if it’s unique per layer (e.g. `env|layer|source_system|object_name`). If lock is per (object_id, layer), add **`layer`** to lock table and to try_acquire/release.
  - Writer: select **layer-specific writer** (BronzeWriter / SilverWriter / GoldWriter) by `layer`, or one **LayerWriter** that takes `layer` and applies layer-specific audit cols and write rules.
  - Load registry and **filter objects by `layer`** (or pass layer into runner and filter in “Select objects” step).

- **`core_runner.py`**
  - Same as run_group: accept **`layer`**, pass to RunContext, StateStore, OpsLogger, ObjectLock, writer selection, and filter objects by layer.

- **`plugin_contract.py`**
  - **RunContext**: add **`layer: str`** (e.g. `"bronze"` | `"silver"` | `"gold"`).

---

## 3. Config loader and registry (target_table, layer)

**Files to change**

- **`config_loader.py`**
  - **`expand_objects`**:  
    - Read **`layer`** from source default or object (default `bronze`).  
    - Set **`obj_cfg["layer"]`**.  
    - Derive **target table**: e.g. for bronze use existing `bronze_schema` and `bronze_table`; for silver/gold use `silver_schema`/`gold_schema` from env/target and a name. For example:  
      - `target_table = f"{catalog}.{schema}.{prefix}{_safe_name(obj_name)}"` where `schema` comes from `target.schema` or `env.bronze_schema` / `env.silver_schema` / `env.gold_schema` by layer.  
    - Set **`obj_cfg["target_table"]`** (and optionally keep **`obj_cfg["bronze_table"]`** when layer=bronze for backward compatibility).  
  - **LoadedObject** dataclass: add **`layer: str`**, and **`target_table: str`** (and keep or drop `bronze_table`).

- **Env and registry YAML**
  - **`configs/env/dev.yml`** (and test, prod): add **`silver_schema`**, **`gold_schema`** (e.g. `silver`, `gold`).  
  - **`configs/registry.yml`**: document layer and target defaults (e.g. default layer `bronze`).  
  - **Sources YAML** (e.g. `sources_file_ingestion.yml`): in **target** or **defaults**, allow **`layer`** and schema per layer; in **target** allow `schema: "${env.bronze_schema}"` or by layer.

---

## 4. Writer and plugins

### 4.1 Writer

- **`bronze_writer.py`**
  - Either: rename to **`layer_writer.py`** and have **`LayerWriter`** with `write(..., target_table, mode, primary_key, layer)` and layer-specific audit columns (bronze: current cols; silver/gold: possibly different cols or none).  
  - Or: keep **BronzeWriter** and add **SilverWriter**, **GoldWriter** (each with `with_audit_cols` and `write`). Runner picks writer by `layer`.

**Files to change**

- **`bronze_writer.py`**: generalize to **target_table** (param name) and optional **layer** for audit columns; or leave as-is and add **`silver_writer.py`**, **`gold_writer.py`** (or one **`layer_writer.py`**).
- **`run_group.py`** / **`core_runner.py`**: replace single `BronzeWriter` with a **writer factory** by layer, e.g. `writers = {"bronze": BronzeWriter(spark), "silver": SilverWriter(spark), "gold": GoldWriter(spark)}` and `writer = writers[run_ctx.layer]`, and call **`writer.write(df, obj["target_table"], ...)`**.

### 4.2 Plugins

- **Bronze**: existing plugins (BigQuery, API, File) **extract** from source and return a DataFrame; runner writes to **target_table** (bronze).
- **Silver / Gold**: “plugins” are **transform** steps: read from Delta (bronze or silver), transform, return DataFrame; runner writes to **target_table** (silver or gold). So you need:
  - **Silver “plugin”** (e.g. a transform that reads from bronze table(s) and returns a single DataFrame per object).
  - **Gold “plugin”** (same idea from silver or bronze).

**Files to change**

- **`plugin_registry.py`**: build registry by **layer**: e.g. for `layer=bronze` register bigquery, api, file; for `layer=silver` register `silver_transform` (or one transform plugin that takes config); for `layer=gold` register `gold_transform`. So **`build_plugin_registry(spark, dbutils, writer, state_store, layer)`** and return different plugins per layer.
- **New plugins** (e.g. **`silver_plugin.py`**, **`gold_plugin.py`**): each has `extract(run_ctx, obj_cfg, prior_state)` which **reads from Delta** (e.g. `spark.table(obj_cfg["source_table"])` or from config) and applies transforms, returns **IngestResult(df=..., ...)**. Object config for silver/gold would have **source_table** (or list) and optional transform spec.

---

## 5. Ops / state / lock: schema and usage

### 5.1 Run log and DQ log

- **`ops_logger.py`**
  - Add **`layer`** to run_log table schema and to every **log_run_start** / **_append_end_event** row (from RunContext or caller).
  - **Canonical view** and run-metrics queries: include **`layer`** and allow filtering by it.

### 5.2 State store

- **`state_store.py`**
  - If state is per (object_id, layer): add **`layer`** column to state table and to **get_state(object_id, layer)** and **update_on_success(..., layer)**.  
  - If object_id is already unique per layer (e.g. `env|layer|source_system|object_name`), no schema change; otherwise add **layer** to state table and all reads/writes.

### 5.3 Lock

- **`lock.py`**
  - If lock is per (object_id, layer): add **`layer`** to lock table and to **try_acquire(object_id, run_id, layer=...)** and **release(..., layer)**.  
  - If object_id is globally unique including layer, no change; otherwise add **layer** to lock table.

---

## 6. DQ executor

- **`dq_executor.py`**
  - Today it uses **`bronze_table`** from obj_cfg to run checks. Change to **`target_table`** (or keep bronze_table for backward compat and use **`obj_cfg.get("target_table") or obj_cfg.get("bronze_table")`**).  
  - DQ rulesets can be layer-specific (e.g. `bronze_minimal`, `silver_minimal`, `gold_minimal`); config already has **ruleset** per object, so no code change if objects pass the right ruleset by layer.

---

## 7. Preflight

- **`preflight.py`**
  - Required fields: add **`layer`** (or default in loader and validate in preflight).  
  - Add validation: **`layer in ("bronze", "silver", "gold")`**.  
  - If you standardize on **target_table**: require **`target_table`** (and/or allow **`bronze_table`** when layer=bronze) so one target field is always present.

---

## 8. Registry reader and discover notebook

- **`registry_reader.py`**
  - **get_object_ids_for_schedule_group**: add optional **`layer`** filter so discovered object_ids are for one layer. Used by parallel-by-object job; notebook or job must pass **layer** (e.g. bronze).

- **Notebook `09_discover_object_ids.py.py`**
  - Add widget **LAYER** (default `bronze`) and pass to **get_object_ids_for_schedule_group(..., layer=LAYER)** if the function supports it.

---

## 9. Notebooks and workflow

- **Notebook `10_run_ingestion_group.py.py`**
  - Add widget **LAYER** (default `bronze`).  
  - Call **run_ingestion_group(..., layer=LAYER)**.

- **Runner entry point**
  - **run_group.run_ingestion_group**: add parameter **`layer: str = "bronze"`**. Filter objects by **`o.get("layer", "bronze") == layer`** after loading registry. Pass **layer** into RunContext and into StateStore/OpsLogger/Lock and writer selection.

- **Workflow YAML and generator**
  - **workflow_bronze_*.yml**: add **LAYER** to base_parameters (e.g. `LAYER: "bronze"`). For silver/gold, duplicate or parameterize job with **LAYER: "silver"** / **LAYER: "gold"**.  
  - **generate_bronze_sharded_job.py**: add **--layer** (default `bronze`) and pass to notebook params; optionally rename script to **generate_ingestion_job.py** and support any layer.

---

## 10. Docs and scripts

- **docs/ORCHESTRATION_FLOW.md**: mention **layer** (bronze/silver/gold) and that writer and plugins are selected by layer.  
- **docs/RUN_METRICS_FROM_EVENTS.md**: add **layer** to run_log and to example queries/views.  
- **configs/README_bronze_sharded.md**: generalize to “ingestion” (bronze/silver/gold) or keep bronze-focused and add a short note that layer can be extended.  
- **pipelines/deploy-bronze-prod.yml**: optional **LAYER** variable (default bronze); same pipeline can deploy silver/gold jobs if registry and notebook support layer.

---

## 11. Summary: files to touch

| Area | Files |
|------|--------|
| **Contract / context** | `plugin_contract.py` (RunContext.layer), object config (`target_table`, `layer`) |
| **Config** | `config_loader.py`, `LoadedObject`, `registry.yml`, `env/*.yml`, sources YAML |
| **Runner** | `run_group.py`, `core_runner.py` (layer param, filter objects, writer by layer, pass layer to services) |
| **Writer** | `bronze_writer.py` (generalize to target_table) and/or add `silver_writer.py`, `gold_writer.py` or `layer_writer.py` |
| **Plugins** | `plugin_registry.py` (layer-aware registry), new `silver_plugin.py`, `gold_plugin.py` (or one transform plugin with config) |
| **State / lock / ops** | `state_store.py`, `lock.py`, `ops_logger.py` (add layer to schema and all calls) |
| **Preflight / DQ** | `preflight.py` (layer + target_table), `dq_executor.py` (target_table) |
| **Registry / discover** | `registry_reader.py` (optional layer filter), `09_discover_object_ids.py.py` (LAYER widget) |
| **Notebooks / workflow** | `10_run_ingestion_group.py.py` (LAYER widget), workflow YAML, `generate_bronze_sharded_job.py` (--layer) |
| **Docs** | `ORCHESTRATION_FLOW.md`, `RUN_METRICS_FROM_EVENTS.md`, `README_bronze_sharded.md`, pipelines README |

---

## 12. Suggested order of implementation

1. **Contract and config**: add `layer` and `target_table` to config loader and LoadedObject; env YAML (silver_schema, gold_schema); preflight.  
2. **RunContext and runner**: add `layer` to RunContext; runner accepts `layer`, filters objects by layer, passes layer to services.  
3. **Ops / state / lock**: add `layer` column and pass layer in all calls.  
4. **Writer**: generalize to target_table; add SilverWriter/GoldWriter or LayerWriter.  
5. **Plugins**: layer-aware plugin registry; add silver/gold transform plugins.  
6. **Notebooks and workflow**: LAYER widget; workflow and generator.  
7. **Docs and pipeline**: update docs and optional pipeline variable.

This gives you a clear scope and order to extend the runner + plugin model for Silver and Gold with a single **layer** concept everywhere it matters.
