# High-Level Design Document — ETL Ingestion Framework

This document describes the design of the bronze (and layer-extensible) ingestion framework: principles, data flow, components, metadata, orchestration, observability, data quality, security, and related topics. It is derived from the codebase, configs, and existing docs in this repository.

---

## 1. Design Principles & Considerations

| Principle | Description |
|-----------|-------------|
| **Layer-aware, single runner** | One runner drives bronze (and optionally silver/gold) with a shared contract. Objects declare `layer`; state, lock, and ops tables use a `layer` column so one schema serves all layers. |
| **Plugin-based extraction** | Source-specific logic is isolated in plugins (BigQuery, API, File, File Autoloader). The runner discovers plugins via a registry and invokes a common contract (`load(run_ctx, object_id, obj_cfg) → DataFrame`). |
| **Configuration as code** | Registry (`registry.yml`), sources (`sources_*.yml`), connections (`connections.yml`), DQ rulesets (`dq_rulesets.yml`), and env overlays (`env/dev.yml`, etc.) are YAML. No secrets in config; use `secret_ref` (scope/key) resolved at runtime. |
| **Concurrency-safe operations** | Append-only run log (events: START, END, SKIP) avoids Delta write conflicts when many tasks run in parallel. State updates use optimistic concurrency (expected `last_run_id`). Locks are per `(object_id, layer)` with TTL. |
| **Fail-safe and observable** | Preflight validates config before any run. Per-object lock prevents concurrent runs for the same object. DQ results are logged; action on fail is configurable (warn/fail). |
| **Environment and token resolution** | Config supports `${env.var}` and `{{token.path}}` expansion. Env overlays (e.g. `env/dev.yml`) supply catalog, schema, storage, secret scope. Tokens are resolved from a context dict (e.g. `primary_key` in DQ rules). |
| **Deployment flexibility** | Jobs can be static YAML (dev/test) or generated in CI from pipeline variables (prod). Two execution modes: sharded (sequential within shard) or parallel-by-object (one task per object). |

---

## 2. Level 2 Data Flow

High-level flow from source systems to the lakehouse, with control and observability data.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  SOURCES                                                                         │
│  (BigQuery, APIs, Files/ADLS, File Autoloader)                                  │
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  CONFIGURATION                                                                   │
│  registry.yml + sources_*.yml + connections.yml + dq_rulesets.yml + env/*.yml   │
│  → ConfigLoader.load() → expand_objects() → Preflight validate                   │
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION (Databricks Job)                                                  │
│  Task 1: Discover object_ids (by schedule_group / layer)                         │
│  Task 2: For-each over object_ids (or shard indices) → run_ingestion_group       │
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  RUNNER (per task)                                                               │
│  RegistryReader → RunContext, StateStore, OpsLogger, Lock, BronzeWriter, DQ      │
│  → For each object: lock → plugin.load() → writer.write() → DQ → state update    │
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐     ┌─────────────────┐     ┌───────────────────────────────┐
│  CONTROL      │     │  OBSERVABILITY  │     │  DATA LAYER (Delta)           │
│  StateStore   │     │  ops_run_log    │     │  Bronze (silver/gold) tables  │
│  ObjectLock   │     │  ops_dq_log     │     │  + audit columns              │
│  (ingestion   │     │  (append-only   │     │  (ingest_ts, run_id,          │
│   state)      │     │   events)       │     │   source_system, etc.)        │
└───────────────┘     └─────────────────┘     └───────────────────────────────┘
```

**Level 2 summary**

- **Source → Config**: Connection and object definitions reference sources; credentials via `secret_ref` (resolved at runtime).
- **Config → Orchestration**: Compiled registry (with env/tokens expanded) drives which objects run; discovery notebook outputs `object_ids` for the For-each.
- **Orchestration → Runner**: Each task runs `run_ingestion_group` with parameters (e.g. `OBJECT_ID`, or `SHARD_ID`/`SHARD_COUNT`).
- **Runner → Plugins**: Runner selects plugin by `source_type`, calls `load()`; plugin uses connection (and resolved secrets) to produce a Spark DataFrame.
- **Runner → Writer**: Writer appends/merges to `target_table` with audit columns; supports merge when primary key is present.
- **Runner → DQ**: After write, DQ executor runs checks (row_count_min, not_null, duplicates) from object/registry DQ config; results go to `ops_dq_log`.
- **Runner → State/Lock**: State is updated only on success (watermark/cursor); lock is acquired at start and released at end (or expiry).

---

## 3. Components

| Component | Responsibility |
|-----------|----------------|
| **ConfigLoader** | Loads and merges registry, sources, connections, DQ rulesets, env overlay; expands `${env.*}` and resolves `{{tokens}}`; produces a list of `LoadedObject` with merged `obj_cfg`. |
| **RegistryReader** | Wraps ConfigLoader; loads compiled registry and optionally filters by path/version; used by runner and discovery notebook. |
| **Preflight** | Validates each object config: required fields (target_table, layer, source_type, etc.), supported source_type and load_mode, no lingering `${...}`, and optional security checks. |
| **RunContext** | Immutable context for a run: run_id, env, layer, config_version, registry_sha256, schedule_group, shard_id, shard_count. Passed to plugins, writer, DQ, ops. |
| **Plugin Registry** | Discovers and instantiates plugins (BigQuery, API, File, File Autoloader) by `source_type`; implements the plugin contract (load). |
| **StateStore** | Delta table keyed by (object_id, layer). get_state() returns last_watermark, last_cursor, last_success_ts, last_run_id. update_on_success() uses optimistic concurrency (expected_last_run_id). |
| **ObjectLock** | Delta table keyed by (object_id, layer). try_acquire(object_id, run_id, layer, ttl_minutes) prevents concurrent runs; release() removes lock. |
| **OpsLogger** | Append-only logging to `ops_run_log` (event_type: START/END/SKIP, plus run/object metadata, status, row counts, error) and `ops_dq_log` (run_id, object_id, check_name, severity, status, details). |
| **BronzeWriter** | Writes plugin output to Delta `target_table` with audit columns (ingest_ts, run_id, source_system, source_object, ingest_mode); supports append and merge (when primary key present). |
| **DQExecutor** | Runs checks defined in object/registry DQ config: row_count_min, not_null, duplicates; logs to ops_dq_log; severity and action_on_fail (warn/fail) from ruleset. |
| **run_ingestion_group** | Entry point: load registry, build services, filter objects by schedule_group and optional object_ids/shard, run preflight, then for each object run _run_one_object (lock → load → write → DQ → state → release). |
| **Discovery notebook (09)** | Loads registry, filters by layer/schedule_group, outputs list of object_ids for the For-each (parallel-by-object mode). |
| **Ingestion notebook (10)** | Accepts widget parameters (TARGET_ENV, REGISTRY_PATH, SCHEDULE_GROUP, OBJECT_ID or SHARD_ID/SHARD_COUNT, LAYER); calls run_ingestion_group. |
| **Job generator script** | generate_bronze_sharded_job.py produces workflow YAML (sharded or parallel_by_object) from CLI/pipeline variables for bundle deploy. |

---

## 4. Metadata Components and Setup

### 4.1 Registry and conventions

- **registry.yml**: `registry_version`, `conventions.audit_columns`, `defaults.schedule`, `defaults.dq` (ruleset, action_on_fail), `defaults.ingestion` (max_retries, retry_backoff_seconds, merge_when_pk_present).
- **Audit columns** (applied by writer): `ingest_ts`, `run_id`, `source_system`, `source_object`, `ingest_mode`.

### 4.2 Control and ops tables (env-driven names)

Defined in `env.*.yml` under `control_tables`:

| Logical name | Purpose |
|--------------|---------|
| source_registry | Optional; can mirror compiled registry for lineage. |
| ingestion_state | StateStore table: (object_id, layer, last_watermark, last_cursor, last_success_ts, last_run_id). |
| run_log | ops_run_log — append-only event stream (START/END/SKIP). |
| dq_log | ops_dq_log — DQ check results per run/object. |

Lock table is hardcoded in the runner as `{catalog}.{ops_schema}.ops_object_lock` (schema: object_id, layer, run_id, acquired_ts, expires_ts). It is not currently defined in env `control_tables`.

### 4.3 Setup expectations

- **Catalogs and schemas**: Env defines `catalog`, `ops_schema`, `bronze_schema`, `silver_schema`, `gold_schema`, `quarantine_schema`. State, lock, and ops tables live in `{catalog}.{ops_schema}.*`.
- **Tables**: StateStore, ObjectLock, and OpsLogger create tables with `CREATE TABLE IF NOT EXISTS` on first use; StateStore/Lock support a migration that adds a `layer` column if missing.
- **Secrets**: Stored in Databricks secret scope; connections reference them via `secret_ref: { scope, key }`. Resolved at runtime only; never persisted in config.

---

## 5. Orchestration Flow

- **CI (e.g. Azure DevOps)**: Pipeline variables → `generate_bronze_sharded_job.py` → generated workflow YAML → `databricks bundle deploy -t prod` (prod). Dev/test may use static YAML.
- **Job run**:
  - **Parallel-by-object**: Task 1 runs discovery notebook → sets task value `object_ids`. Task 2 is For-each over `object_ids` with concurrency N; each iteration runs ingestion notebook with `OBJECT_ID=<id>`.
  - **Sharded**: For-each over shard indices; each task runs ingestion notebook with `SHARD_ID`/`SHARD_COUNT`; inside the task, objects in that shard run sequentially.
- **Per-task flow**: Load compiled registry → build RunContext, StateStore, OpsLogger, Lock, BronzeWriter, DQExecutor, plugin registry → select objects (schedule_group + object_ids or shard) → preflight → for each object: _run_one_object (lock → load → write → DQ → update state → release lock).
- **Per-object flow**: try_acquire lock → log START → get_state → plugin.load() → writer.write() → DQExecutor.run() → update_on_success (if success) → log END/SKIP → release lock.

Detailed Mermaid diagrams are in **docs/ORCHESTRATION_FLOW.md**.

---

## 6. Observability & State Management

### 6.1 Run log (event stream)

- **Table**: `ops_run_log` (or env-defined name).
- **Design**: Append-only; events are START, END, SKIP. No updates to existing rows, so concurrent tasks do not conflict on Delta writes.
- **Key fields**: event_type, event_ts, run_id, object_id, layer, env, config_version, registry_sha256, source_system, source_type, object_name, target_table, bronze_table, schedule_group, shard_id, shard_count, status, start_ts, end_ts, rows_read, rows_written, warnings_json, error, extra_json.
- **Latest status per (run_id, object_id)**: Use a view or query with `ROW_NUMBER() OVER (PARTITION BY run_id, object_id ORDER BY event_ts DESC)` and filter `rn = 1`.
- **Pipeline-level metrics**: Aggregate by time window or by `job_run_id` (if passed as widget and stored in run_log). See **docs/RUN_METRICS_FROM_EVENTS.md** for canonical SQL (per-run and per-pipeline metrics, example views).

### 6.2 DQ log

- **Table**: `ops_dq_log`.
- **Fields**: run_id, object_id, check_name, severity, status, details, ts.
- Used for data quality dashboards and alerting (e.g. on severity=fail or status=FAIL).

### 6.3 State management

- **StateStore**: Persists incremental state per (object_id, layer): last_watermark, last_cursor, last_success_ts, last_run_id. Updated only on successful run. Optimistic concurrency via `expected_last_run_id` to avoid overwriting state from a concurrent run.
- **ObjectLock**: Ensures only one run per (object_id, layer) at a time; TTL so stuck runs do not hold the lock forever. Cleanup of expired locks on try_acquire.

---

## 7. Data Quality Setup and Rules

### 7.1 Configuration

- **Registry**: `defaults.dq` specifies `ruleset` (e.g. `bronze_minimal`) and `action_on_fail` (warn | fail). Object-level config can override.
- **dq_rulesets.yml**: Named rulesets (e.g. `bronze_minimal`, `bronze_pk_guard`) with `description`, `on_fail`, and `checks`. Checks can use tokens (e.g. `{{primary_key}}`) resolved from object config.

### 7.2 Supported checks (DQExecutor)

| Check type | Description |
|------------|-------------|
| row_count_min | Ensures table row count ≥ min (e.g. min=1 for “data landed”). |
| not_null | Fails if any of the listed columns contain nulls (e.g. primary key columns). |
| duplicates | Fails if the listed columns (e.g. primary key) have duplicate values. |

Results are written to `ops_dq_log` with severity and status (PASS/FAIL/SKIP/ERROR). Unsupported check types are logged as SKIP.

### 7.3 Ruleset examples (from dq_rulesets.yml)

- **bronze_minimal**: row_count_non_zero (row_count_min, min=1), severity warn.
- **bronze_pk_guard**: row_count_non_zero + primary_key_not_null (not_null on `{{primary_key}}`) + primary_key_duplicates (duplicates on `{{primary_key}}`); primary key checks can be fail severity.

---

## 8. Security Rules

- **Secrets**: Credentials are not stored in YAML. Connections use `auth.secret_ref: { scope, key }` (and optional keys for OAuth/JDBC). Resolved at runtime via `dbutils.secrets.get(scope, key)` in `secrets_runtime.resolve_secrets_in_config()`; resolved values are not persisted.
- **Config resolution**: Only the `secret_ref` structure is recognized for substitution; all other config is passed through. Scope and key are required for secret_ref.
- **Preflight**: Can include checks for lingering `${...}` and other unsafe patterns so unexpanded or sensitive-looking values are caught before run.
- **Storage and catalog**: Access to ADLS (e.g. abfss) and to the Unity Catalog (catalog/schemas/tables) is assumed to be configured at the platform level (service principal, managed identity, or cluster credentials). The framework does not implement authentication to storage or catalog; it uses the Spark session and connection config (with resolved secrets where needed).

---

## 9. RBAC and ABAC

- **In-scope**: The framework assumes that the Databricks workspace and job execution identity have:
  - Read access to secret scopes used in `secret_ref`.
  - Read/write to the control tables (state, lock, run_log, dq_log) and to target layer tables in the catalog.
  - Read access to source systems (e.g. ADLS paths, BigQuery, APIs) as defined in connections.
- **Not implemented in framework**: The framework does not define or enforce RBAC/ABAC policies. Role-based and attribute-based access control should be implemented at the platform level (e.g. Databricks workspace permissions, Unity Catalog grants, and Azure AD or IdP attributes). Recommendations:
  - **RBAC**: Use Unity Catalog roles and table/schema grants so only intended identities can read/write state, ops, and layer tables; restrict secret scope access to jobs and roles that need it.
  - **ABAC**: If required, apply attribute-based policies (e.g. by project, env, or PII flags) via platform or policy engines; the framework can expose `env`, `layer`, and `source_system` in RunContext for use in such policies or in audit logs.

---

## 10. Other Items

### 10.1 Retries and backoff

- Registry `defaults.ingestion`: `max_retries`, `retry_backoff_seconds`. Applied by the runner when invoking plugin load and write (retries on transient failures).

### 10.2 Write modes and merge

- Supported write modes: append, merge, overwrite (preflight validates). When primary key is present and `merge_when_pk_present` is true, the writer can perform merge instead of append.

### 10.3 Layers (bronze / silver / gold)

- Objects declare `layer`; runner and all shared services (state, lock, ops) are layer-aware. Same code path supports silver/gold; target schema and table come from env (e.g. silver_schema, gold_schema) and target config. See **docs/LAYER_EXTENSION_SCOPE.md**.

### 10.4 File Autoloader

- Plugin for file-based ingestion (CSV, JSON, Parquet, Avro) with optional Autoloader-style listing; configured via sources with `source_type: file_autoloader` and connection pointing to a path (e.g. ADLS).

### 10.5 Deployment and CI

- **pipelines/deploy-bronze-prod.yml**: Example Azure DevOps pipeline; generates job definition from variables (e.g. REGISTRY_PATH_PROD, CONCURRENCY) and deploys via Databricks Asset Bundle. See **pipelines/README.md** and **configs/README_bronze_sharded.md**.

### 10.6 References

- **docs/ORCHESTRATION_FLOW.md** — Component-level and per-object flow diagrams.
- **docs/RUN_METRICS_FROM_EVENTS.md** — Run and pipeline metrics from run_log.
- **docs/GETTING_STARTED.md** — Setup and run instructions.
- **docs/LAYER_EXTENSION_SCOPE.md** — Layer design and extension points.
- **configs/README_bronze_sharded.md** — Sharded vs parallel-by-object and when to run the job generator.
