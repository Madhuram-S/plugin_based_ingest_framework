# Getting started – new developer guide

This framework **ingests data from sources (BigQuery, APIs, files) into Delta tables** using a **runner + plugins** model. Each run is for one **layer** (bronze, silver, or gold) and one **schedule group**; the runner loads a compiled registry, selects objects, and for each object calls the right plugin to extract (or transform) and then writes to a **target table** with state, locking, and ops logging.

---

## 1. Start here (order matters)

| Step | What to do | Why |
|------|------------|-----|
| **1** | Read **this file** (you are here). | Get the big picture and folder map. |
| **2** | Read **`docs/ORCHESTRATION_FLOW.md`** (sections 1–3). | See how CI → job → runner → plugins → tables fit together and the two job modes (shards vs parallel-by-object). |
| **3** | Skim **`configs/README_bronze_sharded.md`**. | Understand when to use shards vs full parallelism and how the job generator works. |
| **4** | Open **`configs/registry.yml`** and **`configs/sources_*.yml`** (e.g. `sources_file_ingestion.yml`). | See how objects are defined: sources, target catalog/schema, schedule_group, layer. |
| **5** | Trace one run in code: **`run_group.py`** → **`run_ingestion_group()`** → **`_run_one_object()`** → plugin **`extract()`** → **`bronze_writer.write()`**. | One pass through the main path. |
| **6** | (Optional) **`docs/LAYER_EXTENSION_SCOPE.md`** and **`docs/RUN_METRICS_FROM_EVENTS.md`**. | Layer design and how to query run metrics from the ops log. |

---

## 2. Folder map

```
etl_ingest_framework/
├── configs/                    # All YAML config
│   ├── registry.yml            # Conventions, defaults, DQ ruleset names
│   ├── connections.yml        # Connection definitions (BigQuery, API, file roots)
│   ├── sources_*.yml           # Source systems + objects (one file per source or area)
│   ├── dq_rulesets.yml         # DQ check definitions (row_count_min, not_null, etc.)
│   ├── env/                    # Per-environment overlay
│   │   ├── dev.yml
│   │   ├── test.yml
│   │   └── prod.yml
│   ├── workflow_bronze_*.yml   # Databricks job definitions (static)
│   └── README_bronze_sharded.md
│
├── docs/                       # Documentation
│   ├── GETTING_STARTED.md      # ← You are here
│   ├── ORCHESTRATION_FLOW.md   # Diagrams: CI → job → runner → tables
│   ├── LAYER_EXTENSION_SCOPE.md
│   └── RUN_METRICS_FROM_EVENTS.md
│
├── notebooks/
│   ├── 09_discover_object_ids  # Task 1: get object_ids for a schedule_group + layer
│   └── 10_run_ingestion_group  # Task 2 (or only task in shards mode): run ingestion
│
├── pipelines/                  # CI (e.g. Azure DevOps)
│   ├── deploy-bronze-prod.yml
│   └── README.md
│
├── scripts/
│   └── generate_bronze_sharded_job.py   # Generate job YAML/JSON (shards or parallel-by-object)
│
└── src/ingestion/
    ├── runner/                 # Core: one place to read first
    │   ├── run_group.py       # Main entry: run_ingestion_group()
    │   ├── core_runner.py     # Simpler entry (no sharding)
    │   ├── plugin_contract.py # RunContext, IngestResult
    │   ├── config_loader.py   # Load + expand registry/sources → objects with target_table, layer
    │   ├── plugin_registry.py # Build plugins by layer (bigquery, api, file)
    │   ├── state_store.py    # Per (object_id, layer) state (watermark, cursor)
    │   ├── lock.py           # Per (object_id, layer) lock
    │   ├── ops_logger.py     # Run log (START/END/SKIP) + DQ log
    │   ├── bronze_writer.py  # Audit cols + write to target_table
    │   ├── dq_executor.py    # Run DQ checks on target table
    │   ├── preflight.py      # Validate object config
    │   └── registry_reader.py # Load compiled registry JSON; get_object_ids_for_schedule_group()
    │
    └── plugins/               # Extract (and later transform) by source type
        ├── bigquery_plugin.py
        ├── api_plugin.py
        ├── file_plugin.py
        └── file_autoloader_plugin.py   # Auto Loader: CSV, JSON, Parquet, Avro
```

---

## 3. Key concepts (in one place)

- **Layer**: `bronze` | `silver` | `gold`. Each *run* is for one layer; objects in the registry have a `layer`; state/lock/ops tables share a `layer` column.
- **Schedule group**: A label (e.g. `P0_bigquery`) used to select which objects run in a job. One job run = one layer + one schedule group.
- **Object**: One ingestible unit (e.g. one BigQuery table, one API endpoint). Defined in `sources_*.yml` with `source_system`, `source_type`, `target`, `schedule_group`, optional `layer`.
- **Compiled registry**: JSON produced from YAML (registry + sources + env overlay). The runner *always* reads a **compiled** registry (path passed in); it does not load YAML at runtime. You need a separate step (or pipeline) to compile and publish it.
- **Target table**: The Delta table written for that object (`catalog.schema.table`). Derived from `target` + env `*_schema` by layer; stored in object config as `target_table` (and `bronze_table` when layer=bronze for compatibility).
- **Plugin**: Implements `extract(run_ctx, obj_cfg, prior_state)` and returns `IngestResult`. Bronze plugins *extract* from source; silver/gold can *transform* from Delta (future). **file_autoloader**: Auto Loader (cloudFiles) for CSV, JSON, Parquet, Avro; uses `landing.path`, `landing.checkpoint`, `landing.schema_location`; one batch per run via trigger(once=True).

---

## 4. First thing to run (local/dev)

1. **Compile a registry** (so the runner has a JSON to read):
   - Use `ConfigLoader` in Python: load base config, apply `configs/env/dev.yml`, call `expand_objects()` then build a dict `{"env": "...", "objects": [lo.raw for lo in loaded]}` and write as JSON to a path (e.g. DBFS or a local path your notebook can read).
   - Or use whatever pipeline you have that publishes `compiled_registry.json` for dev.

2. **Run the ingestion notebook** in Databricks:
   - Open **`notebooks/10_run_ingestion_group`**.
   - Set widgets: **TARGET_ENV** (e.g. `dev`), **LAYER** (e.g. `bronze`), **SCHEDULE_GROUP**, **REGISTRY_PATH** (path to the compiled JSON from step 1), **CONFIG_VERSION**, **STRICT_MODE**.
   - Run. The notebook calls `run_ingestion_group(..., layer=LAYER)` and processes all enabled objects in that schedule group and layer.

3. **Inspect results**:
   - Target Delta tables (e.g. `catalog.bronze.*`).
   - Ops tables: run log, DQ log (see **`docs/RUN_METRICS_FROM_EVENTS.md`**).

---

## 5. Next steps

- **Add a new source/object**: Edit or add a `configs/sources_*.yml` (and `connections.yml` if needed), then recompile the registry and run again.
- **Wire CI for prod**: **`pipelines/README.md`** and **`pipelines/deploy-bronze-prod.yml`** (variable group, bundle include, LAYER).
- **Silver / gold**: Same runner and job shape; use **LAYER** and layer-specific registries/targets; see **`docs/LAYER_EXTENSION_SCOPE.md`**.

If you hit a specific error (e.g. “missing target_table”, “no objects for schedule_group”), check preflight and config: **`target`/env schema**, **schedule_group**, and **layer** must match what the job passes.
