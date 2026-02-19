# Ingestion orchestration – component-level flow (bronze / silver / gold)

Detailed diagrammatic view of how orchestration flows from CI through the runner to layer tables. The runner is **layer-aware**: each run is for one layer (bronze, silver, or gold). Objects are filtered by `layer`; state, lock, and ops tables share a `layer` column; the writer writes to `target_table` per layer.

---

## 1. End-to-end: from CI to job run

```mermaid
flowchart LR
  subgraph CI["CI (DevOps)"]
    A[Pipeline variables] --> B[generate_bronze_sharded_job.py]
    B --> C[workflow_bronze_prod_generated.yml]
    C --> D[databricks bundle deploy -t prod]
  end

  subgraph Databricks["Databricks workspace"]
    D --> E[Job definition]
    E --> F[Job run]
    F --> G[Tasks execute]
  end

  subgraph Bronze["Bronze layer"]
    G --> H[(Delta tables)]
  end
```

- **Dev/test**: Static YAML in repo; developer edits or runs generator locally → deploy.
- **Prod**: CI runs generator with pipeline variables → writes job YAML → bundle deploy uses it → job runs in workspace.

---

## 2. Job structure (parallel-by-object mode)

One job run: first task discovers object IDs; second task is a For-each that runs one ingestion task per object (up to `concurrency` in parallel).

```mermaid
flowchart TB
  subgraph Job["Databricks Job (bronze_ingestion_parallel_by_object)"]
    direction TB
    T1["Task 1: discover_objects<br/>(notebook 09_discover_object_ids)"]
    T2["Task 2: bronze_for_each_object<br/>(For-each over object_ids)"]

    T1 -->|"taskValues.set('object_ids', [id1, id2, ...])"| T2

    subgraph ForEach["For-each iterations (concurrency = 20)"]
      I1["run_one_object<br/>OBJECT_ID=id1"]
      I2["run_one_object<br/>OBJECT_ID=id2"]
      I3["run_one_object<br/>OBJECT_ID=id3"]
      IN["... run_one_object<br/>OBJECT_ID=idN"]
    end

    T2 --> I1
    T2 --> I2
    T2 --> I3
    T2 --> IN
  end

  I1 --> N1[Notebook 10_run_ingestion_group]
  I2 --> N2[Notebook 10_run_ingestion_group]
  I3 --> N3[Notebook 10_run_ingestion_group]
  IN --> NN[Notebook 10_run_ingestion_group]
```

Each iteration runs the same notebook with a different `OBJECT_ID` (one object per task = full parallelism).

---

## 3. Job structure (shards mode)

One job run: one For-each over shard indices; each iteration runs the group notebook for that shard (objects inside the task run sequentially).

```mermaid
flowchart TB
  subgraph Job["Databricks Job (bronze_ingestion_sharded)"]
    subgraph ForEach["For-each over shard indices [0..9]"]
      S0["run_ingestion_shard<br/>SHARD_ID=0, SHARD_COUNT=10"]
      S1["run_ingestion_shard<br/>SHARD_ID=1"]
      S9["run_ingestion_shard<br/>SHARD_ID=9"]
    end
  end

  S0 --> N0[Notebook 10_run_ingestion_group<br/>~N/10 objects sequentially]
  S1 --> N1[Notebook 10_run_ingestion_group]
  S9 --> N9[Notebook 10_run_ingestion_group]
```

---

## 4. Component flow inside the runner (one task)

When a task runs (either one object or one shard’s objects), the notebook calls `run_ingestion_group`. High-level component flow:

```mermaid
flowchart TB
  subgraph Notebook["Notebook 10_run_ingestion_group"]
    W[Widgets: TARGET_ENV, SCHEDULE_GROUP, REGISTRY_PATH,<br/>OBJECT_ID or SHARD_ID/SHARD_COUNT]
    W --> R[run_ingestion_group()]
  end

  subgraph Runner["Runner (run_group.py)"]
    R --> R1[RegistryReader.load_compiled_registry]
    R1 --> R2[RunContext + StateStore, OpsLogger,<br/>BronzeWriter, DQExecutor, ObjectLock]
    R2 --> R3[build_plugin_registry: BigQuery, API, File]
    R3 --> R4[Select objects: schedule_group<br/>+ object_ids or shard filter]
    R4 --> R5[Preflight.validate_object_config]
    R5 --> R6["For each approved object:<br/>_run_one_object(...)"]
  end

  R6 --> RunOne[See diagram 5]
```

---

## 5. Per-object flow: _run_one_object (detailed)

For each approved object in the task, the runner does the following. Ops/DQ/state/lock are shared services; the plugin and writer do the actual extract and write.

```mermaid
sequenceDiagram
  participant Runner
  participant OpsLogger
  participant ObjectLock
  participant StateStore
  participant TokenRuntime
  participant SecretsRuntime
  participant Plugin
  participant BronzeWriter
  participant DQExecutor

  Runner->>OpsLogger: log_run_start(run_id, object_id, ...)
  Runner->>ObjectLock: try_acquire(object_id, run_id)
  alt Lock not acquired
    Runner->>OpsLogger: log_run_skipped
    Runner-->>Runner: return
  end

  Runner->>StateStore: get_state(object_id)
  StateStore-->>Runner: prior (last_watermark, last_cursor)
  Runner->>TokenRuntime: resolve_runtime_tokens(obj, runtime_ctx)
  Runner->>SecretsRuntime: resolve_secrets_in_config(obj, dbutils)
  Runner->>Plugin: extract(run_ctx, obj_resolved, prior_state)

  alt status == SKIPPED
    Runner->>OpsLogger: log_run_skipped
    Runner->>ObjectLock: release
  end

  Plugin-->>Runner: IngestResult(df, next_state, ...)
  Runner->>BronzeWriter: with_audit_cols(df, ...)
  Runner->>BronzeWriter: write(df, bronze_table, mode, primary_key)
  BronzeWriter-->>Runner: rows_written
  Runner->>DQExecutor: run(run_ctx, object_id, obj_resolved)
  Runner->>StateStore: update_on_success(object_id, last_watermark, last_cursor)
  Runner->>OpsLogger: log_run_success(...)
  Runner->>ObjectLock: release(object_id, run_id)

  Note over Runner,ObjectLock: On exception: log_run_failure, then release lock
```

---

## 6. Data and control flow (component view)

Where data lives and how it moves between components.

```mermaid
flowchart TB
  subgraph External["External / config"]
    REG[(compiled_registry.json)]
    SEC[Databricks secrets]
  end

  subgraph Runner["Runner"]
    RC[RunContext]
    RR[RegistryReader]
    PF[Preflight]
    PK[PluginRegistry]
    TS[TokenRuntime]
    SR[SecretsRuntime]
  end

  subgraph Services["Shared services (Delta tables in UC)"]
    SS[(StateStore<br/>ctl_ingestion_state)]
    OPS[(OpsLogger<br/>ops_run_log, ops_dq_log)]
    LOCK[(ObjectLock<br/>ops_object_lock)]
  end

  subgraph Plugins["Plugins (extract only)"]
    BQ[BigQueryPlugin]
    API[ApiPlugin]
    FILE[FilePlugin]
  end

  subgraph Write["Write path"]
    BW[BronzeWriter]
    DQ[DQExecutor]
    BR[(Bronze Delta tables)]
  end

  REG --> RR
  RR --> RC
  RC --> PF
  PF --> TS
  SEC --> SR
  TS --> SR
  SR --> PK
  PK --> BQ
  PK --> API
  PK --> FILE
  BQ --> BW
  API --> BW
  FILE --> BW
  RC --> OPS
  RC --> SS
  RC --> LOCK
  SS --> Plugins
  BW --> BR
  BW --> DQ
  DQ --> OPS
```

---

## 7. Summary table

| Layer | Component | Responsibility |
|-------|-----------|----------------|
| **CI** | Pipeline + generator | Produces job YAML from variables; triggers bundle deploy. |
| **Job** | discover_objects task | Loads registry, sets task value `object_ids` (parallel-by-object only). |
| **Job** | For-each task | Runs N copies of run notebook (by object_id or by shard_id). |
| **Notebook** | 10_run_ingestion_group | Gets widgets, calls `run_ingestion_group`. |
| **Runner** | run_ingestion_group | Loads registry, builds services, selects objects, preflight, loops _run_one_object. |
| **Runner** | _run_one_object | Lock → state → resolve → plugin.extract → write → DQ → state update → ops → release. |
| **Services** | StateStore, OpsLogger, ObjectLock | Delta tables in UC (ops schema); per object_id or run_id. |
| **Plugin** | BigQuery / API / File | Extract only: return DataFrame + next_state. |
| **Writer** | BronzeWriter | Audit columns, append/merge/overwrite to bronze Delta. |
| **DQ** | DQExecutor | Bronze checks (row_count_min, not_null, duplicates); logs to ops_dq_log. |

These diagrams can be rendered in GitHub, Azure DevOps (Wiki or pipeline summary), or any Markdown viewer that supports Mermaid (e.g. VS Code with Mermaid extension).
