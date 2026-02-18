# Run metrics from append-only run log

The run log is an **event stream** (START, END, SKIP). END/SKIP rows **include metadata** (env, schedule_group, source_system, etc.) when the runner passes it, so they are self-contained. For legacy data or a single canonical row per (run_id, object_id), use the **canonical view** (below).

You can run a **metrics process** at the end of the pipeline (or on a schedule) to aggregate these events into run metrics.

---

## 0. Canonical view (coalesce START onto END rows)

If END rows were written without metadata, or you want a single row per (run_id, object_id) with all fields filled, create the canonical view. It joins END/SKIP to START and uses COALESCE so metadata comes from START when END has NULLs.

```python
from src.ingestion.runner.ops_logger import OpsLogger
# After OpsLogger(spark, run_log_table, dq_log_table) has been used at least once:
sql = OpsLogger.canonical_run_log_view_sql("catalog.schema.ops_run_log", "catalog.schema.run_log_canonical")
spark.sql(sql)
```

Or run the SQL directly (replace table and view names):

```sql
CREATE OR REPLACE VIEW catalog.schema.run_log_canonical AS
SELECT
  e.run_id, e.object_id, COALESCE(e.layer, s.layer) AS layer, e.event_type, e.event_ts, e.status,
  COALESCE(e.env, s.env) AS env,
  COALESCE(e.config_version, s.config_version) AS config_version,
  COALESCE(e.registry_sha256, s.registry_sha256) AS registry_sha256,
  COALESCE(e.source_system, s.source_system) AS source_system,
  COALESCE(e.source_type, s.source_type) AS source_type,
  COALESCE(e.object_name, s.object_name) AS object_name,
  COALESCE(e.target_table, s.target_table, e.bronze_table, s.bronze_table) AS target_table,
  COALESCE(e.bronze_table, s.bronze_table) AS bronze_table,
  COALESCE(e.schedule_group, s.schedule_group) AS schedule_group,
  COALESCE(e.shard_id, s.shard_id) AS shard_id,
  COALESCE(e.shard_count, s.shard_count) AS shard_count,
  s.start_ts, e.end_ts, e.rows_read, e.rows_written,
  e.warnings_json, e.error, e.extra_json
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY run_id, object_id ORDER BY event_ts DESC) AS rn
  FROM catalog.schema.ops_run_log
  WHERE event_type IN ('END', 'SKIP')
) e
LEFT JOIN (
  SELECT run_id, object_id, layer, env, config_version, registry_sha256,
         source_system, source_type, object_name, target_table, bronze_table, schedule_group,
         shard_id, shard_count, start_ts
  FROM catalog.schema.ops_run_log
  WHERE event_type = 'START'
) s ON e.run_id = s.run_id AND e.object_id = s.object_id
WHERE e.rn = 1;
```

You can filter by **layer** (bronze, silver, gold) in metrics queries, e.g. `WHERE layer = 'bronze'`.

---

## 1. Per ingestion run (run_id)

Each task has its own `run_id`. To get metrics for a single ingestion run, use terminal events (END, SKIP) for that `run_id`:

```sql
-- Run metrics for a given run_id (one task's run)
SELECT
  run_id,
  COUNT(*) AS objects_processed,
  COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count,
  COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
  COUNT(CASE WHEN status = 'SKIPPED' THEN 1 END) AS skipped_count,
  COALESCE(SUM(rows_read), 0)   AS total_rows_read,
  COALESCE(SUM(rows_written), 0) AS total_rows_written,
  MIN(event_ts) AS first_event_ts,
  MAX(event_ts) AS last_event_ts
FROM run_log
WHERE run_id = '<run_id>'
  AND event_type IN ('END', 'SKIP')
GROUP BY run_id
```

---

## 2. Latest status per object (for a run_id)

```sql
-- One row per (run_id, object_id) with final status
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY run_id, object_id ORDER BY event_ts DESC) AS rn
  FROM run_log
  WHERE run_id = '<run_id>'
) t
WHERE rn = 1
```

---

## 3. Pipeline-level metrics (all tasks in a time window or schedule_group)

If you want metrics for a **whole pipeline** (one Databricks job run with many tasks), you have two options:

**Option A – By time window**  
Assume the pipeline run takes a few minutes; aggregate all events in that window (e.g. last 15 minutes) and group by `schedule_group` and/or `run_id`:

```sql
-- Metrics by schedule_group for recent runs (e.g. last 15 minutes)
SELECT
  schedule_group,
  COUNT(DISTINCT run_id) AS task_runs,
  COUNT(CASE WHEN event_type IN ('END','SKIP') AND status = 'SUCCESS' THEN 1 END) AS success_count,
  COUNT(CASE WHEN event_type IN ('END','SKIP') AND status = 'FAILED' THEN 1 END) AS failed_count,
  COUNT(CASE WHEN event_type IN ('END','SKIP') AND status = 'SKIPPED' THEN 1 END) AS skipped_count,
  COALESCE(SUM(rows_written), 0) AS total_rows_written
FROM run_log
WHERE event_ts >= current_timestamp() - INTERVAL 15 MINUTES
  AND event_type IN ('END', 'SKIP')
GROUP BY schedule_group
```

**Option B – By job run id (recommended for “end of pipeline”)**
- Pass the **Databricks job run id** (e.g. `{{job.run_id}}`) into the ingestion notebook as a widget (e.g. `JOB_RUN_ID`).
- Log it in `log_run_start` (add to the row dict and to the run_log schema if needed).
- At the end of the pipeline, a **final task** (or a separate job) queries `run_log` filtered by `job_run_id` and computes:
  - distinct `run_id` count (number of tasks)
  - counts by status (SUCCESS / FAILED / SKIPPED)
  - sum of rows_read, rows_written
  - min/max event_ts for duration

Then “run metrics at the end of the pipeline” = one query filtered by `job_run_id`.

---

## 4. Example: metrics as a view (per run_id)

You can expose per-run metrics as a view so downstream dashboards or alerts don’t need to repeat the aggregation:

```sql
CREATE OR REPLACE VIEW run_metrics AS
SELECT
  run_id,
  MAX(env) AS env,
  MAX(schedule_group) AS schedule_group,
  COUNT(CASE WHEN event_type IN ('END','SKIP') AND status = 'SUCCESS' THEN 1 END) AS success_count,
  COUNT(CASE WHEN event_type IN ('END','SKIP') AND status = 'FAILED' THEN 1 END) AS failed_count,
  COUNT(CASE WHEN event_type IN ('END','SKIP') AND status = 'SKIPPED' THEN 1 END) AS skipped_count,
  COALESCE(SUM(rows_read), 0)   AS total_rows_read,
  COALESCE(SUM(rows_written), 0) AS total_rows_written,
  MIN(CASE WHEN event_type = 'START' THEN event_ts END) AS run_start_ts,
  MAX(CASE WHEN event_type IN ('END','SKIP') THEN event_ts END) AS run_end_ts
FROM run_log
WHERE event_type IN ('START', 'END', 'SKIP')
GROUP BY run_id
```

---

## Summary

| Goal | Approach |
|------|----------|
| Metrics for one task’s run | Filter by `run_id`, use END/SKIP events, aggregate (counts, sums). |
| “One row per object with final status” | Latest event per `(run_id, object_id)` (see ORCHESTRATION_FLOW / ops_logger comment). |
| Metrics at end of pipeline | Use a time window, or add `job_run_id` to events and run a final step that aggregates by `job_run_id`. |
| Reusable metrics | Create a view (e.g. `run_metrics`) over the event log as above. |

Yes — with the append-only event approach you can create a process (notebook, final job task, or scheduled job) that calculates run metrics at the end of the pipeline using the run_log events.
