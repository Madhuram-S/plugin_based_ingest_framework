# Ingestion jobs: shards vs full parallelism (bronze / silver / gold)

**Detailed orchestration flow (component-level diagrams):** see **`docs/ORCHESTRATION_FLOW.md`** for Mermaid diagrams of CI → job → runner → plugins → layer tables.

Jobs and notebooks accept **LAYER** (`bronze` | `silver` | `gold`); default is `bronze`. Use the same generator and workflows for silver/gold by passing `--layer silver` or `--layer gold` and setting the LAYER notebook parameter. See **`docs/LAYER_EXTENSION_SCOPE.md`** for the full layer design.

---

## When to run `generate_bronze_sharded_job.py`

- **Do not** run it as part of **post-deployment** in a Databricks Asset Bundle. Post-deploy runs after the bundle is already deployed; the generator only writes files and doesn’t apply them, so it doesn’t fit there.
- **Do** either:
  1. **Dev/test – use static YAML** – Reference `workflow_bronze_parallel_by_object.yml` or `workflow_bronze_sharded.yml` in your bundle. Developers edit those files (or run the generator locally and commit) for dev/test.
  2. **Prod – run generator in CI (DevOps)** – Use the Azure DevOps pipeline **`pipelines/deploy-bronze-prod.yml`**: it generates the prod job definition from pipeline variables (e.g. `REGISTRY_PATH_PROD`, `CONCURRENCY`) then deploys the bundle. See **`pipelines/README.md`** for variable group, bundle include, and pipeline setup.

---

## Two modes

| Mode | Behavior | Use when |
|------|----------|----------|
| **Shards** | For-each over shard indices (e.g. 10 tasks). Within each task, objects run **sequentially**. | You want to limit cluster count; each shard processes ~N/10 objects one by one. |
| **Parallel by object** | One task **per object**: discover object_ids, then For-each over them. **No serial work** within a task. | You want maximum parallelism (10 shards + tasks within shards all parallel). |

Both avoid merge/update conflicts on Ops/DQ (each task has its own `run_id`; state/lock are per `object_id`).

---

## Full parallelism (one task per object)

**Recommended** when you want “10 shards and tasks within shards parallel too”: use **parallel by object** so every object runs in its own task.

1. **Static YAML**: `workflow_bronze_parallel_by_object.yml`
   - Task 1: `09_discover_object_ids` loads registry and sets task value `object_ids`.
   - Task 2: For-each over `{{tasks.discover_objects.values.object_ids}}` with `concurrency: 20`; each iteration runs `10_run_ingestion_group` with `OBJECT_ID={{input}}`.
2. Edit `REGISTRY_PATH`, `TARGET_ENV`, `SCHEDULE_GROUP`. Set `notebook_path` to your deployed paths.
3. Deploy with Databricks Asset Bundles.

**Generate** (optional):

```bash
python scripts/generate_bronze_sharded_job.py --mode parallel_by_object --concurrency 20 --out configs/workflow_bronze_parallel_generated.yml
```

---

## Shards only (serial within each shard)

1. **Static YAML**: `workflow_bronze_sharded.yml` — For-each over shard indices; each task runs the full group notebook with `SHARD_ID`/`SHARD_COUNT` (objects within that shard run sequentially).
2. **Generate**: `python scripts/generate_bronze_sharded_job.py --mode shards --shard_count 10 --out configs/workflow_bronze_sharded_generated.yml`

---

## Single-object / single-shard runs

- **One object**: Run `10_run_ingestion_group` with widget `OBJECT_ID` set to that object’s id.
- **One shard**: Run with `SHARD_ID` and `SHARD_COUNT` set (e.g. `SHARD_ID=0`, `SHARD_COUNT=10`).

---

## Optional: run generator in CI (pre-deploy)

If you want the job YAML to be generated in CI before each deploy:

```yaml
# .github/workflows/deploy.yml (example)
- name: Generate bronze job YAML
  run: |
    cd etl_ingest_framework
    python scripts/generate_bronze_sharded_job.py --mode parallel_by_object --concurrency 20 \
      --registry_path "${{ secrets.REGISTRY_PATH }}" \
      --out configs/workflow_bronze_parallel_generated.yml
- name: Deploy bundle
  run: databricks bundle deploy -t dev
```

Then in your bundle, reference `configs/workflow_bronze_parallel_generated.yml` (or include it) so deploy uses the generated file.
