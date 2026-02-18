# CI/CD pipelines (DevOps)

## Prod: generate job definition and deploy

The pipeline **`deploy-bronze-prod.yml`** runs in Azure DevOps and:

1. **Generates** the bronze ingestion job definition for prod (concurrency, registry path, etc. from variables).
2. **Deploys** the Databricks Asset Bundle with target `prod`.

So prod job flow is created in CI; developers do not edit prod YAML by hand.

### 1. Variable group (recommended)

Create a variable group in Azure DevOps, e.g. **`bronze-prod`**, and set:

| Variable | Example | Description |
|----------|---------|-------------|
| `REGISTRY_PATH_PROD` | `abfss://config@myaccount.dfs.core.windows.net/compiled/prod/latest/compiled_registry.json` | Compiled registry path for prod. |
| `CONCURRENCY` | `20` | Max parallel object tasks (parallel_by_object mode). |
| `SCHEDULE_GROUP` | `P0_bigquery` | Schedule group to run. |
| `CONFIG_VERSION_PROD` | `$(Build.BuildId)` or a version string | Config version for run log. |
| `STRICT_MODE_PROD` | `true` | Strict validation in prod. |
| `DATABRICKS_HOST` | `https://adb-xxx.azuredatabricks.net` | (Secret) Databricks workspace URL. |
| `DATABRICKS_TOKEN` | `dapi...` | (Secret) Databricks PAT or token. |

In **`deploy-bronze-prod.yml`**, uncomment and set:

```yaml
variables:
  - group: bronze-prod
```

so the pipeline uses this group (and optionally keep non-secret defaults in the pipeline YAML).

### 2. Bundle must use the generated file for prod

The generator writes to **`etl_ingest_framework/configs/workflow_bronze_prod_generated.yml`** (or the path in `GENERATED_JOB_PATH`). Your Databricks Asset Bundle must **include** this file when deploying to prod.

**Option A – Include in bundle root YAML**

In your main bundle file (e.g. `databricks.yml` or `bundle.yml`), for the prod target include the generated job:

```yaml
# databricks.yml (or your bundle root)
targets:
  prod:
    mode: production
    # Include the job definition generated in CI.
    include:
      - etl_ingest_framework/configs/workflow_bronze_prod_generated.yml
```

**Option B – Single bundle file that references the generated path**

If your bundle is a single file, you can have the prod target point at the generated file as the only or extra resources file so the prod deploy picks it up.

### 3. Add the pipeline in Azure DevOps

1. **Pipelines** → **New pipeline** → **Azure Repos Git** (or GitHub, etc.) → select repo.
2. **Existing Azure Pipelines YAML file** → choose `etl_ingest_framework/pipelines/deploy-bronze-prod.yml` (or the path where you placed it).
3. Save (and optionally set the variable group and any overrides).
4. Run; fix `GENERATED_JOB_PATH` or working directory if your repo layout differs.

### 4. Shards mode for prod

To use **shards** instead of **parallel_by_object**, set in the variable group or pipeline:

- `MODE: shards`
- `SHARD_COUNT: "10"` (and remove or ignore `CONCURRENCY` for this mode)

Then extend the **Generate bronze job definition** step to pass `--mode shards --shard_count $(SHARD_COUNT)` when `MODE == shards` (or add a separate pipeline for shards).
