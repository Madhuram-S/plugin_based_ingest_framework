# BigQuery Ingestion Configuration Guide

This guide explains how to configure BigQuery data sources for ingestion using the `sources_bigquery_ingestion.yml` file.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Connection Setup](#connection-setup)
- [Configuration Reference](#configuration-reference)
- [Configuration Examples](#configuration-examples)
- [Validation Rules](#validation-rules)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

### 1. BigQuery Connector
- The Databricks cluster must have the BigQuery connector installed
- Add the connector via cluster libraries or init scripts
- Maven coordinates: `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:<version>`
- Alternatively, install via pip: `pip install google-cloud-bigquery google-cloud-storage`

### 2. GCP Service Account
- Create a GCP service account with appropriate BigQuery permissions:
  - `BigQuery Read Session User`
  - `BigQuery Data Viewer`
  - For queries with materialization: `BigQuery User`

### 3. Authentication Credentials
Choose one of two authentication methods:
- **service_account_json**: Service account JSON key file
- **service_account_base64**: Base64-encoded service account key stored in Databricks secrets

### 4. Databricks Configuration
- Configure secrets for authentication credentials
- Ensure appropriate ADLS/DBFS access for landing zone
- Configure environment variables referenced in configs

---

## Connection Setup

### Step 1: Configure `connections.yml`

Define your BigQuery connection in `configs/connections.yml`:

```yaml
connections:
  bigquery:
    type: bigquery
    auth:
      mode: service_account_json        # OR service_account_base64
      secret_ref:
        # For service_account_json mode:
        path: "${env.sa_gcp_json}"      # Path to JSON file (DBFS or ABFSS)
        
        # For service_account_base64 mode:
        scope: "${env.secret_scope}"    # Databricks secret scope
        key: "sa_gcp_key"              # Databricks secret key
```

#### Authentication Mode Details

**Option 1: service_account_json**
- Store the service account JSON file in DBFS or ABFSS
- Provide the path in {dev,uat,life}.yml `env.sa_gcp_json`
- Path examples:
  - DBFS: `dbfs:/mnt/configs/sa_gcp_json.json`
  - ABFSS: `abfss://container@storage.dfs.core.windows.net/configs/sa_gcp_json.json`

**Option 2: service_account_base64**
- Base64-encode your service account JSON credentials
- Store in Azure Key Vault secrets
- Provide `secret_scope` in {dev,uat,life}.yml and `secret_ref.key` in connections.yml

### Step 2: Reference Connection in Source Config

In your `sources_bigquery_ingestion.yml`, reference the connection:

```yaml
sources:
  - source_system: system_name
    connection: bigquery  # Must match connection name in connections.yml
    enabled: true
    # ... rest of configuration
```

---

## Configuration Reference

### Top-Level Source Configuration

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `source_system` | Yes | String | Identifier for the source system (e.g., "epic", "salesforce") |
| `source_type` | Yes | String | Type of source (use "table" for BigQuery) |
| `connection` | Yes | String | Connection name from `connections.yml` |
| `enabled` | Yes | Boolean | Whether this source is active |
| `priority` | No | String | Priority level (e.g., "P0", "P1") |
| `defaults` | Yes | Object | Default settings applied to all objects |
| `objects` | Yes | Array | List of tables/queries to ingest |

### Defaults Configuration

#### `source` (Required)

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `project_id` | **Yes** | String | GCP project ID |
| `dataset` | **Yes** | String | BigQuery dataset name |
| `schema` | **Yes** | String | BigQuery schema name |

#### `load` (Required)

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `ingest_mode` | **Yes** | String | Ingestion strategy: `full` or `incremental` |
| `watermark_column` | Conditional* | String | Column used for incremental tracking (e.g., "last_modified_date") |
| `initial_value` | Conditional** | String/Int | Watermark column value for incremental run |

\* Required if `ingest_mode: incremental`  
\*\* Required if `ingest_mode: incremental` and no prior state exists

#### `target` (Required)

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `catalog` | Yes | String | Target Unity Catalog or database |
| `schema` | Yes | String | Target schema/database name |
| `table_prefix` | No | String | Prefix for all target table names |

#### `landing` (Optional)

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `format` | No | String | File format for landing zone (default: "parquet") |
| `options` | No | Object | Format-specific options |
| `path` | No | String | Custom landing path (auto-derived if omitted) |

#### `write` (Required)

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `mode` | Yes | String | Write mode: `append`, `overwrite`, or `upsert` |
| `primary_key` | Conditional* | Array | Primary key columns (required for upsert) |

\* Required if `mode: upsert`

#### `dq` (Optional)

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `ruleset` | No | String | Data quality ruleset name from `dq_rulesets.yml` |
| `action_on_fail` | No | String | Action when DQ fails: `warn` or `fail` |

#### `schedule` (Optional)

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `enabled` | No | Boolean | Enable scheduled execution |
| `group` | No | String | Run group identifier |
| `cron` | No | String | Cron expression for schedule |
| `timezone` | No | String | Timezone for schedule |

### Object Configuration

Each object in the `objects` array can override defaults and must specify:

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `name` | **Yes** | String | Unique object identifier (also used as table name by default) |
| `source.table` | Optional* | String | BigQuery table name (use if different from object `name`) |
| `source.query` | Optional* | String | Custom SQL query instead of table |
| `source.tmp_vw` | Optional** | String | Temporary dataset name for query materialization |

\* Provide either `table` OR `query`, not both  
\*\* Only required when using `query` (defaults to `temp_<timestamp>`)

#### Table-Based vs Query-Based Ingestion

**Table-Based Ingestion:**
```yaml
objects:
  - name: shakespeare
    source:
      table: "shakespeare"  # Just specify the table name
```

**Query-Based Ingestion:**
```yaml
objects:
  - name: shakespeare_filtered
    source:
      query: "SELECT * FROM `project.dataset.shakespeare` WHERE word_count > 10"
      tmp_vw: "temp_shakespeare"  # Temporary dataset for materialization
```

---

## Configuration Examples

### Example 1: Full Load from Table

```yaml
sources:
  - source_system: epic
    source_type: table
    connection: bigquery
    enabled: true

    defaults:
      source:
        project_id: "my-gcp-project"
        dataset: "insurance"
        schema: "life"

      target:
        catalog: "bronze"
        schema: "epic"

      load:
        ingest_mode: full
        watermark_column: NULL
        initial_value: NULL

      write:
        mode: overwrite
        primary_key: []

      dq:
        ruleset: bronze_minimal
        action_on_fail: warn

    objects:
      - name: accounts
        source:
          table: "glAccounts"
```

**Behavior:**
- Extracts all data from `insurance.life.glAccounts`
- Overwrites the target table on each run
- No incremental tracking

### Example 2: Incremental Load with Watermark

```yaml
sources:
  - source_system: salesforce
    source_type: table
    connection: bigquery
    enabled: true

    defaults:
      source:
        project_id: "my-gcp-project"
        dataset: "crm"
        schema: "public"

      target:
        catalog: "bronze"
        schema: "salesforce"

      load:
        ingest_mode: incremental
        watermark_column: "SystemModstamp"
        initial_value: "2024-01-01T00:00:00Z"

      write:
        mode: append
        primary_key: []

    objects:
      - name: accounts
        source:
          table: "Account"
```

**Behavior:**
- **First run**: Extracts records where `SystemModstamp > '2024-01-01T00:00:00Z'`
- **Subsequent runs**: Uses the max watermark value from previous run
- Appends new/updated records to target table
- Stores watermark in state for next run

### Example 3: Query-Based Ingestion

```yaml
sources:
  - source_system: analytics
    source_type: table
    connection: bigquery
    enabled: true

    defaults:
      source:
        project_id: "my-gcp-project"
        dataset: "analytics"
        schema: "reports"

      target:
        catalog: "bronze"
        schema: "analytics"

      load:
        ingest_mode: full
        watermark_column: NULL
        initial_value: NULL

      write:
        mode: overwrite
        primary_key: []

    objects:
      - name: daily_summary
        source:
          query: |
            SELECT 
              date,
              SUM(revenue) as total_revenue,
              COUNT(DISTINCT customer_id) as unique_customers
            FROM `my-gcp-project.analytics.sales`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY date
          tmp_vw: "temp_daily_summary"
```

**Behavior:**
- Executes custom SQL query in BigQuery
- Uses temporary dataset `temp_daily_summary` for materialization
- Useful for complex transformations or joins at source

### Example 4: Multiple Objects with Overrides

```yaml
sources:
  - source_system: ecommerce
    source_type: table
    connection: bigquery
    enabled: true

    defaults:
      source:
        project_id: "ecommerce-life"
        dataset: "transactions"
        schema: "public"

      target:
        catalog: "bronze"
        schema: "ecommerce"

      load:
        ingest_mode: full
        watermark_column: NULL
        initial_value: NULL

      write:
        mode: overwrite
        primary_key: []

    objects:
      - name: orders
        source:
          table: "orders"
        # Uses defaults (full load, overwrite)

      - name: order_items
        source:
          table: "order_items"
        load:
          ingest_mode: incremental
          watermark_column: "updated_at"
          initial_value: "2024-01-01 00:00:00"
        write:
          mode: append
        # Overrides: incremental load with append

      - name: inventory_snapshot
        source:
          query: "SELECT * FROM `ecommerce-life.inventory.current_stock` WHERE quantity > 0"
          tmp_vw: "temp_inventory"
        # Uses defaults for load/write, custom query
```

---

## Validation Rules

The BigQuery plugin validates your configuration and will fail with specific error messages:

### Connection Validation

| Error | Cause | Solution |
|-------|-------|----------|
| `"BigQuery service account json file path is required for service_account_json auth mode."` | `auth.mode = service_account_json` but no `path` specified | Add `env.sa_gcp_json` in {dev,uat,life}.yml | 
| `"Secret scope and key are required for service_account_base64 auth mode for BigQuery."` | `auth.mode = service_account_base64` but `scope` or `key` missing | Add both `secret_ref.scope` and `secret_ref.key` in `connections.yml` |
| `"Supported BigQuery authentication is either service_account_json or service_account_base64."` | Invalid `auth.mode` value | Use `service_account_json` or `service_account_base64` |

### Source Validation

| Error | Cause | Solution |
|-------|-------|----------|
| `"Project ID and dataset are required for BigQuery source."` | Missing `source.project_id` or `source.dataset` | Add both fields to `defaults.source` or object-level `source` |
| `"BigQuery connector is not available on the compute."` | Spark BigQuery connector not installed on cluster | Install BigQuery connector library on your Databricks cluster |

### Load Validation

| Error | Cause | Solution |
|-------|-------|----------|
| `"Load ingest_mode must be either full or incremental."` | Invalid `ingest_mode` value | Use `full` or `incremental` |
| `"Watermark column is required for incremental load."` | `ingest_mode = incremental` but no `watermark_column` | Add `load.watermark_column` field |
| `"Incremental mode requires initial_value or prior_state.last_watermark"` | First incremental run without `initial_value` | Add `load.initial_value` to bootstrap incremental loading |

---

## Troubleshooting

### Common Issues

#### 1. Authentication Failures

**Problem:** `Credentials not found` or authentication errors

**Solutions:**
- **service_account_json mode:**
  - Verify the JSON file exists at the specified path
  - Ensure the path is accessible from Databricks 
  - Check file permissions
  
- **service_account_base64 mode:**
  - Verify secret exists: `dbutils.secrets.get(scope="<scope>", key="<key>")`
  - Ensure the base64 encoding is correct
  - Verify secret scope permissions

#### 2. BigQuery Connector Not Found

**Problem:** `ModuleNotFoundError: BigQuery connector is not available on the compute.`

**Solution:**
- Install the BigQuery connector on your cluster using one of the following methods:

**Option 1: Maven**
  1. Go to Cluster → Libraries → Install New
  2. Select Maven
  3. Coordinates: `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2`
  4. Restart cluster

**Option 2: Pip**
  1. Go to Cluster → Libraries → Install New
  2. Select PyPI
  3. Package: `google-cloud-bigquery google-cloud-storage`
  4. Restart cluster

#### 3. Incremental Load Not Progressing

**Problem:** Incremental load extracts same data every run

**Potential Causes:**
- State is not being persisted between runs
- `watermark_column` data type mismatch
- Watermark column has NULL values

**Solutions:**
- Verify state store is configured correctly
- Ensure `watermark_column` is indexed or sortable
- Use `COALESCE` in query for columns with NULLs
- Check `initial_value` matches the column data type (INT or STRING)

#### 4. Query Materialization Failures

**Problem:** Errors when using `query` mode

**Solutions:**
- Verify the service account has `bigquery.tables.create` permission
- Ensure `tmp_vw` dataset exists or service account can create it
- Use unique `tmp_vw` names to avoid conflicts
- Fully qualify table references in query: `` `project.dataset.table` ``

#### 5. Performance Issues

**Problem:** Slow extraction from large tables

**Solutions:**
- For large full loads, consider partitioning your ingestion
- Use incremental mode with appropriate watermark
- Apply filters in `query` to reduce data volume at source
- Increase cluster size or enable autoscaling
- Review BigQuery query execution plan for optimization

#### 6. Retry Logic

**Behavior:**
- The plugin automatically retries failed operations (default: 3 retries)
- Uses exponential backoff: 5s, 10s, 20s (max 60s)
- **Non-retryable errors** (fail immediately):
  - `ModuleNotFoundError` (connector missing)
  - `ValueError` (configuration errors)
  - `KeyError` (missing required fields)
  - `Py4JJavaError` (Spark/Java errors)

**Custom Retry Configuration:**
```yaml
objects:
  - name: my_table
    source:
      table: "my_table"
    ingestion:
      max_retries: 5
      retry_backoff_seconds: 10
```

---

## Advanced Topics

### State Management

For incremental loads, the framework maintains state containing the last watermark value:

```python
# State structure
{
  "last_watermark": "2024-02-18T10:30:00Z"
}
```

**How it works:**
1. **First Run:** Uses `initial_value` as the starting point
2. **Subsequent Runs:** Queries state for `last_watermark`
3. **After Extraction:** Computes max watermark from extracted batch
4. **State Update:** Persists new watermark only if data was extracted

**Important:** If `initial_value` is omitted and no prior state exists, the job will fail.

### Environment Variables

The configuration supports environment variable interpolation:

```yaml
target:
  catalog: "${env.environment}_bronze"  # Resolves to "dev_bronze" or "life_bronze"

source:
  project_id: "${env.gcp_project_id}"   # Resolves from environment
```

Common environment variables:
- `${env.landing_base_path}` - Base path for landing zone
- `${env.secret_scope}` - Databricks secret scope
- `${env.sa_gcp_json}` - Path to GCP service account JSON
- `${env.environment}` - Environment name (dev/staging/life)

### Data Quality Integration

Configure data quality checks using rulesets:

```yaml
dq:
  ruleset: bronze_minimal      # References dq_rulesets.yml
  action_on_fail: warn         # "warn" or "fail"
```

When configured:
- DQ rules execute after ingestion
- `warn`: Logs issues but continues pipeline
- `fail`: Stops pipeline on DQ violations

Define rulesets in `configs/dq_rulesets.yml`.

---

## Quick Reference Checklist

Before deploying a new BigQuery source:

- [ ] BigQuery connector installed on cluster
- [ ] GCP service account created with necessary permissions
- [ ] Authentication credentials configured in `connections.yml`
- [ ] Connection name matches between files
- [ ] `project_id` and `dataset` specified
- [ ] `ingest_mode` selected: `full` or `incremental`
- [ ] If incremental: `watermark_column` and `initial_value` provided
- [ ] Target `catalog` and `schema` specified
- [ ] `write.mode` chosen: `append`, `overwrite`, or `upsert`
- [ ] If upsert: `primary_key` defined
- [ ] Object `name` unique within source system
- [ ] Either `table` OR `query` specified (not both)
- [ ] If query: `tmp_vw` specified for materialization
- [ ] Configuration validated (no syntax errors)
- [ ] Test run with `enabled: true`

---

## Additional Resources

- **BigQuery Spark Connector Documentation:** https://github.com/GoogleCloudDataproc/spark-bigquery-connector
- **BigQuery Plugin Implementation:** See `src/ingestion/plugins/bigquery_plugin.py`
- **Sample Configuration:** See `configs/source_bigquery_ingestion.yml`

---

**Document Version:** 1.0  
**Last Updated:** February 2026  
**Created By:** Andrea Giralao 
