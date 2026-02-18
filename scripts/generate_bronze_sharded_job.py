#!/usr/bin/env python3
"""
Generate a Databricks job definition for bronze ingestion.

Modes:
  shards              For-each over shard indices (e.g. 10 tasks). Objects within each shard run sequentially.
  parallel_by_object  Discover object_ids then For-each over objects. One task per object = full parallelism.

Usage:
  # Shards (10 parallel shards, serial within each)
  python generate_bronze_sharded_job.py --mode shards --shard_count 10 --out configs/workflow_bronze_sharded_generated.yml

  # Full parallelism (one task per object, concurrency 20)
  python generate_bronze_sharded_job.py --mode parallel_by_object --concurrency 20 --out configs/workflow_bronze_parallel_generated.yml
"""
import argparse
import json
from pathlib import Path


def build_inputs_array(shard_count: int) -> str:
    """JSON array of shard indices [0, 1, ..., shard_count-1]."""
    return json.dumps(list(range(shard_count)))


def build_bundle_yaml(
    shard_count: int,
    notebook_path: str = "../notebooks/10_run_ingestion_group",
    target_env: str = "dev",
    layer: str = "bronze",
    schedule_group: str = "P0_bigquery",
    registry_path: str = "abfss://config@<storage>.dfs.core.windows.net/compiled/dev/users/<alias>/latest/compiled_registry.json",
    config_version: str = "unknown",
    strict_mode: str = "false",
    node_type_id: str = "i3.xlarge",
    spark_version: str = "13.3.x-scala2.12",
) -> str:
    inputs = build_inputs_array(shard_count)
    layer = layer or "bronze"
    return f"""# Generated ingestion job (For-each shards). Edit or re-run generate_bronze_sharded_job.py to change shard_count or params.
resources:
  jobs:
    bronze_ingestion_sharded:
      name: bronze_ingestion_sharded
      max_concurrent_runs: 1
      job_clusters:
        - job_cluster_key: job_cluster
          new_cluster:
            node_type_id: {node_type_id}
            spark_version: {spark_version}
            num_workers: 1
      tasks:
        - task_key: bronze_for_each_shards
          for_each_task:
            inputs: "{inputs}"
            concurrency: {shard_count}
            task:
              task_key: run_ingestion_shard
              job_cluster_key: job_cluster
              notebook_task:
                notebook_path: {notebook_path}
                base_parameters:
                  TARGET_ENV: "{target_env}"
                  LAYER: "{layer}"
                  SCHEDULE_GROUP: "{schedule_group}"
                  REGISTRY_PATH: "{registry_path}"
                  CONFIG_VERSION: "{config_version}"
                  STRICT_MODE: "{strict_mode}"
                  SHARD_ID: "{{{{input}}}}"
                  SHARD_COUNT: "{shard_count}"
"""


def build_jobs_api_json(
    shard_count: int,
    notebook_path: str,
    target_env: str,
    schedule_group: str,
    registry_path: str,
    config_version: str,
    strict_mode: str,
    layer: str = "bronze",
) -> dict:
    """Build job payload for Jobs API 2.1 (e.g. for databricks jobs create --json-file)."""
    inputs = build_inputs_array(shard_count)
    layer = layer or "bronze"
    return {
        "name": "bronze_ingestion_sharded",
        "max_concurrent_runs": 1,
        "job_clusters": [
            {
                "job_cluster_key": "job_cluster",
                "new_cluster": {
                    "node_type_id": "i3.xlarge",
                    "spark_version": "13.3.x-scala2.12",
                    "num_workers": 1,
                },
            }
        ],
        "tasks": [
            {
                "task_key": "bronze_for_each_shards",
                "for_each_task": {
                    "inputs": inputs,
                    "concurrency": shard_count,
                    "task": {
                        "task_key": "run_ingestion_shard",
                        "job_cluster_key": "job_cluster",
                        "notebook_task": {
                            "notebook_path": notebook_path,
                            "base_parameters": {
                                "TARGET_ENV": target_env,
                                "LAYER": layer,
                                "SCHEDULE_GROUP": schedule_group,
                                "REGISTRY_PATH": registry_path,
                                "CONFIG_VERSION": config_version,
                                "STRICT_MODE": strict_mode,
                                "SHARD_ID": "{{input}}",
                                "SHARD_COUNT": str(shard_count),
                            },
                        },
                    },
                },
            }
        ],
    }


def build_bundle_yaml_parallel_by_object(
    concurrency: int = 20,
    discover_notebook_path: str = "../notebooks/09_discover_object_ids",
    run_notebook_path: str = "../notebooks/10_run_ingestion_group",
    target_env: str = "dev",
    layer: str = "bronze",
    schedule_group: str = "P0_bigquery",
    registry_path: str = "abfss://config@<storage>.dfs.core.windows.net/compiled/dev/users/<alias>/latest/compiled_registry.json",
    config_version: str = "unknown",
    strict_mode: str = "false",
    node_type_id: str = "i3.xlarge",
    spark_version: str = "13.3.x-scala2.12",
) -> str:
    """Full parallelism: discover object_ids, then For-each over them (one task per object)."""
    layer = layer or "bronze"
    return f"""# Generated ingestion job (one task per object = full parallelism).
resources:
  jobs:
    bronze_ingestion_parallel_by_object:
      name: bronze_ingestion_parallel_by_object
      max_concurrent_runs: 1
      job_clusters:
        - job_cluster_key: job_cluster
          new_cluster:
            node_type_id: {node_type_id}
            spark_version: {spark_version}
            num_workers: 1
      tasks:
        - task_key: discover_objects
          job_cluster_key: job_cluster
          notebook_task:
            notebook_path: {discover_notebook_path}
            base_parameters:
              TARGET_ENV: "{target_env}"
              LAYER: "{layer}"
              SCHEDULE_GROUP: "{schedule_group}"
              REGISTRY_PATH: "{registry_path}"
        - task_key: bronze_for_each_object
          depends_on:
            - task_key: discover_objects
          for_each_task:
            inputs: "{{{{tasks.discover_objects.values.object_ids}}}}"
            concurrency: {concurrency}
            task:
              task_key: run_one_object
              job_cluster_key: job_cluster
              notebook_task:
                notebook_path: {run_notebook_path}
                base_parameters:
                  TARGET_ENV: "{target_env}"
                  LAYER: "{layer}"
                  SCHEDULE_GROUP: "{schedule_group}"
                  REGISTRY_PATH: "{registry_path}"
                  CONFIG_VERSION: "{config_version}"
                  STRICT_MODE: "{strict_mode}"
                  OBJECT_ID: "{{{{input}}}}"
"""


def build_jobs_api_json_parallel_by_object(
    concurrency: int,
    discover_notebook_path: str,
    run_notebook_path: str,
    target_env: str,
    schedule_group: str,
    registry_path: str,
    config_version: str,
    strict_mode: str,
    layer: str = "bronze",
) -> dict:
    layer = layer or "bronze"
    job_cluster = {
        "job_cluster_key": "job_cluster",
        "new_cluster": {
            "node_type_id": "i3.xlarge",
            "spark_version": "13.3.x-scala2.12",
            "num_workers": 1,
        },
    }
    return {
        "name": "bronze_ingestion_parallel_by_object",
        "max_concurrent_runs": 1,
        "job_clusters": [job_cluster],
        "tasks": [
            {
                "task_key": "discover_objects",
                "job_cluster_key": "job_cluster",
                "notebook_task": {
                    "notebook_path": discover_notebook_path,
                    "base_parameters": {
                        "TARGET_ENV": target_env,
                        "LAYER": layer,
                        "SCHEDULE_GROUP": schedule_group,
                        "REGISTRY_PATH": registry_path,
                    },
                },
            },
            {
                "task_key": "bronze_for_each_object",
                "depends_on": [{"task_key": "discover_objects"}],
                "for_each_task": {
                    "inputs": "{{tasks.discover_objects.values.object_ids}}",
                    "concurrency": concurrency,
                    "task": {
                        "task_key": "run_one_object",
                        "job_cluster_key": "job_cluster",
                        "notebook_task": {
                            "notebook_path": run_notebook_path,
                            "base_parameters": {
                                "TARGET_ENV": target_env,
                                "LAYER": layer,
                                "SCHEDULE_GROUP": schedule_group,
                                "REGISTRY_PATH": registry_path,
                                "CONFIG_VERSION": config_version,
                                "STRICT_MODE": strict_mode,
                                "OBJECT_ID": "{{input}}",
                            },
                        },
                    },
                },
            },
        ],
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Generate bronze ingestion job (shards or parallel by object)")
    p.add_argument("--mode", choices=["shards", "parallel_by_object"], default="shards")
    p.add_argument("--shard_count", type=int, default=10, help="Number of shards (mode=shards)")
    p.add_argument("--concurrency", type=int, default=20, help="Max parallel object tasks (mode=parallel_by_object)")
    p.add_argument("--out", required=True, help="Output file path (.yml or .json)")
    p.add_argument("--format", choices=["yaml", "json"], default=None, help="Output format (default from --out extension)")
    p.add_argument("--notebook_path", default="../notebooks/10_run_ingestion_group", help="Run notebook (mode=shards) or run-one notebook (mode=parallel_by_object)")
    p.add_argument("--discover_notebook_path", default="../notebooks/09_discover_object_ids", help="Discover notebook (mode=parallel_by_object only)")
    p.add_argument("--target_env", default="dev")
    p.add_argument("--layer", default="bronze", help="Layer: bronze | silver | gold")
    p.add_argument("--schedule_group", default="P0_bigquery")
    p.add_argument("--registry_path", default="abfss://config@<storage>.dfs.core.windows.net/compiled/dev/users/<alias>/latest/compiled_registry.json")
    p.add_argument("--config_version", default="unknown")
    p.add_argument("--strict_mode", default="false")
    args = p.parse_args()

    ext = Path(args.out).suffix.lower()
    fmt = args.format or ("yaml" if ext in (".yml", ".yaml") else "json")

    if args.mode == "parallel_by_object":
        if fmt == "yaml":
            content = build_bundle_yaml_parallel_by_object(
                concurrency=args.concurrency,
                discover_notebook_path=args.discover_notebook_path,
                run_notebook_path=args.notebook_path,
                target_env=args.target_env,
                layer=args.layer,
                schedule_group=args.schedule_group,
                registry_path=args.registry_path,
                config_version=args.config_version,
                strict_mode=args.strict_mode,
            )
            Path(args.out).write_text(content, encoding="utf-8")
        else:
            job = build_jobs_api_json_parallel_by_object(
                concurrency=args.concurrency,
                discover_notebook_path=args.discover_notebook_path,
                run_notebook_path=args.notebook_path,
                target_env=args.target_env,
                schedule_group=args.schedule_group,
                registry_path=args.registry_path,
                config_version=args.config_version,
                strict_mode=args.strict_mode,
                layer=args.layer,
            )
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(job, f, indent=2)
        print(f"Wrote {args.out} (mode=parallel_by_object, concurrency={args.concurrency}, layer={args.layer}, format={fmt})")
        return

    if fmt == "yaml":
        content = build_bundle_yaml(
            shard_count=args.shard_count,
            notebook_path=args.notebook_path,
            target_env=args.target_env,
            layer=args.layer,
            schedule_group=args.schedule_group,
            registry_path=args.registry_path,
            config_version=args.config_version,
            strict_mode=args.strict_mode,
        )
        Path(args.out).write_text(content, encoding="utf-8")
    else:
        job = build_jobs_api_json(
            shard_count=args.shard_count,
            notebook_path=args.notebook_path,
            target_env=args.target_env,
            schedule_group=args.schedule_group,
            registry_path=args.registry_path,
            config_version=args.config_version,
            strict_mode=args.strict_mode,
            layer=args.layer,
        )
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(job, f, indent=2)

    print(f"Wrote {args.out} (mode=shards, shard_count={args.shard_count}, format={fmt})")


if __name__ == "__main__":
    main()
