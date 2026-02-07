# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.ops.ctl_file_manifest (
# MAGIC   object_id STRING,
# MAGIC   file_path STRING,
# MAGIC   file_name STRING,
# MAGIC   file_ext STRING,
# MAGIC   file_size BIGINT,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   content_hash STRING,
# MAGIC   discovered_ts TIMESTAMP,
# MAGIC   last_seen_ts TIMESTAMP,
# MAGIC   status STRING,
# MAGIC   run_id STRING,
# MAGIC   batch_id STRING,
# MAGIC   error_class STRING,
# MAGIC   error_message STRING
# MAGIC )
# MAGIC USING delta;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS main.ops.ctl_file_run_items (
# MAGIC   run_id STRING,
# MAGIC   object_id STRING,
# MAGIC   file_path STRING,
# MAGIC   attempt_ts TIMESTAMP,
# MAGIC   status STRING,
# MAGIC   error_message STRING
# MAGIC )
# MAGIC USING delta;
# MAGIC
