# src/manifest/file_inventory.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dataclass(frozen=True)
class InventoryOptions:
    recursive: bool = True
    max_files: Optional[int] = None
    compute_hash: bool = True
    hash_algo: str = "sha256"

def list_files_dbutils(dbutils, root_path: str, recursive: bool = True) -> List[str]:
    """List files under root_path using dbutils.fs.ls (recursive if needed)."""
    results = []
    stack = [root_path]
    while stack:
        p = stack.pop()
        for fi in dbutils.fs.ls(p):
            if fi.isDir():
                if recursive:
                    stack.append(fi.path)
            else:
                results.append(fi.path)
    return results

def build_inventory_df(spark, paths: List[str]) -> DataFrame:
    """Read file metadata using Spark binaryFile for a list of paths."""
    if not paths:
        return spark.createDataFrame([], "path STRING, length BIGINT, modificationTime TIMESTAMP, content BINARY")

    # binaryFile gives (path, modificationTime, length, content)
    # NOTE: reading content can be heavy, but files are ~1MB so OK.
    df = (spark.read.format("binaryFile")
          .option("pathGlobFilter", "*")
          .load(paths))
    return df

def add_path_columns(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("file_path", F.col("path"))
        .withColumn("file_name", F.element_at(F.split(F.col("path"), "/"), -1))
        .withColumn("file_ext", F.lower(F.regexp_extract(F.col("file_name"), r"\.([A-Za-z0-9]+)$", 1)))
        .withColumn("file_size", F.col("length"))
        .withColumn("file_mod_time", F.col("modificationTime"))
    )

def add_content_hash(df: DataFrame, algo: str = "sha256") -> DataFrame:
    # sha2 supports 224/256/384/512; sha256 is common
    bits = 256 if algo.lower() == "sha256" else 256
    return df.withColumn("content_hash", F.sha2(F.col("content"), bits))

def inventory(spark, dbutils, root_path: str, opts: InventoryOptions) -> DataFrame:
    paths = list_files_dbutils(dbutils, root_path, recursive=opts.recursive)
    if opts.max_files:
        paths = paths[:opts.max_files]

    df = build_inventory_df(spark, paths)
    df = add_path_columns(df)

    if opts.compute_hash:
        df = add_content_hash(df, algo=opts.hash_algo)

    # drop content to avoid writing large binaries around
    df = df.drop("content").drop("path").drop("length").drop("modificationTime")
    return df
