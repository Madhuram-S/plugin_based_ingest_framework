# dq_executor.py
from __future__ import annotations

from typing import Any, Dict, List
from pyspark.sql import functions as F

class DQExecutor:
    """
    Bronze-grade checks:
      - row_count_min
      - not_null (columns)
      - duplicates (primary key columns)
    Logs results to ops_dq_log.
    """
    def __init__(self, spark, ops_logger):
        self.spark = spark
        self.ops = ops_logger

    def run(self, run_ctx, object_id: str, obj_cfg: Dict[str, Any]) -> None:
        dq = obj_cfg.get("dq", {}) or {}
        checks: List[Dict[str, Any]] = dq.get("checks", []) or []
        if not checks:
            return

        bronze_table = obj_cfg.get("bronze_table")
        if not bronze_table:
            return

        df = self.spark.table(bronze_table)

        for c in checks:
            ctype = c.get("type")
            name = c.get("name") or ctype or "unknown"
            severity = c.get("severity", "warn")
            try:
                if ctype == "row_count_min":
                    minv = int(c.get("min", 1))
                    # Avoid count() on massive tables in MVP? Up to you.
                    cnt = df.count()
                    status = "PASS" if cnt >= minv else "FAIL"
                    self.ops.log_dq(run_ctx.run_id, object_id, name, severity, status, f"count={cnt}, min={minv}")

                elif ctype == "not_null":
                    cols = c.get("columns", [])
                    if isinstance(cols, str):
                        cols = [cols]
                    missing = [x for x in cols if x not in df.columns]
                    if missing:
                        self.ops.log_dq(run_ctx.run_id, object_id, name, severity, "SKIP", f"missing_columns={missing}")
                        continue
                    cond = " OR ".join([f"`{col}` IS NULL" for col in cols])
                    bad = df.where(cond).count()
                    status = "PASS" if bad == 0 else "FAIL"
                    self.ops.log_dq(run_ctx.run_id, object_id, name, severity, status, f"null_rows={bad} cols={cols}")

                elif ctype == "duplicates":
                    cols = c.get("columns", [])
                    if isinstance(cols, str):
                        cols = [cols]
                    missing = [x for x in cols if x not in df.columns]
                    if missing:
                        self.ops.log_dq(run_ctx.run_id, object_id, name, severity, "SKIP", f"missing_columns={missing}")
                        continue
                    dup = (df.groupBy([F.col(c) for c in cols])
                             .count()
                             .where(F.col("count") > 1)
                             .count())
                    status = "PASS" if dup == 0 else "FAIL"
                    self.ops.log_dq(run_ctx.run_id, object_id, name, severity, status, f"duplicate_keys={dup} cols={cols}")

                else:
                    self.ops.log_dq(run_ctx.run_id, object_id, name, severity, "SKIP", f"unsupported_check={ctype}")

            except Exception as e:
                self.ops.log_dq(run_ctx.run_id, object_id, name, severity, "ERROR", str(e))
