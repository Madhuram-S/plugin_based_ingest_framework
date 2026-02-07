# src/plugins/adf_copy_plugin.py
from typing import Dict, Any
from src.plugins.base import IngestPlugin

class AdfCopyPlugin(IngestPlugin):
    def __init__(self, adf_client, ops_logger=None):
        self.adf = adf_client
        self.ops = ops_logger  # optional, runner already logs

    def ingest(self, run_ctx, obj_cfg: Dict[str, Any], prior_state: Dict[str, Any]) -> Dict[str, Any]:
        adf_cfg = (obj_cfg.get("extract", {}) or {}).get("adf", {})
        subscription_id = adf_cfg["subscription_id"]
        resource_group = adf_cfg["resource_group"]
        factory_name = adf_cfg["factory_name"]
        pipeline_name = adf_cfg["pipeline_name"]
        poll_seconds = int(adf_cfg.get("poll_seconds", 30))
        timeout_minutes = int(adf_cfg.get("timeout_minutes", 120))
        parameters = adf_cfg.get("parameters", {}) or {}

        run_id = self.adf.create_run(subscription_id, resource_group, factory_name, pipeline_name, parameters)
        final = self.adf.wait_for_completion(subscription_id, resource_group, factory_name,
                                             run_id, poll_seconds, timeout_minutes)

        if final.status != "Succeeded":
            raise RuntimeError(f"ADF pipeline failed: status={final.status} run_id={run_id} msg={final.message}")

        return {
            "rows_read": None,
            "rows_written": None,
            "ingest_mode": "ADF_COPY",
            "artifacts": {
                "adf_run_id": run_id,
                "adf_status": final.status
            },
            "next_state": {}  # ADF copy itself doesn't advance watermark; bronze write does
        }
