# in runner where obj_cfg.source_type == "file"
from src.manifest.file_inventory import inventory, InventoryOptions
from src.manifest.manifest_store import ManifestStore, ManifestStoreConfig

def run_file_object(self, run_ctx: dict, obj_cfg: dict):
    object_id = obj_cfg["object_id"]
    landing_path = obj_cfg["landing"]["path"]
    max_files = int((obj_cfg.get("manifest", {}).get("listing", {}) or {}).get("max_files_per_run", 500))
    recursive = bool((obj_cfg.get("manifest", {}).get("listing", {}) or {}).get("recursive", True))
    batch_id = run_ctx.get("batch_id", run_ctx["run_id"])

    store = ManifestStore(self.spark, ManifestStoreConfig(
        manifest_table="main.ops.ctl_file_manifest",
        run_items_table="main.ops.ctl_file_run_items"
    ))

    inv = inventory(self.spark, self.dbutils, landing_path, InventoryOptions(
        recursive=recursive,
        max_files=None,             # list all; claim caps actual work
        compute_hash=True,
        hash_algo="sha256"
    ))
    store.upsert_discovered(object_id=object_id, inv_df=inv, batch_id=batch_id)

    claimed = store.claim_files(object_id=object_id, run_id=run_ctx["run_id"], batch_id=batch_id, max_files=max_files)
    claimed_paths = [r["file_path"] for r in claimed.select("file_path").collect()]

    if not claimed_paths:
        return

    store.write_run_items(run_id=run_ctx["run_id"], object_id=object_id, file_paths=claimed_paths, status="IN_PROGRESS")

    # Read + Write with per-file failure isolation
    # For MVP: process in small batches so one bad file doesn't kill all (e.g., 20 at a time)
    file_plugin = self.plugins["file"]
    chunk_size = 20
    bronze_table = obj_cfg["bronze"]["table"]
    replace = bool((obj_cfg.get("manifest", {}).get("replay", {}) or {}).get("replace_by_file", True))
    file_path_col = obj_cfg["bronze"].get("file_path_column", "_file_path")

    for i in range(0, len(claimed_paths), chunk_size):
        chunk_paths = claimed_paths[i:i+chunk_size]
        try:
            df = file_plugin.read_files(chunk_paths, obj_cfg)
            df = self.bronze_writer.with_audit_cols(df, run_ctx, obj_cfg)

            if replace:
                self.bronze_writer.delete_by_file_paths(bronze_table, file_path_col, chunk_paths)

            self.bronze_writer.append(df, bronze_table)
            store.mark_success(object_id, run_ctx["run_id"], chunk_paths)
            store.write_run_items(run_id=run_ctx["run_id"], object_id=object_id, file_paths=chunk_paths, status="SUCCESS")

        except Exception as e:
            # Mark each file as failed (best effort). If you need per-file granularity, read/write one-by-one for Excel.
            msg = str(e)
            for p in chunk_paths:
                store.mark_failed(object_id, run_ctx["run_id"], p, error_class="FILE_INGEST_ERROR", error_message=msg)
            store.write_run_items(run_id=run_ctx["run_id"], object_id=object_id, file_paths=chunk_paths, status="FAILED", error_message=msg)
            # continue with next chunk
