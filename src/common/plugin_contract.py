from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class RunContext:
    run_id: str
    env: str
    config_version: str
    registry_path: str
    registry_sha256: str
    started_epoch: float

@dataclass
class IngestResult:
    status: str                         # "SUCCESS" | "SKIPPED"
    ingest_mode: str                    # "FULL" | "INCR" | "API" | "FILE" ...
    df: Optional[Any] = None            # Spark DataFrame
    row_count_source: Optional[int] = None
    next_state: Dict[str, Any] = field(default_factory=dict)  # {last_watermark, last_cursor}
    artifacts: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
