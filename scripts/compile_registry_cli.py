#!/usr/bin/env python3
import argparse
import json
import hashlib
from datetime import datetime, timezone

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--env", required=True, help="dev|test|prod")
    p.add_argument("--out", required=True, help="output json path")
    p.add_argument("--version", default="git:unknown|bundle:unknown")
    args = p.parse_args()

    # TODO: replace with real loader
    registry = {
        "env": args.env,
        "config_version": args.version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "objects": []
    }

    content = json.dumps(registry, indent=2, sort_keys=True)
    sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
    registry["content_sha256"] = sha

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, sort_keys=True)

    print(f"Wrote: {args.out}")
    print(f"content_sha256: {sha}")

if __name__ == "__main__":
    main()
