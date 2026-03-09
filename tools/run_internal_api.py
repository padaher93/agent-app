#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.internal_api import create_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Patricius internal API server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db-path", default="runtime/internal_api.sqlite3")
    parser.add_argument("--labels-dir", default="dataset/labels/proxy_v1_full")
    parser.add_argument("--events-log", default="runtime/agent_events.jsonl")
    parser.add_argument("--ui-dir", default="src/agent_app_dataset/ui")
    parser.add_argument("--public-base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--internal-token")
    parser.add_argument("--require-https", action="store_true")
    parser.add_argument("--encryption-key")
    parser.add_argument("--runtime-profile", default="dev", choices=["dev", "staging", "prod"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    app = create_app(
        db_path=Path(args.db_path),
        labels_dir=Path(args.labels_dir),
        events_log_path=Path(args.events_log),
        ui_dir=Path(args.ui_dir),
        public_base_url=args.public_base_url,
        internal_token=args.internal_token,
        require_https=args.require_https,
        encryption_key=args.encryption_key,
        runtime_profile=args.runtime_profile,
    )

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
