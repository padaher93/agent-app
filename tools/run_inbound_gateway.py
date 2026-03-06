#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.inbound_gateway import create_gateway_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run inbound email gateway service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--internal-api-base", default="http://127.0.0.1:8080")
    parser.add_argument("--inbound-token")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    app = create_gateway_app(
        internal_api_base=args.internal_api_base,
        inbound_token=args.inbound_token,
    )

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
