#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.email_adapter import normalized_email_to_ingest_request
from agent_app_dataset.io_utils import read_json


DEFAULT_ENDPOINT = "http://127.0.0.1:8080/internal/v1/packages:ingest"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest normalized email payload into internal API")
    parser.add_argument("--email-json", required=True)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    email_payload = read_json(Path(args.email_json))
    ingest_payload = normalized_email_to_ingest_request(email_payload)

    body = json.dumps(ingest_payload).encode("utf-8")
    req = urllib.request.Request(
        args.endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        out = json.loads(resp.read().decode("utf-8"))

    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
