#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.agent_workflow import check_log_integrity


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify append-only agent event log integrity")
    parser.add_argument("--events-log", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    issues = check_log_integrity(Path(args.events_log))

    if issues:
        print("Event log integrity failed")
        for issue in issues:
            print(issue)
        return 1

    print("Event log integrity passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
