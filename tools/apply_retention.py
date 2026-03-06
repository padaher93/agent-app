#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.retention import apply_retention_policy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply retention policies to package DB and event log")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--events-log", required=True)
    parser.add_argument("--package-months", type=int, default=24)
    parser.add_argument("--log-years", type=int, default=7)
    parser.add_argument("--archive-dir")
    parser.add_argument("--apply", action="store_true", help="Apply changes; default is dry-run")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    summary = apply_retention_policy(
        db_path=Path(args.db_path),
        events_log_path=Path(args.events_log),
        package_retention_months=args.package_months,
        log_retention_years=args.log_years,
        dry_run=not args.apply,
        archive_dir=Path(args.archive_dir) if args.archive_dir else None,
    )

    print("Retention summary")
    for key in [
        "dry_run",
        "now",
        "package_cutoff",
        "log_cutoff",
        "packages_marked",
        "events_marked",
    ]:
        print(f"- {key}: {summary[key]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
