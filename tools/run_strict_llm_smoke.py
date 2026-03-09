#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.release_gates import run_llm_smoke


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run strict LLM smoke over selected package manifests")
    parser.add_argument("--package-manifest", action="append", default=[])
    parser.add_argument("--packages-dir")
    parser.add_argument("--max-packages", type=int, default=1)
    parser.add_argument("--labels-dir")
    parser.add_argument("--events-log", default="runtime/strict_llm_smoke_events.jsonl")
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--extraction-mode", default="llm", choices=["llm", "eval", "runtime"])
    parser.add_argument("--allow-unresolved-hard-blockers", action="store_true")
    parser.add_argument("--max-candidate-flagged", type=int)
    parser.add_argument("--skip-storage-check", action="store_true")
    return parser.parse_args()


def _manifest_paths(args: argparse.Namespace) -> list[Path]:
    explicit = [Path(item) for item in args.package_manifest]
    if explicit:
        return explicit

    if not args.packages_dir:
        raise ValueError("Provide --package-manifest or --packages-dir")

    files = sorted(Path(args.packages_dir).glob("*.json"))
    if not files:
        raise ValueError("No package manifests found")
    return files[: max(1, int(args.max_packages))]


def main() -> int:
    args = parse_args()
    manifests = _manifest_paths(args)

    result = run_llm_smoke(
        package_manifest_paths=manifests,
        labels_dir=Path(args.labels_dir) if args.labels_dir else None,
        events_log_path=Path(args.events_log),
        max_retries=args.max_retries,
        fail_on_unresolved_hard_blocker=not args.allow_unresolved_hard_blockers,
        max_candidate_flagged=args.max_candidate_flagged,
        extraction_mode=args.extraction_mode,
        enforce_storage_support=not args.skip_storage_check,
    )

    print("Strict LLM smoke")
    print(f"- passed: {result.passed}")
    print(f"- package_count: {result.package_count}")
    print(f"- row_count: {result.row_count}")
    print(f"- unresolved_hard_blockers: {result.unresolved_hard_blockers}")
    print(f"- candidate_flagged_count: {result.candidate_flagged_count}")
    if result.issues:
        print("- issues:")
        for issue in result.issues:
            print(f"  - {issue}")

    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
