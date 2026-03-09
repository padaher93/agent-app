#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.io_utils import read_json
from agent_app_dataset.release_gates import run_release_candidate_gate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run release-candidate gate checks with summary artifact")
    parser.add_argument("--runtime-profile", default="prod", choices=["dev", "staging", "prod"])
    parser.add_argument("--skip-strict-config", action="store_true")

    parser.add_argument("--smoke-package-manifest", action="append", default=[])
    parser.add_argument("--smoke-packages-dir", default="dataset/real_shadow_test/packages")
    parser.add_argument("--smoke-labels-dir")
    parser.add_argument("--smoke-max-packages", type=int, default=1)
    parser.add_argument("--smoke-events-log", default="runtime/release_candidate_smoke_events.jsonl")
    parser.add_argument("--smoke-max-retries", type=int, default=1)
    parser.add_argument("--smoke-extraction-mode", default="llm", choices=["llm", "eval", "runtime"])
    parser.add_argument("--smoke-allow-unresolved-hard-blockers", action="store_true")
    parser.add_argument("--smoke-max-candidate-flagged", type=int)

    parser.add_argument("--shadow-packages-dir", default="dataset/real_shadow_test/packages")
    parser.add_argument("--shadow-labels-dir", default="dataset/real_shadow_test/labels")
    parser.add_argument("--shadow-min-packages", type=int, default=20)
    parser.add_argument("--shadow-min-deals", type=int, default=3)
    parser.add_argument("--shadow-min-periods-per-deal", type=int, default=2)
    parser.add_argument("--shadow-skip-storage-check", action="store_true")

    parser.add_argument("--shadow-events-log", default="runtime/release_candidate_shadow_events.jsonl")
    parser.add_argument("--shadow-predictions-output", default="runtime/release_candidate_shadow_predictions.json")
    parser.add_argument("--shadow-report-output", default="runtime/release_candidate_shadow_report.json")
    parser.add_argument("--shadow-history-dir", default="dataset/eval/history/real_shadow_test")
    parser.add_argument("--shadow-dataset-version", default="real_shadow_test")
    parser.add_argument("--shadow-pipeline-version", default="rc_gate")
    parser.add_argument("--shadow-required-streak", type=int, default=3)
    parser.add_argument("--shadow-min-eval-packages", type=int, default=20)
    parser.add_argument("--shadow-failure-taxonomy-file")
    parser.add_argument("--shadow-incident-file")

    parser.add_argument("--output-summary", default="runtime/release_candidate_summary.json")
    return parser.parse_args()


def _smoke_manifests(args: argparse.Namespace) -> list[Path]:
    explicit = [Path(item) for item in args.smoke_package_manifest]
    if explicit:
        return explicit

    files = sorted(Path(args.smoke_packages_dir).glob("*.json"))
    if not files:
        raise ValueError("No smoke package manifests found")
    return files[: max(1, int(args.smoke_max_packages))]


def _load_failure_taxonomy(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    payload = read_json(Path(path))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("failure_taxonomy"), list):
        return payload["failure_taxonomy"]
    raise ValueError("Failure taxonomy payload must be a list or include 'failure_taxonomy'")


def _load_incident_status(path: str | None) -> tuple[bool, str]:
    if not path:
        return False, "No incident file provided"
    payload = read_json(Path(path))
    if isinstance(payload, dict):
        if "blocking" in payload:
            return bool(payload.get("blocking")), str(payload.get("summary", "Explicit incident blocking flag"))
        incidents = payload.get("incidents")
        if isinstance(incidents, list):
            blocking = 0
            for incident in incidents:
                if not isinstance(incident, dict):
                    continue
                status = str(incident.get("status", "")).strip().lower()
                severity = str(incident.get("severity", "")).strip().lower()
                category = str(incident.get("category", "")).strip().lower()
                if status in {"open", "active", "unresolved"} and severity in {"high", "critical"} and category in {"security", "data_integrity", "integrity"}:
                    blocking += 1
            return (blocking > 0), f"Blocking incidents: {blocking}"
    return False, "Incident file parsed; no blocking incidents detected"


def main() -> int:
    args = parse_args()

    strict_config = {
        "internal_token": None if args.skip_strict_config else os.getenv("INTERNAL_API_TOKEN"),
        "require_https": False if args.skip_strict_config else True,
        "openai_api_key": None if args.skip_strict_config else os.getenv("OPENAI_API_KEY"),
        "internal_api_token": None if args.skip_strict_config else os.getenv("INTERNAL_API_TOKEN"),
        "internal_api_require_https": False if args.skip_strict_config else True,
        "postmark_server_token": None if args.skip_strict_config else os.getenv("POSTMARK_INBOUND_SERVER_TOKEN"),
        "outbound_email_mode": "none" if args.skip_strict_config else "postmark",
        "outbound_postmark_server_token": None if args.skip_strict_config else os.getenv("POSTMARK_OUTBOUND_SERVER_TOKEN"),
        "mailgun_signing_key": None,
        "sendgrid_inbound_token": None,
        "attachment_storage_mode": "local" if args.skip_strict_config else "s3",
        "attachment_storage_s3_bucket": None if args.skip_strict_config else os.getenv("INBOUND_ATTACHMENTS_S3_BUCKET"),
    }

    blocking_incident, incident_summary = _load_incident_status(args.shadow_incident_file)
    check_profile = "dev" if args.skip_strict_config else args.runtime_profile

    result = run_release_candidate_gate(
        runtime_profile=check_profile,
        strict_config=strict_config,
        smoke_package_manifest_paths=_smoke_manifests(args),
        smoke_events_log_path=Path(args.smoke_events_log),
        smoke_labels_dir=Path(args.smoke_labels_dir) if args.smoke_labels_dir else None,
        smoke_max_retries=args.smoke_max_retries,
        smoke_fail_on_unresolved_hard_blocker=not args.smoke_allow_unresolved_hard_blockers,
        smoke_max_candidate_flagged=args.smoke_max_candidate_flagged,
        smoke_extraction_mode=args.smoke_extraction_mode,
        shadow_packages_dir=Path(args.shadow_packages_dir),
        shadow_labels_dir=Path(args.shadow_labels_dir),
        shadow_min_packages=args.shadow_min_packages,
        shadow_min_deals=args.shadow_min_deals,
        shadow_min_periods_per_deal=args.shadow_min_periods_per_deal,
        shadow_require_supported_storage=not args.shadow_skip_storage_check,
        shadow_eval_kwargs={
            "events_log_path": Path(args.shadow_events_log),
            "extraction_mode": args.smoke_extraction_mode,
            "max_retries": args.smoke_max_retries,
            "predictions_output_path": Path(args.shadow_predictions_output),
            "report_output_path": Path(args.shadow_report_output),
            "history_dir": Path(args.shadow_history_dir),
            "dataset_version": args.shadow_dataset_version,
            "pipeline_version": args.shadow_pipeline_version,
            "required_streak": args.shadow_required_streak,
            "min_packages": args.shadow_min_eval_packages,
            "failure_taxonomy": _load_failure_taxonomy(args.shadow_failure_taxonomy_file),
            "blocking_incident": blocking_incident,
            "incident_summary": incident_summary,
        },
        summary_output_path=Path(args.output_summary),
    )

    print("Release candidate gate")
    print(f"- passed: {result.passed}")
    print(f"- strict_config_passed: {result.strict_config.passed}")
    print(f"- shadow_readiness_passed: {result.shadow_readiness.passed}")
    print(f"- llm_smoke_passed: {result.llm_smoke.passed}")
    print(f"- shadow_eval_release_ready: {result.shadow_eval_release_ready}")
    print(f"- summary_path: {result.summary_path}")
    if result.strict_config.issues:
        print("- strict_config_issues:")
        for issue in result.strict_config.issues:
            print(f"  - {issue}")
    if result.shadow_readiness.issues:
        print("- shadow_readiness_issues:")
        for issue in result.shadow_readiness.issues:
            print(f"  - {issue}")
    if result.llm_smoke.issues:
        print("- llm_smoke_issues:")
        for issue in result.llm_smoke.issues:
            print(f"  - {issue}")

    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
