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
from agent_app_dataset.release_gates import run_pre_partner_readiness


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run pre-partner readiness gates on proxy dataset while keeping production launch blocked"
    )
    parser.add_argument("--runtime-profile", default="prod", choices=["dev", "staging", "prod"])
    parser.add_argument("--skip-strict-config", action="store_true")

    parser.add_argument("--smoke-package-manifest", action="append", default=[])
    parser.add_argument("--smoke-packages-dir", default="dataset/packages/proxy_v1_full")
    parser.add_argument("--smoke-labels-dir", default="dataset/labels/proxy_v1_full")
    parser.add_argument("--smoke-max-packages", type=int, default=1)
    parser.add_argument("--smoke-events-log", default="runtime/pre_partner_smoke_events.jsonl")
    parser.add_argument("--smoke-max-retries", type=int, default=1)
    parser.add_argument("--smoke-extraction-mode", default="llm", choices=["llm", "runtime", "eval"])
    parser.add_argument("--smoke-allow-unresolved-hard-blockers", action="store_true")
    parser.add_argument("--smoke-max-candidate-flagged", type=int)

    parser.add_argument("--proxy-packages-dir", default="dataset/packages/proxy_v1_full")
    parser.add_argument("--proxy-labels-dir", default="dataset/labels/proxy_v1_full")
    parser.add_argument("--proxy-min-packages", type=int, default=50)
    parser.add_argument("--proxy-min-deals", type=int, default=15)
    parser.add_argument("--proxy-min-periods-per-deal", type=int, default=2)
    parser.add_argument(
        "--proxy-skip-storage-check",
        dest="proxy_skip_storage_check",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--proxy-enforce-storage-check",
        dest="proxy_skip_storage_check",
        action="store_false",
    )

    parser.add_argument("--proxy-events-log", default="runtime/pre_partner_proxy_events.jsonl")
    parser.add_argument("--proxy-predictions-output", default="runtime/pre_partner_proxy_predictions.json")
    parser.add_argument("--proxy-report-output", default="runtime/pre_partner_proxy_eval_report.json")
    parser.add_argument("--proxy-history-dir", default="dataset/eval/history/pre_partner_proxy")
    parser.add_argument("--proxy-dataset-version", default="proxy_v1_full")
    parser.add_argument("--proxy-pipeline-version", default="pre_partner_gate")
    parser.add_argument("--proxy-required-streak", type=int, default=3)
    parser.add_argument("--proxy-min-eval-packages", type=int, default=50)
    parser.add_argument("--proxy-failure-taxonomy-file")
    parser.add_argument("--proxy-incident-file")

    parser.add_argument("--output-summary", default="runtime/pre_partner_readiness_summary.json")
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
                if (
                    status in {"open", "active", "unresolved"}
                    and severity in {"high", "critical"}
                    and category in {"security", "data_integrity", "integrity"}
                ):
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

    blocking_incident, incident_summary = _load_incident_status(args.proxy_incident_file)
    check_profile = "dev" if args.skip_strict_config else args.runtime_profile

    result = run_pre_partner_readiness(
        runtime_profile=check_profile,
        strict_config=strict_config,
        smoke_package_manifest_paths=_smoke_manifests(args),
        smoke_events_log_path=Path(args.smoke_events_log),
        smoke_labels_dir=Path(args.smoke_labels_dir) if args.smoke_labels_dir else None,
        smoke_max_retries=args.smoke_max_retries,
        smoke_fail_on_unresolved_hard_blocker=not args.smoke_allow_unresolved_hard_blockers,
        smoke_max_candidate_flagged=args.smoke_max_candidate_flagged,
        smoke_extraction_mode=args.smoke_extraction_mode,
        proxy_packages_dir=Path(args.proxy_packages_dir),
        proxy_labels_dir=Path(args.proxy_labels_dir),
        proxy_min_packages=args.proxy_min_packages,
        proxy_min_deals=args.proxy_min_deals,
        proxy_min_periods_per_deal=args.proxy_min_periods_per_deal,
        proxy_require_supported_storage=not args.proxy_skip_storage_check,
        proxy_eval_kwargs={
            "events_log_path": Path(args.proxy_events_log),
            "extraction_mode": args.smoke_extraction_mode,
            "max_retries": args.smoke_max_retries,
            "predictions_output_path": Path(args.proxy_predictions_output),
            "report_output_path": Path(args.proxy_report_output),
            "history_dir": Path(args.proxy_history_dir),
            "dataset_version": args.proxy_dataset_version,
            "pipeline_version": args.proxy_pipeline_version,
            "required_streak": args.proxy_required_streak,
            "min_packages": args.proxy_min_eval_packages,
            "failure_taxonomy": _load_failure_taxonomy(args.proxy_failure_taxonomy_file),
            "blocking_incident": blocking_incident,
            "incident_summary": incident_summary,
        },
        summary_output_path=Path(args.output_summary),
    )

    print("Pre-partner readiness gate")
    print(f"- passed: {result.passed}")
    print(f"- strict_config_passed: {result.strict_config.passed}")
    print(f"- proxy_readiness_passed: {result.proxy_readiness.passed}")
    print(f"- llm_smoke_passed: {result.llm_smoke.passed}")
    print(f"- proxy_eval_release_ready: {result.proxy_eval_release_ready}")
    print(f"- production_launch_ready: {result.production_launch_ready}")
    print(f"- summary_path: {result.summary_path}")
    if result.strict_config.issues:
        print("- strict_config_issues:")
        for issue in result.strict_config.issues:
            print(f"  - {issue}")
    if result.proxy_readiness.issues:
        print("- proxy_readiness_issues:")
        for issue in result.proxy_readiness.issues:
            print(f"  - {issue}")
    if result.llm_smoke.issues:
        print("- llm_smoke_issues:")
        for issue in result.llm_smoke.issues:
            print(f"  - {issue}")

    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
