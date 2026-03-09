#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.ops_preflight import run_ops_preflight


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run mailbox and runtime operations preflight checks")
    parser.add_argument("--runtime-profile", default="prod", choices=["dev", "staging", "prod"])
    parser.add_argument("--skip-strict-config", action="store_true")
    parser.add_argument("--inbound-gateway-base")
    parser.add_argument("--internal-api-base")
    parser.add_argument("--dlq-path", default="runtime/inbound_dlq.jsonl")
    parser.add_argument("--check-postmark-api", action="store_true")
    parser.add_argument("--output", default="runtime/ops_preflight_report.json")
    return parser.parse_args()


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

    result = run_ops_preflight(
        runtime_profile=("dev" if args.skip_strict_config else args.runtime_profile),
        strict_config=strict_config,
        inbound_gateway_base=args.inbound_gateway_base,
        internal_api_base=args.internal_api_base,
        dlq_path=Path(args.dlq_path),
        check_postmark_api=args.check_postmark_api,
        postmark_outbound_server_token=os.getenv("POSTMARK_OUTBOUND_SERVER_TOKEN"),
        output_path=Path(args.output),
    )

    print("Ops preflight")
    print(f"- passed: {result.passed}")
    print(f"- strict_config_passed: {result.strict_config.passed}")
    print(f"- dlq_writable: {result.dlq_writable}")
    print(f"- endpoint_checks: {len(result.endpoint_checks)}")
    if result.issues:
        print("- issues:")
        for issue in result.issues:
            print(f"  - {issue}")
    print(f"- report: {args.output}")

    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
