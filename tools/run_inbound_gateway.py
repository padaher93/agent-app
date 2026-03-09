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
    parser.add_argument("--internal-api-token")
    parser.add_argument("--internal-api-require-https", action="store_true")
    parser.add_argument("--attachments-dir", default="runtime/inbound_attachments")
    parser.add_argument("--attachment-storage-mode", default="local", choices=["local", "s3"])
    parser.add_argument("--attachment-storage-s3-bucket")
    parser.add_argument("--attachment-storage-s3-prefix", default="inbound")
    parser.add_argument("--postmark-server-token")
    parser.add_argument("--mailgun-signing-key")
    parser.add_argument("--sendgrid-inbound-token")
    parser.add_argument("--dlq-path", default="runtime/inbound_dlq.jsonl")
    parser.add_argument("--outbound-email-mode", default="none", choices=["none", "smtp", "postmark"])
    parser.add_argument("--outbound-from-email", default="inbound@patrici.us")
    parser.add_argument("--outbound-smtp-host")
    parser.add_argument("--outbound-smtp-port", type=int, default=587)
    parser.add_argument("--outbound-smtp-username")
    parser.add_argument("--outbound-smtp-password")
    parser.add_argument("--outbound-smtp-no-tls", action="store_true")
    parser.add_argument("--outbound-postmark-server-token")
    parser.add_argument("--mailgun-signature-tolerance-seconds", type=int, default=600)
    parser.add_argument("--runtime-profile", default="dev", choices=["dev", "staging", "prod"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    app = create_gateway_app(
        internal_api_base=args.internal_api_base,
        inbound_token=args.inbound_token,
        internal_api_token=args.internal_api_token,
        internal_api_require_https=args.internal_api_require_https,
        attachments_dir=Path(args.attachments_dir),
        attachment_storage_mode=args.attachment_storage_mode,
        attachment_storage_s3_bucket=args.attachment_storage_s3_bucket,
        attachment_storage_s3_prefix=args.attachment_storage_s3_prefix,
        postmark_server_token=args.postmark_server_token,
        mailgun_signing_key=args.mailgun_signing_key,
        sendgrid_inbound_token=args.sendgrid_inbound_token,
        outbound_email_mode=args.outbound_email_mode,
        outbound_from_email=args.outbound_from_email,
        outbound_smtp_host=args.outbound_smtp_host,
        outbound_smtp_port=args.outbound_smtp_port,
        outbound_smtp_username=args.outbound_smtp_username,
        outbound_smtp_password=args.outbound_smtp_password,
        outbound_smtp_use_tls=not args.outbound_smtp_no_tls,
        outbound_postmark_server_token=args.outbound_postmark_server_token,
        mailgun_signature_tolerance_seconds=args.mailgun_signature_tolerance_seconds,
        dlq_path=Path(args.dlq_path),
        runtime_profile=args.runtime_profile,
    )

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
