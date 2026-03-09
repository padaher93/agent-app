from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import time
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from .email_adapter import normalized_email_to_ingest_request
from .outbound_email import OutboundNotifier, create_outbound_notifier
from .runtime_profile import (
    RuntimeProfile,
    is_strict_runtime_profile,
    resolve_runtime_profile,
    validate_gateway_runtime_requirements,
)


SUPPORTED_DOC_TYPES = {"PDF", "XLSX"}


class InboundEmailPayload(BaseModel):
    sender_email: str | None = None
    from_: str | None = Field(default=None, alias="from")
    workspace_id: str | None = None
    source_email_id: str | None = None
    message_id: str | None = None
    deal_id: str | None = None
    period_end_date: str | None = None
    received_at: str | None = None
    variant_tags: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)
    package_id: str | None = None
    attachments: list[dict[str, Any]] = Field(min_length=1)


class InboundProcessRequest(BaseModel):
    async_mode: bool = True
    max_retries: int = Field(default=2, ge=0, le=5)
    extraction_mode: str = Field(default="llm", pattern="^(runtime|eval|llm)$")


class InboundEnvelope(BaseModel):
    email: InboundEmailPayload
    process: InboundProcessRequest = Field(default_factory=InboundProcessRequest)


class PostmarkAttachment(BaseModel):
    Name: str
    ContentType: str | None = None
    Content: str


class PostmarkFromFull(BaseModel):
    Email: str


class PostmarkInboundPayload(BaseModel):
    MessageID: str
    Date: str | None = None
    Subject: str | None = None
    From: str | None = None
    FromFull: PostmarkFromFull | None = None
    Attachments: list[PostmarkAttachment] = Field(default_factory=list)


def _create_s3_client() -> Any:
    try:
        import boto3  # type: ignore
    except Exception as exc:
        raise RuntimeError("boto3_not_installed") from exc

    endpoint_url = None
    region_name = None
    if "AWS_S3_ENDPOINT_URL" in os.environ:
        endpoint_url = os.environ.get("AWS_S3_ENDPOINT_URL")
    if "AWS_REGION" in os.environ:
        region_name = os.environ.get("AWS_REGION")
    elif "AWS_DEFAULT_REGION" in os.environ:
        region_name = os.environ.get("AWS_DEFAULT_REGION")

    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if region_name:
        kwargs["region_name"] = region_name
    return boto3.client("s3", **kwargs)


class _AttachmentStore:
    def __init__(
        self,
        base_dir: Path,
        *,
        mode: str = "local",
        s3_bucket: str | None = None,
        s3_prefix: str = "inbound",
        s3_client: Any | None = None,
    ) -> None:
        self.mode = str(mode).strip().lower()
        if self.mode not in {"local", "s3"}:
            raise ValueError("attachment_storage_mode_must_be_local_or_s3")

        self.base_dir = base_dir
        self.s3_bucket = (s3_bucket or "").strip()
        self.s3_prefix = str(s3_prefix).strip().strip("/")
        self.s3_client = s3_client

        if self.mode == "local":
            self.base_dir.mkdir(parents=True, exist_ok=True)
        else:
            if not self.s3_bucket:
                raise ValueError("attachment_storage_s3_bucket_required")
            if self.s3_client is None:
                self.s3_client = _create_s3_client()

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        clean = re.sub(r"[^a-zA-Z0-9._-]", "_", filename.strip())
        return clean or "attachment.bin"

    def _s3_key(self, message_id: str, filename: str) -> str:
        safe_message_id = self._sanitize_filename(message_id)[:120]
        safe_filename = self._sanitize_filename(filename)
        date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        prefix = f"{self.s3_prefix}/" if self.s3_prefix else ""
        return f"{prefix}{date_path}/{safe_message_id}/{uuid4().hex[:10]}_{safe_filename}"

    def write(self, message_id: str, filename: str, content: bytes, content_type: str | None = None) -> str:
        if self.mode == "s3":
            key = self._s3_key(message_id=message_id, filename=filename)
            put_kwargs: dict[str, Any] = {
                "Bucket": self.s3_bucket,
                "Key": key,
                "Body": content,
            }
            if content_type:
                put_kwargs["ContentType"] = content_type
            self.s3_client.put_object(**put_kwargs)
            return f"s3://{self.s3_bucket}/{key}"

        safe_message_id = self._sanitize_filename(message_id)[:120]
        safe_filename = self._sanitize_filename(filename)
        folder = self.base_dir / datetime.now(timezone.utc).strftime("%Y/%m/%d") / safe_message_id
        folder.mkdir(parents=True, exist_ok=True)
        out_path = folder / f"{uuid4().hex[:10]}_{safe_filename}"
        out_path.write_bytes(content)
        return str(out_path.resolve())


class _DLQStore:
    def __init__(self, dlq_path: Path) -> None:
        self.dlq_path = dlq_path
        self.dlq_path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, payload: dict[str, Any]) -> None:
        record = dict(payload)
        record["recorded_at"] = _utc_now()
        with self.dlq_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _infer_doc_type(filename: str, content_type: str | None = None) -> str | None:
    lowered = filename.lower().strip()
    ctype = (content_type or "").lower().strip()

    if lowered.endswith(".pdf") or "application/pdf" in ctype:
        return "PDF"

    if lowered.endswith(".xlsx") or lowered.endswith(".xlsm"):
        return "XLSX"

    if "spreadsheetml" in ctype or "excel" in ctype:
        return "XLSX"

    return None


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _build_attachment_meta(
    *,
    idx: int,
    filename: str,
    content_type: str | None,
    storage_uri: str,
    checksum: str,
) -> dict[str, Any] | None:
    doc_type = _infer_doc_type(filename, content_type)
    if doc_type not in SUPPORTED_DOC_TYPES:
        return None

    return {
        "file_id": f"file_ext_{idx:02d}",
        "source_id": f"src_ext_{idx:04d}",
        "doc_type": doc_type,
        "filename": filename,
        "storage_uri": storage_uri,
        "checksum": checksum,
        "pages_or_sheets": 1,
    }


def _validate_mailgun_signature(timestamp: str, token: str, signature: str, signing_key: str) -> bool:
    digest = hmac.new(
        signing_key.encode("utf-8"),
        f"{timestamp}{token}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(digest, signature)


def _validate_mailgun_timestamp(timestamp: str, tolerance_seconds: int) -> bool:
    try:
        parsed = int(timestamp)
    except Exception:
        return False
    delta = abs(int(time.time()) - parsed)
    return delta <= int(tolerance_seconds)


def _sender_from_postmark(payload: PostmarkInboundPayload) -> str:
    if payload.FromFull and payload.FromFull.Email:
        return payload.FromFull.Email
    if payload.From:
        m = re.search(r"<([^>]+)>", payload.From)
        if m:
            return m.group(1)
        return payload.From
    raise ValueError("Postmark payload missing sender")


def create_gateway_app(
    internal_api_base: str,
    inbound_token: str | None = None,
    internal_api_token: str | None = None,
    internal_api_require_https: bool = False,
    attachments_dir: Path | str = Path("runtime/inbound_attachments"),
    postmark_server_token: str | None = None,
    mailgun_signing_key: str | None = None,
    sendgrid_inbound_token: str | None = None,
    outbound_email_mode: str = "none",
    outbound_from_email: str = "inbound@patrici.us",
    outbound_smtp_host: str | None = None,
    outbound_smtp_port: int = 587,
    outbound_smtp_username: str | None = None,
    outbound_smtp_password: str | None = None,
    outbound_smtp_use_tls: bool = True,
    outbound_postmark_server_token: str | None = None,
    mailgun_signature_tolerance_seconds: int = 600,
    notifier: OutboundNotifier | None = None,
    dlq_path: Path | str = Path("runtime/inbound_dlq.jsonl"),
    attachment_storage_mode: str = "local",
    attachment_storage_s3_bucket: str | None = None,
    attachment_storage_s3_prefix: str = "inbound",
    attachment_storage_s3_client: Any | None = None,
    runtime_profile: RuntimeProfile | str = "dev",
) -> FastAPI:
    active_profile = resolve_runtime_profile(str(runtime_profile))
    strict_runtime = is_strict_runtime_profile(active_profile)
    validate_gateway_runtime_requirements(
        runtime_profile=active_profile,
        internal_api_token=internal_api_token,
        internal_api_require_https=internal_api_require_https,
        postmark_server_token=postmark_server_token,
        outbound_email_mode=outbound_email_mode,
        outbound_postmark_server_token=outbound_postmark_server_token,
        mailgun_signing_key=mailgun_signing_key,
        sendgrid_inbound_token=sendgrid_inbound_token,
        attachment_storage_mode=attachment_storage_mode,
        attachment_storage_s3_bucket=attachment_storage_s3_bucket,
    )

    app = FastAPI(title="Patricius Inbound Gateway", version="v1")
    attachment_store = _AttachmentStore(
        Path(attachments_dir),
        mode=attachment_storage_mode,
        s3_bucket=attachment_storage_s3_bucket,
        s3_prefix=attachment_storage_s3_prefix,
        s3_client=attachment_storage_s3_client,
    )
    dlq_store = _DLQStore(Path(dlq_path))
    outbound_notifier = notifier or create_outbound_notifier(
        mode=outbound_email_mode,
        from_email=outbound_from_email,
        smtp_host=outbound_smtp_host,
        smtp_port=outbound_smtp_port,
        smtp_username=outbound_smtp_username,
        smtp_password=outbound_smtp_password,
        smtp_use_tls=outbound_smtp_use_tls,
        postmark_server_token=outbound_postmark_server_token,
    )

    def _forward_to_internal(envelope: InboundEnvelope) -> dict[str, Any]:
        if strict_runtime and envelope.process.extraction_mode != "llm":
            raise HTTPException(status_code=400, detail="extraction_mode_not_allowed_in_strict_profile")

        email_payload = envelope.email.model_dump(by_alias=True, exclude_none=True)
        if "from" not in email_payload and envelope.email.sender_email:
            email_payload["from"] = envelope.email.sender_email

        ingest_payload = normalized_email_to_ingest_request(email_payload)
        internal_headers: dict[str, str] = {}
        if internal_api_token is not None:
            internal_headers["X-Internal-Token"] = internal_api_token
        if internal_api_require_https:
            internal_headers["X-Forwarded-Proto"] = "https"

        try:
            with httpx.Client(timeout=45.0) as client:
                ingest_resp = client.post(
                    f"{internal_api_base.rstrip('/')}/internal/v1/packages:ingest",
                    json=ingest_payload,
                    headers=internal_headers,
                )
                if ingest_resp.status_code >= 400:
                    raise HTTPException(status_code=502, detail=f"ingest_failed:{ingest_resp.text}")
                ingest_json = ingest_resp.json()

                package_id = ingest_json["package_id"]
                process_resp = client.post(
                    f"{internal_api_base.rstrip('/')}/internal/v1/packages/{package_id}:process",
                    json=envelope.process.model_dump(),
                    headers={
                        **internal_headers,
                        "X-Workspace-Id": str(ingest_payload.get("workspace_id", "ws_default")),
                    },
                )
                if process_resp.status_code >= 400:
                    raise HTTPException(status_code=502, detail=f"process_failed:{process_resp.text}")
                process_json = process_resp.json()
                if process_json.get("status") == "failed":
                    reason = process_json.get("error_message") or "processing_failed"
                    raise HTTPException(status_code=502, detail=f"process_failed:{reason}")

                onboarding_json: dict[str, Any] = {
                    "status": "skipped",
                    "reason": "onboarding_not_configured",
                }
                onboarding_resp = client.post(
                    f"{internal_api_base.rstrip('/')}/auth/v1/onboarding:ensure",
                    json={"email": ingest_payload.get("sender_email")},
                    headers=internal_headers,
                )
                if onboarding_resp.status_code < 400:
                    onboarding_json = onboarding_resp.json()
                else:
                    onboarding_json = {
                        "status": "failed",
                        "reason": onboarding_resp.text,
                    }
        except Exception as exc:
            dlq_store.append(
                {
                    "failure": str(exc),
                    "ingest_payload": ingest_payload,
                    "process_payload": envelope.process.model_dump(),
                }
            )
            raise

        notification = {
            "sent": False,
            "provider": "none",
            "reason": "not_applicable",
            "message_id": "",
        }
        if onboarding_json.get("needs_password_setup") and onboarding_json.get("magic_link_url"):
            result = outbound_notifier.send_onboarding_email(
                to_email=str(ingest_payload.get("sender_email", "")),
                magic_link_url=str(onboarding_json.get("magic_link_url")),
                package_id=str(ingest_json.get("package_id", "")),
                sender_email=str(ingest_payload.get("sender_email", "")),
            )
            notification = result.as_dict()

        return {
            "status": "accepted",
            "received_at": _utc_now(),
            "package": ingest_json,
            "processing": process_json,
            "onboarding": onboarding_json,
            "notification": notification,
        }

    @app.post("/inbound/v1/email")
    def inbound_email(
        envelope: InboundEnvelope,
        x_inbound_token: str | None = Header(default=None, alias="X-Inbound-Token"),
    ) -> dict[str, Any]:
        if strict_runtime:
            raise HTTPException(status_code=404, detail="endpoint_disabled_in_strict_profile")
        if inbound_token is not None and x_inbound_token != inbound_token:
            raise HTTPException(status_code=401, detail="invalid_inbound_token")
        return _forward_to_internal(envelope)

    @app.post("/inbound/v1/providers/postmark")
    def inbound_postmark(
        payload: PostmarkInboundPayload,
        x_postmark_server_token: str | None = Header(default=None, alias="X-Postmark-Server-Token"),
    ) -> dict[str, Any]:
        if postmark_server_token is not None and x_postmark_server_token != postmark_server_token:
            raise HTTPException(status_code=401, detail="invalid_postmark_token")

        sender = _sender_from_postmark(payload)
        message_id = str(payload.MessageID)
        accepted_attachments: list[dict[str, Any]] = []
        rejected_count = 0

        for idx, attachment in enumerate(payload.Attachments, start=1):
            raw = base64.b64decode(attachment.Content.encode("utf-8"), validate=True)
            storage_uri = attachment_store.write(
                message_id=message_id,
                filename=attachment.Name,
                content=raw,
                content_type=attachment.ContentType,
            )
            meta = _build_attachment_meta(
                idx=idx,
                filename=attachment.Name,
                content_type=attachment.ContentType,
                storage_uri=storage_uri,
                checksum=_sha256(raw),
            )
            if meta is None:
                rejected_count += 1
                continue
            accepted_attachments.append(meta)

        if not accepted_attachments:
            raise HTTPException(status_code=400, detail="no_supported_attachments")

        quality_flags: list[str] = []
        if rejected_count > 0:
            quality_flags.append(f"unsupported_attachments:{rejected_count}")

        envelope = InboundEnvelope(
            email=InboundEmailPayload(
                sender_email=sender,
                message_id=message_id,
                source_email_id=message_id,
                received_at=payload.Date or _utc_now(),
                attachments=accepted_attachments,
                quality_flags=quality_flags,
            )
        )
        return _forward_to_internal(envelope)

    @app.post("/inbound/v1/providers/mailgun")
    async def inbound_mailgun(
        request: Request,
        x_mailgun_signature: str | None = Header(default=None, alias="X-Mailgun-Signature"),
    ) -> dict[str, Any]:
        if strict_runtime:
            raise HTTPException(status_code=404, detail="endpoint_disabled_in_strict_profile")
        form = await request.form()

        timestamp = str(form.get("timestamp") or "")
        token = str(form.get("token") or "")
        signature = str(form.get("signature") or x_mailgun_signature or "")
        if mailgun_signing_key is not None:
            if not timestamp or not token or not signature:
                raise HTTPException(status_code=401, detail="missing_mailgun_signature")
            if not _validate_mailgun_timestamp(timestamp, mailgun_signature_tolerance_seconds):
                raise HTTPException(status_code=401, detail="stale_mailgun_signature")
            if not _validate_mailgun_signature(timestamp, token, signature, mailgun_signing_key):
                raise HTTPException(status_code=401, detail="invalid_mailgun_signature")

        sender = str(form.get("sender") or form.get("from") or "").strip()
        if not sender:
            raise HTTPException(status_code=400, detail="missing_sender")

        message_id = str(form.get("Message-Id") or form.get("message-id") or form.get("message_id") or f"mailgun_{uuid4().hex[:12]}")

        accepted_attachments: list[dict[str, Any]] = []
        rejected_count = 0

        idx = 1
        for key in sorted(form.keys()):
            value = form.get(key)
            if not hasattr(value, "filename") or not hasattr(value, "read"):
                continue

            upload = value
            filename = getattr(upload, "filename", None) or f"attachment_{idx}"
            content = await upload.read()
            storage_uri = attachment_store.write(
                message_id=message_id,
                filename=filename,
                content=content,
                content_type=getattr(upload, "content_type", None),
            )
            meta = _build_attachment_meta(
                idx=idx,
                filename=filename,
                content_type=getattr(upload, "content_type", None),
                storage_uri=storage_uri,
                checksum=_sha256(content),
            )
            idx += 1
            if meta is None:
                rejected_count += 1
                continue
            accepted_attachments.append(meta)

        if not accepted_attachments:
            raise HTTPException(status_code=400, detail="no_supported_attachments")

        quality_flags: list[str] = []
        if rejected_count > 0:
            quality_flags.append(f"unsupported_attachments:{rejected_count}")

        envelope = InboundEnvelope(
            email=InboundEmailPayload(
                sender_email=sender,
                message_id=message_id,
                source_email_id=message_id,
                received_at=_utc_now(),
                attachments=accepted_attachments,
                quality_flags=quality_flags,
            )
        )
        return _forward_to_internal(envelope)

    @app.post("/inbound/v1/providers/sendgrid")
    async def inbound_sendgrid_parse(
        request: Request,
        x_sendgrid_inbound_token: str | None = Header(default=None, alias="X-Sendgrid-Inbound-Token"),
    ) -> dict[str, Any]:
        if strict_runtime:
            raise HTTPException(status_code=404, detail="endpoint_disabled_in_strict_profile")
        if sendgrid_inbound_token is not None and x_sendgrid_inbound_token != sendgrid_inbound_token:
            raise HTTPException(status_code=401, detail="invalid_sendgrid_inbound_token")

        form = await request.form()
        sender = str(form.get("from") or "").strip()
        if not sender:
            raise HTTPException(status_code=400, detail="missing_sender")

        message_id = str(form.get("message_id") or form.get("Message-Id") or f"sendgrid_{uuid4().hex[:12]}")

        accepted_attachments: list[dict[str, Any]] = []
        rejected_count = 0
        idx = 1

        for key in sorted(form.keys()):
            value = form.get(key)
            if not hasattr(value, "filename") or not hasattr(value, "read"):
                continue

            upload = value
            filename = getattr(upload, "filename", None) or f"attachment_{idx}"
            content = await upload.read()
            storage_uri = attachment_store.write(
                message_id=message_id,
                filename=filename,
                content=content,
                content_type=getattr(upload, "content_type", None),
            )
            meta = _build_attachment_meta(
                idx=idx,
                filename=filename,
                content_type=getattr(upload, "content_type", None),
                storage_uri=storage_uri,
                checksum=_sha256(content),
            )
            idx += 1
            if meta is None:
                rejected_count += 1
                continue
            accepted_attachments.append(meta)

        if not accepted_attachments:
            raise HTTPException(status_code=400, detail="no_supported_attachments")

        quality_flags: list[str] = []
        if rejected_count > 0:
            quality_flags.append(f"unsupported_attachments:{rejected_count}")

        envelope = InboundEnvelope(
            email=InboundEmailPayload(
                sender_email=sender,
                message_id=message_id,
                source_email_id=message_id,
                received_at=_utc_now(),
                attachments=accepted_attachments,
                quality_flags=quality_flags,
            )
        )
        return _forward_to_internal(envelope)

    @app.get("/inbound/v1/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "time": _utc_now(),
            "internal_api_base": internal_api_base,
            "token_protected": inbound_token is not None,
            "mailbox_providers": {
                "postmark": postmark_server_token is not None,
                "mailgun": mailgun_signing_key is not None,
                "sendgrid": sendgrid_inbound_token is not None,
            },
            "outbound_notifier_mode": outbound_email_mode,
            "dlq_path": str(Path(dlq_path)),
            "runtime_profile": active_profile,
            "strict_runtime_profile": strict_runtime,
            "allowed_ingress": ["postmark"] if strict_runtime else ["direct", "postmark", "mailgun", "sendgrid"],
            "attachment_storage_mode": attachment_storage_mode,
            "attachment_storage_s3_bucket": attachment_storage_s3_bucket if attachment_storage_mode == "s3" else None,
        }

    return app
