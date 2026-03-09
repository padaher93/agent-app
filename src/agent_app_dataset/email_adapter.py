from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from typing import Any


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_sender(sender: str) -> str:
    return str(sender).strip().lower()


def _default_deal_id(sender: str) -> str:
    normalized = _normalize_sender(sender)
    local_part = normalized.split("@", 1)[0] if "@" in normalized else normalized
    clean_local = re.sub(r"[^a-z0-9]+", "_", local_part).strip("_") or "sender"
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
    return f"deal_{clean_local}_{digest}"


def normalized_email_to_ingest_request(payload: dict[str, Any]) -> dict[str, Any]:
    sender = payload.get("from") or payload.get("sender_email")
    if not sender:
        raise ValueError("Missing sender email ('from' or 'sender_email')")
    normalized_sender = _normalize_sender(str(sender))

    attachments = payload.get("attachments")
    if not isinstance(attachments, list) or not attachments:
        raise ValueError("Email payload must include non-empty attachments[]")

    source_email_id = str(payload.get("source_email_id") or payload.get("message_id") or "email_external")
    workspace_id = str(payload.get("workspace_id") or "ws_default")
    deal_id = str(payload.get("deal_id") or _default_deal_id(normalized_sender))
    period_end_date = str(payload.get("period_end_date") or datetime.now(timezone.utc).date().isoformat())
    received_at = str(payload.get("received_at") or _now_utc())

    files = []
    for idx, attachment in enumerate(attachments, start=1):
        files.append(
            {
                "file_id": str(attachment.get("file_id") or f"file_ext_{idx:02d}"),
                "source_id": str(attachment.get("source_id") or f"src_ext_{idx:04d}"),
                "doc_type": str(attachment.get("doc_type") or "PDF"),
                "filename": str(attachment.get("filename") or f"attachment_{idx}.pdf"),
                "storage_uri": str(attachment.get("storage_uri") or f"s3://external/{idx}"),
                "checksum": str(attachment.get("checksum") or f"checksum_{idx:04d}"),
                "pages_or_sheets": int(attachment.get("pages_or_sheets") or 1),
            }
        )

    return {
        "workspace_id": workspace_id,
        "sender_email": normalized_sender,
        "source_email_id": source_email_id,
        "deal_id": deal_id,
        "period_end_date": period_end_date,
        "received_at": received_at,
        "files": files,
        "variant_tags": list(payload.get("variant_tags", [])),
        "quality_flags": list(payload.get("quality_flags", [])),
        "package_id": payload.get("package_id"),
    }
