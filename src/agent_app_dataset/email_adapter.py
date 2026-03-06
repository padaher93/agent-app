from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalized_email_to_ingest_request(payload: dict[str, Any]) -> dict[str, Any]:
    sender = payload.get("from") or payload.get("sender_email")
    if not sender:
        raise ValueError("Missing sender email ('from' or 'sender_email')")

    attachments = payload.get("attachments")
    if not isinstance(attachments, list) or not attachments:
        raise ValueError("Email payload must include non-empty attachments[]")

    source_email_id = str(payload.get("source_email_id") or payload.get("message_id") or "email_external")
    deal_id = str(payload.get("deal_id") or "deal_inbound")
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
        "sender_email": str(sender),
        "source_email_id": source_email_id,
        "deal_id": deal_id,
        "period_end_date": period_end_date,
        "received_at": received_at,
        "files": files,
        "variant_tags": list(payload.get("variant_tags", [])),
        "quality_flags": list(payload.get("quality_flags", [])),
        "package_id": payload.get("package_id"),
    }
