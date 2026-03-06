from __future__ import annotations

from datetime import datetime, timezone
import hashlib


def _hour_bucket(value: str) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:00:00Z")


def build_idempotency_key(
    sender_email: str,
    received_at: str,
    files: list[dict],
) -> str:
    sender = sender_email.strip().lower()
    bucket = _hour_bucket(received_at)

    signatures = []
    for file in files:
        checksum = str(file.get("checksum", "")).strip()
        fallback = str(file.get("file_id", "")).strip()
        signatures.append(checksum or fallback)

    signatures = sorted(signatures)
    material = f"{sender}|{bucket}|{'|'.join(signatures)}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()
