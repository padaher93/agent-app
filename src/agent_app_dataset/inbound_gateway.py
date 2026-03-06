from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .email_adapter import normalized_email_to_ingest_request


class InboundEmailPayload(BaseModel):
    sender_email: str | None = None
    from_: str | None = Field(default=None, alias="from")
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
    extraction_mode: str = Field(default="runtime", pattern="^(runtime|eval)$")


class InboundEnvelope(BaseModel):
    email: InboundEmailPayload
    process: InboundProcessRequest = Field(default_factory=InboundProcessRequest)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_gateway_app(
    internal_api_base: str,
    inbound_token: str | None = None,
) -> FastAPI:
    app = FastAPI(title="Patricius Inbound Gateway", version="v1")

    @app.post("/inbound/v1/email")
    def inbound_email(
        envelope: InboundEnvelope,
        x_inbound_token: str | None = Header(default=None, alias="X-Inbound-Token"),
    ) -> dict[str, Any]:
        if inbound_token is not None and x_inbound_token != inbound_token:
            raise HTTPException(status_code=401, detail="invalid_inbound_token")

        email_payload = envelope.email.model_dump(by_alias=True, exclude_none=True)
        if "from" not in email_payload and envelope.email.sender_email:
            email_payload["from"] = envelope.email.sender_email

        ingest_payload = normalized_email_to_ingest_request(email_payload)

        with httpx.Client(timeout=30.0) as client:
            ingest_resp = client.post(
                f"{internal_api_base.rstrip('/')}/internal/v1/packages:ingest",
                json=ingest_payload,
            )
            if ingest_resp.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"ingest_failed:{ingest_resp.text}")
            ingest_json = ingest_resp.json()

            package_id = ingest_json["package_id"]
            process_resp = client.post(
                f"{internal_api_base.rstrip('/')}/internal/v1/packages/{package_id}:process",
                json=envelope.process.model_dump(),
            )
            if process_resp.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"process_failed:{process_resp.text}")
            process_json = process_resp.json()

        return {
            "status": "accepted",
            "received_at": _utc_now(),
            "package": ingest_json,
            "processing": process_json,
        }

    @app.get("/inbound/v1/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "time": _utc_now(),
            "internal_api_base": internal_api_base,
            "token_protected": inbound_token is not None,
        }

    return app
