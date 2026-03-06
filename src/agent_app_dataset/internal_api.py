from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .idempotency import build_idempotency_key
from .internal_processing import process_package_manifest
from .internal_store import InternalStore


class PackageFileInput(BaseModel):
    file_id: str
    source_id: str
    doc_type: str
    filename: str
    storage_uri: str
    checksum: str
    pages_or_sheets: int = Field(ge=1)


class PackageIngestRequest(BaseModel):
    sender_email: str
    source_email_id: str
    deal_id: str
    period_end_date: str
    received_at: str
    files: list[PackageFileInput] = Field(min_length=1, max_length=8)
    variant_tags: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)
    package_id: str | None = None


class ProcessRequest(BaseModel):
    async_mode: bool = True
    max_retries: int = Field(default=2, ge=0, le=5)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_package_id(idempotency_key: str) -> str:
    return f"pkg_int_{idempotency_key[:12]}"


def _build_manifest(payload: PackageIngestRequest, package_id: str) -> dict[str, Any]:
    files = [file.model_dump() for file in payload.files]
    return {
        "schema_version": "1.0",
        "package_id": package_id,
        "deal_id": payload.deal_id,
        "period_end_date": payload.period_end_date,
        "source_email_id": payload.source_email_id,
        "received_at": payload.received_at,
        "files": files,
        "source_ids": sorted({file["source_id"] for file in files}),
        "variant_tags": payload.variant_tags,
        "quality_flags": payload.quality_flags,
        "labeling_workflow": {
            "primary_labeler_status": "not_started",
            "reviewer_status": "not_started",
            "adjudication_status": "not_required",
        },
        "notes": "Ingested via internal API",
    }


def create_app(
    db_path: Path,
    labels_dir: Path,
    events_log_path: Path,
) -> FastAPI:
    app = FastAPI(title="Patricius Internal API", version="v1")
    store = InternalStore(db_path)
    executor = ThreadPoolExecutor(max_workers=4)

    def _package_response(record: Any) -> dict[str, Any]:
        return {
            "package_id": record.package_id,
            "idempotency_key": record.idempotency_key,
            "sender_email": record.sender_email,
            "source_email_id": record.source_email_id,
            "deal_id": record.deal_id,
            "period_end_date": record.period_end_date,
            "received_at": record.received_at,
            "status": record.status,
            "error_message": record.error_message,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }

    def _process(package_id: str, max_retries: int) -> None:
        record = store.get_package(package_id)
        if not record:
            return

        try:
            store.update_package_status(package_id, status="processing")
            payload, summary = process_package_manifest(
                package_manifest=record.package_manifest,
                labels_dir=labels_dir,
                events_log_path=events_log_path,
                max_retries=max_retries,
            )
            rows = payload["packages"][0]["rows"]
            store.upsert_traces(
                package_id=package_id,
                deal_id=record.deal_id,
                period_id=package_id,
                rows=rows,
            )
            store.update_package_status(
                package_id=package_id,
                status=summary["status"],
                processed_payload=payload,
            )
        except Exception as exc:  # pragma: no cover - defensive runtime path
            store.update_package_status(
                package_id=package_id,
                status="failed",
                error_message=str(exc),
            )

    @app.post("/internal/v1/packages:ingest")
    def ingest_package(request: PackageIngestRequest) -> dict[str, Any]:
        idempotency_key = build_idempotency_key(
            sender_email=request.sender_email,
            received_at=request.received_at,
            files=[file.model_dump() for file in request.files],
        )

        package_id = request.package_id or _build_package_id(idempotency_key)
        manifest = _build_manifest(request, package_id)

        record, created = store.upsert_package(
            package_id=package_id,
            idempotency_key=idempotency_key,
            sender_email=request.sender_email,
            source_email_id=request.source_email_id,
            deal_id=request.deal_id,
            period_end_date=request.period_end_date,
            received_at=request.received_at,
            status="received",
            package_manifest=manifest,
        )

        response = _package_response(record)
        response["created"] = created
        return response

    @app.post("/internal/v1/packages/{package_id}:process")
    def process_package(package_id: str, request: ProcessRequest) -> dict[str, Any]:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")

        if record.status == "processing":
            return {
                "package_id": package_id,
                "status": "processing",
                "accepted": False,
                "message": "already_processing",
            }

        store.update_package_status(package_id, status="processing")

        if request.async_mode:
            executor.submit(_process, package_id, request.max_retries)
            return {
                "package_id": package_id,
                "status": "processing",
                "accepted": True,
                "mode": "async",
            }

        _process(package_id, request.max_retries)
        updated = store.get_package(package_id)
        if not updated:
            raise HTTPException(status_code=500, detail="package_lost")
        response = _package_response(updated)
        response["accepted"] = True
        response["mode"] = "sync"
        return response

    @app.get("/internal/v1/packages/{package_id}")
    def get_package(package_id: str) -> dict[str, Any]:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        response = _package_response(record)
        response["has_processed_payload"] = record.processed_payload is not None
        return response

    @app.get("/internal/v1/deals/{deal_id}/periods/{period_id}/delta")
    def get_delta(deal_id: str, period_id: str) -> dict[str, Any]:
        delta = store.get_delta(deal_id=deal_id, period_id=period_id)
        if delta is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        return delta

    @app.get("/internal/v1/traces/{trace_id}")
    def get_trace(trace_id: str) -> dict[str, Any]:
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")
        return trace

    @app.get("/internal/v1/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "time": _utc_now(),
            "events_log": str(events_log_path),
            "labels_dir": str(labels_dir),
        }

    return app
