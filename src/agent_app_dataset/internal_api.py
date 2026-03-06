from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .agent_workflow import append_events, check_log_integrity
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


class ResolveTraceRequest(BaseModel):
    resolver: str = "operator"
    selected_evidence: dict[str, Any] = Field(default_factory=dict)
    note: str = ""


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


def _derive_lifecycle_status_from_rows(rows: list[dict[str, Any]]) -> str:
    statuses = {row.get("status") for row in rows}
    if "unresolved" in statuses or "candidate_flagged" in statuses:
        return "needs_review"
    return "completed"


def _read_events(
    events_log_path: Path,
    package_id: str | None = None,
    trace_id: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    if not events_log_path.exists() or events_log_path.stat().st_size == 0:
        return []

    events: list[dict[str, Any]] = []
    with events_log_path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if package_id and event.get("package_id") != package_id:
                continue
            if trace_id and event.get("trace_id") != trace_id:
                continue
            events.append(event)

    if limit <= 0:
        return events
    return events[-limit:]


def create_app(
    db_path: Path,
    labels_dir: Path,
    events_log_path: Path,
    ui_dir: Path | None = None,
) -> FastAPI:
    executor = ThreadPoolExecutor(max_workers=4)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            executor.shutdown(wait=False)

    app = FastAPI(title="Patricius Internal API", version="v1", lifespan=lifespan)
    store = InternalStore(db_path)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if ui_dir is not None and ui_dir.exists():
        app.mount("/app", StaticFiles(directory=ui_dir, html=True), name="app")

        @app.get("/")
        def root() -> RedirectResponse:
            return RedirectResponse(url="/app/")

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
    def get_package(
        package_id: str,
        include_manifest: bool = Query(default=False),
        include_processed_payload: bool = Query(default=False),
    ) -> dict[str, Any]:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        response = _package_response(record)
        response["has_processed_payload"] = record.processed_payload is not None
        if include_manifest:
            response["package_manifest"] = record.package_manifest
        if include_processed_payload and record.processed_payload is not None:
            response["processed_payload"] = record.processed_payload
        return response

    @app.get("/internal/v1/packages/{package_id}/events")
    def get_package_events(package_id: str, limit: int = Query(default=500, ge=1, le=5000)) -> dict[str, Any]:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        events = _read_events(events_log_path, package_id=package_id, limit=limit)
        return {
            "package_id": package_id,
            "events": events,
            "count": len(events),
            "integrity_ok": not bool(check_log_integrity(events_log_path)),
        }

    @app.get("/internal/v1/deals")
    def list_deals() -> dict[str, Any]:
        packages = store.list_packages()
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for package in packages:
            grouped[package.deal_id].append(
                {
                    "package_id": package.package_id,
                    "period_end_date": package.period_end_date,
                    "status": package.status,
                    "received_at": package.received_at,
                }
            )

        deals = []
        for deal_id, periods in sorted(grouped.items(), key=lambda x: x[0]):
            periods_sorted = sorted(
                periods,
                key=lambda p: (p["period_end_date"], p["received_at"]),
                reverse=True,
            )
            deals.append(
                {
                    "deal_id": deal_id,
                    "periods": periods_sorted,
                    "latest_period_id": periods_sorted[0]["package_id"] if periods_sorted else None,
                    "period_count": len(periods_sorted),
                }
            )

        return {
            "deals": deals,
            "count": len(deals),
        }

    @app.get("/internal/v1/deals/{deal_id}/periods")
    def list_periods(deal_id: str) -> dict[str, Any]:
        packages = [pkg for pkg in store.list_packages() if pkg.deal_id == deal_id]
        if not packages:
            raise HTTPException(status_code=404, detail="deal_not_found")

        periods = [
            {
                "package_id": pkg.package_id,
                "period_end_date": pkg.period_end_date,
                "status": pkg.status,
                "received_at": pkg.received_at,
            }
            for pkg in sorted(packages, key=lambda p: (p.period_end_date, p.received_at), reverse=True)
        ]
        return {
            "deal_id": deal_id,
            "periods": periods,
            "count": len(periods),
        }

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

    @app.get("/internal/v1/traces/{trace_id}/events")
    def get_trace_events(trace_id: str, limit: int = Query(default=500, ge=1, le=5000)) -> dict[str, Any]:
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")

        events = _read_events(events_log_path, trace_id=trace_id, limit=limit)
        return {
            "trace_id": trace_id,
            "events": events,
            "count": len(events),
            "integrity_ok": not bool(check_log_integrity(events_log_path)),
        }

    @app.post("/internal/v1/traces/{trace_id}:resolve")
    def resolve_trace(trace_id: str, request: ResolveTraceRequest) -> dict[str, Any]:
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")

        package = store.get_package(trace["package_id"])
        if package is None:
            raise HTTPException(status_code=404, detail="package_not_found")

        if package.processed_payload is None:
            raise HTTPException(status_code=409, detail="package_not_processed")

        payload = json.loads(json.dumps(package.processed_payload))
        target_row: dict[str, Any] | None = None

        for package_item in payload.get("packages", []):
            if package_item.get("package_id") != package.package_id:
                continue
            for row in package_item.get("rows", []):
                if row.get("trace_id") == trace_id:
                    target_row = row
                    break
            if target_row is not None:
                break

        if target_row is None:
            raise HTTPException(status_code=404, detail="trace_row_not_found")

        previous_status = str(target_row.get("status", "unresolved"))
        previous_confidence = float(target_row.get("confidence", 0.0))
        target_row["status"] = "verified"
        target_row["confidence"] = max(previous_confidence, 0.95)
        target_row["user_resolution"] = {
            "resolver": request.resolver,
            "resolved_at": _utc_now(),
            "selected_evidence": request.selected_evidence,
            "note": request.note,
        }
        target_row["resolved_by_user"] = True

        verification = target_row.get("verification", {})
        attempts = list(verification.get("attempts", []))
        attempts.append(
            {
                "attempt": len(attempts),
                "status_before": verification.get("final_status", previous_status),
                "confidence_before": previous_confidence,
                "objections": [],
                "decision": "user_resolved",
                "status_after": "verified",
            }
        )
        target_row["verification"] = {
            "attempts": attempts,
            "retry_count": verification.get("retry_count", 0),
            "max_retries": verification.get("max_retries", 2),
            "final_status": "verified",
            "objections": verification.get("objections", []),
            "resolver": request.resolver,
        }

        row_list: list[dict[str, Any]] = []
        for package_item in payload.get("packages", []):
            if package_item.get("package_id") == package.package_id:
                row_list = package_item.get("rows", [])
                break

        lifecycle_status = _derive_lifecycle_status_from_rows(row_list)

        store.update_trace_row(trace_id, target_row)
        store.update_package_status(
            package_id=package.package_id,
            status=lifecycle_status,
            processed_payload=payload,
            error_message=None,
        )

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "user_resolved",
                    "phase": "publish",
                    "package_id": package.package_id,
                    "trace_id": trace_id,
                    "payload": {
                        "resolver": request.resolver,
                        "status": "verified",
                        "selected_evidence": request.selected_evidence,
                        "note": request.note,
                    },
                }
            ],
        )

        return {
            "trace_id": trace_id,
            "package_id": package.package_id,
            "deal_id": package.deal_id,
            "status": "verified",
            "package_status": lifecycle_status,
            "row": target_row,
        }

    @app.get("/internal/v1/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "time": _utc_now(),
            "events_log": str(events_log_path),
            "labels_dir": str(labels_dir),
            "ui_enabled": bool(ui_dir and ui_dir.exists()),
        }

    return app
