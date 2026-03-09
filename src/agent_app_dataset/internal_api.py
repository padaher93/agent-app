from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any, Literal

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .ai_takeaways import TakeawaysGenerationError, build_period_takeaways
from .auth import expires_in, generate_token, hash_password, hash_token, utc_iso_now, verify_password
from .agent_workflow import append_events, check_log_integrity
from .constants import CONCEPT_LABELS, STARTER_CONCEPT_IDS
from .evidence_preview import (
    build_document_locator_preview,
    build_evidence_preview,
    resolve_storage_uri,
)
from .idempotency import build_idempotency_key
from .internal_processing import process_package_manifest
from .internal_store import InternalStore
from .reporting_obligation_candidates import (
    CANDIDATE_SUPPORTED_CONCEPTS,
    GROUNDED_SUPPORTED_CONCEPTS,
    discover_reporting_obligation_candidates,
    summarize_candidate_states,
)
from .reporting_obligations import (
    SUPPORTED_REPORTING_OBLIGATION_CONCEPTS,
    extract_reporting_obligations,
)
from .review_queue import build_review_queue_payload
from .runtime_profile import (
    RuntimeProfile,
    is_strict_runtime_profile,
    resolve_runtime_profile,
    validate_internal_api_runtime_requirements,
)
from .storage import is_supported_storage_uri


class PackageFileInput(BaseModel):
    file_id: str
    source_id: str
    doc_type: Literal["PDF", "XLSX"]
    filename: str
    storage_uri: str
    checksum: str
    pages_or_sheets: int = Field(ge=1)


class ReportingObligationDocInput(BaseModel):
    doc_id: str
    doc_type: Literal["PDF", "XLSX"]
    filename: str
    storage_uri: str
    checksum: str = ""
    pages_or_sheets: int = Field(default=1, ge=1)


class PackageIngestRequest(BaseModel):
    workspace_id: str = Field(default="ws_default", min_length=1)
    sender_email: str
    source_email_id: str
    deal_id: str
    period_end_date: str
    received_at: str
    files: list[PackageFileInput] = Field(min_length=1, max_length=8)
    variant_tags: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)
    package_id: str | None = None


class ReportingObligationIngestRequest(BaseModel):
    docs: list[ReportingObligationDocInput] = Field(min_length=1, max_length=20)
    clear_existing_for_docs: bool = True
    llm_discovery: Literal["auto", "on", "off"] = "auto"


class ProcessRequest(BaseModel):
    async_mode: bool = True
    max_retries: int = Field(default=2, ge=0, le=5)
    extraction_mode: str = Field(default="llm", pattern="^(runtime|eval|llm)$")


class ResolveTraceRequest(BaseModel):
    resolver: str = "operator"
    selected_evidence: dict[str, Any] = Field(default_factory=dict)
    note: str = ""


class ReviewCaseFeedbackRequest(BaseModel):
    action_id: str = Field(min_length=1, max_length=120)
    outcome: Literal["confirmed", "dismissed", "expected_noise", "borrower_followup"]
    actor: str = Field(default="operator_ui", min_length=1, max_length=120)
    note: str = Field(default="", max_length=2000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DraftWorkflowEventRequest(BaseModel):
    event_type: Literal["draft_opened", "draft_prepared", "draft_edited", "draft_copied", "draft_closed"]
    actor: str = Field(default="operator_ui", min_length=1, max_length=120)
    subject: str = Field(default="", max_length=500)
    draft_text: str = Field(default="", max_length=20000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AnalystNoteUpsertRequest(BaseModel):
    actor: str = Field(default="operator_ui", min_length=1, max_length=120)
    subject: str = Field(default="", max_length=500)
    note_text: str = Field(min_length=1, max_length=20000)
    memo_ready: bool = False
    export_ready: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class UpdateDealRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=120)


class CreateDealConceptOverride(BaseModel):
    concept_id: str = Field(min_length=1, max_length=120)
    selected: bool = True


class CreateDealRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=120)
    deal_id: str | None = Field(default=None, min_length=1, max_length=120)
    template_id: str | None = Field(default=None, min_length=1, max_length=120)
    concept_overrides: list[CreateDealConceptOverride] = Field(default_factory=list)
    reporting_requirement_docs: list[ReportingObligationDocInput] = Field(default_factory=list, max_length=20)
    reporting_requirement_llm_discovery: Literal["auto", "on", "off"] = "auto"


class ReassignPackageRequest(BaseModel):
    target_deal_id: str = Field(min_length=1, max_length=120)
    actor: str = "operator_ui"
    note: str = ""


class OnboardingEnsureRequest(BaseModel):
    email: str


class MagicLinkRequest(BaseModel):
    email: str


class MagicLinkConsumeRequest(BaseModel):
    token: str = Field(min_length=10)
    password: str = Field(min_length=8, max_length=200)


class LoginRequest(BaseModel):
    email: str
    password: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_package_id(idempotency_key: str) -> str:
    return f"pkg_int_{idempotency_key[:12]}"


def _build_manifest(payload: PackageIngestRequest, package_id: str) -> dict[str, Any]:
    files = [file.model_dump() for file in payload.files]
    return {
        "schema_version": "1.0",
        "package_id": package_id,
        "workspace_id": payload.workspace_id,
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


def _assert_role_can_resolve(role: str) -> None:
    normalized = role.strip().lower()
    if normalized not in {"owner", "operator"}:
        raise HTTPException(status_code=403, detail="insufficient_role")


def _workspace_for_record(record: Any) -> str:
    manifest = getattr(record, "package_manifest", {}) or {}
    return str(manifest.get("workspace_id", "ws_default"))


def _assert_workspace_access(record: Any, workspace_id: str) -> None:
    if _workspace_for_record(record) != workspace_id:
        raise HTTPException(status_code=404, detail="resource_not_found")


def _packages_for_deal_workspace(store: InternalStore, deal_id: str, workspace_id: str) -> list[Any]:
    return [
        pkg for pkg in store.list_packages()
        if pkg.deal_id == deal_id and _workspace_for_record(pkg) == workspace_id
    ]


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


def _public_user(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "email": user.get("email"),
        "status": user.get("status"),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
        "last_login_at": user.get("last_login_at"),
        "has_password": bool(user.get("password_hash")),
    }


def _bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="missing_authorization")
    value = authorization.strip()
    if not value.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="invalid_authorization_scheme")
    token = value[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing_bearer_token")
    return token


def _resolve_actor_identity(
    *,
    store: InternalStore,
    authorization: str | None,
    requested_actor: str,
) -> str:
    fallback = str(requested_actor).strip() or "operator_ui"
    if not authorization:
        return fallback
    try:
        token = _bearer_token(authorization)
    except HTTPException:
        return fallback
    session = store.get_session(hash_token(token), now_iso=utc_iso_now())
    if session is None:
        return fallback
    email = str(session.get("email", "")).strip()
    return email or fallback


def create_app(
    db_path: Path,
    labels_dir: Path,
    events_log_path: Path,
    ui_dir: Path | None = None,
    public_base_url: str = "http://127.0.0.1:8080",
    internal_token: str | None = None,
    require_https: bool = False,
    encryption_key: str | None = None,
    runtime_profile: RuntimeProfile | str = "dev",
    reporting_obligation_llm_client: Any | None = None,
) -> FastAPI:
    active_profile = resolve_runtime_profile(str(runtime_profile))
    strict_runtime = is_strict_runtime_profile(active_profile)
    validate_internal_api_runtime_requirements(
        runtime_profile=active_profile,
        internal_token=internal_token,
        require_https=require_https,
        openai_api_key=os.getenv("OPENAI_API_KEY"),
    )

    executor = ThreadPoolExecutor(max_workers=4)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            executor.shutdown(wait=False)

    app = FastAPI(title="Patricius Internal API", version="v1", lifespan=lifespan)
    store = InternalStore(db_path, encryption_key=encryption_key)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def _security_guard(request: Request, call_next):  # type: ignore[no-untyped-def]
        if internal_token is not None:
            token = request.headers.get("X-Internal-Token")
            if token != internal_token:
                return JSONResponse(status_code=401, content={"detail": "invalid_internal_token"})

        if require_https:
            forwarded_proto = request.headers.get("X-Forwarded-Proto", "http")
            if forwarded_proto.lower() != "https":
                return JSONResponse(status_code=400, content={"detail": "https_required"})

        return await call_next(request)

    if ui_dir is not None and ui_dir.exists():
        app.mount("/app", StaticFiles(directory=ui_dir, html=True), name="app")

        @app.get("/")
        def root() -> RedirectResponse:
            return RedirectResponse(url="/app/")

        @app.get("/delta-review")
        def delta_review() -> FileResponse:
            review_page = ui_dir / "delta-review.html"
            if not review_page.exists():
                raise HTTPException(status_code=404, detail="delta_review_not_found")
            return FileResponse(path=str(review_page))

    def _package_response(record: Any) -> dict[str, Any]:
        workspace_id = _workspace_for_record(record)
        return {
            "package_id": record.package_id,
            "workspace_id": workspace_id,
            "idempotency_key": record.idempotency_key,
            "sender_email": record.sender_email,
            "source_email_id": record.source_email_id,
            "deal_id": record.deal_id,
            "period_end_date": record.period_end_date,
            "received_at": record.received_at,
            "period_revision": record.period_revision,
            "status": record.status,
            "error_message": record.error_message,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }

    def _normalize_deal_identifier(display_name: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "_", str(display_name).strip().lower())
        normalized = normalized.strip("_")
        if not normalized:
            normalized = "deal"
        return normalized[:80]

    def _humanize_deal_label(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        normalized = text.replace("-", " ").replace("_", " ").strip()
        if not normalized:
            return text
        if normalized.islower() or normalized.isupper():
            normalized = " ".join(part.capitalize() for part in normalized.split())
        return normalized

    def _next_available_deal_id(base_name: str) -> str:
        base_slug = _normalize_deal_identifier(base_name)
        candidate = f"deal_{base_slug}"
        if store.get_deal_meta(candidate) is None:
            return candidate
        suffix = 2
        while True:
            candidate = f"deal_{base_slug}_{suffix}"
            if store.get_deal_meta(candidate) is None:
                return candidate
            suffix += 1

    def _ensure_deal_workspace_membership(deal_id: str, workspace_id: str) -> None:
        if store.get_deal_meta(deal_id) is None:
            store.ensure_deal(deal_id)
        store.assign_deal_workspace(deal_id=deal_id, workspace_id=workspace_id)

    def _unresolvable_manifest_files(manifest: dict[str, Any]) -> list[str]:
        unresolved: list[str] = []
        for file_meta in manifest.get("files", []):
            storage_uri = str(file_meta.get("storage_uri", ""))
            if resolve_storage_uri(storage_uri) is not None:
                continue
            file_id = str(file_meta.get("file_id", "")).strip() or "unknown_file"
            unresolved.append(file_id)
        return unresolved

    def _ingest_reporting_obligation_docs(
        *,
        deal_id: str,
        docs: list[dict[str, Any]],
        clear_existing_for_docs: bool,
        llm_discovery_mode: str,
        workspace_id: str,
        emit_event_type: str,
    ) -> dict[str, Any]:
        deterministic_extracted = extract_reporting_obligations(
            deal_id=deal_id,
            docs=docs,
        )

        llm_mode = str(llm_discovery_mode).strip().lower()
        llm_discovery_attempted = False
        llm_discovery_status = "skipped"
        llm_discovery_error = ""
        llm_discovery_model = ""
        candidate_rows: list[dict[str, Any]] = []
        promoted_from_candidates: list[dict[str, Any]] = []

        should_run_llm = False
        if llm_mode == "on":
            should_run_llm = True
        elif llm_mode == "auto":
            should_run_llm = bool(reporting_obligation_llm_client is not None or os.getenv("OPENAI_API_KEY"))

        if should_run_llm:
            llm_discovery_attempted = True
            try:
                candidate_result = discover_reporting_obligation_candidates(
                    deal_id=deal_id,
                    docs=docs,
                    llm_client=reporting_obligation_llm_client,
                )
                llm_discovery_status = "completed"
                llm_discovery_model = str(candidate_result.get("model_name", "")).strip()
                candidate_rows = [
                    row for row in candidate_result.get("candidates", [])
                    if isinstance(row, dict)
                ]
                promoted_from_candidates = [
                    row for row in candidate_result.get("promoted_obligations", [])
                    if isinstance(row, dict)
                ]
            except Exception as exc:
                llm_discovery_error = str(exc)
                llm_discovery_status = "failed"
                if llm_mode == "on":
                    raise HTTPException(status_code=400, detail=f"llm_discovery_failed:{llm_discovery_error}")
        elif llm_mode == "off":
            llm_discovery_status = "disabled"
        else:
            llm_discovery_status = "unavailable"

        merged_obligations_by_id: dict[str, dict[str, Any]] = {}
        for row in deterministic_extracted:
            if not isinstance(row, dict):
                continue
            obligation_id = str(row.get("obligation_id", "")).strip()
            if not obligation_id:
                continue
            merged_obligations_by_id[obligation_id] = row
        for row in promoted_from_candidates:
            if not isinstance(row, dict):
                continue
            obligation_id = str(row.get("obligation_id", "")).strip()
            if not obligation_id:
                continue
            merged_obligations_by_id.setdefault(obligation_id, row)
        extracted = list(merged_obligations_by_id.values())

        clear_doc_ids = [str(doc["doc_id"]).strip() for doc in docs] if clear_existing_for_docs else None
        candidate_upsert = store.upsert_reporting_obligation_candidates(
            deal_id=deal_id,
            candidates=candidate_rows,
            clear_doc_ids=clear_doc_ids,
        )
        upsert = store.upsert_reporting_obligations(
            deal_id=deal_id,
            obligations=extracted,
            clear_doc_ids=clear_doc_ids,
        )

        candidate_catalog = store.list_reporting_obligation_candidates(deal_id=deal_id)
        candidate_summary = summarize_candidate_states(candidate_catalog)
        catalog = store.list_reporting_obligations(deal_id=deal_id)
        grounded = sum(1 for row in catalog if str(row.get("grounding_state", "")) == "grounded")
        ambiguous = sum(1 for row in catalog if str(row.get("grounding_state", "")) == "ambiguous")
        unsupported = sum(1 for row in catalog if str(row.get("grounding_state", "")) == "unsupported")

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": emit_event_type,
                    "phase": "publish",
                    "package_id": "",
                    "trace_id": "",
                    "payload": {
                        "deal_id": deal_id,
                        "workspace_id": workspace_id,
                        "docs_received": len(docs),
                        "deterministic_obligations_extracted": len(deterministic_extracted),
                        "candidate_discovery_attempted": llm_discovery_attempted,
                        "candidate_discovery_status": llm_discovery_status,
                        "candidate_rows_extracted": len(candidate_rows),
                        "candidate_rows_promoted": len(promoted_from_candidates),
                        "obligations_extracted": len(extracted),
                        "grounded_extracted": sum(
                            1
                            for row in extracted
                            if str(row.get("grounding_state", "")) == "grounded"
                        ),
                        "clear_existing_for_docs": bool(clear_existing_for_docs),
                    },
                }
            ],
        )

        return {
            "docs_received": len(docs),
            "deterministic_obligations_extracted": len(deterministic_extracted),
            "obligations_extracted": len(extracted),
            "grounded_extracted": sum(
                1
                for row in extracted
                if str(row.get("grounding_state", "")) == "grounded"
            ),
            "upsert": upsert,
            "candidate_upsert": candidate_upsert,
            "candidate_discovery": {
                "mode": llm_mode,
                "attempted": llm_discovery_attempted,
                "status": llm_discovery_status,
                "model_name": llm_discovery_model,
                "error": llm_discovery_error,
                "summary": candidate_summary,
            },
            "catalog": {
                "total": len(catalog),
                "grounded": grounded,
                "ambiguous": ambiguous,
                "unsupported": unsupported,
            },
            "supported_concepts": list(SUPPORTED_REPORTING_OBLIGATION_CONCEPTS),
            "candidate_supported_concepts": list(CANDIDATE_SUPPORTED_CONCEPTS),
            "grounded_supported_concepts": list(GROUNDED_SUPPORTED_CONCEPTS),
        }

    def _process(package_id: str, max_retries: int, extraction_mode: str) -> None:
        record = store.get_package(package_id)
        if not record:
            return

        try:
            if strict_runtime and extraction_mode != "llm":
                raise ValueError("extraction_mode_not_allowed_in_strict_profile")

            if strict_runtime:
                unresolved_files = _unresolvable_manifest_files(record.package_manifest)
                if unresolved_files:
                    joined = ",".join(unresolved_files)
                    raise ValueError(f"unresolvable_evidence_storage:{joined}")

            store.update_package_status(package_id, status="processing")
            payload, summary = process_package_manifest(
                package_manifest=record.package_manifest,
                labels_dir=labels_dir,
                events_log_path=events_log_path,
                max_retries=max_retries,
                extraction_mode=extraction_mode,
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
        if strict_runtime:
            unresolved_files = [
                file.file_id
                for file in request.files
                if not is_supported_storage_uri(file.storage_uri)
            ]
            if unresolved_files:
                joined = ",".join(sorted(set(unresolved_files)))
                raise HTTPException(status_code=400, detail=f"unsupported_storage_uri:{joined}")

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
        _ensure_deal_workspace_membership(request.deal_id, request.workspace_id)

        response = _package_response(record)
        response["created"] = created
        return response

    @app.post("/internal/v1/packages/{package_id}:process")
    def process_package(
        package_id: str,
        request: ProcessRequest,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if strict_runtime and request.extraction_mode != "llm":
            raise HTTPException(status_code=400, detail="extraction_mode_not_allowed_in_strict_profile")

        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(record, x_workspace_id)

        if record.status == "processing":
            return {
                "package_id": package_id,
                "status": "processing",
                "accepted": False,
                "message": "already_processing",
            }

        store.update_package_status(package_id, status="processing")

        if request.async_mode:
            executor.submit(_process, package_id, request.max_retries, request.extraction_mode)
            return {
                "package_id": package_id,
                "status": "processing",
                "accepted": True,
                "mode": "async",
            }

        _process(package_id, request.max_retries, request.extraction_mode)
        updated = store.get_package(package_id)
        if not updated:
            raise HTTPException(status_code=500, detail="package_lost")
        if updated.status == "failed":
            raise HTTPException(status_code=500, detail=updated.error_message or "processing_failed")
        response = _package_response(updated)
        response["accepted"] = True
        response["mode"] = "sync"
        return response

    @app.get("/internal/v1/packages/{package_id}")
    def get_package(
        package_id: str,
        include_manifest: bool = Query(default=False),
        include_processed_payload: bool = Query(default=False),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(record, x_workspace_id)
        response = _package_response(record)
        response["has_processed_payload"] = record.processed_payload is not None
        if include_manifest:
            response["package_manifest"] = record.package_manifest
        if include_processed_payload and record.processed_payload is not None:
            response["processed_payload"] = record.processed_payload
        return response

    @app.get("/internal/v1/packages/{package_id}/files/{file_id}:download")
    def download_package_file(
        package_id: str,
        file_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> FileResponse:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(record, x_workspace_id)

        file_meta = None
        for item in record.package_manifest.get("files", []):
            if str(item.get("file_id", "")) == file_id:
                file_meta = item
                break

        if file_meta is None:
            raise HTTPException(status_code=404, detail="file_not_found")

        resolved = resolve_storage_uri(str(file_meta.get("storage_uri", "")))
        if resolved is None:
            raise HTTPException(status_code=404, detail="file_unavailable")

        doc_type = str(file_meta.get("doc_type", "")).upper()
        media_type = "application/octet-stream"
        if doc_type == "PDF":
            media_type = "application/pdf"
        elif doc_type == "XLSX":
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        filename = str(file_meta.get("filename", resolved.name))
        return FileResponse(path=str(resolved), media_type=media_type, filename=filename)

    @app.get("/internal/v1/packages/{package_id}/files/{file_id}/evidence-preview")
    def preview_package_file_locator(
        package_id: str,
        file_id: str,
        locator_type: str = Query(default="paragraph"),
        locator_value: str = Query(default=""),
        page_or_sheet: str = Query(default=""),
        source_snippet: str = Query(default=""),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(record, x_workspace_id)

        file_meta = None
        for item in record.package_manifest.get("files", []):
            if str(item.get("file_id", "")).strip() == str(file_id).strip():
                file_meta = item
                break
        if file_meta is None:
            raise HTTPException(status_code=404, detail="file_not_found")

        evidence_preview = build_document_locator_preview(
            doc_id=str(file_id),
            doc_type=str(file_meta.get("doc_type", "")).upper(),
            filename=str(file_meta.get("filename", "")),
            storage_uri=str(file_meta.get("storage_uri", "")),
            locator_type=str(locator_type),
            locator_value=str(locator_value),
            page_or_sheet=str(page_or_sheet),
            source_snippet=str(source_snippet),
        )
        evidence_preview["package_id"] = record.package_id
        evidence_preview["download_url"] = (
            f"/internal/v1/packages/{record.package_id}/files/{file_id}:download"
        )

        return {
            "package_id": record.package_id,
            "deal_id": record.deal_id,
            "doc_id": str(file_id),
            "evidence_preview": evidence_preview,
        }

    @app.get("/internal/v1/packages/{package_id}/events")
    def get_package_events(
        package_id: str,
        limit: int = Query(default=500, ge=1, le=5000),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        record = store.get_package(package_id)
        if not record:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(record, x_workspace_id)
        events = _read_events(events_log_path, package_id=package_id, limit=limit)
        return {
            "package_id": package_id,
            "events": events,
            "count": len(events),
            "integrity_ok": not bool(check_log_integrity(events_log_path)),
        }

    def _issue_magic_link(email: str) -> tuple[str, str]:
        token = generate_token()
        token_hash = hash_token(token)
        expires_at = expires_in(30)
        store.create_magic_link(email=email, token_hash=token_hash, expires_at=expires_at)
        return token, expires_at

    @app.post("/auth/v1/onboarding:ensure")
    def ensure_onboarding(request: OnboardingEnsureRequest) -> dict[str, Any]:
        existing = store.get_user(request.email)
        created = existing is None
        user = store.ensure_user(request.email)

        if user.get("password_hash"):
            return {
                "email": user["email"],
                "created": created,
                "needs_password_setup": False,
                "magic_link_url": None,
            }

        token, expires_at = _issue_magic_link(user["email"])
        return {
            "email": user["email"],
            "created": created,
            "needs_password_setup": True,
            "magic_link_url": f"{public_base_url.rstrip('/')}/app/?magic_token={token}",
            "expires_at": expires_at,
        }

    @app.post("/auth/v1/magic-link/request")
    def request_magic_link(request: MagicLinkRequest) -> dict[str, Any]:
        user = store.ensure_user(request.email)
        token, expires_at = _issue_magic_link(user["email"])
        return {
            "status": "issued",
            "email": user["email"],
            "magic_link_url": f"{public_base_url.rstrip('/')}/app/?magic_token={token}",
            "expires_at": expires_at,
        }

    @app.post("/auth/v1/magic-link/consume")
    def consume_magic_link(request: MagicLinkConsumeRequest) -> dict[str, Any]:
        consumed = store.consume_magic_link(
            token_hash=hash_token(request.token),
            consumed_at=utc_iso_now(),
        )
        if consumed is None:
            raise HTTPException(status_code=401, detail="invalid_or_expired_magic_link")

        try:
            password_hash = hash_password(request.password)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        try:
            user = store.set_user_password(consumed["email"], password_hash=password_hash)
        except KeyError:
            raise HTTPException(status_code=404, detail="user_not_found")

        session_token = generate_token()
        session_expires = expires_in(60 * 24 * 14)
        store.create_session(
            email=user["email"],
            token_hash=hash_token(session_token),
            expires_at=session_expires,
        )

        return {
            "status": "authenticated",
            "user": _public_user(user),
            "session_token": session_token,
            "expires_at": session_expires,
        }

    @app.post("/auth/v1/login")
    def login(request: LoginRequest) -> dict[str, Any]:
        user = store.get_user(request.email)
        if user is None or not user.get("password_hash"):
            raise HTTPException(status_code=401, detail="invalid_credentials")
        if not verify_password(request.password, str(user["password_hash"])):
            raise HTTPException(status_code=401, detail="invalid_credentials")

        session_token = generate_token()
        session_expires = expires_in(60 * 24 * 14)
        store.create_session(
            email=user["email"],
            token_hash=hash_token(session_token),
            expires_at=session_expires,
        )
        refreshed_user = store.get_user(request.email) or user
        return {
            "status": "authenticated",
            "user": _public_user(refreshed_user),
            "session_token": session_token,
            "expires_at": session_expires,
        }

    @app.get("/auth/v1/me")
    def me(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
        session_token = _bearer_token(authorization)
        session = store.get_session(hash_token(session_token), now_iso=utc_iso_now())
        if session is None:
            raise HTTPException(status_code=401, detail="invalid_session")

        user = store.get_user(session["email"])
        if user is None:
            raise HTTPException(status_code=401, detail="invalid_session")

        return {
            "status": "ok",
            "user": _public_user(user),
            "session_expires_at": session["expires_at"],
        }

    @app.post("/auth/v1/logout")
    def logout(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
        session_token = _bearer_token(authorization)
        store.revoke_session(hash_token(session_token))
        return {"status": "logged_out"}

    @app.get("/internal/v1/deals")
    def list_deals(x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id")) -> dict[str, Any]:
        packages = [pkg for pkg in store.list_packages() if _workspace_for_record(pkg) == x_workspace_id]
        for package in packages:
            _ensure_deal_workspace_membership(package.deal_id, x_workspace_id)
        active_deals = {
            item["deal_id"]: item
            for item in store.list_deals_for_workspace(
                workspace_id=x_workspace_id,
                include_archived=False,
            )
        }
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for package in packages:
            if package.deal_id not in active_deals:
                continue
            grouped[package.deal_id].append(
                {
                    "package_id": package.package_id,
                    "period_end_date": package.period_end_date,
                    "period_revision": package.period_revision,
                    "status": package.status,
                    "received_at": package.received_at,
                }
            )

        deals = []
        for deal_id in sorted(active_deals.keys()):
            periods = grouped.get(deal_id, [])
            periods_sorted = sorted(
                periods,
                key=lambda p: (p["period_end_date"], p.get("period_revision", 1), p["received_at"]),
                reverse=True,
            )
            meta = active_deals.get(deal_id, {"display_name": deal_id, "archived": False})
            raw_display_name = str(meta.get("display_name", "")).strip()
            if not raw_display_name or raw_display_name == deal_id:
                display_name = _humanize_deal_label(deal_id) or deal_id
            else:
                display_name = raw_display_name
            create_config = store.get_deal_create_config(deal_id)
            deals.append(
                {
                    "deal_id": deal_id,
                    "display_name": display_name,
                    "archived": bool(meta.get("archived", False)),
                    "periods": periods_sorted,
                    "latest_period_id": periods_sorted[0]["package_id"] if periods_sorted else None,
                    "period_count": len(periods_sorted),
                    "template_id": create_config.get("template_id") if create_config else None,
                    "forwarding_address": create_config.get("forwarding_address") if create_config else None,
                    "quick_instruction": create_config.get("quick_instruction") if create_config else None,
                }
            )

        return {
            "deals": deals,
            "count": len(deals),
        }

    @app.post("/internal/v1/deals")
    def create_deal(
        request: CreateDealRequest,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        display_name = request.display_name.strip()
        if not display_name:
            raise HTTPException(status_code=400, detail="display_name_required")

        requested_id = (request.deal_id or "").strip().lower()
        if requested_id:
            normalized_id = _normalize_deal_identifier(requested_id)
            if not normalized_id.startswith("deal_"):
                normalized_id = f"deal_{normalized_id}"
            deal_id = normalized_id
        else:
            deal_id = _next_available_deal_id(display_name)

        existing = store.get_deal_meta(deal_id)
        if existing is not None and not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=409, detail="deal_id_conflict")
        if existing is not None and store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=409, detail="deal_already_exists")

        store.ensure_deal(deal_id=deal_id, display_name=display_name)
        store.assign_deal_workspace(deal_id=deal_id, workspace_id=x_workspace_id)
        inbound_address = str(os.getenv("INBOUND_ROUTING_EMAIL", "inbound@patrici.us")).strip() or "inbound@patrici.us"
        template_id = str(request.template_id or "tpl_fixed_starter_v1").strip() or "tpl_fixed_starter_v1"

        if request.concept_overrides:
            selected_overrides = [override for override in request.concept_overrides if override.selected]
        else:
            selected_overrides = [
                CreateDealConceptOverride(
                    concept_id=concept_id,
                    selected=True,
                )
                for concept_id in STARTER_CONCEPT_IDS
            ]

        if not selected_overrides:
            raise HTTPException(status_code=400, detail="at_least_one_concept_required")

        concept_overrides: list[dict[str, Any]] = []
        for override in selected_overrides:
            concept_id = str(override.concept_id).strip()
            if concept_id not in CONCEPT_LABELS:
                raise HTTPException(status_code=400, detail=f"unsupported_concept:{concept_id}")
            concept_overrides.append(
                {
                    "concept_id": concept_id,
                    "concept_label": CONCEPT_LABELS[concept_id],
                    "selected": True,
                }
            )

        quick_instruction = (
            f"Send borrower reporting emails with PDF/XLSX attachments to {inbound_address}. "
            f"Use deal name '{display_name}' in the subject for clean routing."
        )
        create_config = store.upsert_deal_create_config(
            deal_id=deal_id,
            template_id=template_id,
            forwarding_address=inbound_address,
            quick_instruction=quick_instruction,
            concept_overrides=concept_overrides,
        )
        meta = store.get_deal_meta(deal_id)
        if meta is None:
            raise HTTPException(status_code=500, detail="deal_create_failed")

        reporting_setup: dict[str, Any] = {
            "mode": "not_provided",
            "docs_received": 0,
            "deterministic_obligations_extracted": 0,
            "obligations_extracted": 0,
            "grounded_extracted": 0,
            "candidate_discovery": {
                "mode": str(request.reporting_requirement_llm_discovery).strip().lower(),
                "attempted": False,
                "status": "not_provided",
                "model_name": "",
                "error": "",
                "summary": {
                    "total": 0,
                    "grounded": 0,
                    "ambiguous": 0,
                    "unsupported": 0,
                    "promoted": 0,
                },
            },
            "catalog": {
                "total": 0,
                "grounded": 0,
                "ambiguous": 0,
                "unsupported": 0,
            },
            "supported_concepts": list(SUPPORTED_REPORTING_OBLIGATION_CONCEPTS),
            "candidate_supported_concepts": list(CANDIDATE_SUPPORTED_CONCEPTS),
            "grounded_supported_concepts": list(GROUNDED_SUPPORTED_CONCEPTS),
        }

        if request.reporting_requirement_docs:
            reporting_setup = {
                "mode": "ingested_during_deal_setup",
                **_ingest_reporting_obligation_docs(
                    deal_id=deal_id,
                    docs=[doc.model_dump() for doc in request.reporting_requirement_docs],
                    clear_existing_for_docs=True,
                    llm_discovery_mode=request.reporting_requirement_llm_discovery,
                    workspace_id=x_workspace_id,
                    emit_event_type="deal_setup_reporting_obligations_ingested",
                ),
            }

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "deal_created",
                    "phase": "publish",
                    "package_id": "",
                    "trace_id": "",
                    "payload": {
                        "deal_id": deal_id,
                        "display_name": display_name,
                        "workspace_id": x_workspace_id,
                        "template_id": template_id,
                        "forwarding_address": inbound_address,
                        "concept_overrides": concept_overrides,
                        "reporting_requirement_docs": len(request.reporting_requirement_docs),
                        "reporting_requirement_setup_mode": reporting_setup.get("mode"),
                    },
                }
            ],
        )

        return {
            "deal_id": deal_id,
            "display_name": meta.get("display_name", display_name),
            "archived": bool(meta.get("archived", False)),
            "periods": [],
            "latest_period_id": None,
            "period_count": 0,
            "created": True,
            "template_id": template_id,
            "forwarding_address": create_config.get("forwarding_address"),
            "quick_instruction": create_config.get("quick_instruction"),
            "concept_overrides": create_config.get("concept_overrides", []),
            "reporting_requirement_setup": reporting_setup,
        }

    @app.post("/internal/v1/deals/{deal_id}/reporting-obligations:ingest")
    def ingest_reporting_obligations(
        deal_id: str,
        request: ReportingObligationIngestRequest,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="deal_not_found")
        deal_meta = store.get_deal_meta(deal_id)
        if deal_meta is None:
            raise HTTPException(status_code=404, detail="deal_not_found")
        if not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="deal_not_found")

        payload = _ingest_reporting_obligation_docs(
            deal_id=deal_id,
            docs=[doc.model_dump() for doc in request.docs],
            clear_existing_for_docs=request.clear_existing_for_docs,
            llm_discovery_mode=request.llm_discovery,
            workspace_id=x_workspace_id,
            emit_event_type="reporting_obligations_ingested",
        )
        return {
            "deal_id": deal_id,
            "deal_name": str(deal_meta.get("display_name", deal_id)),
            **payload,
        }

    @app.get("/internal/v1/deals/{deal_id}/reporting-obligations")
    def list_reporting_obligations(
        deal_id: str,
        grounding_state: str | None = Query(default=None),
        required_concept_id: str | None = Query(default=None),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="deal_not_found")
        if store.get_deal_meta(deal_id) is None:
            raise HTTPException(status_code=404, detail="deal_not_found")
        if not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="deal_not_found")

        rows = store.list_reporting_obligations(
            deal_id=deal_id,
            grounding_state=grounding_state,
            required_concept_id=required_concept_id,
        )
        obligations = [
            {
                **row,
                "download_url": (
                    f"/internal/v1/deals/{deal_id}/reporting-obligations/{row['obligation_id']}/document:download"
                ),
            }
            for row in rows
        ]
        return {
            "deal_id": deal_id,
            "count": len(obligations),
            "obligations": obligations,
        }

    @app.get("/internal/v1/deals/{deal_id}/reporting-obligation-candidates")
    def list_reporting_obligation_candidates(
        deal_id: str,
        grounding_state: str | None = Query(default=None),
        candidate_concept_id: str | None = Query(default=None),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="deal_not_found")
        if store.get_deal_meta(deal_id) is None:
            raise HTTPException(status_code=404, detail="deal_not_found")
        if not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="deal_not_found")

        rows = store.list_reporting_obligation_candidates(
            deal_id=deal_id,
            grounding_state=grounding_state,
            candidate_concept_id=candidate_concept_id,
        )
        candidates = [
            {
                **row,
                "promoted": bool(row.get("promoted_obligation_id")),
                "download_url": (
                    f"/internal/v1/deals/{deal_id}/reporting-obligations/{row['promoted_obligation_id']}/document:download"
                    if row.get("promoted_obligation_id")
                    else ""
                ),
            }
            for row in rows
        ]
        return {
            "deal_id": deal_id,
            "count": len(candidates),
            "summary": summarize_candidate_states(candidates),
            "candidates": candidates,
        }

    @app.get("/internal/v1/deals/{deal_id}/reporting-obligations/{obligation_id}/document:download")
    def download_reporting_obligation_document(
        deal_id: str,
        obligation_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> FileResponse:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="obligation_not_found")
        if not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="obligation_not_found")
        obligation = store.get_reporting_obligation(obligation_id)
        if obligation is None or str(obligation.get("deal_id", "")).strip() != deal_id:
            raise HTTPException(status_code=404, detail="obligation_not_found")

        resolved = resolve_storage_uri(str(obligation.get("storage_uri", "")))
        if resolved is None:
            raise HTTPException(status_code=404, detail="file_unavailable")

        doc_type = str(obligation.get("doc_type", "")).upper()
        media_type = "application/octet-stream"
        if doc_type == "PDF":
            media_type = "application/pdf"
        elif doc_type == "XLSX":
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        filename = str(obligation.get("doc_name", resolved.name))
        return FileResponse(path=resolved, filename=filename, media_type=media_type)

    @app.get("/internal/v1/deals/{deal_id}/reporting-obligations/{obligation_id}/preview")
    def preview_reporting_obligation_document(
        deal_id: str,
        obligation_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="obligation_not_found")
        if not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="obligation_not_found")
        obligation = store.get_reporting_obligation(obligation_id)
        if obligation is None or str(obligation.get("deal_id", "")).strip() != deal_id:
            raise HTTPException(status_code=404, detail="obligation_not_found")

        evidence_preview = build_document_locator_preview(
            doc_id=str(obligation.get("doc_id", "")),
            doc_type=str(obligation.get("doc_type", "")),
            filename=str(obligation.get("doc_name", "")),
            storage_uri=str(obligation.get("storage_uri", "")),
            locator_type=str(obligation.get("locator_type", "")),
            locator_value=str(obligation.get("locator_value", "")),
            page_or_sheet=str(obligation.get("page_or_sheet", "")),
            source_snippet=str(obligation.get("source_snippet", "")),
        )
        evidence_preview["download_url"] = (
            f"/internal/v1/deals/{deal_id}/reporting-obligations/{obligation_id}/document:download"
        )
        return {
            "deal_id": deal_id,
            "obligation_id": obligation_id,
            "evidence_preview": evidence_preview,
        }

    @app.get("/internal/v1/deals/{deal_id}/periods")
    def list_periods(
        deal_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        packages = _packages_for_deal_workspace(store, deal_id, x_workspace_id)
        if packages and not store.deal_in_workspace(deal_id, x_workspace_id):
            store.assign_deal_workspace(deal_id=deal_id, workspace_id=x_workspace_id)
        if not packages and not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="deal_not_found")
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="deal_not_found")

        deal_meta = store.get_deal_meta(deal_id) or {"display_name": deal_id, "archived": False}
        periods = [
            {
                "package_id": pkg.package_id,
                "period_end_date": pkg.period_end_date,
                "period_revision": pkg.period_revision,
                "status": pkg.status,
                "received_at": pkg.received_at,
            }
            for pkg in sorted(
                packages,
                key=lambda p: (p.period_end_date, p.period_revision, p.received_at),
                reverse=True,
            )
        ]
        return {
            "deal_id": deal_id,
            "display_name": deal_meta.get("display_name", deal_id),
            "archived": bool(deal_meta.get("archived", False)),
            "periods": periods,
            "count": len(periods),
        }

    @app.patch("/internal/v1/deals/{deal_id}")
    def update_deal(
        deal_id: str,
        request: UpdateDealRequest,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        packages = _packages_for_deal_workspace(store, deal_id, x_workspace_id)
        if packages and not store.deal_in_workspace(deal_id, x_workspace_id):
            store.assign_deal_workspace(deal_id=deal_id, workspace_id=x_workspace_id)
        if not packages and not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="deal_not_found")
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="deal_not_found")

        try:
            meta = store.update_deal_display_name(deal_id, request.display_name)
        except KeyError:
            raise HTTPException(status_code=404, detail="deal_not_found")

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "deal_renamed",
                    "phase": "publish",
                    "package_id": "",
                    "trace_id": "",
                    "payload": {
                        "deal_id": deal_id,
                        "display_name": request.display_name,
                    },
                }
            ],
        )

        return meta

    @app.delete("/internal/v1/deals/{deal_id}")
    def delete_deal(
        deal_id: str,
        x_role: str = Header(default="Owner", alias="X-Role"),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        _assert_role_can_resolve(x_role)
        packages = _packages_for_deal_workspace(store, deal_id, x_workspace_id)
        if packages and not store.deal_in_workspace(deal_id, x_workspace_id):
            store.assign_deal_workspace(deal_id=deal_id, workspace_id=x_workspace_id)
        if not packages and not store.deal_in_workspace(deal_id, x_workspace_id):
            raise HTTPException(status_code=404, detail="deal_not_found")

        try:
            meta = store.set_deal_archived(deal_id, archived=True)
        except KeyError:
            raise HTTPException(status_code=404, detail="deal_not_found")

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "deal_archived",
                    "phase": "publish",
                    "package_id": "",
                    "trace_id": "",
                    "payload": {
                        "deal_id": deal_id,
                        "actor_role": x_role,
                    },
                }
            ],
        )

        return {
            "deal_id": deal_id,
            "archived": bool(meta.get("archived", False)),
            "status": "archived",
        }

    @app.post("/internal/v1/packages/{package_id}:reassign")
    def reassign_package(
        package_id: str,
        request: ReassignPackageRequest,
        x_role: str = Header(default="Owner", alias="X-Role"),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        _assert_role_can_resolve(x_role)
        package = store.get_package(package_id)
        if package is None:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(package, x_workspace_id)

        target_deal_id = request.target_deal_id.strip()
        if not target_deal_id:
            raise HTTPException(status_code=400, detail="invalid_target_deal_id")

        target_packages = [pkg for pkg in store.list_packages() if pkg.deal_id == target_deal_id]
        cross_workspace = [pkg for pkg in target_packages if _workspace_for_record(pkg) != x_workspace_id]
        if cross_workspace:
            raise HTTPException(status_code=409, detail="target_deal_workspace_conflict")
        if store.get_deal_meta(target_deal_id) is not None and not store.deal_in_workspace(target_deal_id, x_workspace_id):
            if target_packages:
                raise HTTPException(status_code=409, detail="target_deal_workspace_conflict")
            store.assign_deal_workspace(deal_id=target_deal_id, workspace_id=x_workspace_id)

        source_deal_id = package.deal_id
        try:
            updated = store.reassign_package_deal(package_id=package_id, target_deal_id=target_deal_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="package_not_found")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        _ensure_deal_workspace_membership(target_deal_id, x_workspace_id)

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "package_reassigned",
                    "phase": "publish",
                    "package_id": package_id,
                    "trace_id": "",
                    "payload": {
                        "source_deal_id": source_deal_id,
                        "target_deal_id": target_deal_id,
                        "actor": request.actor,
                        "note": request.note,
                    },
                }
            ],
        )

        response = _package_response(updated)
        response["source_deal_id"] = source_deal_id
        response["target_deal_id"] = target_deal_id
        return response

    @app.get("/internal/v1/deals/{deal_id}/periods/{period_id}/delta")
    def get_delta(
        deal_id: str,
        period_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")
        package = store.get_package(period_id)
        if not package:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(package, x_workspace_id)
        delta = store.get_delta(deal_id=deal_id, period_id=period_id)
        if delta is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        return delta

    @app.get("/internal/v1/deals/{deal_id}/periods/{period_id}/review_queue")
    def get_review_queue(
        deal_id: str,
        period_id: str,
        baseline_period_id: str | None = Query(default=None),
        include_resolved: bool = Query(default=False),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")

        current_package = store.get_package(period_id)
        if current_package is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(current_package, x_workspace_id)
        if current_package.deal_id != deal_id:
            raise HTTPException(status_code=404, detail="period_not_found")

        deal_packages = _packages_for_deal_workspace(store, deal_id, x_workspace_id)
        if not deal_packages:
            raise HTTPException(status_code=404, detail="period_not_found")

        try:
            return build_review_queue_payload(
                store=store,
                deal_id=deal_id,
                period_id=period_id,
                deal_packages=deal_packages,
                baseline_period_id=baseline_period_id,
                include_resolved=include_resolved,
            )
        except ValueError as exc:
            detail = str(exc).strip() or "review_queue_invalid_request"
            if detail in {"period_not_found", "baseline_period_not_found"}:
                raise HTTPException(status_code=404, detail=detail)
            if detail == "period_not_processed":
                raise HTTPException(status_code=409, detail=detail)
            if detail == "baseline_period_cannot_match_current":
                raise HTTPException(status_code=400, detail=detail)
            raise HTTPException(status_code=400, detail=detail)

    @app.post("/internal/v1/deals/{deal_id}/periods/{period_id}/review_queue/items/{item_id}:feedback")
    def submit_review_case_feedback(
        deal_id: str,
        period_id: str,
        item_id: str,
        request: ReviewCaseFeedbackRequest,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")

        period_package = store.get_package(period_id)
        if period_package is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(period_package, x_workspace_id)
        if period_package.deal_id != deal_id:
            raise HTTPException(status_code=404, detail="period_not_found")

        deal_packages = _packages_for_deal_workspace(store, deal_id, x_workspace_id)
        if not deal_packages:
            raise HTTPException(status_code=404, detail="period_not_found")

        payload = build_review_queue_payload(
            store=store,
            deal_id=deal_id,
            period_id=period_id,
            deal_packages=deal_packages,
            include_resolved=True,
        )
        target_item = next(
            (item for item in payload.get("items", []) if str(item.get("id", "")).strip() == str(item_id).strip()),
            None,
        )
        if target_item is None:
            raise HTTPException(status_code=404, detail="review_item_not_found")
        if str(target_item.get("concept_maturity", "")).strip().lower() != "review":
            raise HTTPException(status_code=400, detail="feedback_requires_review_tier_item")

        feedback = store.record_review_case_feedback(
            deal_id=deal_id,
            period_id=period_id,
            item_id=str(target_item.get("id", item_id)),
            concept_id=str(target_item.get("metric_key", "")).strip(),
            concept_maturity=str(target_item.get("concept_maturity", "")).strip(),
            trust_tier=str(target_item.get("trust_tier", "")).strip(),
            case_mode=str(target_item.get("case_mode", "")).strip(),
            action_id=request.action_id,
            outcome=request.outcome,
            actor=request.actor,
            note=request.note,
            metadata=request.metadata,
        )

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "review_case_feedback_recorded",
                    "phase": "publish",
                    "package_id": period_id,
                    "trace_id": str((target_item.get("trace_ids") or [""])[0] if isinstance(target_item.get("trace_ids"), list) else ""),
                    "payload": {
                        "deal_id": deal_id,
                        "period_id": period_id,
                        "item_id": item_id,
                        "concept_id": str(target_item.get("metric_key", "")),
                        "case_mode": str(target_item.get("case_mode", "")),
                        "action_id": request.action_id,
                        "outcome": request.outcome,
                        "actor": request.actor,
                    },
                }
            ],
        )
        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "item_id": item_id,
            "feedback": feedback,
        }

    @app.get("/internal/v1/deals/{deal_id}/periods/{period_id}/review_queue/feedback")
    def list_review_queue_feedback(
        deal_id: str,
        period_id: str,
        item_id: str | None = Query(default=None),
        limit: int = Query(default=200, ge=1, le=1000),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")
        period_package = store.get_package(period_id)
        if period_package is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(period_package, x_workspace_id)
        if period_package.deal_id != deal_id:
            raise HTTPException(status_code=404, detail="period_not_found")

        feedback = store.list_review_case_feedback(
            deal_id=deal_id,
            period_id=period_id,
            item_id=item_id,
            limit=limit,
        )
        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "item_id": item_id or "",
            "count": len(feedback),
            "feedback": feedback,
        }

    @app.post("/internal/v1/deals/{deal_id}/periods/{period_id}/review_queue/items/{item_id}:draft_event")
    def submit_review_queue_draft_event(
        deal_id: str,
        period_id: str,
        item_id: str,
        request: DraftWorkflowEventRequest,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")

        period_package = store.get_package(period_id)
        if period_package is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(period_package, x_workspace_id)
        if period_package.deal_id != deal_id:
            raise HTTPException(status_code=404, detail="period_not_found")

        deal_packages = _packages_for_deal_workspace(store, deal_id, x_workspace_id)
        if not deal_packages:
            raise HTTPException(status_code=404, detail="period_not_found")

        payload = build_review_queue_payload(
            store=store,
            deal_id=deal_id,
            period_id=period_id,
            deal_packages=deal_packages,
            include_resolved=True,
        )
        target_item = next(
            (item for item in payload.get("items", []) if str(item.get("id", "")).strip() == str(item_id).strip()),
            None,
        )
        if target_item is None:
            raise HTTPException(status_code=404, detail="review_item_not_found")

        event = store.record_borrower_draft_event(
            deal_id=deal_id,
            period_id=period_id,
            item_id=str(target_item.get("id", item_id)),
            concept_id=str(target_item.get("metric_key", "")).strip(),
            concept_maturity=str(target_item.get("concept_maturity", "")).strip(),
            trust_tier=str(target_item.get("trust_tier", "")).strip(),
            case_mode=str(target_item.get("case_mode", "")).strip(),
            event_type=request.event_type,
            actor=request.actor,
            subject=request.subject,
            draft_text=request.draft_text,
            metadata=request.metadata,
        )

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "borrower_draft_workflow_recorded",
                    "phase": "publish",
                    "package_id": period_id,
                    "trace_id": str((target_item.get("trace_ids") or [""])[0] if isinstance(target_item.get("trace_ids"), list) else ""),
                    "payload": {
                        "deal_id": deal_id,
                        "period_id": period_id,
                        "item_id": item_id,
                        "concept_id": str(target_item.get("metric_key", "")),
                        "case_mode": str(target_item.get("case_mode", "")),
                        "event_type": request.event_type,
                        "actor": request.actor,
                    },
                }
            ],
        )
        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "item_id": item_id,
            "event": event,
        }

    @app.get("/internal/v1/deals/{deal_id}/periods/{period_id}/review_queue/draft_events")
    def list_review_queue_draft_events(
        deal_id: str,
        period_id: str,
        item_id: str | None = Query(default=None),
        limit: int = Query(default=200, ge=1, le=1000),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")
        period_package = store.get_package(period_id)
        if period_package is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(period_package, x_workspace_id)
        if period_package.deal_id != deal_id:
            raise HTTPException(status_code=404, detail="period_not_found")

        events = store.list_borrower_draft_events(
            deal_id=deal_id,
            period_id=period_id,
            item_id=item_id,
            limit=limit,
        )
        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "item_id": item_id or "",
            "count": len(events),
            "events": events,
        }

    @app.get("/internal/v1/deals/{deal_id}/periods/{period_id}/review_queue/items/{item_id}/analyst_note")
    def get_review_queue_analyst_note(
        deal_id: str,
        period_id: str,
        item_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")
        period_package = store.get_package(period_id)
        if period_package is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(period_package, x_workspace_id)
        if period_package.deal_id != deal_id:
            raise HTTPException(status_code=404, detail="period_not_found")

        note = store.get_analyst_note(
            deal_id=deal_id,
            period_id=period_id,
            item_id=item_id,
        )
        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "item_id": item_id,
            "note": note,
        }

    @app.put("/internal/v1/deals/{deal_id}/periods/{period_id}/review_queue/items/{item_id}/analyst_note")
    def upsert_review_queue_analyst_note(
        deal_id: str,
        period_id: str,
        item_id: str,
        request: AnalystNoteUpsertRequest,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
        authorization: str | None = Header(default=None, alias="Authorization"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")
        period_package = store.get_package(period_id)
        if period_package is None:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(period_package, x_workspace_id)
        if period_package.deal_id != deal_id:
            raise HTTPException(status_code=404, detail="period_not_found")

        deal_packages = _packages_for_deal_workspace(store, deal_id, x_workspace_id)
        if not deal_packages:
            raise HTTPException(status_code=404, detail="period_not_found")

        payload = build_review_queue_payload(
            store=store,
            deal_id=deal_id,
            period_id=period_id,
            deal_packages=deal_packages,
            include_resolved=True,
        )
        target_item = next(
            (item for item in payload.get("items", []) if str(item.get("id", "")).strip() == str(item_id).strip()),
            None,
        )
        if target_item is None:
            raise HTTPException(status_code=404, detail="review_item_not_found")

        actor = _resolve_actor_identity(
            store=store,
            authorization=authorization,
            requested_actor=request.actor,
        )
        try:
            note = store.upsert_analyst_note(
                deal_id=deal_id,
                period_id=period_id,
                item_id=str(target_item.get("id", item_id)).strip(),
                concept_id=str(target_item.get("metric_key", "")).strip(),
                concept_maturity=str(target_item.get("concept_maturity", "")).strip(),
                trust_tier=str(target_item.get("trust_tier", "")).strip(),
                case_mode=str(target_item.get("case_mode", "")).strip(),
                author=actor,
                subject=request.subject,
                note_text=request.note_text,
                memo_ready=bool(request.memo_ready),
                export_ready=bool(request.export_ready),
                metadata=request.metadata,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        append_events(
            events_log_path,
            [
                {
                    "timestamp": _utc_now(),
                    "event_type": "analyst_note_upserted",
                    "phase": "publish",
                    "package_id": period_id,
                    "trace_id": str((target_item.get("trace_ids") or [""])[0] if isinstance(target_item.get("trace_ids"), list) else ""),
                    "payload": {
                        "deal_id": deal_id,
                        "period_id": period_id,
                        "item_id": item_id,
                        "concept_id": str(target_item.get("metric_key", "")),
                        "case_mode": str(target_item.get("case_mode", "")),
                        "actor": actor,
                        "memo_ready": bool(request.memo_ready),
                        "export_ready": bool(request.export_ready),
                    },
                }
            ],
        )

        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "item_id": item_id,
            "note": note,
        }

    @app.get("/internal/v1/deals/{deal_id}/periods/{period_id}/takeaways")
    def get_period_takeaways(
        deal_id: str,
        period_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        if store.is_deal_archived(deal_id):
            raise HTTPException(status_code=404, detail="period_not_found")
        package = store.get_package(period_id)
        if not package:
            raise HTTPException(status_code=404, detail="period_not_found")
        _assert_workspace_access(package, x_workspace_id)
        delta = store.get_delta(deal_id=deal_id, period_id=period_id)
        if delta is None:
            raise HTTPException(status_code=404, detail="period_not_found")

        rows = [row for row in delta.get("rows", []) if isinstance(row, dict)]
        try:
            summary = build_period_takeaways(
                deal_id=deal_id,
                period_end_date=package.period_end_date,
                rows=rows,
            )
        except TakeawaysGenerationError as exc:
            detail = f"takeaways_ai_failed:{exc.code}"
            if exc.message:
                detail = f"{detail}:{exc.message}"
            raise HTTPException(status_code=503, detail=detail)
        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "period_end_date": package.period_end_date,
            "row_count": len(rows),
            **summary,
        }

    @app.get("/internal/v1/traces/{trace_id}")
    def get_trace(
        trace_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")
        package = store.get_package(trace["package_id"])
        if package is None:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(package, x_workspace_id)
        return trace

    @app.get("/internal/v1/traces/{trace_id}/evidence")
    def get_trace_evidence(
        trace_id: str,
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")

        package = store.get_package(trace["package_id"])
        if package is None:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(package, x_workspace_id)

        evidence_preview = build_evidence_preview(
            trace_row=trace.get("row", {}),
            package_manifest=package.package_manifest,
        )
        doc_id = str(evidence_preview.get("doc_id", ""))
        if doc_id:
            evidence_preview["package_id"] = package.package_id
            evidence_preview["download_url"] = (
                f"/internal/v1/packages/{package.package_id}/files/{doc_id}:download"
            )

        return {
            "trace_id": trace_id,
            "package_id": package.package_id,
            "deal_id": package.deal_id,
            "evidence_preview": evidence_preview,
        }

    @app.get("/internal/v1/traces/{trace_id}/events")
    def get_trace_events(
        trace_id: str,
        limit: int = Query(default=500, ge=1, le=5000),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")
        package = store.get_package(trace["package_id"])
        if package is None:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(package, x_workspace_id)

        events = _read_events(events_log_path, trace_id=trace_id, limit=limit)
        return {
            "trace_id": trace_id,
            "events": events,
            "count": len(events),
            "integrity_ok": not bool(check_log_integrity(events_log_path)),
        }

    @app.get("/internal/v1/traces/{trace_id}/history")
    def get_trace_history(
        trace_id: str,
        limit: int = Query(default=100, ge=1, le=1000),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")
        package = store.get_package(trace["package_id"])
        if package is None:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(package, x_workspace_id)

        history = store.list_trace_resolutions(trace_id=trace_id, limit=limit)
        return {
            "trace_id": trace_id,
            "history": history,
            "count": len(history),
        }

    @app.post("/internal/v1/traces/{trace_id}:resolve")
    def resolve_trace(
        trace_id: str,
        request: ResolveTraceRequest,
        x_role: str = Header(default="Owner", alias="X-Role"),
        x_workspace_id: str = Header(default="ws_default", alias="X-Workspace-Id"),
    ) -> dict[str, Any]:
        _assert_role_can_resolve(x_role)
        trace = store.get_trace(trace_id)
        if trace is None:
            raise HTTPException(status_code=404, detail="trace_not_found")

        package = store.get_package(trace["package_id"])
        if package is None:
            raise HTTPException(status_code=404, detail="package_not_found")
        _assert_workspace_access(package, x_workspace_id)

        if package.processed_payload is None:
            raise HTTPException(status_code=409, detail="package_not_processed")

        row_before = json.loads(json.dumps(trace.get("row", {})))
        row_before.pop("resolution", None)
        if str(row_before.get("trace_id", "")).strip() != trace_id:
            raise HTTPException(status_code=404, detail="trace_row_not_found")

        row_after = json.loads(json.dumps(row_before))
        previous_status = str(row_after.get("status", "unresolved"))
        previous_confidence = float(row_after.get("confidence", 0.0))
        row_after["status"] = "verified"
        row_after["confidence"] = max(previous_confidence, 0.95)
        row_after["user_resolution"] = {
            "resolver": request.resolver,
            "resolved_at": _utc_now(),
            "selected_evidence": request.selected_evidence,
            "note": request.note,
        }
        row_after["resolved_by_user"] = True

        verification = row_after.get("verification", {})
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
        row_after["verification"] = {
            "attempts": attempts,
            "retry_count": verification.get("retry_count", 0),
            "max_retries": verification.get("max_retries", 2),
            "final_status": "verified",
            "objections": verification.get("objections", []),
            "resolver": request.resolver,
        }

        resolution_id = store.append_trace_resolution(
            trace_id=trace_id,
            package_id=package.package_id,
            resolver=request.resolver,
            selected_evidence=request.selected_evidence,
            note=request.note,
            row_before=row_before,
            row_after=row_after,
        )

        lifecycle_status = store.compute_effective_package_status(package.package_id)
        store.update_package_status(
            package_id=package.package_id,
            status=lifecycle_status,
            error_message=None,
            preserve_payload=True,
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

        refreshed = store.get_trace(trace_id)
        response_row = refreshed.get("row", row_after) if refreshed is not None else row_after

        return {
            "trace_id": trace_id,
            "package_id": package.package_id,
            "deal_id": package.deal_id,
            "resolution_id": resolution_id,
            "status": "verified",
            "package_status": lifecycle_status,
            "row": response_row,
        }

    @app.get("/internal/v1/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "time": _utc_now(),
            "events_log": str(events_log_path),
            "labels_dir": str(labels_dir),
            "ui_enabled": bool(ui_dir and ui_dir.exists()),
            "auth_enabled": internal_token is not None,
            "https_required": require_https,
            "at_rest_encryption": encryption_key is not None,
            "account_onboarding": True,
            "public_base_url": public_base_url,
            "runtime_profile": active_profile,
            "strict_runtime_profile": strict_runtime,
            "allowed_extraction_modes": ["llm"] if strict_runtime else ["runtime", "eval", "llm"],
        }

    return app
