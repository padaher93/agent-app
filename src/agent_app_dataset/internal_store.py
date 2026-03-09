from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from .constants import CONCEPT_LABELS


PACKAGE_STATUSES = ("received", "processing", "completed", "needs_review", "failed")


@dataclass
class PackageRecord:
    package_id: str
    idempotency_key: str
    sender_email: str
    source_email_id: str
    deal_id: str
    period_end_date: str
    received_at: str
    period_revision: int
    status: str
    package_manifest: dict[str, Any]
    processed_payload: dict[str, Any] | None
    error_message: str | None
    created_at: str
    updated_at: str


class InternalStore:
    def __init__(self, db_path: Path, encryption_key: str | None = None) -> None:
        self.db_path = db_path
        self._cipher: Any | None = None
        if encryption_key:
            try:
                from cryptography.fernet import Fernet
            except Exception as exc:  # pragma: no cover - optional dependency path
                raise RuntimeError(
                    "Encryption key provided but 'cryptography' is not installed"
                ) from exc

            try:
                self._cipher = Fernet(encryption_key.encode("utf-8"))
            except Exception as exc:  # pragma: no cover - invalid key path
                raise RuntimeError("Invalid encryption key for Fernet") from exc

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _parse_iso(value: str) -> datetime:
        return datetime.fromisoformat(value)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deals (
                    deal_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deal_workspaces (
                    deal_id TEXT NOT NULL,
                    workspace_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (deal_id, workspace_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deal_create_configs (
                    deal_id TEXT PRIMARY KEY,
                    template_id TEXT NOT NULL,
                    forwarding_address TEXT NOT NULL,
                    quick_instruction TEXT NOT NULL,
                    concept_overrides_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    email TEXT PRIMARY KEY,
                    password_hash TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_login_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS magic_links (
                    token_hash TEXT PRIMARY KEY,
                    email TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    consumed_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    token_hash TEXT PRIMARY KEY,
                    email TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS packages (
                    package_id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE NOT NULL,
                    sender_email TEXT NOT NULL,
                    source_email_id TEXT NOT NULL,
                    deal_id TEXT NOT NULL,
                    period_end_date TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    period_revision INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL,
                    package_manifest_json TEXT NOT NULL,
                    processed_payload_json TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS traces (
                    trace_id TEXT PRIMARY KEY,
                    package_id TEXT NOT NULL,
                    deal_id TEXT NOT NULL,
                    period_id TEXT NOT NULL,
                    concept_id TEXT NOT NULL,
                    row_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trace_resolutions (
                    resolution_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_id TEXT NOT NULL,
                    package_id TEXT NOT NULL,
                    resolver TEXT NOT NULL,
                    resolved_at TEXT NOT NULL,
                    status_before TEXT NOT NULL,
                    status_after TEXT NOT NULL,
                    confidence_before REAL NOT NULL,
                    confidence_after REAL NOT NULL,
                    note TEXT,
                    selected_evidence_json TEXT NOT NULL,
                    row_before_json TEXT NOT NULL,
                    row_after_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reporting_obligations (
                    obligation_id TEXT PRIMARY KEY,
                    deal_id TEXT NOT NULL,
                    doc_id TEXT NOT NULL,
                    doc_name TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    storage_uri TEXT NOT NULL,
                    locator_type TEXT NOT NULL,
                    locator_value TEXT NOT NULL,
                    page_or_sheet TEXT,
                    source_snippet TEXT NOT NULL,
                    obligation_type TEXT NOT NULL,
                    required_concept_id TEXT NOT NULL,
                    required_concept_label TEXT NOT NULL,
                    cadence TEXT,
                    source_role TEXT NOT NULL,
                    grounding_state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reporting_obligation_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    deal_id TEXT NOT NULL,
                    doc_id TEXT NOT NULL,
                    doc_name TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    storage_uri TEXT NOT NULL,
                    locator_type TEXT NOT NULL,
                    locator_value TEXT NOT NULL,
                    page_or_sheet TEXT,
                    source_snippet TEXT NOT NULL,
                    candidate_obligation_type TEXT NOT NULL,
                    candidate_concept_id TEXT NOT NULL,
                    candidate_concept_label TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    certainty_bucket TEXT NOT NULL,
                    model_name TEXT,
                    extraction_mode TEXT NOT NULL,
                    raw_model_output_json TEXT NOT NULL,
                    grounding_state TEXT NOT NULL,
                    promoted_obligation_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS review_case_feedback (
                    feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    deal_id TEXT NOT NULL,
                    period_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    concept_id TEXT NOT NULL,
                    concept_maturity TEXT NOT NULL,
                    trust_tier TEXT NOT NULL,
                    case_mode TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    note TEXT,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS borrower_draft_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    deal_id TEXT NOT NULL,
                    period_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    concept_id TEXT NOT NULL,
                    concept_maturity TEXT NOT NULL,
                    trust_tier TEXT NOT NULL,
                    case_mode TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    subject TEXT,
                    draft_text TEXT,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analyst_notes (
                    note_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    deal_id TEXT NOT NULL,
                    period_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    concept_id TEXT NOT NULL,
                    concept_maturity TEXT NOT NULL,
                    trust_tier TEXT NOT NULL,
                    case_mode TEXT NOT NULL,
                    author TEXT NOT NULL,
                    subject TEXT,
                    note_text TEXT NOT NULL,
                    memo_ready INTEGER NOT NULL DEFAULT 0,
                    export_ready INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (deal_id, period_id, item_id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_packages_deal_period ON packages(deal_id, period_end_date)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_packages_period_revision ON packages(deal_id, period_end_date, period_revision)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_archived ON deals(archived, deal_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_deal_workspaces_workspace ON deal_workspaces(workspace_id, deal_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_deal_workspaces_deal ON deal_workspaces(deal_id, workspace_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_deal_create_configs_template ON deal_create_configs(template_id, deal_id)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_magic_links_email ON magic_links(email, created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_email ON sessions(email, created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_package ON traces(package_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trace_resolutions_trace_id ON trace_resolutions(trace_id, resolution_id DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reporting_obligations_deal ON reporting_obligations(deal_id, updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reporting_obligations_concept ON reporting_obligations(deal_id, required_concept_id, grounding_state)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reporting_obligation_candidates_deal ON reporting_obligation_candidates(deal_id, updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reporting_obligation_candidates_concept ON reporting_obligation_candidates(deal_id, candidate_concept_id, grounding_state)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_review_case_feedback_period ON review_case_feedback(deal_id, period_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_review_case_feedback_item ON review_case_feedback(item_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_borrower_draft_events_period ON borrower_draft_events(deal_id, period_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_borrower_draft_events_item ON borrower_draft_events(item_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_analyst_notes_period ON analyst_notes(deal_id, period_id, updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_analyst_notes_item ON analyst_notes(item_id, updated_at DESC)"
            )

            package_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(packages)").fetchall()
            }
            if "period_revision" not in package_columns:
                conn.execute(
                    "ALTER TABLE packages ADD COLUMN period_revision INTEGER NOT NULL DEFAULT 1"
                )

    def ensure_deal(self, deal_id: str, display_name: str | None = None) -> None:
        normalized_deal_id = str(deal_id).strip()
        if not normalized_deal_id:
            raise ValueError("deal_id cannot be empty")

        now = datetime.now(timezone.utc).isoformat()
        desired_display = str(display_name or normalized_deal_id).strip() or normalized_deal_id
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT deal_id FROM deals WHERE deal_id = ?",
                (normalized_deal_id,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO deals (deal_id, display_name, archived, created_at, updated_at)
                    VALUES (?, ?, 0, ?, ?)
                    """,
                    (normalized_deal_id, desired_display, now, now),
                )
                return

            conn.execute(
                """
                UPDATE deals
                SET archived = 0,
                    updated_at = ?
                WHERE deal_id = ?
                """,
                (now, normalized_deal_id),
            )

    def list_deals_meta(self, include_archived: bool = False) -> list[dict[str, Any]]:
        query = "SELECT deal_id, display_name, archived, created_at, updated_at FROM deals"
        params: tuple[Any, ...] = ()
        if not include_archived:
            query += " WHERE archived = 0"
        query += " ORDER BY deal_id ASC"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()

        return [
            {
                "deal_id": str(row["deal_id"]),
                "display_name": str(row["display_name"]),
                "archived": bool(int(row["archived"])),
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        ]

    @staticmethod
    def _normalize_workspace_id(workspace_id: str) -> str:
        normalized = str(workspace_id).strip()
        if not normalized:
            raise ValueError("workspace_id_required")
        return normalized

    def assign_deal_workspace(self, deal_id: str, workspace_id: str) -> None:
        normalized_deal_id = str(deal_id).strip()
        if not normalized_deal_id:
            raise ValueError("deal_id cannot be empty")
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        now = datetime.now(timezone.utc).isoformat()

        with self._connect() as conn:
            deal_row = conn.execute(
                "SELECT deal_id FROM deals WHERE deal_id = ?",
                (normalized_deal_id,),
            ).fetchone()
            if deal_row is None:
                raise KeyError("deal_not_found")

            existing = conn.execute(
                "SELECT deal_id FROM deal_workspaces WHERE deal_id = ? AND workspace_id = ?",
                (normalized_deal_id, normalized_workspace),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO deal_workspaces (deal_id, workspace_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (normalized_deal_id, normalized_workspace, now, now),
                )
                return

            conn.execute(
                """
                UPDATE deal_workspaces
                SET updated_at = ?
                WHERE deal_id = ? AND workspace_id = ?
                """,
                (now, normalized_deal_id, normalized_workspace),
            )

    def deal_in_workspace(self, deal_id: str, workspace_id: str) -> bool:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT deal_id FROM deal_workspaces WHERE deal_id = ? AND workspace_id = ?",
                (deal_id, normalized_workspace),
            ).fetchone()
        return row is not None

    def list_deals_for_workspace(
        self,
        workspace_id: str,
        include_archived: bool = False,
    ) -> list[dict[str, Any]]:
        normalized_workspace = self._normalize_workspace_id(workspace_id)
        query = """
            SELECT d.deal_id, d.display_name, d.archived, d.created_at, d.updated_at
            FROM deals d
            INNER JOIN deal_workspaces dw
                ON dw.deal_id = d.deal_id
            WHERE dw.workspace_id = ?
        """
        params: list[Any] = [normalized_workspace]
        if not include_archived:
            query += " AND d.archived = 0"
        query += " ORDER BY d.deal_id ASC"

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        return [
            {
                "deal_id": str(row["deal_id"]),
                "display_name": str(row["display_name"]),
                "archived": bool(int(row["archived"])),
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        ]

    def get_deal_meta(self, deal_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT deal_id, display_name, archived, created_at, updated_at FROM deals WHERE deal_id = ?",
                (deal_id,),
            ).fetchone()

        if row is None:
            return None

        return {
            "deal_id": str(row["deal_id"]),
            "display_name": str(row["display_name"]),
            "archived": bool(int(row["archived"])),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def upsert_deal_create_config(
        self,
        *,
        deal_id: str,
        template_id: str,
        forwarding_address: str,
        quick_instruction: str,
        concept_overrides: list[dict[str, Any]],
    ) -> dict[str, Any]:
        normalized_deal_id = str(deal_id).strip()
        normalized_template_id = str(template_id).strip()
        normalized_forwarding_address = str(forwarding_address).strip()
        normalized_instruction = str(quick_instruction).strip()
        if not normalized_deal_id:
            raise ValueError("deal_id cannot be empty")
        if not normalized_template_id:
            raise ValueError("template_id cannot be empty")
        if not normalized_forwarding_address:
            raise ValueError("forwarding_address cannot be empty")
        if not normalized_instruction:
            raise ValueError("quick_instruction cannot be empty")
        overrides_json = json.dumps(list(concept_overrides), sort_keys=True)
        now = datetime.now(timezone.utc).isoformat()

        with self._connect() as conn:
            deal_row = conn.execute(
                "SELECT deal_id FROM deals WHERE deal_id = ?",
                (normalized_deal_id,),
            ).fetchone()
            if deal_row is None:
                raise KeyError("deal_not_found")

            existing = conn.execute(
                "SELECT deal_id FROM deal_create_configs WHERE deal_id = ?",
                (normalized_deal_id,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO deal_create_configs (
                        deal_id,
                        template_id,
                        forwarding_address,
                        quick_instruction,
                        concept_overrides_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_deal_id,
                        normalized_template_id,
                        normalized_forwarding_address,
                        normalized_instruction,
                        overrides_json,
                        now,
                        now,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE deal_create_configs
                    SET template_id = ?,
                        forwarding_address = ?,
                        quick_instruction = ?,
                        concept_overrides_json = ?,
                        updated_at = ?
                    WHERE deal_id = ?
                    """,
                    (
                        normalized_template_id,
                        normalized_forwarding_address,
                        normalized_instruction,
                        overrides_json,
                        now,
                        normalized_deal_id,
                    ),
                )

        config = self.get_deal_create_config(normalized_deal_id)
        if config is None:
            raise RuntimeError("deal_create_config_upsert_failed")
        return config

    def get_deal_create_config(self, deal_id: str) -> dict[str, Any] | None:
        normalized_deal_id = str(deal_id).strip()
        if not normalized_deal_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT deal_id, template_id, forwarding_address, quick_instruction, concept_overrides_json, created_at, updated_at
                FROM deal_create_configs
                WHERE deal_id = ?
                """,
                (normalized_deal_id,),
            ).fetchone()

        if row is None:
            return None
        try:
            concept_overrides = json.loads(str(row["concept_overrides_json"]))
        except json.JSONDecodeError:
            concept_overrides = []

        return {
            "deal_id": str(row["deal_id"]),
            "template_id": str(row["template_id"]),
            "forwarding_address": str(row["forwarding_address"]),
            "quick_instruction": str(row["quick_instruction"]),
            "concept_overrides": concept_overrides if isinstance(concept_overrides, list) else [],
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _reporting_obligation_id(
        *,
        deal_id: str,
        doc_id: str,
        locator_type: str,
        locator_value: str,
        required_concept_id: str,
    ) -> str:
        seed = "|".join(
            [
                deal_id.strip().lower(),
                doc_id.strip(),
                locator_type.strip().lower(),
                locator_value.strip(),
                required_concept_id.strip().lower(),
            ]
        )
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
        return f"obl_{digest}"

    def upsert_reporting_obligations(
        self,
        *,
        deal_id: str,
        obligations: list[dict[str, Any]],
        clear_doc_ids: list[str] | None = None,
    ) -> dict[str, int]:
        normalized_deal_id = str(deal_id).strip()
        if not normalized_deal_id:
            raise ValueError("deal_id_required")

        self.ensure_deal(normalized_deal_id)

        doc_ids_to_clear = sorted(
            {
                str(doc_id).strip()
                for doc_id in (clear_doc_ids or [])
                if str(doc_id).strip()
            }
        )

        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        updated = 0

        with self._connect() as conn:
            if doc_ids_to_clear:
                placeholders = ",".join("?" for _ in doc_ids_to_clear)
                conn.execute(
                    f"""
                    DELETE FROM reporting_obligations
                    WHERE deal_id = ? AND doc_id IN ({placeholders})
                    """,
                    (normalized_deal_id, *doc_ids_to_clear),
                )

            for obligation in obligations:
                if not isinstance(obligation, dict):
                    continue

                doc_id = str(obligation.get("doc_id", "")).strip()
                locator_type = str(obligation.get("locator_type", "")).strip()
                locator_value = str(obligation.get("locator_value", "")).strip()
                required_concept_id = str(obligation.get("required_concept_id", "")).strip().lower()
                if not doc_id or not locator_type or not locator_value or not required_concept_id:
                    continue

                obligation_id = str(obligation.get("obligation_id", "")).strip() or self._reporting_obligation_id(
                    deal_id=normalized_deal_id,
                    doc_id=doc_id,
                    locator_type=locator_type,
                    locator_value=locator_value,
                    required_concept_id=required_concept_id,
                )

                grounding_state = str(obligation.get("grounding_state", "unsupported")).strip().lower()
                if grounding_state not in {"grounded", "ambiguous", "unsupported"}:
                    grounding_state = "unsupported"

                existing = conn.execute(
                    "SELECT obligation_id FROM reporting_obligations WHERE obligation_id = ?",
                    (obligation_id,),
                ).fetchone()

                conn.execute(
                    """
                    INSERT INTO reporting_obligations (
                        obligation_id,
                        deal_id,
                        doc_id,
                        doc_name,
                        doc_type,
                        storage_uri,
                        locator_type,
                        locator_value,
                        page_or_sheet,
                        source_snippet,
                        obligation_type,
                        required_concept_id,
                        required_concept_label,
                        cadence,
                        source_role,
                        grounding_state,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(obligation_id)
                    DO UPDATE SET
                        deal_id = excluded.deal_id,
                        doc_id = excluded.doc_id,
                        doc_name = excluded.doc_name,
                        doc_type = excluded.doc_type,
                        storage_uri = excluded.storage_uri,
                        locator_type = excluded.locator_type,
                        locator_value = excluded.locator_value,
                        page_or_sheet = excluded.page_or_sheet,
                        source_snippet = excluded.source_snippet,
                        obligation_type = excluded.obligation_type,
                        required_concept_id = excluded.required_concept_id,
                        required_concept_label = excluded.required_concept_label,
                        cadence = excluded.cadence,
                        source_role = excluded.source_role,
                        grounding_state = excluded.grounding_state,
                        updated_at = excluded.updated_at
                    """,
                    (
                        obligation_id,
                        normalized_deal_id,
                        doc_id,
                        str(obligation.get("doc_name", "")).strip(),
                        str(obligation.get("doc_type", "")).strip().upper(),
                        str(obligation.get("storage_uri", "")).strip(),
                        locator_type,
                        locator_value,
                        str(obligation.get("page_or_sheet", "")).strip(),
                        str(obligation.get("source_snippet", "")).strip(),
                        str(obligation.get("obligation_type", "")).strip() or "reporting_requirement",
                        required_concept_id,
                        str(obligation.get("required_concept_label", "")).strip() or CONCEPT_LABELS.get(required_concept_id, required_concept_id),
                        str(obligation.get("cadence", "")).strip() or None,
                        str(obligation.get("source_role", "")).strip() or "deal_reporting_document",
                        grounding_state,
                        now,
                        now,
                    ),
                )

                if existing is None:
                    inserted += 1
                else:
                    updated += 1

        return {
            "inserted": inserted,
            "updated": updated,
            "total": inserted + updated,
        }

    def list_reporting_obligations(
        self,
        *,
        deal_id: str,
        grounding_state: str | None = None,
        required_concept_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_deal_id = str(deal_id).strip()
        if not normalized_deal_id:
            return []

        clauses = ["deal_id = ?"]
        params: list[Any] = [normalized_deal_id]

        normalized_state = str(grounding_state or "").strip().lower()
        if normalized_state:
            clauses.append("grounding_state = ?")
            params.append(normalized_state)

        normalized_concept = str(required_concept_id or "").strip().lower()
        if normalized_concept:
            clauses.append("required_concept_id = ?")
            params.append(normalized_concept)

        where = " AND ".join(clauses)
        query = f"""
            SELECT *
            FROM reporting_obligations
            WHERE {where}
            ORDER BY
                CASE grounding_state
                    WHEN 'grounded' THEN 0
                    WHEN 'ambiguous' THEN 1
                    ELSE 2
                END ASC,
                updated_at DESC,
                obligation_id ASC
        """

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        return [self._row_to_reporting_obligation(row) for row in rows]

    def get_reporting_obligation(self, obligation_id: str) -> dict[str, Any] | None:
        normalized = str(obligation_id).strip()
        if not normalized:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM reporting_obligations WHERE obligation_id = ?",
                (normalized,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_reporting_obligation(row)

    @staticmethod
    def _row_to_reporting_obligation(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "obligation_id": str(row["obligation_id"]),
            "deal_id": str(row["deal_id"]),
            "doc_id": str(row["doc_id"]),
            "doc_name": str(row["doc_name"]),
            "doc_type": str(row["doc_type"]),
            "storage_uri": str(row["storage_uri"]),
            "locator_type": str(row["locator_type"]),
            "locator_value": str(row["locator_value"]),
            "page_or_sheet": str(row["page_or_sheet"] or ""),
            "source_snippet": str(row["source_snippet"]),
            "obligation_type": str(row["obligation_type"]),
            "required_concept_id": str(row["required_concept_id"]),
            "required_concept_label": str(row["required_concept_label"]),
            "cadence": str(row["cadence"] or "") or None,
            "source_role": str(row["source_role"]),
            "grounding_state": str(row["grounding_state"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _reporting_obligation_candidate_id(
        *,
        deal_id: str,
        doc_id: str,
        locator_type: str,
        locator_value: str,
        candidate_concept_id: str,
        source_snippet: str,
    ) -> str:
        seed = "|".join(
            [
                deal_id.strip().lower(),
                doc_id.strip(),
                locator_type.strip().lower(),
                locator_value.strip(),
                candidate_concept_id.strip().lower(),
                source_snippet.strip()[:240],
            ]
        )
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
        return f"oblc_{digest}"

    def upsert_reporting_obligation_candidates(
        self,
        *,
        deal_id: str,
        candidates: list[dict[str, Any]],
        clear_doc_ids: list[str] | None = None,
    ) -> dict[str, int]:
        normalized_deal_id = str(deal_id).strip()
        if not normalized_deal_id:
            raise ValueError("deal_id_required")
        self.ensure_deal(normalized_deal_id)

        doc_ids_to_clear = sorted(
            {
                str(doc_id).strip()
                for doc_id in (clear_doc_ids or [])
                if str(doc_id).strip()
            }
        )
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        updated = 0

        with self._connect() as conn:
            if doc_ids_to_clear:
                placeholders = ",".join("?" for _ in doc_ids_to_clear)
                conn.execute(
                    f"""
                    DELETE FROM reporting_obligation_candidates
                    WHERE deal_id = ? AND doc_id IN ({placeholders})
                    """,
                    (normalized_deal_id, *doc_ids_to_clear),
                )

            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue

                doc_id = str(candidate.get("doc_id", "")).strip()
                locator_type = str(candidate.get("locator_type", "")).strip().lower()
                locator_value = str(candidate.get("locator_value", "")).strip()
                candidate_concept_id = str(candidate.get("candidate_concept_id", "")).strip().lower()
                if not doc_id or not locator_type or not locator_value or not candidate_concept_id:
                    continue

                candidate_id = str(candidate.get("candidate_id", "")).strip() or self._reporting_obligation_candidate_id(
                    deal_id=normalized_deal_id,
                    doc_id=doc_id,
                    locator_type=locator_type,
                    locator_value=locator_value,
                    candidate_concept_id=candidate_concept_id,
                    source_snippet=str(candidate.get("source_snippet", "")).strip(),
                )

                grounding_state = str(candidate.get("grounding_state", "candidate")).strip().lower()
                if grounding_state not in {"candidate", "grounded", "ambiguous", "unsupported"}:
                    grounding_state = "candidate"

                existing = conn.execute(
                    "SELECT candidate_id FROM reporting_obligation_candidates WHERE candidate_id = ?",
                    (candidate_id,),
                ).fetchone()

                conn.execute(
                    """
                    INSERT INTO reporting_obligation_candidates (
                        candidate_id,
                        deal_id,
                        doc_id,
                        doc_name,
                        doc_type,
                        storage_uri,
                        locator_type,
                        locator_value,
                        page_or_sheet,
                        source_snippet,
                        candidate_obligation_type,
                        candidate_concept_id,
                        candidate_concept_label,
                        reason,
                        certainty_bucket,
                        model_name,
                        extraction_mode,
                        raw_model_output_json,
                        grounding_state,
                        promoted_obligation_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(candidate_id)
                    DO UPDATE SET
                        deal_id = excluded.deal_id,
                        doc_id = excluded.doc_id,
                        doc_name = excluded.doc_name,
                        doc_type = excluded.doc_type,
                        storage_uri = excluded.storage_uri,
                        locator_type = excluded.locator_type,
                        locator_value = excluded.locator_value,
                        page_or_sheet = excluded.page_or_sheet,
                        source_snippet = excluded.source_snippet,
                        candidate_obligation_type = excluded.candidate_obligation_type,
                        candidate_concept_id = excluded.candidate_concept_id,
                        candidate_concept_label = excluded.candidate_concept_label,
                        reason = excluded.reason,
                        certainty_bucket = excluded.certainty_bucket,
                        model_name = excluded.model_name,
                        extraction_mode = excluded.extraction_mode,
                        raw_model_output_json = excluded.raw_model_output_json,
                        grounding_state = excluded.grounding_state,
                        promoted_obligation_id = excluded.promoted_obligation_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        candidate_id,
                        normalized_deal_id,
                        doc_id,
                        str(candidate.get("doc_name", "")).strip(),
                        str(candidate.get("doc_type", "")).strip().upper(),
                        str(candidate.get("storage_uri", "")).strip(),
                        locator_type,
                        locator_value,
                        str(candidate.get("page_or_sheet", "")).strip(),
                        str(candidate.get("source_snippet", "")).strip(),
                        str(candidate.get("candidate_obligation_type", "")).strip() or "reporting_requirement",
                        candidate_concept_id,
                        str(candidate.get("candidate_concept_label", "")).strip()
                        or CONCEPT_LABELS.get(candidate_concept_id, candidate_concept_id),
                        str(candidate.get("reason", "")).strip() or "candidate_discovered",
                        str(candidate.get("certainty_bucket", "")).strip().lower() or "unknown",
                        str(candidate.get("model_name", "")).strip() or None,
                        str(candidate.get("extraction_mode", "")).strip() or "llm_candidate_discovery",
                        self._encode_payload(candidate.get("raw_model_output", {})),
                        grounding_state,
                        (
                            (str(candidate.get("promoted_obligation_id")).strip())
                            if isinstance(candidate.get("promoted_obligation_id"), str)
                            and str(candidate.get("promoted_obligation_id")).strip()
                            and str(candidate.get("promoted_obligation_id")).strip().lower() != "none"
                            else None
                        ),
                        now,
                        now,
                    ),
                )

                if existing is None:
                    inserted += 1
                else:
                    updated += 1

        return {
            "inserted": inserted,
            "updated": updated,
            "total": inserted + updated,
        }

    def list_reporting_obligation_candidates(
        self,
        *,
        deal_id: str,
        grounding_state: str | None = None,
        candidate_concept_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_deal_id = str(deal_id).strip()
        if not normalized_deal_id:
            return []

        clauses = ["deal_id = ?"]
        params: list[Any] = [normalized_deal_id]

        normalized_state = str(grounding_state or "").strip().lower()
        if normalized_state:
            clauses.append("grounding_state = ?")
            params.append(normalized_state)

        normalized_concept = str(candidate_concept_id or "").strip().lower()
        if normalized_concept:
            clauses.append("candidate_concept_id = ?")
            params.append(normalized_concept)

        where = " AND ".join(clauses)
        query = f"""
            SELECT *
            FROM reporting_obligation_candidates
            WHERE {where}
            ORDER BY
                CASE grounding_state
                    WHEN 'grounded' THEN 0
                    WHEN 'ambiguous' THEN 1
                    WHEN 'candidate' THEN 2
                    ELSE 3
                END ASC,
                updated_at DESC,
                candidate_id ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_reporting_obligation_candidate(row) for row in rows]

    def _row_to_reporting_obligation_candidate(self, row: sqlite3.Row) -> dict[str, Any]:
        promoted = str(row["promoted_obligation_id"] or "").strip()
        if promoted.lower() == "none":
            promoted = ""
        return {
            "candidate_id": str(row["candidate_id"]),
            "deal_id": str(row["deal_id"]),
            "doc_id": str(row["doc_id"]),
            "doc_name": str(row["doc_name"]),
            "doc_type": str(row["doc_type"]),
            "storage_uri": str(row["storage_uri"]),
            "locator_type": str(row["locator_type"]),
            "locator_value": str(row["locator_value"]),
            "page_or_sheet": str(row["page_or_sheet"] or ""),
            "source_snippet": str(row["source_snippet"]),
            "candidate_obligation_type": str(row["candidate_obligation_type"]),
            "candidate_concept_id": str(row["candidate_concept_id"]),
            "candidate_concept_label": str(row["candidate_concept_label"]),
            "reason": str(row["reason"] or ""),
            "certainty_bucket": str(row["certainty_bucket"] or "unknown"),
            "model_name": str(row["model_name"] or ""),
            "extraction_mode": str(row["extraction_mode"]),
            "raw_model_output": self._decode_payload(str(row["raw_model_output_json"])),
            "grounding_state": str(row["grounding_state"]),
            "promoted_obligation_id": promoted or None,
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def is_deal_archived(self, deal_id: str) -> bool:
        meta = self.get_deal_meta(deal_id)
        if meta is None:
            return False
        return bool(meta.get("archived", False))

    def update_deal_display_name(self, deal_id: str, display_name: str) -> dict[str, Any]:
        desired = str(display_name).strip()
        if not desired:
            raise ValueError("display_name cannot be empty")

        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT deal_id FROM deals WHERE deal_id = ?",
                (deal_id,),
            ).fetchone()
            if row is None:
                raise KeyError("deal_not_found")

            conn.execute(
                """
                UPDATE deals
                SET display_name = ?,
                    updated_at = ?
                WHERE deal_id = ?
                """,
                (desired, now, deal_id),
            )

        result = self.get_deal_meta(deal_id)
        if result is None:
            raise KeyError("deal_not_found")
        return result

    def set_deal_archived(self, deal_id: str, archived: bool) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT deal_id FROM deals WHERE deal_id = ?",
                (deal_id,),
            ).fetchone()
            if row is None:
                raise KeyError("deal_not_found")

            conn.execute(
                """
                UPDATE deals
                SET archived = ?,
                    updated_at = ?
                WHERE deal_id = ?
                """,
                (1 if archived else 0, now, deal_id),
            )

        result = self.get_deal_meta(deal_id)
        if result is None:
            raise KeyError("deal_not_found")
        return result

    @staticmethod
    def _normalize_email(email: str) -> str:
        normalized = str(email).strip().lower()
        if not normalized:
            raise ValueError("email_required")
        return normalized

    def ensure_user(self, email: str) -> dict[str, Any]:
        normalized = self._normalize_email(email)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT email FROM users WHERE email = ?",
                (normalized,),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO users (email, password_hash, status, created_at, updated_at, last_login_at)
                    VALUES (?, NULL, 'pending_password', ?, ?, NULL)
                    """,
                    (normalized, now, now),
                )

        result = self.get_user(normalized)
        if result is None:
            raise RuntimeError("user_upsert_failed")
        return result

    def get_user(self, email: str) -> dict[str, Any] | None:
        normalized = self._normalize_email(email)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT email, password_hash, status, created_at, updated_at, last_login_at
                FROM users
                WHERE email = ?
                """,
                (normalized,),
            ).fetchone()

        if row is None:
            return None
        return {
            "email": str(row["email"]),
            "password_hash": row["password_hash"],
            "status": str(row["status"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "last_login_at": row["last_login_at"],
        }

    def set_user_password(self, email: str, password_hash: str) -> dict[str, Any]:
        normalized = self._normalize_email(email)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            updated = conn.execute(
                """
                UPDATE users
                SET password_hash = ?,
                    status = 'active',
                    updated_at = ?
                WHERE email = ?
                """,
                (password_hash, now, normalized),
            )
            if updated.rowcount == 0:
                raise KeyError("user_not_found")

        result = self.get_user(normalized)
        if result is None:
            raise KeyError("user_not_found")
        return result

    def create_magic_link(self, email: str, token_hash: str, expires_at: str) -> None:
        normalized = self._normalize_email(email)
        self.ensure_user(normalized)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO magic_links (token_hash, email, created_at, expires_at, consumed_at)
                VALUES (?, ?, ?, ?, NULL)
                """,
                (token_hash, normalized, now, expires_at),
            )

    def consume_magic_link(self, token_hash: str, consumed_at: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT token_hash, email, created_at, expires_at, consumed_at
                FROM magic_links
                WHERE token_hash = ?
                """,
                (token_hash,),
            ).fetchone()

            if row is None:
                return None

            if row["consumed_at"] is not None:
                return None

            expires_at = str(row["expires_at"])
            if self._parse_iso(expires_at) < self._parse_iso(consumed_at):
                return None

            conn.execute(
                """
                UPDATE magic_links
                SET consumed_at = ?
                WHERE token_hash = ?
                """,
                (consumed_at, token_hash),
            )

        return {
            "email": str(row["email"]),
            "created_at": str(row["created_at"]),
            "expires_at": str(row["expires_at"]),
            "consumed_at": consumed_at,
        }

    def create_session(self, email: str, token_hash: str, expires_at: str) -> None:
        normalized = self._normalize_email(email)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sessions (token_hash, email, created_at, expires_at, revoked_at)
                VALUES (?, ?, ?, ?, NULL)
                """,
                (token_hash, normalized, now, expires_at),
            )
            conn.execute(
                """
                UPDATE users
                SET last_login_at = ?,
                    updated_at = ?
                WHERE email = ?
                """,
                (now, now, normalized),
            )

    def get_session(self, token_hash: str, now_iso: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT token_hash, email, created_at, expires_at, revoked_at
                FROM sessions
                WHERE token_hash = ?
                """,
                (token_hash,),
            ).fetchone()

        if row is None:
            return None
        if row["revoked_at"] is not None:
            return None
        if self._parse_iso(str(row["expires_at"])) < self._parse_iso(now_iso):
            return None
        return {
            "token_hash": str(row["token_hash"]),
            "email": str(row["email"]),
            "created_at": str(row["created_at"]),
            "expires_at": str(row["expires_at"]),
        }

    def revoke_session(self, token_hash: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE sessions
                SET revoked_at = ?
                WHERE token_hash = ?
                """,
                (now, token_hash),
            )

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            text = value.strip()
            if not text or text.upper() == "N/A":
                return None
            try:
                return float(text.replace(",", ""))
            except ValueError:
                return None
        return None

    def _next_period_revision(self, deal_id: str, period_end_date: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(period_revision) AS max_revision
                FROM packages
                WHERE deal_id = ? AND period_end_date = ?
                """,
                (deal_id, period_end_date),
            ).fetchone()
        max_revision = row["max_revision"] if row and row["max_revision"] is not None else 0
        return int(max_revision) + 1

    def upsert_package(
        self,
        package_id: str,
        idempotency_key: str,
        sender_email: str,
        source_email_id: str,
        deal_id: str,
        period_end_date: str,
        received_at: str,
        status: str,
        package_manifest: dict[str, Any],
    ) -> tuple[PackageRecord, bool]:
        existing = self.get_package_by_idempotency(idempotency_key)
        if existing:
            return existing, False

        self.ensure_deal(deal_id)
        period_revision = self._next_period_revision(deal_id=deal_id, period_end_date=period_end_date)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO packages (
                    package_id,
                    idempotency_key,
                    sender_email,
                    source_email_id,
                    deal_id,
                    period_end_date,
                    received_at,
                    period_revision,
                    status,
                    package_manifest_json,
                    processed_payload_json,
                    error_message,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                    (
                        package_id,
                        idempotency_key,
                    sender_email,
                    source_email_id,
                    deal_id,
                    period_end_date,
                    received_at,
                        period_revision,
                        status,
                        self._encode_payload(package_manifest),
                        now,
                        now,
                    ),
            )

        created = self.get_package(package_id)
        if not created:
            raise RuntimeError("Failed to create package")
        return created, True

    def get_package_by_idempotency(self, idempotency_key: str) -> PackageRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM packages WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return self._row_to_package(row)

    def get_package(self, package_id: str) -> PackageRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM packages WHERE package_id = ?",
                (package_id,),
            ).fetchone()
        return self._row_to_package(row)

    def list_packages(self) -> list[PackageRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM packages
                ORDER BY deal_id ASC, period_end_date DESC, period_revision DESC, created_at DESC
                """
            ).fetchall()
        records: list[PackageRecord] = []
        for row in rows:
            record = self._row_to_package(row)
            if record is not None:
                records.append(record)
        return records

    def update_package_status(
        self,
        package_id: str,
        status: str,
        processed_payload: dict[str, Any] | None = None,
        error_message: str | None = None,
        preserve_payload: bool = False,
    ) -> None:
        if status not in PACKAGE_STATUSES:
            raise ValueError(f"invalid status: {status}")

        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            if preserve_payload:
                conn.execute(
                    """
                    UPDATE packages
                    SET status = ?,
                        error_message = ?,
                        updated_at = ?
                    WHERE package_id = ?
                    """,
                    (
                        status,
                        error_message,
                        now,
                        package_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE packages
                    SET status = ?,
                        processed_payload_json = ?,
                        error_message = ?,
                        updated_at = ?
                    WHERE package_id = ?
                    """,
                    (
                        status,
                        self._encode_payload(processed_payload) if processed_payload else None,
                        error_message,
                        now,
                        package_id,
                    ),
                )

    def upsert_traces(
        self,
        package_id: str,
        deal_id: str,
        period_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            for row in rows:
                trace_id = str(row.get("trace_id", "")).strip()
                if not trace_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO traces (
                        trace_id,
                        package_id,
                        deal_id,
                        period_id,
                        concept_id,
                        row_json,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(trace_id)
                    DO NOTHING
                    """,
                    (
                        trace_id,
                        package_id,
                        deal_id,
                        period_id,
                        row.get("concept_id", ""),
                        self._encode_payload(row),
                        now,
                    ),
                )

    def _encode_payload(self, payload: dict[str, Any]) -> str:
        text = json.dumps(payload, sort_keys=True)
        if self._cipher is None:
            return text
        token = self._cipher.encrypt(text.encode("utf-8")).decode("utf-8")
        return f"enc:{token}"

    def _decode_payload(self, value: str) -> dict[str, Any]:
        text = str(value)
        if text.startswith("enc:"):
            if self._cipher is None:
                raise RuntimeError("Encrypted payload found but store has no encryption key")
            text = self._cipher.decrypt(text[4:].encode("utf-8")).decode("utf-8")

        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
        return {}

    @staticmethod
    def _compute_lifecycle_status(rows: list[dict[str, Any]]) -> str:
        statuses = {str(row.get("status", "unresolved")) for row in rows}
        if "unresolved" in statuses or "candidate_flagged" in statuses:
            return "needs_review"
        return "completed"

    def _latest_resolution_rows(self, trace_ids: list[str]) -> dict[str, sqlite3.Row]:
        normalized_ids = [trace_id for trace_id in trace_ids if trace_id]
        if not normalized_ids:
            return {}

        placeholders = ",".join("?" for _ in normalized_ids)
        query = f"""
            SELECT *
            FROM trace_resolutions
            WHERE trace_id IN ({placeholders})
            ORDER BY resolution_id DESC
        """

        with self._connect() as conn:
            rows = conn.execute(query, tuple(normalized_ids)).fetchall()

        latest: dict[str, sqlite3.Row] = {}
        for row in rows:
            trace_id = str(row["trace_id"])
            if trace_id in latest:
                continue
            latest[trace_id] = row
        return latest

    def _apply_latest_resolution(
        self,
        row: dict[str, Any],
        latest_resolution: sqlite3.Row | None,
    ) -> dict[str, Any]:
        if latest_resolution is None:
            return row

        resolved_row = self._decode_payload(str(latest_resolution["row_after_json"]))
        resolved_row["resolution"] = {
            "resolution_id": int(latest_resolution["resolution_id"]),
            "resolver": str(latest_resolution["resolver"]),
            "resolved_at": str(latest_resolution["resolved_at"]),
        }
        return resolved_row

    def _resolve_rows_with_latest(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        trace_ids = [str(row.get("trace_id", "")) for row in rows]
        latest = self._latest_resolution_rows(trace_ids)

        resolved: list[dict[str, Any]] = []
        for row in rows:
            trace_id = str(row.get("trace_id", ""))
            resolved.append(self._apply_latest_resolution(dict(row), latest.get(trace_id)))
        return resolved

    def append_trace_resolution(
        self,
        *,
        trace_id: str,
        package_id: str,
        resolver: str,
        selected_evidence: dict[str, Any],
        note: str,
        row_before: dict[str, Any],
        row_after: dict[str, Any],
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        status_before = str(row_before.get("status", "unresolved"))
        status_after = str(row_after.get("status", status_before))
        confidence_before = float(row_before.get("confidence", 0.0) or 0.0)
        confidence_after = float(row_after.get("confidence", confidence_before) or confidence_before)

        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO trace_resolutions (
                    trace_id,
                    package_id,
                    resolver,
                    resolved_at,
                    status_before,
                    status_after,
                    confidence_before,
                    confidence_after,
                    note,
                    selected_evidence_json,
                    row_before_json,
                    row_after_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    package_id,
                    resolver,
                    now,
                    status_before,
                    status_after,
                    confidence_before,
                    confidence_after,
                    note,
                    self._encode_payload(selected_evidence),
                    self._encode_payload(row_before),
                    self._encode_payload(row_after),
                ),
            )
            return int(cursor.lastrowid)

    def get_latest_trace_resolution(self, trace_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM trace_resolutions
                WHERE trace_id = ?
                ORDER BY resolution_id DESC
                LIMIT 1
                """,
                (trace_id,),
            ).fetchone()

        if row is None:
            return None

        return {
            "resolution_id": int(row["resolution_id"]),
            "trace_id": str(row["trace_id"]),
            "package_id": str(row["package_id"]),
            "resolver": str(row["resolver"]),
            "resolved_at": str(row["resolved_at"]),
            "status_before": str(row["status_before"]),
            "status_after": str(row["status_after"]),
            "confidence_before": float(row["confidence_before"]),
            "confidence_after": float(row["confidence_after"]),
            "note": str(row["note"] or ""),
            "selected_evidence": self._decode_payload(str(row["selected_evidence_json"])),
            "row_before": self._decode_payload(str(row["row_before_json"])),
            "row_after": self._decode_payload(str(row["row_after_json"])),
        }

    def list_trace_resolutions(self, trace_id: str, limit: int = 100) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 1000))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM trace_resolutions
                WHERE trace_id = ?
                ORDER BY resolution_id ASC
                LIMIT ?
                """,
                (trace_id, bounded_limit),
            ).fetchall()

        history: list[dict[str, Any]] = []
        for row in rows:
            history.append(
                {
                    "resolution_id": int(row["resolution_id"]),
                    "trace_id": str(row["trace_id"]),
                    "package_id": str(row["package_id"]),
                    "resolver": str(row["resolver"]),
                    "resolved_at": str(row["resolved_at"]),
                    "status_before": str(row["status_before"]),
                    "status_after": str(row["status_after"]),
                    "confidence_before": float(row["confidence_before"]),
                    "confidence_after": float(row["confidence_after"]),
                    "note": str(row["note"] or ""),
                    "selected_evidence": self._decode_payload(str(row["selected_evidence_json"])),
                }
            )
        return history

    def record_review_case_feedback(
        self,
        *,
        deal_id: str,
        period_id: str,
        item_id: str,
        concept_id: str,
        concept_maturity: str,
        trust_tier: str,
        case_mode: str,
        action_id: str,
        outcome: str,
        actor: str,
        note: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_deal_id = str(deal_id).strip()
        normalized_period_id = str(period_id).strip()
        normalized_item_id = str(item_id).strip()
        normalized_concept_id = str(concept_id).strip().lower()
        normalized_maturity = str(concept_maturity).strip().lower()
        normalized_trust_tier = str(trust_tier).strip().lower()
        normalized_case_mode = str(case_mode).strip().lower()
        normalized_action_id = str(action_id).strip().lower()
        normalized_outcome = str(outcome).strip().lower()
        normalized_actor = str(actor).strip() or "operator_ui"
        normalized_note = str(note).strip()
        allowed_outcomes = {"confirmed", "dismissed", "expected_noise", "borrower_followup"}
        if normalized_outcome not in allowed_outcomes:
            raise ValueError("invalid_review_feedback_outcome")
        if not normalized_deal_id or not normalized_period_id or not normalized_item_id:
            raise ValueError("invalid_review_feedback_scope")
        if not normalized_concept_id or not normalized_maturity or not normalized_action_id:
            raise ValueError("invalid_review_feedback_payload")

        now = datetime.now(timezone.utc).isoformat()
        payload = metadata if isinstance(metadata, dict) else {}

        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO review_case_feedback (
                    deal_id,
                    period_id,
                    item_id,
                    concept_id,
                    concept_maturity,
                    trust_tier,
                    case_mode,
                    action_id,
                    outcome,
                    actor,
                    note,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_deal_id,
                    normalized_period_id,
                    normalized_item_id,
                    normalized_concept_id,
                    normalized_maturity,
                    normalized_trust_tier,
                    normalized_case_mode,
                    normalized_action_id,
                    normalized_outcome,
                    normalized_actor,
                    normalized_note or None,
                    self._encode_payload(payload),
                    now,
                ),
            )
            feedback_id = int(cursor.lastrowid)

        return {
            "feedback_id": feedback_id,
            "deal_id": normalized_deal_id,
            "period_id": normalized_period_id,
            "item_id": normalized_item_id,
            "concept_id": normalized_concept_id,
            "concept_maturity": normalized_maturity,
            "trust_tier": normalized_trust_tier,
            "case_mode": normalized_case_mode,
            "action_id": normalized_action_id,
            "outcome": normalized_outcome,
            "actor": normalized_actor,
            "note": normalized_note,
            "metadata": payload,
            "created_at": now,
        }

    def list_review_case_feedback(
        self,
        *,
        deal_id: str,
        period_id: str,
        item_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        normalized_deal_id = str(deal_id).strip()
        normalized_period_id = str(period_id).strip()
        normalized_item_id = str(item_id or "").strip()
        if not normalized_deal_id or not normalized_period_id:
            return []

        bounded_limit = max(1, min(int(limit), 1000))
        clauses = ["deal_id = ?", "period_id = ?"]
        params: list[Any] = [normalized_deal_id, normalized_period_id]
        if normalized_item_id:
            clauses.append("item_id = ?")
            params.append(normalized_item_id)

        query = f"""
            SELECT *
            FROM review_case_feedback
            WHERE {' AND '.join(clauses)}
            ORDER BY feedback_id DESC
            LIMIT ?
        """
        params.append(bounded_limit)

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        feedback: list[dict[str, Any]] = []
        for row in rows:
            feedback.append(
                {
                    "feedback_id": int(row["feedback_id"]),
                    "deal_id": str(row["deal_id"]),
                    "period_id": str(row["period_id"]),
                    "item_id": str(row["item_id"]),
                    "concept_id": str(row["concept_id"]),
                    "concept_maturity": str(row["concept_maturity"]),
                    "trust_tier": str(row["trust_tier"]),
                    "case_mode": str(row["case_mode"]),
                    "action_id": str(row["action_id"]),
                    "outcome": str(row["outcome"]),
                    "actor": str(row["actor"]),
                    "note": str(row["note"] or ""),
                    "metadata": self._decode_payload(str(row["metadata_json"])),
                    "created_at": str(row["created_at"]),
                }
            )
        return feedback

    def record_borrower_draft_event(
        self,
        *,
        deal_id: str,
        period_id: str,
        item_id: str,
        concept_id: str,
        concept_maturity: str,
        trust_tier: str,
        case_mode: str,
        event_type: str,
        actor: str,
        subject: str = "",
        draft_text: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_deal_id = str(deal_id).strip()
        normalized_period_id = str(period_id).strip()
        normalized_item_id = str(item_id).strip()
        normalized_concept_id = str(concept_id).strip().lower()
        normalized_maturity = str(concept_maturity).strip().lower()
        normalized_trust_tier = str(trust_tier).strip().lower()
        normalized_case_mode = str(case_mode).strip().lower()
        normalized_event_type = str(event_type).strip().lower()
        normalized_actor = str(actor).strip() or "operator_ui"
        normalized_subject = str(subject).strip()
        normalized_draft_text = str(draft_text)
        allowed_event_types = {
            "draft_opened",
            "draft_prepared",
            "draft_edited",
            "draft_copied",
            "draft_closed",
        }
        if normalized_event_type not in allowed_event_types:
            raise ValueError("invalid_draft_event_type")
        if not normalized_deal_id or not normalized_period_id or not normalized_item_id:
            raise ValueError("invalid_draft_event_scope")
        if not normalized_concept_id or not normalized_maturity:
            raise ValueError("invalid_draft_event_payload")

        now = datetime.now(timezone.utc).isoformat()
        payload = metadata if isinstance(metadata, dict) else {}
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO borrower_draft_events (
                    deal_id,
                    period_id,
                    item_id,
                    concept_id,
                    concept_maturity,
                    trust_tier,
                    case_mode,
                    event_type,
                    actor,
                    subject,
                    draft_text,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_deal_id,
                    normalized_period_id,
                    normalized_item_id,
                    normalized_concept_id,
                    normalized_maturity,
                    normalized_trust_tier,
                    normalized_case_mode,
                    normalized_event_type,
                    normalized_actor,
                    normalized_subject or None,
                    normalized_draft_text or None,
                    self._encode_payload(payload),
                    now,
                ),
            )
            event_id = int(cursor.lastrowid)

        return {
            "event_id": event_id,
            "deal_id": normalized_deal_id,
            "period_id": normalized_period_id,
            "item_id": normalized_item_id,
            "concept_id": normalized_concept_id,
            "concept_maturity": normalized_maturity,
            "trust_tier": normalized_trust_tier,
            "case_mode": normalized_case_mode,
            "event_type": normalized_event_type,
            "actor": normalized_actor,
            "subject": normalized_subject,
            "draft_text": normalized_draft_text,
            "metadata": payload,
            "created_at": now,
        }

    def list_borrower_draft_events(
        self,
        *,
        deal_id: str,
        period_id: str,
        item_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        normalized_deal_id = str(deal_id).strip()
        normalized_period_id = str(period_id).strip()
        normalized_item_id = str(item_id or "").strip()
        if not normalized_deal_id or not normalized_period_id:
            return []

        bounded_limit = max(1, min(int(limit), 1000))
        clauses = ["deal_id = ?", "period_id = ?"]
        params: list[Any] = [normalized_deal_id, normalized_period_id]
        if normalized_item_id:
            clauses.append("item_id = ?")
            params.append(normalized_item_id)

        query = f"""
            SELECT *
            FROM borrower_draft_events
            WHERE {' AND '.join(clauses)}
            ORDER BY event_id DESC
            LIMIT ?
        """
        params.append(bounded_limit)

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        events: list[dict[str, Any]] = []
        for row in rows:
            events.append(
                {
                    "event_id": int(row["event_id"]),
                    "deal_id": str(row["deal_id"]),
                    "period_id": str(row["period_id"]),
                    "item_id": str(row["item_id"]),
                    "concept_id": str(row["concept_id"]),
                    "concept_maturity": str(row["concept_maturity"]),
                    "trust_tier": str(row["trust_tier"]),
                    "case_mode": str(row["case_mode"]),
                    "event_type": str(row["event_type"]),
                    "actor": str(row["actor"]),
                    "subject": str(row["subject"] or ""),
                    "draft_text": str(row["draft_text"] or ""),
                    "metadata": self._decode_payload(str(row["metadata_json"])),
                    "created_at": str(row["created_at"]),
                }
            )
        return events

    def get_analyst_note(
        self,
        *,
        deal_id: str,
        period_id: str,
        item_id: str,
    ) -> dict[str, Any] | None:
        normalized_deal_id = str(deal_id).strip()
        normalized_period_id = str(period_id).strip()
        normalized_item_id = str(item_id).strip()
        if not normalized_deal_id or not normalized_period_id or not normalized_item_id:
            return None

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM analyst_notes
                WHERE deal_id = ? AND period_id = ? AND item_id = ?
                LIMIT 1
                """,
                (normalized_deal_id, normalized_period_id, normalized_item_id),
            ).fetchone()

        if row is None:
            return None
        return {
            "note_id": int(row["note_id"]),
            "deal_id": str(row["deal_id"]),
            "period_id": str(row["period_id"]),
            "item_id": str(row["item_id"]),
            "concept_id": str(row["concept_id"]),
            "concept_maturity": str(row["concept_maturity"]),
            "trust_tier": str(row["trust_tier"]),
            "case_mode": str(row["case_mode"]),
            "author": str(row["author"]),
            "subject": str(row["subject"] or ""),
            "note_text": str(row["note_text"] or ""),
            "memo_ready": bool(int(row["memo_ready"])),
            "export_ready": bool(int(row["export_ready"])),
            "metadata": self._decode_payload(str(row["metadata_json"])),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def upsert_analyst_note(
        self,
        *,
        deal_id: str,
        period_id: str,
        item_id: str,
        concept_id: str,
        concept_maturity: str,
        trust_tier: str,
        case_mode: str,
        author: str,
        subject: str,
        note_text: str,
        memo_ready: bool,
        export_ready: bool,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_deal_id = str(deal_id).strip()
        normalized_period_id = str(period_id).strip()
        normalized_item_id = str(item_id).strip()
        normalized_concept_id = str(concept_id).strip().lower()
        normalized_maturity = str(concept_maturity).strip().lower()
        normalized_trust_tier = str(trust_tier).strip().lower()
        normalized_case_mode = str(case_mode).strip().lower()
        normalized_author = str(author).strip() or "operator_ui"
        normalized_subject = str(subject).strip()
        normalized_note_text = str(note_text).strip()
        payload = metadata if isinstance(metadata, dict) else {}

        if not normalized_deal_id or not normalized_period_id or not normalized_item_id:
            raise ValueError("invalid_analyst_note_scope")
        if not normalized_concept_id or not normalized_maturity:
            raise ValueError("invalid_analyst_note_payload")
        if not normalized_note_text:
            raise ValueError("analyst_note_text_required")

        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT note_id, created_at
                FROM analyst_notes
                WHERE deal_id = ? AND period_id = ? AND item_id = ?
                LIMIT 1
                """,
                (normalized_deal_id, normalized_period_id, normalized_item_id),
            ).fetchone()

            if existing is None:
                cursor = conn.execute(
                    """
                    INSERT INTO analyst_notes (
                        deal_id,
                        period_id,
                        item_id,
                        concept_id,
                        concept_maturity,
                        trust_tier,
                        case_mode,
                        author,
                        subject,
                        note_text,
                        memo_ready,
                        export_ready,
                        metadata_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_deal_id,
                        normalized_period_id,
                        normalized_item_id,
                        normalized_concept_id,
                        normalized_maturity,
                        normalized_trust_tier,
                        normalized_case_mode,
                        normalized_author,
                        normalized_subject or None,
                        normalized_note_text,
                        1 if memo_ready else 0,
                        1 if export_ready else 0,
                        self._encode_payload(payload),
                        now,
                        now,
                    ),
                )
                note_id = int(cursor.lastrowid)
            else:
                note_id = int(existing["note_id"])
                conn.execute(
                    """
                    UPDATE analyst_notes
                    SET concept_id = ?,
                        concept_maturity = ?,
                        trust_tier = ?,
                        case_mode = ?,
                        author = ?,
                        subject = ?,
                        note_text = ?,
                        memo_ready = ?,
                        export_ready = ?,
                        metadata_json = ?,
                        updated_at = ?
                    WHERE note_id = ?
                    """,
                    (
                        normalized_concept_id,
                        normalized_maturity,
                        normalized_trust_tier,
                        normalized_case_mode,
                        normalized_author,
                        normalized_subject or None,
                        normalized_note_text,
                        1 if memo_ready else 0,
                        1 if export_ready else 0,
                        self._encode_payload(payload),
                        now,
                        note_id,
                    ),
                )

        saved = self.get_analyst_note(
            deal_id=normalized_deal_id,
            period_id=normalized_period_id,
            item_id=normalized_item_id,
        )
        if saved is None:
            raise RuntimeError("analyst_note_upsert_failed")
        return saved

    def compute_effective_package_status(self, package_id: str) -> str:
        package = self.get_package(package_id)
        if package is None or package.processed_payload is None:
            return "received" if package is None else package.status

        package_rows: list[dict[str, Any]] = []
        for item in package.processed_payload.get("packages", []):
            if item.get("package_id") != package_id:
                continue
            package_rows = item.get("rows", [])
            break

        effective_rows = self._resolve_rows_with_latest(package_rows)
        return self._compute_lifecycle_status(effective_rows)

    def get_delta(self, deal_id: str, period_id: str) -> dict[str, Any] | None:
        package = self.get_package(period_id)
        if not package or package.deal_id != deal_id:
            return None

        if not package.processed_payload:
            return {
                "deal_id": deal_id,
                "period_id": period_id,
                "status": package.status,
                "period_revision": package.period_revision,
                "prior_period_id": None,
                "is_baseline": True,
                "rows": [],
            }

        packages = package.processed_payload.get("packages", [])
        package_rows: list[dict[str, Any]] = []
        for item in packages:
            if item.get("package_id") == period_id:
                package_rows = item.get("rows", [])
                break
        package_rows = self._resolve_rows_with_latest(package_rows)

        deal_packages = [
            pkg for pkg in self.list_packages()
            if pkg.deal_id == deal_id and pkg.processed_payload is not None
        ]
        deal_packages_sorted = sorted(
            deal_packages,
            key=lambda item: (item.period_end_date, item.period_revision, item.received_at),
        )

        current_idx = -1
        for idx, item in enumerate(deal_packages_sorted):
            if item.package_id == period_id:
                current_idx = idx
                break

        prior_rows_by_concept: dict[str, dict[str, Any]] = {}
        prior_period_id: str | None = None
        if current_idx > 0:
            prior_package = deal_packages_sorted[current_idx - 1]
            prior_period_id = prior_package.package_id
            for prior_item in prior_package.processed_payload.get("packages", []):
                if prior_item.get("package_id") != prior_package.package_id:
                    continue
                for row in prior_item.get("rows", []):
                    prior_rows_by_concept[str(row.get("concept_id", ""))] = row
                break
            if prior_rows_by_concept:
                prior_rows_resolved = self._resolve_rows_with_latest(list(prior_rows_by_concept.values()))
                prior_rows_by_concept = {
                    str(row.get("concept_id", "")): row
                    for row in prior_rows_resolved
                }

        enriched_rows: list[dict[str, Any]] = []
        for row in package_rows:
            item = dict(row)
            concept_id = str(item.get("concept_id", ""))

            current_value = item.get("current_value", item.get("normalized_value"))
            prior_row = prior_rows_by_concept.get(concept_id)
            prior_value = (
                prior_row.get("current_value", prior_row.get("normalized_value"))
                if prior_row is not None
                else None
            )

            current_num = self._as_float(current_value)
            prior_num = self._as_float(prior_value)

            if prior_num is None:
                prior_output: float | str = "N/A"
                abs_delta: float | str = "N/A"
                pct_delta: float | str = "N/A"
            else:
                prior_output = prior_num
                if current_num is None:
                    abs_delta = "N/A"
                    pct_delta = "N/A"
                else:
                    abs_delta = round(current_num - prior_num, 4)
                    if prior_num == 0:
                        pct_delta = "N/A"
                    else:
                        pct_delta = round(((current_num - prior_num) / abs(prior_num)) * 100.0, 4)

            evidence = item.get("evidence", {})
            item["label"] = item.get("label", CONCEPT_LABELS.get(concept_id, concept_id))
            item["prior_value"] = prior_output
            item["current_value"] = current_value
            item["abs_delta"] = abs_delta
            item["pct_delta"] = pct_delta
            item["dictionary_version"] = item.get("dictionary_version", "v1.0")
            item["evidence_link"] = item.get(
                "evidence_link",
                {
                    "doc_id": evidence.get("doc_id", ""),
                    "doc_name": evidence.get("doc_name", ""),
                    "page_or_sheet": evidence.get("page_or_sheet", ""),
                    "locator_type": evidence.get("locator_type", "paragraph"),
                    "locator_value": evidence.get("locator_value", ""),
                },
            )
            enriched_rows.append(item)

        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "status": package.status,
            "period_revision": package.period_revision,
            "prior_period_id": prior_period_id,
            "is_baseline": prior_period_id is None,
            "rows": enriched_rows,
        }

    def get_trace(self, trace_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM traces WHERE trace_id = ?",
                (trace_id,),
            ).fetchone()

        if not row:
            return None

        base_row = self._decode_payload(str(row["row_json"]))
        latest_resolution = self.get_latest_trace_resolution(trace_id)
        effective_row = base_row
        if latest_resolution is not None:
            effective_row = dict(latest_resolution["row_after"])
            effective_row["resolution"] = {
                "resolution_id": latest_resolution["resolution_id"],
                "resolver": latest_resolution["resolver"],
                "resolved_at": latest_resolution["resolved_at"],
            }

        return {
            "trace_id": row["trace_id"],
            "package_id": row["package_id"],
            "deal_id": row["deal_id"],
            "period_id": row["period_id"],
            "concept_id": row["concept_id"],
            "row": effective_row,
            "base_row": base_row,
            "has_resolution": latest_resolution is not None,
            "latest_resolution": latest_resolution,
            "created_at": row["created_at"],
        }

    def update_trace_row(self, trace_id: str, row_payload: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE traces
                SET row_json = ?
                WHERE trace_id = ?
                """,
                (self._encode_payload(row_payload), trace_id),
            )

    def reassign_package_deal(self, package_id: str, target_deal_id: str) -> PackageRecord:
        package = self.get_package(package_id)
        if package is None:
            raise KeyError("package_not_found")

        normalized_target = str(target_deal_id).strip()
        if not normalized_target:
            raise ValueError("target_deal_id cannot be empty")

        if normalized_target == package.deal_id:
            return package

        self.ensure_deal(normalized_target)
        next_revision = self._next_period_revision(
            deal_id=normalized_target,
            period_end_date=package.period_end_date,
        )

        manifest = dict(package.package_manifest)
        manifest["deal_id"] = normalized_target

        processed_payload = package.processed_payload
        if processed_payload is not None:
            updated_processed = json.loads(json.dumps(processed_payload))
            for item in updated_processed.get("packages", []):
                if item.get("package_id") == package_id:
                    item["deal_id"] = normalized_target
            processed_payload = updated_processed

        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE packages
                SET deal_id = ?,
                    period_revision = ?,
                    package_manifest_json = ?,
                    processed_payload_json = ?,
                    updated_at = ?
                WHERE package_id = ?
                """,
                (
                    normalized_target,
                    next_revision,
                    self._encode_payload(manifest),
                    self._encode_payload(processed_payload) if processed_payload is not None else None,
                    now,
                    package_id,
                ),
            )

            conn.execute(
                """
                UPDATE traces
                SET deal_id = ?
                WHERE package_id = ?
                """,
                (normalized_target, package_id),
            )

        updated = self.get_package(package_id)
        if updated is None:
            raise RuntimeError("package_update_failed")
        return updated

    def _row_to_package(self, row: sqlite3.Row | None) -> PackageRecord | None:
        if row is None:
            return None

        return PackageRecord(
            package_id=row["package_id"],
            idempotency_key=row["idempotency_key"],
            sender_email=row["sender_email"],
            source_email_id=row["source_email_id"],
            deal_id=row["deal_id"],
            period_end_date=row["period_end_date"],
            received_at=row["received_at"],
            period_revision=int(row["period_revision"]) if row["period_revision"] is not None else 1,
            status=row["status"],
            package_manifest=self._decode_payload(str(row["package_manifest_json"])),
            processed_payload=(
                self._decode_payload(str(row["processed_payload_json"]))
                if row["processed_payload_json"]
                else None
            ),
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
