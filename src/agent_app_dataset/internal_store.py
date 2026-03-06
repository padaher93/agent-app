from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_packages_deal_period ON packages(deal_id, period_end_date)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_packages_period_revision ON packages(deal_id, period_end_date, period_revision)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_package ON traces(package_id)")

            package_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(packages)").fetchall()
            }
            if "period_revision" not in package_columns:
                conn.execute(
                    "ALTER TABLE packages ADD COLUMN period_revision INTEGER NOT NULL DEFAULT 1"
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
                    json.dumps(package_manifest, sort_keys=True),
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
    ) -> None:
        if status not in PACKAGE_STATUSES:
            raise ValueError(f"invalid status: {status}")

        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
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
                    json.dumps(processed_payload, sort_keys=True) if processed_payload else None,
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
                    DO UPDATE SET row_json = excluded.row_json
                    """,
                    (
                        trace_id,
                        package_id,
                        deal_id,
                        period_id,
                        row.get("concept_id", ""),
                        json.dumps(row, sort_keys=True),
                        now,
                    ),
                )

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

        return {
            "trace_id": row["trace_id"],
            "package_id": row["package_id"],
            "deal_id": row["deal_id"],
            "period_id": row["period_id"],
            "concept_id": row["concept_id"],
            "row": json.loads(row["row_json"]),
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
                (json.dumps(row_payload, sort_keys=True), trace_id),
            )

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
            package_manifest=json.loads(row["package_manifest_json"]),
            processed_payload=(
                json.loads(row["processed_payload_json"])
                if row["processed_payload_json"]
                else None
            ),
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
