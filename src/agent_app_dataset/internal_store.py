from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path
from typing import Any


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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_package ON traces(package_id)")

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
                    status,
                    package_manifest_json,
                    processed_payload_json,
                    error_message,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    package_id,
                    idempotency_key,
                    sender_email,
                    source_email_id,
                    deal_id,
                    period_end_date,
                    received_at,
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
                "rows": [],
            }

        packages = package.processed_payload.get("packages", [])
        package_rows: list[dict[str, Any]] = []
        for item in packages:
            if item.get("package_id") == period_id:
                package_rows = item.get("rows", [])
                break

        return {
            "deal_id": deal_id,
            "period_id": period_id,
            "status": package.status,
            "rows": package_rows,
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
