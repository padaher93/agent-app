from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from .agent_workflow import append_events


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _cutoff_months(now: datetime, months: int) -> datetime:
    return now - timedelta(days=30 * months)


def _cutoff_years(now: datetime, years: int) -> datetime:
    return now - timedelta(days=365 * years)


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def apply_retention_policy(
    db_path: Path,
    events_log_path: Path,
    package_retention_months: int = 24,
    log_retention_years: int = 7,
    dry_run: bool = True,
    archive_dir: Path | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    package_cutoff = _cutoff_months(now, package_retention_months)
    log_cutoff = _cutoff_years(now, log_retention_years)

    archive_root = archive_dir or (events_log_path.parent / "archive")
    archive_root.mkdir(parents=True, exist_ok=True)

    with _connect(db_path) as conn:
        old_packages_rows = conn.execute(
            """
            SELECT package_id
            FROM packages
            WHERE received_at < ?
            """,
            (package_cutoff.isoformat(),),
        ).fetchall()

    old_package_ids = [str(row["package_id"]) for row in old_packages_rows]

    event_lines: list[dict[str, Any]] = []
    old_event_count = 0
    if events_log_path.exists() and events_log_path.stat().st_size > 0:
        for raw in events_log_path.read_text(encoding="utf-8").splitlines():
            text = raw.strip()
            if not text:
                continue
            try:
                event = json.loads(text)
            except json.JSONDecodeError:
                continue
            ts = event.get("timestamp")
            if not isinstance(ts, str):
                old_event_count += 1
                continue
            if _parse_dt(ts) < log_cutoff:
                old_event_count += 1
                continue
            event_lines.append(event)

    if not dry_run:
        with _connect(db_path) as conn:
            if old_package_ids:
                placeholders = ",".join("?" for _ in old_package_ids)
                conn.execute(f"DELETE FROM traces WHERE package_id IN ({placeholders})", tuple(old_package_ids))
                conn.execute(f"DELETE FROM packages WHERE package_id IN ({placeholders})", tuple(old_package_ids))

        if events_log_path.exists() and old_event_count > 0:
            archive_file = archive_root / f"events_archive_{now.strftime('%Y%m%dT%H%M%SZ')}.jsonl"
            archive_file.write_text(events_log_path.read_text(encoding="utf-8"), encoding="utf-8")

            rebuilt_events = []
            for event in event_lines:
                item = dict(event)
                item.pop("sequence_id", None)
                item.pop("previous_hash", None)
                item.pop("event_hash", None)
                rebuilt_events.append(item)

            events_log_path.write_text("", encoding="utf-8")
            if rebuilt_events:
                append_events(events_log_path, rebuilt_events)

    return {
        "dry_run": dry_run,
        "now": now.isoformat(),
        "package_cutoff": package_cutoff.isoformat(),
        "log_cutoff": log_cutoff.isoformat(),
        "packages_marked": len(old_package_ids),
        "events_marked": old_event_count,
        "packages_marked_ids": old_package_ids,
    }
