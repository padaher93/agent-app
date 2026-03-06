from __future__ import annotations

import json
from pathlib import Path

from agent_app_dataset.agent_workflow import append_events
from agent_app_dataset.internal_store import InternalStore
from agent_app_dataset.retention import apply_retention_policy


def _manifest(package_id: str) -> dict:
    return {
        "schema_version": "1.0",
        "package_id": package_id,
        "deal_id": "deal_retention",
        "period_end_date": "2025-12-31",
        "source_email_id": f"email_{package_id}",
        "received_at": "2026-03-06T12:00:00+00:00",
        "files": [],
        "source_ids": [],
        "variant_tags": [],
        "quality_flags": [],
        "labeling_workflow": {
            "primary_labeler_status": "not_started",
            "reviewer_status": "not_started",
            "adjudication_status": "not_required",
        },
    }


def test_retention_policy_dry_run_and_apply(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime" / "api.sqlite3"
    log_path = tmp_path / "runtime" / "events.jsonl"
    store = InternalStore(db_path)

    old_pkg, _ = store.upsert_package(
        package_id="pkg_old",
        idempotency_key="old_key",
        sender_email="a@b.com",
        source_email_id="email_old",
        deal_id="deal_retention",
        period_end_date="2015-12-31",
        received_at="2015-12-31T10:00:00+00:00",
        status="received",
        package_manifest=_manifest("pkg_old"),
    )
    assert old_pkg.package_id == "pkg_old"

    new_pkg, _ = store.upsert_package(
        package_id="pkg_new",
        idempotency_key="new_key",
        sender_email="a@b.com",
        source_email_id="email_new",
        deal_id="deal_retention",
        period_end_date="2025-12-31",
        received_at="2026-03-06T10:00:00+00:00",
        status="received",
        package_manifest=_manifest("pkg_new"),
    )
    assert new_pkg.package_id == "pkg_new"

    append_events(
        log_path,
        [
            {
                "timestamp": "2010-01-01T00:00:00+00:00",
                "event_type": "old_event",
                "phase": "extract",
                "package_id": "pkg_old",
                "trace_id": "tr_old",
                "payload": {},
            },
            {
                "timestamp": "2026-03-06T00:00:00+00:00",
                "event_type": "new_event",
                "phase": "extract",
                "package_id": "pkg_new",
                "trace_id": "tr_new",
                "payload": {},
            },
        ],
    )

    dry = apply_retention_policy(
        db_path=db_path,
        events_log_path=log_path,
        package_retention_months=24,
        log_retention_years=7,
        dry_run=True,
    )
    assert dry["packages_marked"] == 1
    assert dry["events_marked"] == 1

    applied = apply_retention_policy(
        db_path=db_path,
        events_log_path=log_path,
        package_retention_months=24,
        log_retention_years=7,
        dry_run=False,
    )
    assert applied["packages_marked"] == 1
    assert applied["events_marked"] == 1

    assert store.get_package("pkg_old") is None
    assert store.get_package("pkg_new") is not None

    kept_events = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(kept_events) == 1
    assert kept_events[0]["event_type"] == "new_event"
