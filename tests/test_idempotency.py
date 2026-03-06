from __future__ import annotations

from agent_app_dataset.idempotency import build_idempotency_key


def test_idempotency_key_is_stable_for_same_material() -> None:
    files_a = [
        {"checksum": "b"},
        {"checksum": "a"},
    ]
    files_b = [
        {"checksum": "a"},
        {"checksum": "b"},
    ]

    key1 = build_idempotency_key(
        sender_email="OPS@Borrower.com",
        received_at="2026-03-06T12:15:00+00:00",
        files=files_a,
    )
    key2 = build_idempotency_key(
        sender_email="ops@borrower.com",
        received_at="2026-03-06T12:59:59+00:00",
        files=files_b,
    )

    assert key1 == key2
