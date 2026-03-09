from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

from agent_app_dataset.internal_api import create_app


def _extract_magic_token(magic_link_url: str) -> str:
    parsed = urlparse(magic_link_url)
    query = parse_qs(parsed.query)
    values = query.get("magic_token", [])
    assert values
    return values[0]


def test_magic_link_password_and_session_flow(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
        public_base_url="https://app.patrici.us",
    )
    client = TestClient(app)

    onboarding = client.post("/auth/v1/onboarding:ensure", json={"email": "pm@example.com"})
    assert onboarding.status_code == 200
    body = onboarding.json()
    assert body["created"] is True
    assert body["needs_password_setup"] is True
    assert body["magic_link_url"].startswith("https://app.patrici.us/app/?magic_token=")

    token = _extract_magic_token(body["magic_link_url"])

    consume = client.post(
        "/auth/v1/magic-link/consume",
        json={
            "token": token,
            "password": "StrongPassword123",
        },
    )
    assert consume.status_code == 200
    consume_body = consume.json()
    assert consume_body["status"] == "authenticated"
    assert consume_body["user"]["email"] == "pm@example.com"
    assert consume_body["user"]["has_password"] is True

    session_token = consume_body["session_token"]

    me = client.get("/auth/v1/me", headers={"Authorization": f"Bearer {session_token}"})
    assert me.status_code == 200
    assert me.json()["user"]["email"] == "pm@example.com"

    logout = client.post("/auth/v1/logout", headers={"Authorization": f"Bearer {session_token}"})
    assert logout.status_code == 200

    revoked = client.get("/auth/v1/me", headers={"Authorization": f"Bearer {session_token}"})
    assert revoked.status_code == 401

    login = client.post(
        "/auth/v1/login",
        json={
            "email": "pm@example.com",
            "password": "StrongPassword123",
        },
    )
    assert login.status_code == 200
    assert login.json()["status"] == "authenticated"


def test_magic_link_cannot_be_reused(tmp_path: Path) -> None:
    app = create_app(
        db_path=tmp_path / "runtime" / "api.sqlite3",
        labels_dir=tmp_path / "labels",
        events_log_path=tmp_path / "runtime" / "events.jsonl",
    )
    client = TestClient(app)

    request_link = client.post("/auth/v1/magic-link/request", json={"email": "ops@example.com"})
    assert request_link.status_code == 200

    token = _extract_magic_token(request_link.json()["magic_link_url"])

    first = client.post(
        "/auth/v1/magic-link/consume",
        json={"token": token, "password": "GoodPass123"},
    )
    assert first.status_code == 200

    second = client.post(
        "/auth/v1/magic-link/consume",
        json={"token": token, "password": "GoodPass123"},
    )
    assert second.status_code == 401
