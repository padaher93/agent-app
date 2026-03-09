from __future__ import annotations

import agent_app_dataset.outbound_email as outbound_email
from agent_app_dataset.outbound_email import (
    NoopNotifier,
    PostmarkNotifier,
    SMTPNotifier,
    create_outbound_notifier,
)


def test_noop_notifier_returns_not_sent() -> None:
    notifier = NoopNotifier()
    result = notifier.send_onboarding_email(
        to_email="ops@borrower.com",
        magic_link_url="https://app.patrici.us/app/?magic_token=abc",
        package_id="pkg_001",
        sender_email="ops@borrower.com",
    )
    assert result.sent is False
    assert result.provider == "none"


def test_create_outbound_notifier_requires_smtp_host() -> None:
    try:
        create_outbound_notifier(mode="smtp", from_email="inbound@patrici.us")
    except ValueError as exc:
        assert str(exc) == "smtp_host_required"
    else:
        raise AssertionError("Expected smtp_host_required")


def test_smtp_notifier_sends_message(monkeypatch) -> None:
    sent = {"count": 0}

    class _FakeSMTP:
        def __init__(self, host, port, timeout):
            self.host = host
            self.port = port
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def starttls(self):
            return None

        def login(self, username, password):
            return None

        def send_message(self, msg):
            sent["count"] += 1
            assert msg["To"] == "ops@borrower.com"

    monkeypatch.setattr(outbound_email.smtplib, "SMTP", _FakeSMTP)

    notifier = SMTPNotifier(
        from_email="inbound@patrici.us",
        host="smtp.example.com",
        port=587,
        username="user",
        password="pass",
        use_tls=True,
    )
    result = notifier.send_onboarding_email(
        to_email="ops@borrower.com",
        magic_link_url="https://app.patrici.us/app/?magic_token=abc",
        package_id="pkg_001",
        sender_email="ops@borrower.com",
    )
    assert result.sent is True
    assert result.provider == "smtp"
    assert sent["count"] == 1


def test_postmark_notifier_success(monkeypatch) -> None:
    class _FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"MessageID": "pm_123"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, headers, json):
            assert "postmarkapp.com" in url
            assert headers["X-Postmark-Server-Token"] == "pm_token"
            return _FakeResponse()

    monkeypatch.setattr(outbound_email.httpx, "Client", _FakeClient)

    notifier = PostmarkNotifier(
        from_email="inbound@patrici.us",
        server_token="pm_token",
    )
    result = notifier.send_onboarding_email(
        to_email="ops@borrower.com",
        magic_link_url="https://app.patrici.us/app/?magic_token=abc",
        package_id="pkg_001",
        sender_email="ops@borrower.com",
    )
    assert result.sent is True
    assert result.provider == "postmark"
    assert result.message_id == "pm_123"
