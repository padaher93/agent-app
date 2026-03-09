from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import smtplib
from typing import Any

import httpx


@dataclass
class NotificationResult:
    sent: bool
    provider: str
    reason: str = ""
    message_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "sent": self.sent,
            "provider": self.provider,
            "reason": self.reason,
            "message_id": self.message_id,
        }


class OutboundNotifier:
    def send_onboarding_email(
        self,
        *,
        to_email: str,
        magic_link_url: str,
        package_id: str,
        sender_email: str,
    ) -> NotificationResult:
        raise NotImplementedError


class NoopNotifier(OutboundNotifier):
    def send_onboarding_email(
        self,
        *,
        to_email: str,
        magic_link_url: str,
        package_id: str,
        sender_email: str,
    ) -> NotificationResult:
        return NotificationResult(
            sent=False,
            provider="none",
            reason="outbound_notifier_disabled",
        )


class SMTPNotifier(OutboundNotifier):
    def __init__(
        self,
        *,
        from_email: str,
        host: str,
        port: int,
        username: str | None,
        password: str | None,
        use_tls: bool,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.from_email = from_email
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.timeout_seconds = timeout_seconds

    def send_onboarding_email(
        self,
        *,
        to_email: str,
        magic_link_url: str,
        package_id: str,
        sender_email: str,
    ) -> NotificationResult:
        msg = EmailMessage()
        msg["From"] = self.from_email
        msg["To"] = to_email
        msg["Subject"] = "Patricius: package received and ready"
        msg.set_content(
            "\n".join(
                [
                    "We received your borrower package and finished initial processing.",
                    "",
                    f"Package ID: {package_id}",
                    f"Sender: {sender_email}",
                    "",
                    "Use this secure link to set your password and access your account:",
                    magic_link_url,
                ]
            )
        )

        try:
            with smtplib.SMTP(self.host, self.port, timeout=self.timeout_seconds) as smtp:
                if self.use_tls:
                    smtp.starttls()
                if self.username and self.password:
                    smtp.login(self.username, self.password)
                smtp.send_message(msg)
            return NotificationResult(sent=True, provider="smtp")
        except Exception as exc:
            return NotificationResult(sent=False, provider="smtp", reason=str(exc))


class PostmarkNotifier(OutboundNotifier):
    def __init__(
        self,
        *,
        from_email: str,
        server_token: str,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.from_email = from_email
        self.server_token = server_token
        self.timeout_seconds = timeout_seconds

    def send_onboarding_email(
        self,
        *,
        to_email: str,
        magic_link_url: str,
        package_id: str,
        sender_email: str,
    ) -> NotificationResult:
        payload = {
            "From": self.from_email,
            "To": to_email,
            "Subject": "Patricius: package received and ready",
            "TextBody": "\n".join(
                [
                    "We received your borrower package and finished initial processing.",
                    "",
                    f"Package ID: {package_id}",
                    f"Sender: {sender_email}",
                    "",
                    "Use this secure link to set your password and access your account:",
                    magic_link_url,
                ]
            ),
            "MessageStream": "outbound",
        }

        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(
                    "https://api.postmarkapp.com/email",
                    headers={
                        "X-Postmark-Server-Token": self.server_token,
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )

            if response.status_code >= 400:
                return NotificationResult(
                    sent=False,
                    provider="postmark",
                    reason=f"postmark_http_{response.status_code}",
                )

            try:
                body = response.json()
            except Exception:
                body = {}
            message_id = str(body.get("MessageID", ""))
            return NotificationResult(sent=True, provider="postmark", message_id=message_id)
        except Exception as exc:
            return NotificationResult(sent=False, provider="postmark", reason=str(exc))


def create_outbound_notifier(
    *,
    mode: str,
    from_email: str,
    smtp_host: str | None = None,
    smtp_port: int = 587,
    smtp_username: str | None = None,
    smtp_password: str | None = None,
    smtp_use_tls: bool = True,
    postmark_server_token: str | None = None,
) -> OutboundNotifier:
    normalized = str(mode).strip().lower()

    if normalized == "smtp":
        if not smtp_host:
            raise ValueError("smtp_host_required")
        return SMTPNotifier(
            from_email=from_email,
            host=smtp_host,
            port=smtp_port,
            username=smtp_username,
            password=smtp_password,
            use_tls=smtp_use_tls,
        )

    if normalized == "postmark":
        if not postmark_server_token:
            raise ValueError("postmark_server_token_required")
        return PostmarkNotifier(
            from_email=from_email,
            server_token=postmark_server_token,
        )

    return NoopNotifier()
