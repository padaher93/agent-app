from __future__ import annotations

import os
from typing import Literal


RuntimeProfile = Literal["dev", "staging", "prod"]
VALID_RUNTIME_PROFILES = {"dev", "staging", "prod"}
STRICT_RUNTIME_PROFILES = {"staging", "prod"}


def resolve_runtime_profile(value: str | None = None) -> RuntimeProfile:
    raw = (value or os.getenv("PATRICIUS_RUNTIME_PROFILE") or os.getenv("APP_ENV") or "dev").strip().lower()
    if raw == "production":
        raw = "prod"
    if raw not in VALID_RUNTIME_PROFILES:
        valid = ", ".join(sorted(VALID_RUNTIME_PROFILES))
        raise ValueError(f"invalid_runtime_profile:{raw} (expected one of: {valid})")
    return raw  # type: ignore[return-value]


def is_strict_runtime_profile(profile: RuntimeProfile | str) -> bool:
    return str(profile).strip().lower() in STRICT_RUNTIME_PROFILES


def validate_internal_api_runtime_requirements(
    *,
    runtime_profile: RuntimeProfile,
    internal_token: str | None,
    require_https: bool,
    openai_api_key: str | None,
) -> None:
    if not is_strict_runtime_profile(runtime_profile):
        return

    missing: list[str] = []
    if not (internal_token or "").strip():
        missing.append("internal_token")
    if not require_https:
        missing.append("require_https")
    if not (openai_api_key or "").strip():
        missing.append("OPENAI_API_KEY")

    if missing:
        raise RuntimeError(
            "strict_runtime_profile_requires:" + ",".join(missing)
        )


def validate_gateway_runtime_requirements(
    *,
    runtime_profile: RuntimeProfile,
    internal_api_token: str | None,
    internal_api_require_https: bool,
    postmark_server_token: str | None,
    outbound_email_mode: str,
    outbound_postmark_server_token: str | None,
    mailgun_signing_key: str | None,
    sendgrid_inbound_token: str | None,
    attachment_storage_mode: str,
    attachment_storage_s3_bucket: str | None,
) -> None:
    if not is_strict_runtime_profile(runtime_profile):
        return

    violations: list[str] = []
    if not (internal_api_token or "").strip():
        violations.append("internal_api_token_required")
    if not internal_api_require_https:
        violations.append("internal_api_require_https_required")
    if not (postmark_server_token or "").strip():
        violations.append("postmark_server_token_required")
    if str(attachment_storage_mode).strip().lower() != "s3":
        violations.append("attachment_storage_mode_must_be_s3")
    if not (attachment_storage_s3_bucket or "").strip():
        violations.append("attachment_storage_s3_bucket_required")
    if str(outbound_email_mode).strip().lower() != "postmark":
        violations.append("outbound_email_mode_must_be_postmark")
    if not (outbound_postmark_server_token or "").strip():
        violations.append("outbound_postmark_server_token_required")
    if (mailgun_signing_key or "").strip():
        violations.append("mailgun_disabled_in_strict_profile")
    if (sendgrid_inbound_token or "").strip():
        violations.append("sendgrid_disabled_in_strict_profile")

    if violations:
        raise RuntimeError("strict_runtime_profile_violation:" + ",".join(violations))
