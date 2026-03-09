from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import secrets


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso_now() -> str:
    return utc_now().isoformat()


def generate_token(length: int = 32) -> str:
    return secrets.token_urlsafe(length)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def hash_password(password: str, *, iterations: int = 260_000) -> str:
    if len(password) < 8:
        raise ValueError("password_too_short")

    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, iterations_raw, salt_hex, digest_hex = stored.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
        salt = bytes.fromhex(salt_hex)
    except Exception:
        return False

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(candidate.hex(), digest_hex)


def expires_in(minutes: int) -> str:
    return (utc_now() + timedelta(minutes=minutes)).isoformat()
