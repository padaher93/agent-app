from __future__ import annotations

from pathlib import Path

from agent_app_dataset.ops_preflight import run_ops_preflight


def _strict_config_dev() -> dict[str, object]:
    return {
        "internal_token": None,
        "require_https": False,
        "openai_api_key": None,
        "internal_api_token": None,
        "internal_api_require_https": False,
        "postmark_server_token": None,
        "outbound_email_mode": "none",
        "outbound_postmark_server_token": None,
        "mailgun_signing_key": None,
        "sendgrid_inbound_token": None,
        "attachment_storage_mode": "local",
        "attachment_storage_s3_bucket": None,
    }


def test_run_ops_preflight_passes_with_healthy_endpoints(tmp_path: Path) -> None:
    def _request(method: str, url: str, **kwargs):
        if url.endswith("/internal/v1/health"):
            return 200, ""
        if url.endswith("/auth/v1/magic-link/request"):
            return 422, ""
        if url.endswith("/inbound/v1/health"):
            return 200, ""
        if url.endswith("/inbound/v1/providers/postmark"):
            return 401, ""
        return 404, ""

    result = run_ops_preflight(
        runtime_profile="dev",
        strict_config=_strict_config_dev(),
        inbound_gateway_base="http://localhost:8090",
        internal_api_base="http://localhost:8080",
        dlq_path=tmp_path / "inbound_dlq.jsonl",
        check_postmark_api=False,
        postmark_outbound_server_token=None,
        request_fn=_request,
    )
    assert result.passed is True
    assert result.issues == []


def test_run_ops_preflight_fails_when_route_missing(tmp_path: Path) -> None:
    def _request(method: str, url: str, **kwargs):
        if url.endswith("/internal/v1/health"):
            return 200, ""
        if url.endswith("/auth/v1/magic-link/request"):
            return 404, ""
        if url.endswith("/inbound/v1/health"):
            return 200, ""
        if url.endswith("/inbound/v1/providers/postmark"):
            return 401, ""
        return 404, ""

    result = run_ops_preflight(
        runtime_profile="dev",
        strict_config=_strict_config_dev(),
        inbound_gateway_base="http://localhost:8090",
        internal_api_base="http://localhost:8080",
        dlq_path=tmp_path / "inbound_dlq.jsonl",
        check_postmark_api=False,
        postmark_outbound_server_token=None,
        request_fn=_request,
    )
    assert result.passed is False
    assert any("route_missing_or_wrong_method:404" in issue for issue in result.issues)


def test_run_ops_preflight_fails_postmark_check_without_token(tmp_path: Path) -> None:
    def _request(method: str, url: str, **kwargs):
        if url.endswith("/internal/v1/health"):
            return 200, ""
        return 422, ""

    result = run_ops_preflight(
        runtime_profile="dev",
        strict_config=_strict_config_dev(),
        inbound_gateway_base=None,
        internal_api_base="http://localhost:8080",
        dlq_path=tmp_path / "inbound_dlq.jsonl",
        check_postmark_api=True,
        postmark_outbound_server_token=None,
        request_fn=_request,
    )
    assert result.passed is False
    assert any("missing_postmark_outbound_server_token" in issue for issue in result.issues)
