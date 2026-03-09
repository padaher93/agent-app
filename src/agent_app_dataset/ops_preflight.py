from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx

from .io_utils import write_json
from .release_gates import StrictConfigCheck, check_strict_config


_ROUTE_EXISTS_STATUS_CODES = {200, 400, 401, 403, 404, 405, 409, 415, 422}
_ROUTE_EXISTS_PASS_CODES = {200, 400, 401, 403, 409, 415, 422}


@dataclass(frozen=True)
class EndpointCheck:
    name: str
    method: str
    url: str
    status_code: int | None
    passed: bool
    message: str


@dataclass(frozen=True)
class OpsPreflightResult:
    passed: bool
    generated_at: str
    strict_config: StrictConfigCheck
    dlq_writable: bool
    endpoint_checks: list[EndpointCheck]
    issues: list[str]


def _request_default(method: str, url: str, **kwargs: Any) -> tuple[int | None, str]:
    try:
        response = httpx.request(method=method, url=url, timeout=15.0, **kwargs)
    except Exception as exc:
        return None, f"request_error:{exc}"
    return response.status_code, ""


def _check_endpoint(
    *,
    name: str,
    method: str,
    url: str,
    expected_codes: set[int],
    request_fn: Callable[..., tuple[int | None, str]],
    **request_kwargs: Any,
) -> EndpointCheck:
    status_code, error = request_fn(method, url, **request_kwargs)
    if status_code is None:
        return EndpointCheck(
            name=name,
            method=method,
            url=url,
            status_code=None,
            passed=False,
            message=error or "unreachable",
        )
    if status_code not in expected_codes:
        return EndpointCheck(
            name=name,
            method=method,
            url=url,
            status_code=status_code,
            passed=False,
            message="unexpected_status",
        )
    return EndpointCheck(
        name=name,
        method=method,
        url=url,
        status_code=status_code,
        passed=True,
        message="ok",
    )


def _check_dlq_writable(path: Path) -> tuple[bool, str]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
        with path.open("a", encoding="utf-8") as f:
            f.write("")
        return True, ""
    except Exception as exc:
        return False, str(exc)


def run_ops_preflight(
    *,
    runtime_profile: str,
    strict_config: dict[str, Any],
    inbound_gateway_base: str | None,
    internal_api_base: str | None,
    dlq_path: Path,
    check_postmark_api: bool,
    postmark_outbound_server_token: str | None,
    output_path: Path | None = None,
    request_fn: Callable[..., tuple[int | None, str]] = _request_default,
) -> OpsPreflightResult:
    strict = check_strict_config(runtime_profile=runtime_profile, **strict_config)
    endpoint_checks: list[EndpointCheck] = []
    issues: list[str] = []

    if strict.issues:
        issues.extend([f"strict_config:{item}" for item in strict.issues])

    dlq_ok, dlq_error = _check_dlq_writable(dlq_path)
    if not dlq_ok:
        issues.append(f"dlq_not_writable:{dlq_error}")

    if internal_api_base:
        endpoint_checks.append(
            _check_endpoint(
                name="internal_health",
                method="GET",
                url=f"{internal_api_base.rstrip('/')}/internal/v1/health",
                expected_codes={200},
                request_fn=request_fn,
            )
        )
        endpoint_checks.append(
            _check_endpoint(
                name="auth_magic_link_route",
                method="POST",
                url=f"{internal_api_base.rstrip('/')}/auth/v1/magic-link/request",
                expected_codes=_ROUTE_EXISTS_PASS_CODES,
                request_fn=request_fn,
                json={},
            )
        )

    if inbound_gateway_base:
        endpoint_checks.append(
            _check_endpoint(
                name="inbound_health",
                method="GET",
                url=f"{inbound_gateway_base.rstrip('/')}/inbound/v1/health",
                expected_codes={200},
                request_fn=request_fn,
            )
        )
        endpoint_checks.append(
            _check_endpoint(
                name="postmark_inbound_route",
                method="POST",
                url=f"{inbound_gateway_base.rstrip('/')}/inbound/v1/providers/postmark",
                expected_codes=_ROUTE_EXISTS_PASS_CODES,
                request_fn=request_fn,
                json={},
            )
        )

    if check_postmark_api:
        if not postmark_outbound_server_token:
            endpoint_checks.append(
                EndpointCheck(
                    name="postmark_api_auth",
                    method="GET",
                    url="https://api.postmarkapp.com/server",
                    status_code=None,
                    passed=False,
                    message="missing_postmark_outbound_server_token",
                )
            )
        else:
            endpoint_checks.append(
                _check_endpoint(
                    name="postmark_api_auth",
                    method="GET",
                    url="https://api.postmarkapp.com/server",
                    expected_codes={200},
                    request_fn=request_fn,
                    headers={"X-Postmark-Server-Token": postmark_outbound_server_token},
                )
            )

    for check in endpoint_checks:
        if not check.passed:
            if check.status_code in _ROUTE_EXISTS_STATUS_CODES and check.status_code not in _ROUTE_EXISTS_PASS_CODES:
                issues.append(f"{check.name}:route_missing_or_wrong_method:{check.status_code}")
            else:
                issues.append(f"{check.name}:{check.message}")

    result = OpsPreflightResult(
        passed=(strict.passed and dlq_ok and all(item.passed for item in endpoint_checks)),
        generated_at=datetime.now(timezone.utc).isoformat(),
        strict_config=strict,
        dlq_writable=dlq_ok,
        endpoint_checks=endpoint_checks,
        issues=sorted(set(issues)),
    )

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_json(
            output_path,
            {
                "passed": result.passed,
                "generated_at": result.generated_at,
                "strict_config": {
                    "passed": result.strict_config.passed,
                    "issues": result.strict_config.issues,
                },
                "dlq_writable": result.dlq_writable,
                "endpoint_checks": [asdict(item) for item in result.endpoint_checks],
                "issues": result.issues,
            },
        )

    return result
