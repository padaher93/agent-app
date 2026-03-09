from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .internal_processing import process_package_manifest
from .io_utils import read_json, write_json
from .runtime_profile import (
    resolve_runtime_profile,
    validate_gateway_runtime_requirements,
    validate_internal_api_runtime_requirements,
)
from .schemas import validate_with_schema
from .shadow_eval import run_shadow_eval
from .storage import is_supported_storage_uri


@dataclass(frozen=True)
class StrictConfigCheck:
    passed: bool
    issues: list[str]


@dataclass(frozen=True)
class ShadowReadinessCheck:
    passed: bool
    issues: list[str]
    package_count: int
    label_count: int
    deal_count: int
    min_periods_seen: int


@dataclass(frozen=True)
class LLMSmokeCheck:
    passed: bool
    issues: list[str]
    package_count: int
    row_count: int
    unresolved_hard_blockers: int
    candidate_flagged_count: int


@dataclass(frozen=True)
class ReleaseCandidateCheck:
    passed: bool
    strict_config: StrictConfigCheck
    shadow_readiness: ShadowReadinessCheck
    llm_smoke: LLMSmokeCheck
    shadow_eval_release_ready: bool
    summary_path: Path


@dataclass(frozen=True)
class PrePartnerReadinessCheck:
    passed: bool
    strict_config: StrictConfigCheck
    proxy_readiness: ShadowReadinessCheck
    llm_smoke: LLMSmokeCheck
    proxy_eval_release_ready: bool
    production_launch_ready: bool
    summary_path: Path


def check_strict_config(
    *,
    runtime_profile: str,
    internal_token: str | None,
    require_https: bool,
    openai_api_key: str | None,
    internal_api_token: str | None,
    internal_api_require_https: bool,
    postmark_server_token: str | None,
    outbound_email_mode: str,
    outbound_postmark_server_token: str | None,
    mailgun_signing_key: str | None,
    sendgrid_inbound_token: str | None,
    attachment_storage_mode: str,
    attachment_storage_s3_bucket: str | None,
) -> StrictConfigCheck:
    issues: list[str] = []
    profile = resolve_runtime_profile(runtime_profile)

    try:
        validate_internal_api_runtime_requirements(
            runtime_profile=profile,
            internal_token=internal_token,
            require_https=require_https,
            openai_api_key=openai_api_key,
        )
    except Exception as exc:
        issues.append(str(exc))

    try:
        validate_gateway_runtime_requirements(
            runtime_profile=profile,
            internal_api_token=internal_api_token,
            internal_api_require_https=internal_api_require_https,
            postmark_server_token=postmark_server_token,
            outbound_email_mode=outbound_email_mode,
            outbound_postmark_server_token=outbound_postmark_server_token,
            mailgun_signing_key=mailgun_signing_key,
            sendgrid_inbound_token=sendgrid_inbound_token,
            attachment_storage_mode=attachment_storage_mode,
            attachment_storage_s3_bucket=attachment_storage_s3_bucket,
        )
    except Exception as exc:
        issues.append(str(exc))

    return StrictConfigCheck(passed=(len(issues) == 0), issues=issues)


def validate_shadow_partition(
    *,
    packages_dir: Path,
    labels_dir: Path,
    min_packages: int,
    min_deals: int,
    min_periods_per_deal: int,
    require_supported_storage: bool = True,
) -> ShadowReadinessCheck:
    issues: list[str] = []

    if not packages_dir.exists():
        issues.append(f"packages_dir_missing:{packages_dir}")
    if not labels_dir.exists():
        issues.append(f"labels_dir_missing:{labels_dir}")
    if issues:
        return ShadowReadinessCheck(
            passed=False,
            issues=issues,
            package_count=0,
            label_count=0,
            deal_count=0,
            min_periods_seen=0,
        )

    package_files = sorted(packages_dir.glob("*.json"))
    label_files = sorted(labels_dir.glob("*.ground_truth.json"))
    labels_by_package = {p.name.replace(".ground_truth.json", ""): p for p in label_files}

    if len(package_files) < int(min_packages):
        issues.append(f"min_packages_not_met:{len(package_files)}<{min_packages}")

    deal_periods: dict[str, set[str]] = {}
    for package_file in package_files:
        package_payload = read_json(package_file)
        schema_errors = validate_with_schema("package_manifest", package_payload)
        for error in schema_errors:
            issues.append(f"{package_file}:{error}")
        if schema_errors:
            continue

        package_id = str(package_payload.get("package_id", "")).strip()
        deal_id = str(package_payload.get("deal_id", "")).strip()
        period = str(package_payload.get("period_end_date", "")).strip()

        if not package_id:
            issues.append(f"{package_file}:missing_package_id")
            continue
        if not deal_id:
            issues.append(f"{package_file}:missing_deal_id")
            continue
        if not period:
            issues.append(f"{package_file}:missing_period_end_date")
            continue

        if require_supported_storage:
            for file_meta in package_payload.get("files", []):
                storage_uri = str(file_meta.get("storage_uri", ""))
                if not is_supported_storage_uri(storage_uri):
                    file_id = str(file_meta.get("file_id", "")).strip() or "unknown_file"
                    issues.append(f"{package_file}:unsupported_storage_uri:{file_id}")

        label_path = labels_by_package.get(package_id)
        if label_path is None:
            issues.append(f"{package_file}:missing_label_for_package")
            continue

        label_payload = read_json(label_path)
        label_schema_errors = validate_with_schema("ground_truth_file", label_payload)
        for error in label_schema_errors:
            issues.append(f"{label_path}:{error}")
        if label_schema_errors:
            continue

        label_package_id = str(label_payload.get("package_id", "")).strip()
        if label_package_id != package_id:
            issues.append(f"{label_path}:package_id_mismatch:{label_package_id}!={package_id}")

        deal_periods.setdefault(deal_id, set()).add(period)

    deal_count = len(deal_periods)
    if deal_count < int(min_deals):
        issues.append(f"min_deals_not_met:{deal_count}<{min_deals}")

    min_periods_seen = min((len(periods) for periods in deal_periods.values()), default=0)
    if deal_count > 0 and min_periods_seen < int(min_periods_per_deal):
        issues.append(
            f"min_periods_per_deal_not_met:{min_periods_seen}<{min_periods_per_deal}"
        )

    return ShadowReadinessCheck(
        passed=(len(issues) == 0),
        issues=sorted(set(issues)),
        package_count=len(package_files),
        label_count=len(label_files),
        deal_count=deal_count,
        min_periods_seen=min_periods_seen,
    )


def run_llm_smoke(
    *,
    package_manifest_paths: list[Path],
    labels_dir: Path | None,
    events_log_path: Path,
    max_retries: int,
    fail_on_unresolved_hard_blocker: bool,
    max_candidate_flagged: int | None,
    extraction_mode: str = "llm",
    enforce_storage_support: bool = True,
    process_fn: Callable[..., tuple[dict[str, Any], dict[str, Any]]] = process_package_manifest,
) -> LLMSmokeCheck:
    issues: list[str] = []
    package_count = 0
    row_count = 0
    unresolved_hard_blockers = 0
    candidate_flagged_count = 0

    events_log_path.parent.mkdir(parents=True, exist_ok=True)

    for manifest_path in package_manifest_paths:
        package_payload = read_json(manifest_path)
        package_count += 1
        package_id = str(package_payload.get("package_id", manifest_path.stem))

        if enforce_storage_support:
            for file_meta in package_payload.get("files", []):
                storage_uri = str(file_meta.get("storage_uri", ""))
                if not is_supported_storage_uri(storage_uri):
                    file_id = str(file_meta.get("file_id", "")).strip() or "unknown_file"
                    issues.append(f"{package_id}:unsupported_storage_uri:{file_id}")

        try:
            workflow_payload, _summary = process_fn(
                package_manifest=package_payload,
                labels_dir=labels_dir,
                events_log_path=events_log_path,
                max_retries=max_retries,
                extraction_mode=extraction_mode,
            )
        except Exception as exc:
            issues.append(f"{package_id}:processing_error:{exc}")
            continue

        rows = workflow_payload.get("packages", [{}])[0].get("rows", [])
        row_count += len(rows)
        for row in rows:
            status = str(row.get("status", ""))
            blockers = row.get("hard_blockers", []) or []
            evidence = row.get("evidence", {}) or {}

            if status == "candidate_flagged":
                candidate_flagged_count += 1

            required_evidence_keys = ("doc_id", "locator_type", "locator_value")
            for key in required_evidence_keys:
                if not str(evidence.get(key, "")).strip():
                    issues.append(f"{package_id}:missing_evidence_key:{key}")

            if status == "unresolved" and blockers:
                unresolved_hard_blockers += 1
                if fail_on_unresolved_hard_blocker:
                    concept_id = str(row.get("concept_id", ""))
                    issues.append(f"{package_id}:unresolved_hard_blocker:{concept_id}")

    if max_candidate_flagged is not None and candidate_flagged_count > max_candidate_flagged:
        issues.append(
            f"candidate_flagged_exceeds_limit:{candidate_flagged_count}>{max_candidate_flagged}"
        )

    return LLMSmokeCheck(
        passed=(len(issues) == 0),
        issues=sorted(set(issues)),
        package_count=package_count,
        row_count=row_count,
        unresolved_hard_blockers=unresolved_hard_blockers,
        candidate_flagged_count=candidate_flagged_count,
    )


def run_release_candidate_gate(
    *,
    runtime_profile: str,
    strict_config: dict[str, Any],
    smoke_package_manifest_paths: list[Path],
    smoke_events_log_path: Path,
    smoke_labels_dir: Path | None,
    smoke_max_retries: int,
    smoke_fail_on_unresolved_hard_blocker: bool,
    smoke_max_candidate_flagged: int | None,
    smoke_extraction_mode: str,
    shadow_packages_dir: Path,
    shadow_labels_dir: Path,
    shadow_min_packages: int,
    shadow_min_deals: int,
    shadow_min_periods_per_deal: int,
    shadow_require_supported_storage: bool,
    shadow_eval_kwargs: dict[str, Any],
    summary_output_path: Path,
) -> ReleaseCandidateCheck:
    strict = check_strict_config(runtime_profile=runtime_profile, **strict_config)
    readiness = validate_shadow_partition(
        packages_dir=shadow_packages_dir,
        labels_dir=shadow_labels_dir,
        min_packages=shadow_min_packages,
        min_deals=shadow_min_deals,
        min_periods_per_deal=shadow_min_periods_per_deal,
        require_supported_storage=shadow_require_supported_storage,
    )
    smoke = run_llm_smoke(
        package_manifest_paths=smoke_package_manifest_paths,
        labels_dir=smoke_labels_dir,
        events_log_path=smoke_events_log_path,
        max_retries=smoke_max_retries,
        fail_on_unresolved_hard_blocker=(
            smoke_fail_on_unresolved_hard_blocker if smoke_extraction_mode == "llm" else False
        ),
        max_candidate_flagged=smoke_max_candidate_flagged,
        extraction_mode=smoke_extraction_mode,
        enforce_storage_support=(smoke_extraction_mode == "llm"),
    )

    shadow_eval_release_ready = False
    shadow_eval_error = ""
    if readiness.passed:
        try:
            summary = run_shadow_eval(
                packages_dir=shadow_packages_dir,
                labels_dir=shadow_labels_dir,
                **shadow_eval_kwargs,
            )
            shadow_eval_release_ready = summary.release_ready
        except Exception as exc:
            shadow_eval_error = str(exc)
    else:
        shadow_eval_error = "shadow_readiness_failed"

    passed = strict.passed and readiness.passed and smoke.passed and shadow_eval_release_ready
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(
        summary_output_path,
        {
            "passed": passed,
            "runtime_profile": runtime_profile,
            "strict_config": {
                "passed": strict.passed,
                "issues": strict.issues,
            },
            "shadow_readiness": {
                "passed": readiness.passed,
                "issues": readiness.issues,
                "package_count": readiness.package_count,
                "label_count": readiness.label_count,
                "deal_count": readiness.deal_count,
                "min_periods_seen": readiness.min_periods_seen,
            },
            "llm_smoke": {
                "passed": smoke.passed,
                "issues": smoke.issues,
                "package_count": smoke.package_count,
                "row_count": smoke.row_count,
                "unresolved_hard_blockers": smoke.unresolved_hard_blockers,
                "candidate_flagged_count": smoke.candidate_flagged_count,
                "extraction_mode": smoke_extraction_mode,
            },
            "shadow_eval": {
                "release_ready": shadow_eval_release_ready,
                "error": shadow_eval_error,
            },
        },
    )

    return ReleaseCandidateCheck(
        passed=passed,
        strict_config=strict,
        shadow_readiness=readiness,
        llm_smoke=smoke,
        shadow_eval_release_ready=shadow_eval_release_ready,
        summary_path=summary_output_path,
    )


def run_pre_partner_readiness(
    *,
    runtime_profile: str,
    strict_config: dict[str, Any],
    smoke_package_manifest_paths: list[Path],
    smoke_events_log_path: Path,
    smoke_labels_dir: Path | None,
    smoke_max_retries: int,
    smoke_fail_on_unresolved_hard_blocker: bool,
    smoke_max_candidate_flagged: int | None,
    smoke_extraction_mode: str,
    proxy_packages_dir: Path,
    proxy_labels_dir: Path,
    proxy_min_packages: int,
    proxy_min_deals: int,
    proxy_min_periods_per_deal: int,
    proxy_require_supported_storage: bool,
    proxy_eval_kwargs: dict[str, Any],
    summary_output_path: Path,
    shadow_eval_fn: Callable[..., Any] = run_shadow_eval,
) -> PrePartnerReadinessCheck:
    strict = check_strict_config(runtime_profile=runtime_profile, **strict_config)
    readiness = validate_shadow_partition(
        packages_dir=proxy_packages_dir,
        labels_dir=proxy_labels_dir,
        min_packages=proxy_min_packages,
        min_deals=proxy_min_deals,
        min_periods_per_deal=proxy_min_periods_per_deal,
        require_supported_storage=proxy_require_supported_storage,
    )
    smoke = run_llm_smoke(
        package_manifest_paths=smoke_package_manifest_paths,
        labels_dir=smoke_labels_dir,
        events_log_path=smoke_events_log_path,
        max_retries=smoke_max_retries,
        fail_on_unresolved_hard_blocker=(
            smoke_fail_on_unresolved_hard_blocker if smoke_extraction_mode == "llm" else False
        ),
        max_candidate_flagged=smoke_max_candidate_flagged,
        extraction_mode=smoke_extraction_mode,
        enforce_storage_support=(smoke_extraction_mode == "llm"),
    )

    proxy_eval_release_ready = False
    proxy_eval_error = ""
    if readiness.passed:
        try:
            summary = shadow_eval_fn(
                packages_dir=proxy_packages_dir,
                labels_dir=proxy_labels_dir,
                **proxy_eval_kwargs,
            )
            proxy_eval_release_ready = summary.release_ready
        except Exception as exc:
            proxy_eval_error = str(exc)
    else:
        proxy_eval_error = "proxy_readiness_failed"

    passed = strict.passed and readiness.passed and smoke.passed and proxy_eval_release_ready
    production_launch_ready = False

    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(
        summary_output_path,
        {
            "passed": passed,
            "runtime_profile": runtime_profile,
            "strict_config": {
                "passed": strict.passed,
                "issues": strict.issues,
            },
            "proxy_readiness": {
                "passed": readiness.passed,
                "issues": readiness.issues,
                "package_count": readiness.package_count,
                "label_count": readiness.label_count,
                "deal_count": readiness.deal_count,
                "min_periods_seen": readiness.min_periods_seen,
            },
            "llm_smoke": {
                "passed": smoke.passed,
                "issues": smoke.issues,
                "package_count": smoke.package_count,
                "row_count": smoke.row_count,
                "unresolved_hard_blockers": smoke.unresolved_hard_blockers,
                "candidate_flagged_count": smoke.candidate_flagged_count,
                "extraction_mode": smoke_extraction_mode,
            },
            "proxy_eval": {
                "release_ready": proxy_eval_release_ready,
                "error": proxy_eval_error,
            },
            "production_launch_ready": production_launch_ready,
            "blocked_by": ["real_shadow_partner_gate_pending"],
        },
    )

    return PrePartnerReadinessCheck(
        passed=passed,
        strict_config=strict,
        proxy_readiness=readiness,
        llm_smoke=smoke,
        proxy_eval_release_ready=proxy_eval_release_ready,
        production_launch_ready=production_launch_ready,
        summary_path=summary_output_path,
    )
