from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any

from .constants import CONCEPT_LABELS
from .llm_runtime import LLMClient, create_default_llm_client
from .reporting_obligations import (
    build_reporting_obligation_id,
    collect_reporting_obligation_lines,
    detect_reporting_obligation_cadence,
    detect_reporting_obligation_source_role,
    reporting_requirement_strength,
)
from .source_grounding import is_structured_locator


CANDIDATE_SUPPORTED_CONCEPTS = (
    "net_income",
    "ebitda_reported",
    "cash_and_equivalents",
)

# Conservative promotion gate: keep production truth narrow until verifier coverage is stronger.
GROUNDED_SUPPORTED_CONCEPTS = ("net_income",)

_CONCEPT_ALIASES: dict[str, tuple[str, ...]] = {
    "net_income": ("net_income", "net income", "net earnings", "net profit", "profit"),
    "ebitda_reported": ("ebitda_reported", "ebitda", "ebitda reported", "reported ebitda"),
    "cash_and_equivalents": (
        "cash_and_equivalents",
        "cash and equivalents",
        "cash equivalents",
        "cash balance",
        "cash",
    ),
}

_REPORTING_SIGNAL_TERMS = (
    "reporting package",
    "deliverable",
    "financial statement",
    "compliance certificate",
    "shall",
    "must",
    "required",
)

_CERTAINTY_BUCKETS = {"high", "medium", "low", "unknown"}


@dataclass(frozen=True)
class VerifiedCandidate:
    row: dict[str, Any]
    promoted_obligation: dict[str, Any] | None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_concept_id(raw: Any) -> str:
    text = _normalize_text(raw).lower()
    if not text:
        return ""

    normalized = text.replace("-", "_").replace("/", "_")
    normalized = "_".join(part for part in normalized.split() if part)
    if normalized in CANDIDATE_SUPPORTED_CONCEPTS:
        return normalized

    compact = " ".join(normalized.split("_"))
    for concept_id, aliases in _CONCEPT_ALIASES.items():
        for alias in aliases:
            alias_norm = " ".join(str(alias).strip().lower().replace("-", " ").replace("_", " ").split())
            if alias_norm and (compact == alias_norm or alias_norm in compact):
                return concept_id
    return ""


def _candidate_id(
    *,
    deal_id: str,
    doc_id: str,
    locator_type: str,
    locator_value: str,
    concept_id: str,
    snippet: str,
) -> str:
    seed = "|".join(
        [
            _normalize_text(deal_id).lower(),
            _normalize_text(doc_id),
            _normalize_text(locator_type).lower(),
            _normalize_text(locator_value),
            _normalize_text(concept_id).lower(),
            _normalize_text(snippet)[:240],
        ]
    )
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"oblc_{digest}"


def _line_indexes(
    lines: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    by_locator: dict[tuple[str, str, str], dict[str, Any]] = {}
    by_doc: dict[str, list[dict[str, Any]]] = {}
    for line in lines:
        doc_id = _normalize_text(line.get("doc_id"))
        locator_type = _normalize_text(line.get("locator_type")).lower()
        locator_value = _normalize_text(line.get("locator_value"))
        if doc_id and locator_type and locator_value:
            by_locator[(doc_id, locator_type, locator_value)] = line
        by_doc.setdefault(doc_id, []).append(line)
    return by_locator, by_doc


def _resolve_line_for_candidate(
    *,
    raw: dict[str, Any],
    by_locator: dict[tuple[str, str, str], dict[str, Any]],
    by_doc: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    doc_id = _normalize_text(raw.get("doc_id"))
    locator_type = _normalize_text(raw.get("locator_type")).lower()
    locator_value = _normalize_text(raw.get("locator_value"))

    if doc_id and locator_type and locator_value:
        direct = by_locator.get((doc_id, locator_type, locator_value))
        if isinstance(direct, dict):
            return direct

    snippet = _normalize_text(raw.get("source_snippet"))
    if not doc_id or not snippet:
        return None

    lowered_snippet = snippet.lower()
    for line in by_doc.get(doc_id, []):
        text = _normalize_text(line.get("text"))
        if not text:
            continue
        lowered_text = text.lower()
        if lowered_snippet in lowered_text or lowered_text in lowered_snippet:
            return line
    return None


def _candidate_rows_for_prompt(lines: list[dict[str, Any]], max_lines: int = 220) -> list[dict[str, Any]]:
    prioritized: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for line in lines:
        text = _normalize_text(line.get("text"))
        if not text:
            continue
        payload_line = {
            "doc_id": _normalize_text(line.get("doc_id")),
            "doc_name": _normalize_text(line.get("doc_name")),
            "locator_type": _normalize_text(line.get("locator_type")).lower(),
            "locator_value": _normalize_text(line.get("locator_value")),
            "page_or_sheet": _normalize_text(line.get("page_or_sheet")),
            "text": text,
        }
        lowered = text.lower()
        if any(term in lowered for term in _REPORTING_SIGNAL_TERMS):
            prioritized.append(payload_line)
        else:
            fallback.append(payload_line)

    ordered = prioritized + fallback
    return ordered[:max_lines]


def _build_prompt_payload(*, deal_id: str, lines: list[dict[str, Any]]) -> dict[str, Any]:
    concept_registry = [
        {
            "concept_id": concept_id,
            "concept_label": CONCEPT_LABELS.get(concept_id, concept_id),
            "aliases": list(_CONCEPT_ALIASES.get(concept_id, (concept_id,))),
            "grounding_supported": concept_id in GROUNDED_SUPPORTED_CONCEPTS,
        }
        for concept_id in CANDIDATE_SUPPORTED_CONCEPTS
    ]
    return {
        "deal_id": deal_id,
        "task": "Extract candidate reporting obligations from lines.",
        "concept_registry": concept_registry,
        "lines": _candidate_rows_for_prompt(lines),
        "output_contract": {
            "candidates": [
                {
                    "doc_id": "string",
                    "locator_type": "cell|paragraph|line|bbox",
                    "locator_value": "string",
                    "source_snippet": "exact or near-exact source line from provided text",
                    "candidate_obligation_type": "reporting_requirement",
                    "candidate_concept_id": "one concept_id from concept_registry",
                    "reason": "short extraction reason",
                    "certainty_bucket": "high|medium|low",
                }
            ]
        },
    }


def _candidate_system_prompt() -> str:
    return (
        "Extract candidate reporting obligations from provided document lines. "
        "Return JSON only with key 'candidates'. "
        "Use only the provided lines; never invent doc_id, locator, or snippet. "
        "Do not make legal conclusions or breach statements. "
        "Only return candidate facts."
    )


def _to_certainty_bucket(value: Any) -> str:
    text = _normalize_text(value).lower()
    if text in _CERTAINTY_BUCKETS:
        return text
    return "unknown"


def _snippets_linked(raw_snippet: str, matched_text: str) -> bool:
    raw = _normalize_text(raw_snippet).lower()
    matched = _normalize_text(matched_text).lower()
    if not raw or not matched:
        return False
    return raw in matched or matched in raw


def _snippet_mentions_concept(concept_id: str, snippet: str) -> bool:
    text = _normalize_text(snippet).lower()
    if not text:
        return False
    aliases = _CONCEPT_ALIASES.get(concept_id, (concept_id,))
    for alias in aliases:
        normalized_alias = " ".join(
            str(alias).strip().lower().replace("-", " ").replace("_", " ").split()
        )
        if normalized_alias and normalized_alias in text:
            return True
    return False


def _verify_candidate(
    *,
    deal_id: str,
    raw_candidate: dict[str, Any],
    line_match: dict[str, Any] | None,
    model_name: str,
    raw_model_output: dict[str, Any],
) -> VerifiedCandidate:
    raw_doc_id = _normalize_text(raw_candidate.get("doc_id"))
    raw_locator_type = _normalize_text(raw_candidate.get("locator_type")).lower()
    raw_locator_value = _normalize_text(raw_candidate.get("locator_value"))
    raw_snippet = _normalize_text(raw_candidate.get("source_snippet"))

    matched_doc_id = _normalize_text((line_match or {}).get("doc_id"))
    matched_locator_type = _normalize_text((line_match or {}).get("locator_type")).lower()
    matched_locator_value = _normalize_text((line_match or {}).get("locator_value"))
    matched_snippet = _normalize_text((line_match or {}).get("text"))

    has_line_match = isinstance(line_match, dict)
    locator_consistent = True
    if has_line_match and raw_locator_type and raw_locator_value:
        locator_consistent = raw_locator_type == matched_locator_type and raw_locator_value == matched_locator_value
    snippet_consistent = True
    if has_line_match and raw_snippet:
        snippet_consistent = _snippets_linked(raw_snippet, matched_snippet)
    source_linked = has_line_match and locator_consistent and snippet_consistent

    if has_line_match:
        doc_id = matched_doc_id
        locator_type = matched_locator_type
        locator_value = matched_locator_value
        page_or_sheet = _normalize_text((line_match or {}).get("page_or_sheet"))
        source_snippet = matched_snippet
        doc_name = _normalize_text((line_match or {}).get("doc_name"))
        doc_type = _normalize_text((line_match or {}).get("doc_type")).upper()
        storage_uri = _normalize_text((line_match or {}).get("storage_uri"))
    else:
        doc_id = raw_doc_id
        locator_type = raw_locator_type
        locator_value = raw_locator_value
        page_or_sheet = ""
        source_snippet = raw_snippet
        doc_name = _normalize_text(raw_candidate.get("doc_name"))
        doc_type = _normalize_text(raw_candidate.get("doc_type")).upper()
        storage_uri = _normalize_text(raw_candidate.get("storage_uri"))

    concept_id = _normalize_concept_id(
        raw_candidate.get("candidate_concept_id") or raw_candidate.get("candidate_concept_label")
    )
    concept_label = CONCEPT_LABELS.get(concept_id, concept_id)
    strength = reporting_requirement_strength(source_snippet)
    has_locator = is_structured_locator(locator_type, locator_value)
    concept_linked = _snippet_mentions_concept(concept_id, source_snippet)
    linkage_reason = _normalize_text(raw_candidate.get("reason")) or "candidate_discovered_from_requirement_doc"
    if not has_line_match:
        linkage_reason = f"{linkage_reason}; source_line_not_linked"
    elif not locator_consistent:
        linkage_reason = f"{linkage_reason}; locator_mismatch"
    elif not snippet_consistent:
        linkage_reason = f"{linkage_reason}; snippet_mismatch"
    elif not concept_linked:
        linkage_reason = f"{linkage_reason}; concept_not_explicit_in_snippet"

    if not concept_id or concept_id not in CANDIDATE_SUPPORTED_CONCEPTS:
        grounding_state = "unsupported"
    elif not source_linked:
        grounding_state = "unsupported"
    elif not concept_linked:
        grounding_state = "unsupported"
    elif not (doc_id and source_snippet and has_locator):
        grounding_state = "unsupported"
    elif strength == "grounded" and concept_id in GROUNDED_SUPPORTED_CONCEPTS:
        grounding_state = "grounded"
    elif strength in {"grounded", "ambiguous"}:
        grounding_state = "ambiguous"
    else:
        grounding_state = "unsupported"

    candidate_row = {
        "candidate_id": _candidate_id(
            deal_id=deal_id,
            doc_id=doc_id,
            locator_type=locator_type,
            locator_value=locator_value,
            concept_id=concept_id,
            snippet=source_snippet,
        ),
        "deal_id": deal_id,
        "doc_id": doc_id,
        "doc_name": doc_name,
        "doc_type": doc_type,
        "storage_uri": storage_uri,
        "locator_type": locator_type,
        "locator_value": locator_value,
        "page_or_sheet": page_or_sheet,
        "source_snippet": source_snippet,
        "candidate_obligation_type": _normalize_text(raw_candidate.get("candidate_obligation_type"))
        or "reporting_requirement",
        "candidate_concept_id": concept_id,
        "candidate_concept_label": concept_label,
        "reason": linkage_reason,
        "certainty_bucket": _to_certainty_bucket(raw_candidate.get("certainty_bucket")),
        "model_name": model_name,
        "extraction_mode": "llm_candidate_discovery",
        "raw_model_output": raw_model_output,
        "grounding_state": grounding_state,
        "promoted_obligation_id": None,
        "source_linked": source_linked,
    }

    promoted: dict[str, Any] | None = None
    if grounding_state == "grounded":
        obligation_id = build_reporting_obligation_id(
            deal_id=deal_id,
            doc_id=doc_id,
            locator_type=locator_type,
            locator_value=locator_value,
            required_concept_id=concept_id,
        )
        candidate_row["promoted_obligation_id"] = obligation_id
        promoted = {
            "obligation_id": obligation_id,
            "deal_id": deal_id,
            "doc_id": doc_id,
            "doc_name": doc_name,
            "doc_type": doc_type,
            "storage_uri": storage_uri,
            "locator_type": locator_type,
            "locator_value": locator_value,
            "page_or_sheet": page_or_sheet,
            "source_snippet": source_snippet,
            "obligation_type": "reporting_requirement",
            "required_concept_id": concept_id,
            "required_concept_label": concept_label,
            "cadence": detect_reporting_obligation_cadence(source_snippet),
            "source_role": detect_reporting_obligation_source_role(doc_name, source_snippet),
            "grounding_state": "grounded",
        }

    return VerifiedCandidate(row=candidate_row, promoted_obligation=promoted)


def discover_reporting_obligation_candidates(
    *,
    deal_id: str,
    docs: list[dict[str, Any]],
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    lines = collect_reporting_obligation_lines(docs)
    if not lines:
        return {
            "model_name": "",
            "candidates": [],
            "promoted_obligations": [],
            "raw_model_output": {},
            "line_count": 0,
        }

    client = llm_client or create_default_llm_client()
    model_name = _normalize_text(getattr(client, "model_name", "")) or "unknown_model"
    prompt_payload = _build_prompt_payload(deal_id=deal_id, lines=lines)
    raw_model_output = client.run_json(
        agent_id="agent_reporting_obligation_candidates",
        system_prompt=_candidate_system_prompt(),
        user_payload=prompt_payload,
    )

    raw_candidates = raw_model_output.get("candidates", [])
    if not isinstance(raw_candidates, list):
        raw_candidates = []

    by_locator, by_doc = _line_indexes(lines)
    verified_candidates: list[dict[str, Any]] = []
    promoted_obligations: list[dict[str, Any]] = []
    seen_candidate_ids: set[str] = set()
    seen_obligation_ids: set[str] = set()

    for raw in raw_candidates:
        if not isinstance(raw, dict):
            continue
        matched_line = _resolve_line_for_candidate(raw=raw, by_locator=by_locator, by_doc=by_doc)
        verified = _verify_candidate(
            deal_id=deal_id,
            raw_candidate=raw,
            line_match=matched_line,
            model_name=model_name,
            raw_model_output=raw_model_output,
        )
        candidate_id = str(verified.row.get("candidate_id", "")).strip()
        if not candidate_id or candidate_id in seen_candidate_ids:
            continue
        seen_candidate_ids.add(candidate_id)
        verified_candidates.append(verified.row)

        promoted = verified.promoted_obligation
        if not isinstance(promoted, dict):
            continue
        obligation_id = str(promoted.get("obligation_id", "")).strip()
        if not obligation_id or obligation_id in seen_obligation_ids:
            continue
        seen_obligation_ids.add(obligation_id)
        promoted_obligations.append(promoted)

    return {
        "model_name": model_name,
        "candidates": verified_candidates,
        "promoted_obligations": promoted_obligations,
        "raw_model_output": raw_model_output,
        "line_count": len(lines),
    }


def summarize_candidate_states(candidates: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "total": 0,
        "grounded": 0,
        "ambiguous": 0,
        "unsupported": 0,
        "promoted": 0,
    }
    for row in candidates:
        if not isinstance(row, dict):
            continue
        summary["total"] += 1
        state = _normalize_text(row.get("grounding_state")).lower()
        if state in {"grounded", "ambiguous", "unsupported"}:
            summary[state] += 1
        if _normalize_text(row.get("promoted_obligation_id")):
            summary["promoted"] += 1
    return summary


def serialize_candidate_row(candidate: dict[str, Any]) -> dict[str, Any]:
    row = dict(candidate)
    raw_output = row.get("raw_model_output")
    if isinstance(raw_output, dict):
        row["raw_model_output"] = raw_output
    elif isinstance(raw_output, str):
        try:
            row["raw_model_output"] = json.loads(raw_output)
        except Exception:
            row["raw_model_output"] = {"raw": raw_output}
    else:
        row["raw_model_output"] = {}
    return row
