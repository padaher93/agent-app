from __future__ import annotations

from typing import Any


def is_structured_locator(locator_type: str, locator_value: str) -> bool:
    kind = str(locator_type or "").strip().lower()
    value = str(locator_value or "").strip()
    if not kind or not value:
        return False

    lowered = value.lower()
    if lowered.startswith("unresolved:") or lowered.startswith("inferred:"):
        return False
    if "missing" in lowered:
        return False

    if kind == "cell":
        if len(value) < 2:
            return False
        letters = []
        digits = []
        for char in value:
            if char.isalpha() and not digits:
                letters.append(char)
            elif char.isdigit():
                digits.append(char)
            else:
                return False
        return bool(letters and digits)

    return kind in {"paragraph", "line", "bbox"}


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def is_trustworthy_anchor(anchor: dict[str, Any]) -> bool:
    if not str(anchor.get("doc_id", "")).strip():
        return False
    if not is_structured_locator(
        str(anchor.get("locator_type", "")),
        str(anchor.get("locator_value", "")),
    ):
        return False
    return _as_float(anchor.get("normalized_value")) is not None


def normalize_anchor(anchor: dict[str, Any]) -> dict[str, Any]:
    return {
        "anchor_id": str(anchor.get("anchor_id", "")).strip(),
        "doc_id": str(anchor.get("doc_id", "")).strip(),
        "doc_name": str(anchor.get("doc_name", "")).strip(),
        "page_or_sheet": str(anchor.get("page_or_sheet", "")).strip(),
        "locator_type": str(anchor.get("locator_type", "")).strip(),
        "locator_value": str(anchor.get("locator_value", "")).strip(),
        "source_snippet": str(anchor.get("source_snippet", "")).strip(),
        "raw_value_text": str(anchor.get("raw_value_text", "")).strip(),
        "normalized_value": _as_float(anchor.get("normalized_value")),
        "unit_currency": str(anchor.get("unit_currency", "")).strip() or "USD",
        "concept_id": str(anchor.get("concept_id", "")).strip(),
        "concept_label": str(anchor.get("concept_label", "")).strip(),
        "period_id": str(anchor.get("period_id", "")).strip(),
        "trace_id": str(anchor.get("trace_id", "")).strip(),
        "source_role": str(anchor.get("source_role", "")).strip() or "submitted_source",
        "confidence": _as_float(anchor.get("confidence")),
    }


def unique_trustworthy_anchors(
    anchors: list[dict[str, Any]],
    *,
    max_items: int = 8,
) -> list[dict[str, Any]]:
    normalized = [normalize_anchor(anchor) for anchor in anchors if isinstance(anchor, dict)]
    seen: set[tuple[str, str, str, float]] = set()
    result: list[dict[str, Any]] = []
    for anchor in normalized:
        if not is_trustworthy_anchor(anchor):
            continue
        value = _as_float(anchor.get("normalized_value"))
        if value is None:
            continue
        key = (
            str(anchor.get("doc_id", "")).strip(),
            str(anchor.get("locator_type", "")).strip().lower(),
            str(anchor.get("locator_value", "")).strip(),
            round(value, 6),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(anchor)
        if len(result) >= max_items:
            break
    return result


def select_conflict_pair(anchors: list[dict[str, Any]]) -> dict[str, Any] | None:
    trusted = unique_trustworthy_anchors(anchors, max_items=16)
    if len(trusted) < 2:
        return None

    best: tuple[tuple[float, float, float, str], dict[str, Any], dict[str, Any]] | None = None
    for idx, left in enumerate(trusted):
        left_value = _as_float(left.get("normalized_value"))
        if left_value is None:
            continue
        for right in trusted[idx + 1 :]:
            right_value = _as_float(right.get("normalized_value"))
            if right_value is None:
                continue
            if abs(left_value - right_value) < 1e-9:
                continue

            different_docs = 1.0 if left.get("doc_id") != right.get("doc_id") else 0.0
            delta = abs(left_value - right_value)
            confidence_sum = (_as_float(left.get("confidence")) or 0.0) + (_as_float(right.get("confidence")) or 0.0)
            lexical = "|".join(
                sorted(
                    [
                        f"{left.get('doc_id')}:{left.get('locator_type')}={left.get('locator_value')}",
                        f"{right.get('doc_id')}:{right.get('locator_type')}={right.get('locator_value')}",
                    ]
                )
            )
            score = (different_docs, delta, confidence_sum, lexical)
            if best is None or score > best[0]:
                best = (score, left, right)

    if best is None:
        return None

    _, anchor_a, anchor_b = best
    value_a = _as_float(anchor_a.get("normalized_value")) or 0.0
    value_b = _as_float(anchor_b.get("normalized_value")) or 0.0
    return {
        "type": "value_mismatch",
        "anchor_ids": [str(anchor_a.get("anchor_id", "")), str(anchor_b.get("anchor_id", ""))],
        "value_delta": round(abs(value_a - value_b), 4),
        "values": [value_a, value_b],
    }


def anchors_for_conflict(
    anchors: list[dict[str, Any]],
    conflict: dict[str, Any],
) -> list[dict[str, Any]]:
    ids = {str(value) for value in conflict.get("anchor_ids", []) if str(value)}
    trusted = unique_trustworthy_anchors(anchors, max_items=16)
    if not ids:
        return trusted[:2]
    selected = [anchor for anchor in trusted if str(anchor.get("anchor_id", "")) in ids]
    if len(selected) >= 2:
        return selected[:2]
    return trusted[:2]
