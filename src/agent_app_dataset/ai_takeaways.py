from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
import re
from typing import Any


_CONCEPT_PRIORITY: dict[str, float] = {
    "revenue_total": 3.0,
    "ebitda_reported": 2.0,
    "ebitda_adjusted": 2.0,
    "operating_income_ebit": 1.5,
    "interest_expense": 1.5,
    "total_debt": 1.5,
    "total_assets": 1.0,
    "total_liabilities": 1.0,
}


class TakeawaysGenerationError(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        self.code = str(code).strip() or "unknown_takeaways_error"
        self.message = str(message).strip()
        super().__init__(self.message or self.code)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"na", "n/a", "none", "null"}:
        return None
    compact = text.replace(",", "")
    try:
        return float(compact)
    except ValueError:
        match = re.search(r"-?[0-9]+(?:\.[0-9]+)?", compact)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None


def _status(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unresolved"
    return text


def _materiality_score(row: dict[str, Any]) -> float:
    prior_value = _parse_number(row.get("prior_value"))
    current_value = _parse_number(row.get("current_value"))
    explicit_abs_delta = _parse_number(row.get("abs_delta"))
    explicit_pct_delta = _parse_number(row.get("pct_delta"))
    fallback_magnitude = _parse_number(row.get("normalized_value"))

    abs_delta = explicit_abs_delta
    if abs_delta is None:
        if prior_value is not None and current_value is not None:
            abs_delta = abs(current_value - prior_value)
        else:
            abs_delta = abs(fallback_magnitude or 0.0)

    pct_delta = explicit_pct_delta
    if pct_delta is None:
        if prior_value not in (None, 0) and current_value is not None:
            pct_delta = abs((current_value - prior_value) / prior_value) * 100
        else:
            pct_delta = 0.0

    abs_delta_norm = min(1.0, math.log10(abs(abs_delta) + 1.0) / 6.0)
    pct_delta_norm = min(1.0, abs(pct_delta) / 100.0)
    confidence = _parse_number(row.get("confidence")) or 0.0
    confidence_norm = max(0.0, min(1.0, confidence))
    concept_boost = _CONCEPT_PRIORITY.get(str(row.get("concept_id") or "").strip().lower(), 0.0)
    concept_norm = min(1.0, concept_boost / 3.0)
    return round(abs_delta_norm * 0.6 + pct_delta_norm * 0.3 + confidence_norm * 0.1 + concept_norm * 0.15, 6)


def _sorted_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: _materiality_score(row), reverse=True)


def _truncate(text: Any, limit: int = 220) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return f"{value[: max(0, limit - 3)].rstrip()}..."


def _llm_takeaways_payload(
    *,
    deal_id: str,
    period_end_date: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    ranked = _sorted_rows(rows)
    compact_rows: list[dict[str, Any]] = []
    for row in ranked[:20]:
        compact_rows.append(
            {
                "concept_id": row.get("concept_id"),
                "label": row.get("label"),
                "status": row.get("status"),
                "current_value": row.get("current_value"),
                "prior_value": row.get("prior_value"),
                "abs_delta": row.get("abs_delta"),
                "pct_delta": row.get("pct_delta"),
                "confidence": row.get("confidence"),
                "materiality": _materiality_score(row),
                "doc_id": ((row.get("evidence") or {}).get("doc_id")),
                "locator": ((row.get("evidence") or {}).get("locator_value")),
            }
        )

    unresolved_count = sum(
        1 for row in ranked if _status(row.get("status")) in {"unresolved", "candidate_flagged"}
    )
    verified_count = sum(1 for row in ranked if _status(row.get("status")) == "verified")
    low_conf_count = sum(1 for row in ranked if (_parse_number(row.get("confidence")) or 0.0) < 0.9)

    return {
        "deal_id": deal_id,
        "period_end_date": period_end_date,
        "row_count": len(rows),
        "verified_count": verified_count,
        "unresolved_count": unresolved_count,
        "low_confidence_count": low_conf_count,
        "rows_ranked": compact_rows,
    }


def _generate_llm_takeaways(
    *,
    deal_id: str,
    period_end_date: str,
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise TakeawaysGenerationError("missing_openai_api_key", "OPENAI_API_KEY is required for takeaways generation.")

    try:
        from openai import OpenAI
    except Exception as exc:
        raise TakeawaysGenerationError("openai_sdk_unavailable", "OpenAI SDK is not installed.") from exc

    model_name = str(
        os.getenv("PATRICIUS_TAKEAWAYS_MODEL")
        or os.getenv("PATRICIUS_LLM_MODEL")
        or "gpt-5-mini"
    ).strip()
    timeout_seconds = float(os.getenv("OPENAI_TIMEOUT_SECONDS") or "45")
    base_url = str(os.getenv("OPENAI_BASE_URL") or "").strip() or None

    client_kwargs: dict[str, Any] = {"api_key": api_key, "timeout": timeout_seconds}
    if base_url:
        client_kwargs["base_url"] = base_url

    client = OpenAI(**client_kwargs)
    user_payload = _llm_takeaways_payload(deal_id=deal_id, period_end_date=period_end_date, rows=rows)

    system_prompt = (
        "You are Patricius Takeaways Agent. Produce exactly four concise, factual lines from the payload. "
        "Return strict JSON with keys: top_change, primary_risk, confidence_note, bottom_line. "
        "Do not invent values. Keep each line <= 220 chars. No markdown."
    )

    try:
        response = client.chat.completions.create(
            model=model_name,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, sort_keys=True)},
            ],
        )
    except Exception as exc:
        raise TakeawaysGenerationError("llm_request_failed", str(exc)) from exc

    raw = response.choices[0].message.content or "{}"
    if isinstance(raw, list):
        raw = "".join(part.get("text", "") for part in raw if isinstance(part, dict))

    try:
        parsed = json.loads(str(raw))
    except Exception as exc:
        raise TakeawaysGenerationError("takeaways_invalid_json", "Model did not return valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise TakeawaysGenerationError("takeaways_response_not_dict", "Model response JSON must be an object.")

    takeaways = {
        "top_change": _truncate(parsed.get("top_change")),
        "primary_risk": _truncate(parsed.get("primary_risk")),
        "confidence_note": _truncate(parsed.get("confidence_note")),
        "bottom_line": _truncate(parsed.get("bottom_line")),
    }
    if not all(takeaways.values()):
        raise TakeawaysGenerationError("takeaways_missing_fields", "Model response is missing one or more required fields.")

    return takeaways, model_name


def build_period_takeaways(
    *,
    deal_id: str,
    period_end_date: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    llm_takeaways, model_name = _generate_llm_takeaways(
        deal_id=deal_id,
        period_end_date=period_end_date,
        rows=rows,
    )

    generated_at = _utc_now()
    return {
        "takeaways": llm_takeaways,
        "generator": {
            "type": "ai",
            "model": model_name,
            "generated_at": generated_at,
        },
    }
