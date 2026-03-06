from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StatusPolicy:
    verified_threshold: float = 0.90
    candidate_threshold: float = 0.80


def classify_status(
    confidence: float,
    hard_blockers: list[str] | tuple[str, ...],
    policy: StatusPolicy | None = None,
) -> str:
    active = policy or StatusPolicy()

    if confidence < active.candidate_threshold:
        return "unresolved"

    if hard_blockers or confidence < active.verified_threshold:
        return "candidate_flagged"

    return "verified"
