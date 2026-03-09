#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any


@dataclass
class ReplayResult:
    total: int
    attempted: int
    replayed: int
    failed: int
    skipped: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay failed inbound records from DLQ")
    parser.add_argument("--dlq-path", default="runtime/inbound_dlq.jsonl")
    parser.add_argument("--internal-api-base", default="http://127.0.0.1:8080")
    parser.add_argument("--internal-token")
    parser.add_argument("--require-https-header", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--failed-output", default="runtime/inbound_dlq.replay_failed.jsonl")
    parser.add_argument("--report-path", default="runtime/inbound_dlq.replay_report.json")
    return parser.parse_args()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []

    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                records.append(parsed)
    return records


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")


def _base_headers(args: argparse.Namespace) -> dict[str, str]:
    headers: dict[str, str] = {}
    if args.internal_token:
        headers["X-Internal-Token"] = args.internal_token
    if args.require_https_header:
        headers["X-Forwarded-Proto"] = "https"
    return headers


def replay(args: argparse.Namespace) -> tuple[ReplayResult, list[dict[str, Any]]]:
    dlq_path = Path(args.dlq_path)
    all_rows = _read_jsonl(dlq_path)
    selected_rows = all_rows[: max(0, int(args.limit))]

    if args.dry_run:
        result = ReplayResult(
            total=len(all_rows),
            attempted=0,
            replayed=0,
            failed=0,
            skipped=len(selected_rows),
        )
        return result, selected_rows

    failed_rows: list[dict[str, Any]] = []
    replayed = 0
    attempted = 0
    skipped = 0

    base_headers = _base_headers(args)
    api_base = args.internal_api_base.rstrip("/")
    try:
        import httpx
    except Exception as exc:
        raise RuntimeError("httpx is required for non-dry-run replay") from exc

    with httpx.Client(timeout=30.0) as client:
        for row in selected_rows:
            ingest_payload = row.get("ingest_payload")
            process_payload = row.get("process_payload")
            if not isinstance(ingest_payload, dict):
                skipped += 1
                failed_rows.append({**row, "replay_error": "missing_ingest_payload"})
                continue

            if not isinstance(process_payload, dict):
                process_payload = {
                    "async_mode": True,
                    "max_retries": 2,
                    "extraction_mode": "llm",
                }

            attempted += 1
            try:
                ingest_resp = client.post(
                    f"{api_base}/internal/v1/packages:ingest",
                    json=ingest_payload,
                    headers=base_headers,
                )
                if ingest_resp.status_code >= 400:
                    failed_rows.append(
                        {
                            **row,
                            "replay_error": f"ingest_failed:{ingest_resp.status_code}",
                            "replay_response": ingest_resp.text,
                        }
                    )
                    continue

                ingest_json = ingest_resp.json()
                package_id = ingest_json.get("package_id")
                if not package_id:
                    failed_rows.append({**row, "replay_error": "missing_package_id"})
                    continue

                process_headers = {
                    **base_headers,
                    "X-Workspace-Id": str(ingest_payload.get("workspace_id", "ws_default")),
                }
                process_resp = client.post(
                    f"{api_base}/internal/v1/packages/{package_id}:process",
                    json=process_payload,
                    headers=process_headers,
                )
                if process_resp.status_code >= 400:
                    failed_rows.append(
                        {
                            **row,
                            "replay_error": f"process_failed:{process_resp.status_code}",
                            "replay_response": process_resp.text,
                            "replay_package_id": package_id,
                        }
                    )
                    continue

                replayed += 1
            except Exception as exc:
                failed_rows.append({**row, "replay_error": str(exc)})

    result = ReplayResult(
        total=len(all_rows),
        attempted=attempted,
        replayed=replayed,
        failed=len(failed_rows),
        skipped=skipped,
    )
    return result, failed_rows


def main() -> int:
    args = parse_args()
    result, failed_rows = replay(args)

    report = {
        "total": result.total,
        "attempted": result.attempted,
        "replayed": result.replayed,
        "failed": result.failed,
        "skipped": result.skipped,
        "dry_run": bool(args.dry_run),
        "dlq_path": str(Path(args.dlq_path)),
        "failed_output": str(Path(args.failed_output)),
    }

    _write_json(Path(args.report_path), report)

    if not args.dry_run:
        _write_jsonl(Path(args.failed_output), failed_rows)

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
