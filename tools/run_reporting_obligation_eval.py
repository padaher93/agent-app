#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_app_dataset.reporting_obligation_eval import (
    evaluate_real_reporting_obligation_corpus,
    write_reporting_obligation_eval_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate reporting-obligation grounding on real/anonymized requirement clauses"
    )
    parser.add_argument(
        "--corpus",
        default="tests/fixtures/real_reporting_obligation_corpus.jsonl",
        help="Path to JSONL corpus with real/anonymized requirement language",
    )
    parser.add_argument(
        "--run-dir",
        default="runtime/reporting_obligation_eval/run",
        help="Scratch directory for generated evaluation docs and intermediate artifacts",
    )
    parser.add_argument(
        "--output-dir",
        default="runtime/reporting_obligation_eval/report",
        help="Output directory for summary/report artifacts",
    )
    parser.add_argument(
        "--llm-mode",
        default="probe",
        choices=["probe", "off", "auto", "on"],
        help="probe=offline deterministic probe candidates; on/auto use configured LLM candidate extractor",
    )
    parser.add_argument(
        "--fail-on-false-promotions",
        action="store_true",
        help="Exit non-zero if any false promotion occurs",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    corpus_path = Path(args.corpus)
    run_dir = Path(args.run_dir)
    output_dir = Path(args.output_dir)

    result = evaluate_real_reporting_obligation_corpus(
        corpus_path=corpus_path,
        run_dir=run_dir,
        llm_mode=args.llm_mode,
    )
    artifacts = write_reporting_obligation_eval_report(
        result=result,
        output_dir=output_dir,
    )

    summary = result.get("summary", {})
    print("Reporting-obligation evaluation complete")
    print(f"- corpus: {corpus_path}")
    print(f"- llm_mode: {args.llm_mode}")
    print(f"- total_examples: {summary.get('total_examples', 0)}")
    print(f"- false_promotions: {summary.get('false_promotions', 0)}")
    print(f"- misses_on_grounded_examples: {summary.get('misses_on_grounded_examples', 0)}")
    print(f"- precision: {summary.get('precision', 0)}")
    print(f"- recall_on_grounded_examples: {summary.get('recall_on_grounded_examples', 0)}")
    print(f"- report_markdown: {artifacts.get('report_markdown', '')}")
    print(f"- summary_json: {artifacts.get('summary_json', '')}")
    print(f"- rows_jsonl: {artifacts.get('rows_jsonl', '')}")
    print(f"- rows_csv: {artifacts.get('rows_csv', '')}")

    if args.fail_on_false_promotions and int(summary.get("false_promotions", 0)) > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
