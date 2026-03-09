from __future__ import annotations

from pathlib import Path
import subprocess
import textwrap


def _run_render_script(script_body: str) -> None:
    completed = subprocess.run(
        ["node", "--input-type=module", "--eval", script_body],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_render_intake_hides_baseline_control_and_singularizes_summary_counts() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const state = {{
          deals: [{{ deal_id: "deal_alderon", display_name: "Deal Alderon" }}],
          currentDealId: "deal_alderon",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "first_package_intake",
            product_state: {{ screen_mode: "first_package_intake" }},
            deal: {{ id: "deal_alderon", name: "Deal Alderon" }},
            periods: {{ current: {{ id: "pkg_001", label: "Sep 2025" }}, baseline: null, comparison_basis: "none" }},
            period_options: [{{ id: "pkg_001", label: "Sep 2025", is_current: true, is_baseline: false }}],
            summary: {{ blockers: 1, review_signals: 2, confirmed_findings: 0, total: 3 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "confirmed_findings"],
              section_order: ["blockers", "review_signals", "confirmed_findings"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                confirmed_findings: "Confirmed Findings"
              }}
            }},
            items: []
          }},
          selectedItemId: "",
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);

        if (elements.baselineSwitcher.disabled !== true) {{
          throw new Error("baseline switcher should be disabled in intake mode");
        }}
        if (elements.baselineSwitcher.hidden !== true) {{
          throw new Error("baseline switcher should be hidden in intake mode");
        }}
        if (elements.baselineControl.hidden !== true) {{
          throw new Error("baseline control should be hidden in intake mode");
        }}
        if (elements.baselineControl.style.display !== "none") {{
          throw new Error("baseline control should have no rendered footprint in intake mode");
        }}
        const counts = elements.summaryCounts.innerHTML;
        if (!counts.includes("1</strong> blocker")) {{
          throw new Error("summary should singularize blocker when count is 1");
        }}
        if (!counts.includes("2</strong> review signals")) {{
          throw new Error("summary should pluralize review signals when count is not 1");
        }}
        """
    )
    _run_render_script(script)


def test_render_intake_review_detail_avoids_taxonomy_stack_and_delta_case_label() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const item = {{
          id: "rq_pkg_001_ebitda_adjusted",
          case_mode: "review_possible_material_change",
          concept_maturity: "review",
          case_certainty: "review_signal",
          case_certainty_label: "Review signal",
          review_reason_code: "candidate_from_text_only",
          review_reason_label: "Candidate extracted from text-only source",
          review_reason_detail: "Candidate came from narrative text, not an exact table cell.",
          workspace_mode: "investigation_mode",
          headline: "Possible EBITDA extraction needs review",
          metric_label: "EBITDA (Adjusted)",
          previous_value_display: "unresolved",
          current_value_display: "2,620,000",
          delta_display: "N/A",
          current_search_state: "candidate_only",
          grounded_implication: "Current package suggests an EBITDA extraction that still needs confirmation.",
          why_it_matters: "Current package suggests an EBITDA extraction that still needs confirmation.",
          primary_action: {{ id: "review_possible_requirement", label: "Review possible requirement" }},
          available_actions: [
            {{ id: "review_possible_requirement", label: "Review possible requirement" }},
            {{ id: "confirm_source_of_record", label: "Confirm source of record" }},
            {{ id: "prepare_borrower_follow_up", label: "Prepare borrower follow-up" }}
          ],
          trace_ids: [],
          proof_compare_mode: "current_vs_candidate",
          evidence: {{ baseline: {{}}, current: {{ file_name: "borrower_update.xlsx", locator: "Sheet: Coverage • cell=B11", excerpt: "EBITDA adjusted candidate 2,620,000" }} }},
        }};

        const state = {{
          deals: [{{ deal_id: "deal_alderon", display_name: "Deal Alderon" }}],
          currentDealId: "deal_alderon",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "first_package_intake",
            product_state: {{ screen_mode: "first_package_intake" }},
            deal: {{ id: "deal_alderon", name: "Deal Alderon" }},
            periods: {{ current: {{ id: "pkg_001", label: "Sep 2025" }}, baseline: null, comparison_basis: "none" }},
            period_options: [{{ id: "pkg_001", label: "Sep 2025", is_current: true, is_baseline: false }}],
            summary: {{ blockers: 1, review_signals: 1, confirmed_findings: 0, total: 1 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "confirmed_findings"],
              section_order: ["blockers", "review_signals", "confirmed_findings"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                confirmed_findings: "Confirmed Findings"
              }}
            }},
            items: [item]
          }},
          selectedItemId: item.id,
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const html = elements.detailContent.innerHTML;
        if (!html.includes("Review signal")) {{
          throw new Error("expected primary review signal label");
        }}
        if (!html.includes("Reason")) {{
          throw new Error("expected review reason line in detail");
        }}
        if (!html.includes("Candidate extracted from text-only source")) {{
          throw new Error("expected deterministic review reason label");
        }}
        if (html.includes("Current package investigation")) {{
          throw new Error("intake review detail should not show stacked investigation workspace label");
        }}
        if (html.includes("Possible change")) {{
          throw new Error("intake review detail should not include delta taxonomy label");
        }}
        if (html.includes("Review concept") || html.includes("Grounded concept")) {{
          throw new Error("detail should not show concept taxonomy line");
        }}
        """
    )
    _run_render_script(script)


def test_render_review_reason_block_hidden_when_no_concrete_reason() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const item = {{
          id: "rq_pkg_001_ebitda_adjusted",
          case_mode: "review_possible_material_change",
          concept_maturity: "review",
          case_certainty: "review_signal",
          case_certainty_label: "Review signal",
          workspace_mode: "investigation_mode",
          headline: "Possible EBITDA extraction needs review",
          metric_label: "EBITDA (Adjusted)",
          previous_value_display: "2,610,000",
          current_value_display: "2,620,000",
          delta_display: "+10,000",
          current_search_state: "found_verified",
          grounded_implication: "Current package suggests an EBITDA extraction that still needs confirmation.",
          why_it_matters: "Current package suggests an EBITDA extraction that still needs confirmation.",
          primary_action: {{ id: "confirm_source_of_record", label: "Confirm source of record" }},
          available_actions: [{{ id: "confirm_source_of_record", label: "Confirm source of record" }}],
          trace_ids: [],
          proof_compare_mode: "current_only",
          evidence: {{ baseline: {{}}, current: {{ file_name: "borrower_update.xlsx", locator: "Sheet: Coverage • cell=B11", excerpt: "EBITDA adjusted 2,620,000." }} }},
        }};

        const state = {{
          deals: [{{ deal_id: "deal_alderon", display_name: "Deal Alderon" }}],
          currentDealId: "deal_alderon",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "first_package_intake",
            product_state: {{ screen_mode: "first_package_intake" }},
            deal: {{ id: "deal_alderon", name: "Deal Alderon" }},
            periods: {{ current: {{ id: "pkg_001", label: "Sep 2025" }}, baseline: null, comparison_basis: "none" }},
            period_options: [{{ id: "pkg_001", label: "Sep 2025", is_current: true, is_baseline: false }}],
            summary: {{ blockers: 0, review_signals: 1, confirmed_findings: 0, total: 1 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "confirmed_findings"],
              section_order: ["blockers", "review_signals", "confirmed_findings"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                confirmed_findings: "Confirmed Findings"
              }}
            }},
            items: [item]
          }},
          selectedItemId: item.id,
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const html = elements.detailContent.innerHTML;
        if (html.includes("dr-detail-review-reason")) {{
          throw new Error("review reason block should be omitted when no concrete deterministic reason exists");
        }}
        """
    )
    _run_render_script(script)


def test_render_queue_prefers_reason_line_for_review_and_candidate_cases() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const reviewItem = {{
          id: "rq_pkg_001_ebitda_adjusted",
          case_mode: "review_possible_requirement",
          concept_maturity: "review",
          case_certainty: "candidate_only",
          case_certainty_label: "Candidate only",
          review_reason_code: "exact_row_header_missing",
          review_reason_label: "Exact row header missing",
          headline: "Possible EBITDA support missing",
          metric_label: "EBITDA (Adjusted)",
          previous_value_display: "unresolved",
          current_value_display: "2,620,000",
          delta_display: "N/A",
          current_search_state: "candidate_only",
          grounded_implication: "Confirm source evidence before relying on this item.",
          why_it_matters: "This is long consequence copy that should not become the third line when a concrete reason exists.",
          primary_action: {{ id: "review_possible_requirement", label: "Review possible requirement" }},
          available_actions: [{{ id: "review_possible_requirement", label: "Review possible requirement" }}],
          trace_ids: [],
          proof_compare_mode: "current_vs_candidate",
          evidence: {{ baseline: {{}}, current: {{ file_name: "borrower_update.xlsx", locator: "Sheet: Coverage • cell=B11", excerpt: "EBITDA adjusted candidate 2,620,000" }} }},
          display_group: "review_signals",
          severity: "medium",
        }};

        const noReasonItem = {{
          id: "rq_pkg_001_net_income",
          case_mode: "investigation_missing_required_reporting",
          concept_maturity: "grounded",
          case_certainty: "missing_required_support",
          case_certainty_label: "Missing required support",
          headline: "Net Income missing from current package",
          metric_label: "Net Income",
          previous_value_display: "980,000",
          current_value_display: "unresolved",
          delta_display: "N/A",
          current_search_state: "missing",
          grounded_implication: "Required support for Net Income is missing from the current package.",
          why_it_matters: "Required support missing from current package.",
          primary_action: {{ id: "request_borrower_update", label: "Request borrower update" }},
          available_actions: [{{ id: "request_borrower_update", label: "Request borrower update" }}],
          trace_ids: [],
          proof_compare_mode: "current_plus_requirement",
          evidence: {{ baseline: {{}}, current: {{ file_name: "borrower_update.xlsx", locator: "Sheet: Coverage • unresolved:not_found", excerpt: "Net income missing from package." }} }},
          display_group: "blockers",
          severity: "blocker",
        }};

        const state = {{
          deals: [{{ deal_id: "deal_alderon", display_name: "Deal Alderon" }}],
          currentDealId: "deal_alderon",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "first_package_intake",
            product_state: {{ screen_mode: "first_package_intake" }},
            deal: {{ id: "deal_alderon", name: "Deal Alderon" }},
            periods: {{ current: {{ id: "pkg_001", label: "Sep 2025" }}, baseline: null, comparison_basis: "none" }},
            period_options: [{{ id: "pkg_001", label: "Sep 2025", is_current: true, is_baseline: false }}],
            summary: {{ blockers: 1, review_signals: 1, confirmed_findings: 0, total: 2 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "confirmed_findings"],
              section_order: ["blockers", "review_signals", "confirmed_findings"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                confirmed_findings: "Confirmed Findings"
              }}
            }},
            items: [noReasonItem, reviewItem]
          }},
          selectedItemId: reviewItem.id,
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const queueHtml = elements.queueContent.innerHTML;
        if (!queueHtml.includes("Exact row header missing")) {{
          throw new Error("review row should surface concrete reason line");
        }}
        if (queueHtml.includes("should not become the third line")) {{
          throw new Error("review row should prioritize reason over long consequence copy");
        }}
        if (!queueHtml.includes("Required support missing from current package.")) {{
          throw new Error("rows without concrete reason should keep consequence line");
        }}
        """
    )
    _run_render_script(script)


def test_render_hides_generic_placeholder_reason_even_when_payload_contains_it() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const item = {{
          id: "rq_pkg_001_ebitda_adjusted",
          case_mode: "review_possible_requirement",
          concept_maturity: "review",
          case_certainty: "candidate_only",
          case_certainty_label: "Candidate only",
          review_reason_code: "candidate_needs_confirmation",
          review_reason_label: "Review-tier confirmation required",
          review_reason_detail: "Analyst confirmation required before use.",
          headline: "Possible EBITDA support missing",
          metric_label: "EBITDA (Adjusted)",
          previous_value_display: "unresolved",
          current_value_display: "2,620,000",
          delta_display: "N/A",
          current_search_state: "candidate_only",
          grounded_implication: "Confirm source evidence before relying on this item.",
          why_it_matters: "Confirm source evidence before relying on this item.",
          primary_action: {{ id: "review_possible_requirement", label: "Review possible requirement" }},
          available_actions: [{{ id: "review_possible_requirement", label: "Review possible requirement" }}],
          trace_ids: [],
          proof_compare_mode: "current_vs_candidate",
          evidence: {{ baseline: {{}}, current: {{ file_name: "borrower_update.xlsx", locator: "Sheet: Coverage • cell=B11", excerpt: "EBITDA adjusted candidate 2,620,000" }} }},
          display_group: "review_signals",
          severity: "medium",
        }};

        const state = {{
          deals: [{{ deal_id: "deal_alderon", display_name: "Deal Alderon" }}],
          currentDealId: "deal_alderon",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "first_package_intake",
            product_state: {{ screen_mode: "first_package_intake" }},
            deal: {{ id: "deal_alderon", name: "Deal Alderon" }},
            periods: {{ current: {{ id: "pkg_001", label: "Sep 2025" }}, baseline: null, comparison_basis: "none" }},
            period_options: [{{ id: "pkg_001", label: "Sep 2025", is_current: true, is_baseline: false }}],
            summary: {{ blockers: 0, review_signals: 1, confirmed_findings: 0, total: 1 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "confirmed_findings"],
              section_order: ["blockers", "review_signals", "confirmed_findings"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                confirmed_findings: "Confirmed Findings"
              }}
            }},
            items: [item]
          }},
          selectedItemId: item.id,
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const queueHtml = elements.queueContent.innerHTML;
        const detailHtml = elements.detailContent.innerHTML;
        if (queueHtml.includes("Review-tier confirmation required")) {{
          throw new Error("generic placeholder reason should be hidden in queue");
        }}
        if (detailHtml.includes("Review-tier confirmation required")) {{
          throw new Error("generic placeholder reason should be hidden in detail");
        }}
        if (detailHtml.includes("dr-detail-review-reason")) {{
          throw new Error("generic placeholder reason block should be omitted");
        }}
        """
    )
    _run_render_script(script)


def test_render_spreadsheet_preview_hides_debug_like_cells_and_marks_group_classes() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const blockerItem = {{
          id: "rq_pkg_001_net_income",
          case_mode: "investigation_missing_required_reporting",
          concept_maturity: "grounded",
          case_certainty: "missing_required_support",
          case_certainty_label: "Missing required support",
          headline: "Net Income missing from current package",
          metric_label: "Net Income",
          previous_value_display: "980,000",
          current_value_display: "unresolved",
          delta_display: "N/A",
          grounded_implication: "Required support for Net Income is missing from the current package.",
          why_it_matters: "Required support for Net Income is missing from the current package.",
          primary_action: {{ id: "request_borrower_update", label: "Request borrower update" }},
          available_actions: [{{ id: "request_borrower_update", label: "Request borrower update" }}],
          trace_ids: [],
          proof_compare_mode: "current_plus_requirement",
          evidence: {{ baseline: {{}}, current: {{ file_name: "borrower_update.xlsx", locator: "Sheet: Coverage • unresolved:not_found", excerpt: "Net income missing from package." }} }},
          display_group: "blockers",
          severity: "blocker",
        }};

        const reviewItem = {{
          id: "rq_pkg_001_revenue_total",
          case_mode: "review_possible_source_conflict",
          concept_maturity: "review",
          case_certainty: "review_signal",
          case_certainty_label: "Review signal",
          review_reason_label: "Source conflict across rows",
          headline: "Possible Revenue source conflict in current package",
          metric_label: "Revenue (Total)",
          previous_value_display: "12,450,000",
          current_value_display: "12,150,000",
          delta_display: "source_conflict",
          grounded_implication: "Conflicting sources detected. Confirm source evidence before relying on this item.",
          why_it_matters: "Conflicting sources detected. Confirm source evidence before relying on this item.",
          primary_action: {{ id: "confirm_source_of_record", label: "Confirm source of record" }},
          available_actions: [{{ id: "confirm_source_of_record", label: "Confirm source of record" }}],
          trace_ids: ["tr_pkg_001_revenue_total"],
          proof_compare_mode: "current_only",
          evidence: {{
            baseline: {{}},
            current: {{
              file_name: "borrower_update.xlsx",
              locator: "Sheet: Coverage • cell=B10",
              excerpt: "Revenue total 12,450,000 per coverage sheet.",
              preview_url: "/preview/revenue",
              download_url: "/internal/v1/packages/pkg_001/files/file_001:download"
            }}
          }},
          display_group: "review_signals",
          severity: "medium",
        }};

        const state = {{
          deals: [{{ deal_id: "deal_alderon", display_name: "Deal Alderon" }}],
          currentDealId: "deal_alderon",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map([
            ["/preview/revenue", {{
              evidence_preview: {{
                doc_type: "XLSX",
                preview: {{
                  kind: "xlsx_sheet",
                  rows: [
                    [{{ value: "Revenue (Total)" }}, {{ value: "12450000", highlight: true }}, {{ value: "from borrower package" }}],
                    [{{ value: "Notes" }}, {{ value: "" }}, {{ value: "highlight target" }}]
                  ]
                }}
              }}
            }}]
          ]),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "first_package_intake",
            product_state: {{ screen_mode: "first_package_intake" }},
            deal: {{ id: "deal_alderon", name: "Deal Alderon" }},
            periods: {{ current: {{ id: "pkg_001", label: "Sep 2025" }}, baseline: null, comparison_basis: "none" }},
            period_options: [{{ id: "pkg_001", label: "Sep 2025", is_current: true, is_baseline: false }}],
            summary: {{ blockers: 1, review_signals: 1, confirmed_findings: 0, total: 2 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "confirmed_findings"],
              section_order: ["blockers", "review_signals", "confirmed_findings"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                confirmed_findings: "Confirmed Findings"
              }}
            }},
            items: [blockerItem, reviewItem]
          }},
          selectedItemId: reviewItem.id,
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);

        const queueHtml = elements.queueContent.innerHTML;
        if (!queueHtml.includes("group-blockers")) {{
          throw new Error("expected blocker row group class");
        }}
        if (!queueHtml.includes("group-review_signals")) {{
          throw new Error("expected review row group class");
        }}

        const detailHtml = elements.detailContent.innerHTML;
        if (detailHtml.includes("from borrower package") || detailHtml.includes("highlight target")) {{
          throw new Error("debug-like preview labels should be removed from proof table");
        }}
        if (!detailHtml.includes("dr-preview-row-target")) {{
          throw new Error("expected target row marker in spreadsheet preview");
        }}
        """
    )
    _run_render_script(script)


def test_render_delta_uses_canonical_review_vs_verified_sections() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const verifiedItem = {{
          id: "rq_pkg_001_net_income",
          case_mode: "verified_review",
          concept_maturity: "grounded",
          case_certainty: "grounded_fact",
          case_certainty_label: "Grounded fact",
          headline: "Net Income down 1.0 percent versus prior period",
          metric_label: "Net Income",
          previous_value_display: "980,000",
          current_value_display: "970,000",
          delta_display: "-10,000 (-1.0%)",
          grounded_implication: "Change recorded.",
          why_it_matters: "Change recorded.",
          primary_action: {{ id: "view_source_evidence", label: "View source evidence" }},
          available_actions: [{{ id: "view_source_evidence", label: "View source evidence" }}],
          trace_ids: [],
          proof_compare_mode: "baseline_vs_current",
          evidence: {{ baseline: {{}}, current: {{ file_name: "deal_northstar_current.xlsx", locator: "Sheet: Coverage • cell=B5", excerpt: "Net income 970,000 from submitted package." }} }},
          display_group: "verified_changes",
          severity: "low",
        }};

        const reviewItem = {{
          id: "rq_pkg_001_ebitda_reported",
          case_mode: "review_possible_material_change",
          concept_maturity: "review",
          case_certainty: "review_signal",
          case_certainty_label: "Review signal",
          headline: "Possible EBITDA (Reported) change needs review",
          metric_label: "EBITDA (Reported)",
          previous_value_display: "2,580,000",
          current_value_display: "2,580,000",
          delta_display: "+0",
          grounded_implication: "Confirm source evidence before relying on this item.",
          why_it_matters: "Confirm source evidence before relying on this item.",
          primary_action: {{ id: "confirm_source_of_record", label: "Confirm source of record" }},
          available_actions: [{{ id: "confirm_source_of_record", label: "Confirm source of record" }}],
          trace_ids: [],
          proof_compare_mode: "baseline_vs_current",
          evidence: {{ baseline: {{}}, current: {{ file_name: "deal_northstar_current.xlsx", locator: "Sheet: Coverage • cell=B3", excerpt: "EBITDA reported 2,580,000 from structured coverage row." }} }},
          display_group: "review_signals",
          severity: "medium",
        }};

        const state = {{
          deals: [{{ deal_id: "deal_northstar", display_name: "Northstar Credit Partners" }}],
          currentDealId: "deal_northstar",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "pkg_000",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "delta_review",
            product_state: {{ screen_mode: "delta_review" }},
            deal: {{ id: "deal_northstar", name: "Northstar Credit Partners" }},
            periods: {{
              current: {{ id: "pkg_001", label: "Sep 2025" }},
              baseline: {{ id: "pkg_000", label: "Jun 2025" }},
              comparison_basis: "prior_verified_period"
            }},
            period_options: [
              {{ id: "pkg_001", label: "Sep 2025", is_current: true, is_baseline: false }},
              {{ id: "pkg_000", label: "Jun 2025", is_current: false, is_baseline: true }}
            ],
            summary: {{ blockers: 0, review_signals: 1, verified_changes: 1, total: 2 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "verified_changes"],
              section_order: ["blockers", "review_signals", "verified_changes"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                verified_changes: "Verified Changes"
              }}
            }},
            items: [reviewItem, verifiedItem]
          }},
          selectedItemId: reviewItem.id,
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const counts = elements.summaryCounts.innerHTML;
        const queueHtml = elements.queueContent.innerHTML;

        if (!counts.includes("1</strong> review signal")) {{
          throw new Error("delta summary should include review signal count");
        }}
        if (!counts.includes("1</strong> verified change")) {{
          throw new Error("delta summary should include verified change count");
        }}
        if (queueHtml.includes("Material Changes")) {{
          throw new Error("delta queue should not render material changes section when taxonomy uses review_signals");
        }}
        if (!queueHtml.includes("Review Signals")) {{
          throw new Error("delta queue should render review signals section");
        }}
        if (!queueHtml.includes("Verified Changes")) {{
          throw new Error("delta queue should render verified changes section");
        }}
      """
    )
    _run_render_script(script)


def test_render_conflict_proof_is_mismatch_first_with_inline_source_confirm() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const conflictItem = {{
          id: "rq_pkg_001_revenue_total",
          case_mode: "review_possible_source_conflict",
          concept_maturity: "review",
          case_certainty: "conflict_detected",
          case_certainty_label: "Conflict detected",
          review_reason_code: "source_conflict_across_rows",
          review_reason_label: "Source conflict across rows",
          headline: "Possible Revenue (Total) source conflict",
          metric_label: "Revenue (Total)",
          previous_value_display: "12,450,000",
          current_value_display: "12,150,000",
          delta_display: "source_conflict",
          grounded_implication: "Conflicting sources detected. Confirm source evidence before relying on this item.",
          why_it_matters: "Conflicting sources detected. Confirm source evidence before relying on this item.",
          primary_action: {{ id: "confirm_source_of_record", label: "Resolve conflict" }},
          available_actions: [
            {{ id: "confirm_source_of_record", label: "Resolve conflict" }},
            {{ id: "prepare_borrower_follow_up", label: "Prepare borrower follow-up" }}
          ],
          proof_compare_mode: "source_vs_source",
          competing_anchors: [
            {{
              doc_id: "file_a",
              doc_name: "borrower_package.xlsx",
              locator_type: "cell",
              locator_value: "B2",
              locator_display: "Sheet: Coverage • cell=B2",
              source_snippet: "Revenue total 12,450,000 in borrower package.",
              value_display: "12,450,000",
              preview_url: "/preview/a",
              download_url: "/download/a"
            }},
            {{
              doc_id: "file_b",
              doc_name: "management_memo.xlsx",
              locator_type: "cell",
              locator_value: "D14",
              locator_display: "Sheet: Memo • cell=D14",
              source_snippet: "Revenue total 12,150,000 in management memo.",
              value_display: "12,150,000",
              preview_url: "/preview/b",
              download_url: "/download/b"
            }}
          ],
          trace_ids: ["tr_pkg_001_revenue_total"]
        }};

        const state = {{
          deals: [{{ deal_id: "deal_northstar", display_name: "Northstar Credit Partners" }}],
          currentDealId: "deal_northstar",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "pkg_000",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map([
            ["/preview/a", {{
              evidence_preview: {{
                doc_type: "XLSX",
                preview: {{
                  kind: "xlsx_sheet",
                  rows: [
                    [{{ value: "Revenue (Total)" }}, {{ value: "12450000", highlight: true }}],
                    [{{ value: "EBITDA (Adjusted)" }}, {{ value: "2450000" }}],
                    [{{ value: "Net Income" }}, {{ value: "970000" }}]
                  ]
                }}
              }}
            }}],
            ["/preview/b", {{
              evidence_preview: {{
                doc_type: "XLSX",
                preview: {{
                  kind: "xlsx_sheet",
                  rows: [
                    [{{ value: "Revenue (Total)" }}, {{ value: "12150000", highlight: true }}],
                    [{{ value: "EBITDA (Adjusted)" }}, {{ value: "2460000" }}],
                    [{{ value: "Net Income" }}, {{ value: "960000" }}]
                  ]
                }}
              }}
            }}]
          ]),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "delta_review",
            product_state: {{ screen_mode: "delta_review" }},
            deal: {{ id: "deal_northstar", name: "Northstar Credit Partners" }},
            periods: {{
              current: {{ id: "pkg_001", label: "Sep 2025" }},
              baseline: {{ id: "pkg_000", label: "Jun 2025" }},
              comparison_basis: "prior_verified_period"
            }},
            period_options: [],
            summary: {{ blockers: 1, review_signals: 1, verified_changes: 0, total: 1 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "verified_changes"],
              section_order: ["blockers", "review_signals", "verified_changes"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                verified_changes: "Verified Changes"
              }}
            }},
            items: [conflictItem]
          }},
          selectedItemId: conflictItem.id,
          activeDraft: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const html = elements.detailContent.innerHTML;
        if (!html.includes("Mismatch focus")) {{
          throw new Error("conflict proof should include mismatch focus marker");
        }}
        if (!html.includes("dr-preview-row-mismatch")) {{
          throw new Error("conflict proof should strongly highlight mismatched row");
        }}
        if (!html.includes("dr-preview-row-muted")) {{
          throw new Error("conflict proof should reduce emphasis on non-mismatched rows");
        }}
        const confirmCount = (html.match(/Confirm this as source of record/g) || []).length;
        if (confirmCount < 2) {{
          throw new Error("conflict proof should expose inline source-confirm actions for both sources");
        }}
      """
    )
    _run_render_script(script)


def test_render_grounded_missing_support_places_requirement_before_package_proof() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const groundedBlocker = {{
          id: "rq_pkg_002_net_income",
          case_mode: "investigation_missing_required_reporting",
          concept_maturity: "grounded",
          case_certainty: "missing_required_support",
          case_certainty_label: "Missing required support",
          headline: "Net Income missing from current package",
          metric_label: "Net Income",
          previous_value_display: "970,000",
          current_value_display: "unresolved",
          delta_display: "N/A",
          current_search_state: "missing",
          grounded_implication: "Required support for Net Income is missing from the current package.",
          why_it_matters: "Required support for Net Income is missing from the current package.",
          primary_action: {{ id: "request_borrower_update", label: "Request borrower update" }},
          available_actions: [{{ id: "request_borrower_update", label: "Request borrower update" }}],
          trace_ids: [],
          proof_compare_mode: "baseline_current_plus_requirement",
          evidence: {{
            baseline: {{ file_name: "deal_northstar_current.xlsx", locator: "Sheet: Coverage • cell=B5", excerpt: "Net income 970,000 from submitted package." }},
            current: {{ file_name: "deal_northstar_followup.xlsx", locator: "Sheet: Coverage • paragraph=unresolved:not_found", excerpt: "Net income line is missing." }}
          }},
          requirement_anchor: {{
            grounded: true,
            doc_name: "deal_northstar_followup_reporting_requirements.pdf",
            locator_display: "Page 1 • paragraph=p1:l3",
            source_snippet: "Quarterly reporting package must include Net Income statement for the reporting period."
          }},
          display_group: "blockers",
          severity: "blocker",
        }};

        const reviewNoRequirement = {{
          id: "rq_pkg_002_cash",
          case_mode: "review_possible_requirement",
          concept_maturity: "review",
          case_certainty: "candidate_only",
          case_certainty_label: "Candidate only",
          headline: "Possible Cash and Equivalents support gap",
          metric_label: "Cash and Equivalents",
          previous_value_display: "1,430,000",
          current_value_display: "1,020,000",
          delta_display: "-410,000 (-28.7%)",
          current_search_state: "candidate_only",
          grounded_implication: "Review source evidence before treating this as a requirement gap.",
          why_it_matters: "Review source evidence before treating this as a requirement gap.",
          primary_action: {{ id: "review_possible_requirement", label: "Review possible requirement" }},
          available_actions: [{{ id: "review_possible_requirement", label: "Review possible requirement" }}],
          trace_ids: [],
          proof_compare_mode: "baseline_vs_current_candidate",
          evidence: {{
            baseline: {{ file_name: "deal_northstar_current.xlsx", locator: "Sheet: Coverage • cell=B6", excerpt: "Cash 1,430,000 from submitted package." }},
            current: {{ file_name: "deal_northstar_followup.xlsx", locator: "Sheet: Coverage • cell=B6", excerpt: "Cash candidate 1,020,000." }}
          }},
          display_group: "blockers",
          severity: "high",
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
        }};

        const baseState = {{
          deals: [{{ deal_id: "deal_northstar", display_name: "Northstar Credit Partners" }}],
          currentDealId: "deal_northstar",
          currentPeriodId: "pkg_002",
          baselinePeriodId: "pkg_001",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "delta_review",
            product_state: {{ screen_mode: "delta_review" }},
            deal: {{ id: "deal_northstar", name: "Northstar Credit Partners" }},
            periods: {{
              current: {{ id: "pkg_002", label: "Dec 2025" }},
              baseline: {{ id: "pkg_001", label: "Sep 2025" }},
              comparison_basis: "prior_verified_period"
            }},
            period_options: [],
            summary: {{ blockers: 1, review_signals: 0, verified_changes: 0, total: 1 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "verified_changes"],
              section_order: ["blockers", "review_signals", "verified_changes"],
              section_labels: {{
                blockers: "Blockers",
                review_signals: "Review Signals",
                verified_changes: "Verified Changes"
              }}
            }},
            items: [groundedBlocker]
          }},
          selectedItemId: groundedBlocker.id,
          activeDraft: null,
        }};

        renderScreen(baseState, elements, handlers);
        const groundedHtml = elements.detailContent.innerHTML;
        if (!groundedHtml.includes("requirement-first")) {{
          throw new Error("grounded missing-support blocker should render requirement-first proof layout");
        }}
        const reqPos = groundedHtml.indexOf(">Requirement<");
        const basePos = groundedHtml.indexOf(">Baseline<");
        if (reqPos < 0 || basePos < 0 || reqPos > basePos) {{
          throw new Error("requirement proof should appear before baseline/current package proof");
        }}

        const reviewState = {{
          ...baseState,
          queuePayload: {{
            ...baseState.queuePayload,
            items: [reviewNoRequirement]
          }},
          selectedItemId: reviewNoRequirement.id,
        }};

        renderScreen(reviewState, elements, handlers);
        const reviewHtml = elements.detailContent.innerHTML;
        if (reviewHtml.includes("requirement-first")) {{
          throw new Error("review-tier case without grounded requirement should not use requirement-first proof layout");
        }}
      """
    )
    _run_render_script(script)


def test_render_detail_uses_backend_action_hierarchy_with_overflow() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const item = {{
          id: "rq_pkg_001_net_income",
          case_mode: "investigation_missing_required_reporting",
          concept_maturity: "grounded",
          case_certainty: "missing_required_support",
          case_certainty_label: "Missing required support",
          headline: "Net Income missing from current package",
          metric_label: "Net Income",
          previous_value_display: "980,000",
          current_value_display: "unresolved",
          delta_display: "N/A",
          grounded_implication: "Required support for Net Income is missing from the current package.",
          why_it_matters: "Required support missing from current package.",
          primary_action: {{ id: "request_borrower_update", label: "Request borrower update" }},
          secondary_actions: [{{ id: "confirm_alternate_source", label: "Confirm alternate source" }}],
          overflow_actions: [
            {{ id: "view_source_evidence", label: "View source evidence" }},
            {{ id: "view_review_history", label: "View review history" }}
          ],
          available_actions: [
            {{ id: "request_borrower_update", label: "Request borrower update" }},
            {{ id: "confirm_alternate_source", label: "Confirm alternate source" }},
            {{ id: "view_source_evidence", label: "View source evidence" }},
            {{ id: "view_review_history", label: "View review history" }}
          ],
          proof_compare_mode: "baseline_current_plus_requirement",
          evidence: {{
            baseline: {{ file_name: "deal_northstar_current.xlsx", locator: "Sheet: Coverage • cell=B5", excerpt: "Net income 980,000 from submitted package." }},
            current: {{ file_name: "deal_northstar_followup.xlsx", locator: "Sheet: Coverage • paragraph=unresolved:not_found", excerpt: "Net income line is missing." }}
          }},
          trace_ids: [],
        }};

        const state = {{
          deals: [{{ deal_id: "deal_northstar", display_name: "Northstar Credit Partners" }}],
          currentDealId: "deal_northstar",
          currentPeriodId: "pkg_001",
          baselinePeriodId: "pkg_000",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "delta_review",
            product_state: {{ screen_mode: "delta_review" }},
            deal: {{ id: "deal_northstar", name: "Northstar Credit Partners" }},
            periods: {{
              current: {{ id: "pkg_001", label: "Sep 2025" }},
              baseline: {{ id: "pkg_000", label: "Jun 2025" }},
              comparison_basis: "prior_verified_period"
            }},
            period_options: [],
            summary: {{ blockers: 1, review_signals: 0, verified_changes: 0, total: 1 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "verified_changes"],
              section_order: ["blockers", "review_signals", "verified_changes"],
              section_labels: {{ blockers: "Blockers", review_signals: "Review Signals", verified_changes: "Verified Changes" }}
            }},
            items: [item]
          }},
          selectedItemId: item.id,
          activeDraft: null,
          activeAnalystNote: null,
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
          onAnalystNoteChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const html = elements.detailContent.innerHTML;
        if (!html.includes("Request borrower update")) {{
          throw new Error("primary action should render");
        }}
        if (!html.includes("Confirm alternate source")) {{
          throw new Error("secondary action should render");
        }}
        if (!html.includes("More actions")) {{
          throw new Error("overflow actions should render in details control");
        }}
        if (!html.includes("View source evidence") || !html.includes("View review history")) {{
          throw new Error("overflow actions should remain available");
        }}
      """
    )
    _run_render_script(script)


def test_render_detail_shows_durable_analyst_note_panel_when_open() -> None:
    render_module = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "agent_app_dataset"
        / "ui"
        / "delta-review-render.js"
    ).as_uri()

    script = textwrap.dedent(
        f"""
        import {{ renderScreen }} from "{render_module}";

        function makeEl() {{
          return {{
            textContent: "",
            innerHTML: "",
            value: "",
            checked: false,
            disabled: false,
            hidden: false,
            style: {{ display: "" }},
            addEventListener() {{}},
            querySelectorAll() {{ return []; }},
          }};
        }}

        const elements = {{
          dealTitle: makeEl(),
          dealSwitcher: makeEl(),
          comparisonPair: makeEl(),
          comparisonNote: makeEl(),
          summaryCounts: makeEl(),
          periodRail: makeEl(),
          baselineSwitcher: makeEl(),
          baselineControl: makeEl(),
          includeResolved: makeEl(),
          queueContent: makeEl(),
          detailContent: makeEl(),
        }};

        const reviewItem = {{
          id: "rq_pkg_001_ebitda_adjusted",
          case_mode: "review_possible_material_change",
          concept_maturity: "review",
          case_certainty: "review_signal",
          case_certainty_label: "Review signal",
          review_reason_code: "exact_row_header_missing",
          review_reason_label: "Exact row header missing",
          review_reason_detail: "Candidate value found, but no exact structured anchor was captured.",
          headline: "Possible EBITDA (Adjusted) change needs review",
          metric_label: "EBITDA (Adjusted)",
          previous_value_display: "2,560,000",
          current_value_display: "2,390,000",
          delta_display: "-170,000 (-6.6%)",
          grounded_implication: "Current package suggests an EBITDA extraction that still needs confirmation.",
          why_it_matters: "Current package suggests an EBITDA extraction that still needs confirmation.",
          primary_action: {{ id: "confirm_source_of_record", label: "Confirm source evidence" }},
          available_actions: [{{ id: "confirm_source_of_record", label: "Confirm source evidence" }}],
          proof_compare_mode: "baseline_vs_current_candidate",
          evidence: {{
            baseline: {{ file_name: "deal_northstar_current.xlsx", locator: "Sheet: Coverage • cell=B3", excerpt: "Adjusted EBITDA 2,560,000 in prior package." }},
            current: {{ file_name: "deal_northstar_followup.xlsx", locator: "Sheet: Coverage • paragraph=p1:l5", excerpt: "Adjusted EBITDA candidate 2,390,000." }}
          }},
          trace_ids: [],
        }};

        const state = {{
          deals: [{{ deal_id: "deal_northstar", display_name: "Northstar Credit Partners" }}],
          currentDealId: "deal_northstar",
          currentPeriodId: "pkg_002",
          baselinePeriodId: "pkg_001",
          includeResolved: false,
          loading: {{ queue: false }},
          errors: {{ queue: "" }},
          traceEvidenceCache: new Map(),
          traceHistoryCache: new Map(),
          queuePayload: {{
            product_mode: "delta_review",
            product_state: {{ screen_mode: "delta_review" }},
            deal: {{ id: "deal_northstar", name: "Northstar Credit Partners" }},
            periods: {{
              current: {{ id: "pkg_002", label: "Dec 2025" }},
              baseline: {{ id: "pkg_001", label: "Sep 2025" }},
              comparison_basis: "prior_verified_period"
            }},
            period_options: [],
            summary: {{ blockers: 0, review_signals: 1, verified_changes: 0, total: 1 }},
            screen_taxonomy: {{
              summary_keys: ["blockers", "review_signals", "verified_changes"],
              section_order: ["blockers", "review_signals", "verified_changes"],
              section_labels: {{ blockers: "Blockers", review_signals: "Review Signals", verified_changes: "Verified Changes" }}
            }},
            items: [reviewItem]
          }},
          selectedItemId: reviewItem.id,
          activeDraft: null,
          activeAnalystNote: {{
            itemId: reviewItem.id,
            noteId: 11,
            author: "operator_ui",
            createdAt: "2026-03-09T10:00:00+00:00",
            updatedAt: "2026-03-09T10:05:00+00:00",
            subject: "EBITDA extraction review",
            text: "Candidate found in narrative text; confirm exact source row before relying on this metric.",
            memoReady: true,
            exportReady: false,
            dirty: false,
          }},
        }};

        const handlers = {{
          onPeriodChange() {{}},
          onItemSelect() {{}},
          onAction() {{}},
          onOpenPreview() {{}},
          onDraftChange() {{}},
          onAnalystNoteChange() {{}},
        }};

        renderScreen(state, elements, handlers);
        const html = elements.detailContent.innerHTML;
        if (!html.includes("Analyst note")) {{
          throw new Error("analyst note panel should render when activeAnalystNote is open");
        }}
        if (!html.includes("Mark ready for memo")) {{
          throw new Error("memo-ready note flag should render");
        }}
        if (!html.includes("Mark export-ready")) {{
          throw new Error("export-ready note flag should render");
        }}
        if (!html.includes("Save note")) {{
          throw new Error("save note action should render");
        }}
      """
    )
    _run_render_script(script)
