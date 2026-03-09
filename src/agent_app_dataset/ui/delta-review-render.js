function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function proofStateLabel(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return "unknown";
  if (normalized === "needs_confirmation") return "needs confirmation";
  if (normalized === "missing_source") return "missing source";
  if (normalized === "conflict_detected") return "conflict detected";
  return normalized.replaceAll("_", " ");
}

function caseModeLabel(mode) {
  const normalized = String(mode || "").trim().toLowerCase();
  if (normalized === "investigation_conflict") return "Conflict";
  if (normalized === "investigation_missing_required_reporting") return "Missing support";
  if (normalized === "investigation_missing_source") return "Missing source";
  if (normalized === "investigation_candidate_only") return "Candidate only";
  if (normalized === "review_possible_source_conflict") return "Possible conflict";
  if (normalized === "review_possible_missing_reporting_item") return "Possible gap";
  if (normalized === "review_possible_requirement") return "Possible requirement";
  if (normalized === "review_possible_material_change") return "Possible change";
  if (normalized === "verified_review") return "Verified";
  return "Review";
}

function caseCertaintyLabel(item) {
  const backendLabel = String(item?.case_certainty_label || "").trim();
  if (backendLabel) return backendLabel;

  const certainty = String(item?.case_certainty || "").trim().toLowerCase();
  if (certainty === "grounded_fact") return "Grounded fact";
  if (certainty === "missing_required_support") return "Missing required support";
  if (certainty === "confirmed_current_extraction") return "Confirmed extraction";
  if (certainty === "candidate_only") return "Candidate only";
  if (certainty === "conflict_detected") return "Conflict detected";
  if (certainty === "missing_source") return "Missing source";
  return "Review signal";
}

function caseCertaintyClass(item) {
  const certainty = String(item?.case_certainty || "").trim().toLowerCase();
  if (!certainty) return "certainty-review_signal";
  return `certainty-${certainty}`;
}

const GENERIC_REVIEW_REASON_PHRASES = [
  "review-tier confirmation required",
  "analyst confirmation required",
  "analyst confirmation needed",
  "candidate needs confirmation",
  "review lane",
];

function concreteReviewReason(item) {
  const code = String(item?.review_reason_code || "").trim().toLowerCase();
  const label = String(item?.review_reason_label || "").trim();
  const detail = String(item?.review_reason_detail || "").trim();
  if (!code || !label) return null;
  const combined = `${label} ${detail}`.toLowerCase();
  if (GENERIC_REVIEW_REASON_PHRASES.some((token) => combined.includes(token))) return null;
  return { code, label, detail };
}

function groupLabel(group, taxonomy) {
  const labels = taxonomy?.section_labels || {};
  const label = labels[group];
  if (typeof label === "string" && label.trim()) return label.trim();
  return String(group || "").replaceAll("_", " ");
}

function renderCounts(summary, screenMode, taxonomy) {
  const safe = summary || {};
  const keys = Array.isArray(taxonomy?.summary_keys) && taxonomy.summary_keys.length
    ? taxonomy.summary_keys
    : screenMode === "first_package_intake"
      ? ["blockers", "missing_support", "review_signals", "confirmed_findings"]
      : ["blockers", "review_signals", "verified_changes"];
  const keyLabel = {
    blockers: { singular: "blocker", plural: "blockers" },
    material_changes: { singular: "material change", plural: "material changes" },
    verified: { singular: "verified", plural: "verified" },
    verified_changes: { singular: "verified change", plural: "verified changes" },
    missing_support: { singular: "missing support", plural: "missing support" },
    review_signals: { singular: "review signal", plural: "review signals" },
    confirmed_findings: { singular: "confirmed finding", plural: "confirmed findings" },
  };
  const bits = [];
  for (const key of keys) {
    const value = Number(safe[key] ?? 0);
    const labels = keyLabel[key] || { singular: key, plural: key };
    const label = value === 1 ? labels.singular : labels.plural;
    bits.push(
      `<span class="dr-count"><strong class="dr-numeric">${escapeHtml(value)}</strong> ${escapeHtml(label)}</span>`
    );
  }
  return bits.join('<span class="dr-count-divider">•</span>');
}

function isUnresolvedDisplay(value) {
  const text = String(value || "").trim().toLowerCase();
  return !text || text === "unresolved" || text === "n/a" || text === "na";
}

function movementMarkup(item, { forDetail = false, screenMode = "delta_review" } = {}) {
  const metric = String(item.metric_label || item.metric_key || "Metric");
  const conflictAnchors = Array.isArray(item.competing_anchors) ? item.competing_anchors : [];
  const mode = String(item.case_mode || "");
  const hasConflictPair =
    (mode === "investigation_conflict" || mode === "review_possible_source_conflict") &&
    conflictAnchors.length >= 2;
  const previous = hasConflictPair
    ? String(conflictAnchors[0]?.value_display || item.previous_value_display || "unresolved")
    : String(item.previous_value_display || "unresolved");
  const current = hasConflictPair
    ? String(conflictAnchors[1]?.value_display || item.current_value_display || "unresolved")
    : String(item.current_value_display || "unresolved");
  const delta = String(item.delta_display || "N/A");
  const currentUnresolved = isUnresolvedDisplay(current);
  const isIntakeMode = screenMode === "first_package_intake";

  if (isIntakeMode && !hasConflictPair) {
    const searchState = String(item.current_search_state || "");
    const lead =
      searchState === "candidate_only"
        ? "Candidate"
        : searchState === "missing" || currentUnresolved
          ? "Current"
          : "Current";

    if (forDetail) {
      return {
        movement: `
          <div class="dr-movement-block">
            <p class="dr-movement-label">Current package</p>
            <p class="dr-detail-movement dr-numeric">
              <span class="dr-detail-metric">${escapeHtml(metric)}</span>
              <span class="dr-detail-move-before">${escapeHtml(lead)}</span>
              <span class="dr-detail-move-arrow">:</span>
              <span class="dr-detail-move-after ${currentUnresolved ? "is-unresolved" : ""}">${escapeHtml(current)}</span>
            </p>
          </div>
        `,
        deltaLine: "",
      };
    }

    return `
      <p class="dr-item-movement dr-numeric">
        <span class="dr-move-before">${escapeHtml(lead)}:</span>
        <span class="dr-move-after ${currentUnresolved ? "is-unresolved" : ""}">${escapeHtml(current)}</span>
      </p>
    `;
  }

  if (forDetail) {
    const deltaLine =
      delta && delta !== "N/A" && delta !== "source_conflict"
        ? `<p class="dr-detail-delta dr-numeric">Delta ${escapeHtml(delta)}</p>`
        : "";
    return {
      movement: `
        <div class="dr-movement-block">
          <p class="dr-movement-label">Movement</p>
          <p class="dr-detail-movement dr-numeric">
            <span class="dr-detail-metric">${escapeHtml(metric)}</span>
            <span class="dr-detail-move-before">${escapeHtml(previous)}</span>
            <span class="dr-detail-move-arrow">${hasConflictPair ? "↔" : "→"}</span>
            <span class="dr-detail-move-after ${currentUnresolved ? "is-unresolved" : ""}">${escapeHtml(current)}</span>
          </p>
        </div>
      `,
      deltaLine,
    };
  }

  const tail =
    delta && delta !== "N/A" && delta !== "source_conflict"
      ? `<span class="dr-move-tail">• Δ ${escapeHtml(delta)}</span>`
      : "";
  return `
    <p class="dr-item-movement dr-numeric">
      <span class="dr-move-before">${escapeHtml(previous)}</span>
      <span class="dr-move-arrow">${hasConflictPair ? "↔" : "→"}</span>
      <span class="dr-move-after ${currentUnresolved ? "is-unresolved" : ""}">${escapeHtml(current)}</span>
      ${tail}
    </p>
  `;
}

function renderQueueGroups(items, selectedItemId, { screenMode, taxonomy }) {
  if (!items || !items.length) {
    const emptyTitle =
      screenMode === "first_package_intake"
        ? "No intake issues in current package"
        : "No delta review items for this comparison";
    const emptyCopy =
      screenMode === "first_package_intake"
        ? "No blockers, missing support, or review signals are pending."
        : "No decision objects are pending under this filter.";
    return `
      <div class="dr-empty">
        <p class="dr-empty-title">${escapeHtml(emptyTitle)}</p>
        <p class="dr-empty-copy">${escapeHtml(emptyCopy)}</p>
      </div>
    `;
  }

  const grouped = new Map();
  const displayGroupKey = (item) => String(item.display_group || item.group || "verified_changes");
  for (const item of items) {
    const key = displayGroupKey(item);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  }

  const orderedKeys =
    Array.isArray(taxonomy?.section_order) && taxonomy.section_order.length
      ? taxonomy.section_order
      : screenMode === "first_package_intake"
        ? ["blockers", "review_signals", "confirmed_findings"]
        : ["blockers", "review_signals", "verified_changes"];
  const sections = [];
  for (const key of orderedKeys) {
    const list = grouped.get(key);
    if (!list || !list.length) continue;

    const rows = list
      .map((item) => {
        const selected = item.id === selectedItemId ? "is-selected" : "";
        const maturity = String(item.concept_maturity || "").trim().toLowerCase();
        const maturityClass = maturity === "review" ? "is-review" : "is-grounded";
        const certaintyClass = caseCertaintyClass(item);
        const groupClass = `group-${displayGroupKey(item)}`;
        const reason = concreteReviewReason(item);
        const reviewReasonLabel = reason ? reason.label : "";
        const caseCertainty = String(item.case_certainty || "").trim().toLowerCase();
        const queueConsequence =
          maturity === "grounded" &&
          String(item.case_mode || "").trim().toLowerCase() === "investigation_missing_required_reporting"
            ? "Required support missing from current package."
            : String(item.why_it_matters || "");
        const preferReasonLine =
          Boolean(reviewReasonLabel) && (maturity === "review" || caseCertainty === "candidate_only");
        return `
          <button class="dr-item ${selected} ${maturityClass} ${certaintyClass} ${groupClass}" type="button" data-item-id="${escapeHtml(item.id)}" aria-current="${selected ? "true" : "false"}">
            <span class="dr-item-rail severity-${escapeHtml(item.severity || "low")}"></span>
            <span class="dr-item-main">
              <p class="dr-item-headline">${escapeHtml(item.headline || "")}</p>
              ${movementMarkup(item, { screenMode })}
              ${
                preferReasonLine
                  ? `<p class="dr-item-review-reason">${escapeHtml(reviewReasonLabel)}</p>`
                  : `<p class="dr-item-why">${escapeHtml(queueConsequence)}</p>`
              }
            </span>
            <span class="dr-item-state">${escapeHtml(caseCertaintyLabel(item))}</span>
          </button>
        `;
      })
      .join("");

    sections.push(`
      <section class="dr-section" data-group="${escapeHtml(key)}">
        <h2 class="dr-section-title">${escapeHtml(groupLabel(key, taxonomy))}</h2>
        ${rows}
      </section>
    `);
  }

  return sections.join("");
}

function compactSnippet(text) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return "";
  const clipped = lines.slice(0, 5).join("\n");
  if (clipped.length > 520) return `${clipped.slice(0, 520)}…`;
  return clipped;
}

function valueHighlightVariants(valueDisplay) {
  const raw = String(valueDisplay || "").trim();
  if (!raw || isUnresolvedDisplay(raw)) return [];
  const variants = new Set([raw]);
  const numeric = raw.replaceAll(",", "");
  if (/^-?[0-9]+(?:\.[0-9]+)?$/.test(numeric)) {
    const parsed = Number(numeric);
    if (Number.isFinite(parsed)) {
      variants.add(parsed.toLocaleString("en-US"));
      variants.add(parsed.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
      variants.add(String(parsed));
      variants.add(String(Math.trunc(parsed)));
    }
  }
  return Array.from(variants).filter((variant) => variant.length > 1);
}

function highlightSnippetText(snippet, anchorValue, stateKind) {
  let html = escapeHtml(snippet);
  let matched = false;

  for (const variant of valueHighlightVariants(anchorValue)) {
    const regex = new RegExp(escapeRegex(variant), "g");
    const next = html.replace(regex, '<mark class="dr-anchor-hit">$&</mark>');
    if (next !== html) {
      html = next;
      matched = true;
      break;
    }
  }

  if (!matched && stateKind !== "found") {
    const keywords =
      stateKind === "conflict"
        ? ["conflict", "mismatch", "inconsistent", "different"]
        : stateKind === "candidate"
          ? ["candidate", "not confirmed", "unresolved", "review"]
          : ["missing", "not found", "unresolved", "no source"];

    for (const keyword of keywords) {
      const regex = new RegExp(escapeRegex(keyword), "ig");
      const next = html.replace(regex, '<mark class="dr-anchor-hit is-warning">$&</mark>');
      if (next !== html) {
        html = next;
        matched = true;
        break;
      }
    }
  }

  return { html, matched };
}

function sideState(sideKey, item, side) {
  const proofState = String(item.proof_state || "");
  const status = String(item.status || "");
  const currentSearchState = String(item.current_search_state || "");
  const hasAnchor = Boolean((side.file_id || side.file_name) && side.locator);

  if (sideKey === "Requirement") {
    if (Boolean(side.requirement_grounded)) {
      return { kind: "found", text: "Grounded reporting requirement" };
    }
    return { kind: "candidate", text: "Possible requirement evidence" };
  }

  if (String(item.case_mode || "") === "investigation_conflict") {
    return { kind: "conflict", text: "Conflicting submitted source" };
  }

  if (sideKey === "Baseline") {
    if (hasAnchor) {
      return { kind: "found", text: "Found in prior period" };
    }
    return { kind: "missing", text: "No prior anchor recorded" };
  }

  if (proofState === "conflict_detected") {
    return { kind: "conflict", text: "Conflicts with submitted value" };
  }
  if (String(item.case_mode || "") === "investigation_missing_required_reporting") {
    if (currentSearchState === "candidate_only") {
      return { kind: "candidate", text: "Candidate found, not confirmed" };
    }
    return { kind: "missing", text: "Required line item not found in current package" };
  }
  if (proofState === "missing_source") {
    return { kind: "missing", text: "No trustworthy anchor found in current package" };
  }
  if (proofState === "needs_confirmation" || status === "candidate_flagged") {
    return { kind: "candidate", text: "Candidate found, not confirmed" };
  }
  if (hasAnchor) {
    return { kind: "found", text: "Found in borrower package" };
  }
  return { kind: "missing", text: "No trustworthy anchor found in current package" };
}

function anchorToEvidenceSide(anchor) {
  if (!anchor || typeof anchor !== "object") return {};
  return {
    file_id: anchor.doc_id || "",
    file_name: anchor.doc_name || "Source unavailable",
    locator: anchor.locator_display || `${anchor.locator_type || "locator"}=${anchor.locator_value || ""}`,
    excerpt: anchor.source_snippet || "",
    confidence: anchor.confidence ?? null,
    preview_url: anchor.preview_url || "",
    download_url: anchor.download_url || "",
    anchor_value_display: anchor.value_display || "unresolved",
    locator_type: anchor.locator_type || "",
    locator_value: anchor.locator_value || "",
  };
}

function proofSides(item, { screenMode = "delta_review" } = {}) {
  const mode = String(item?.proof_compare_mode || "");
  if (mode === "source_vs_source" && Array.isArray(item?.competing_anchors) && item.competing_anchors.length >= 2) {
    const anchors = item.competing_anchors.slice(0, 2);
    return anchors.map((anchor, index) => ({
      sideKey: index === 0 ? "Source A" : "Source B",
      side: {
        file_id: anchor.doc_id || "",
        file_name: anchor.doc_name || "Source unavailable",
        locator: anchor.locator_display || `${anchor.locator_type || "locator"}=${anchor.locator_value || ""}`,
        excerpt: anchor.source_snippet || "",
        confidence: null,
        preview_url: anchor.preview_url || "",
        download_url: anchor.download_url || "",
        anchor_value_display: anchor.value_display || "unresolved",
      },
      anchorIndex: index,
    }));
  }

  let baselineSide = item?.evidence?.baseline || {};
  let currentSide = item?.evidence?.current || {};

  if (item?.baseline_anchor && typeof item.baseline_anchor === "object") {
    baselineSide = anchorToEvidenceSide(item.baseline_anchor);
  }
  if (
    (mode === "baseline_vs_current_candidate" || String(item?.current_search_state || "") === "candidate_only") &&
    item?.current_candidate_anchor &&
    typeof item.current_candidate_anchor === "object"
  ) {
    currentSide = anchorToEvidenceSide(item.current_candidate_anchor);
  }

  if (screenMode === "first_package_intake") {
    return [
      { sideKey: "Current package", side: currentSide, anchorIndex: null },
    ];
  }

  return [
    { sideKey: "Baseline", side: baselineSide, anchorIndex: null },
    { sideKey: "Current", side: currentSide, anchorIndex: null },
  ];
}

function requirementSideEntry(item) {
  const requirement = item?.requirement_anchor;
  if (!requirement || typeof requirement !== "object" || !requirement.grounded) return null;

  return {
    sideKey: "Requirement",
    side: {
      file_id: requirement.doc_id || "",
      file_name: requirement.doc_name || "Requirement document",
      locator: requirement.locator_display || `${requirement.locator_type || "locator"}=${requirement.locator_value || ""}`,
      excerpt: requirement.source_snippet || "",
      confidence: null,
      preview_url: requirement.preview_url || "",
      download_url: requirement.download_url || "",
      anchor_value_display: "",
      locator_type: requirement.locator_type || "",
      locator_value: requirement.locator_value || "",
      requirement_grounded: Boolean(requirement.grounded),
      required_concept_label: requirement.required_concept_label || item?.metric_label || "",
    },
    anchorIndex: null,
  };
}

function renderSheetPreview(previewPayload, { mismatchFocus = false } = {}) {
  const preview = previewPayload?.preview || null;
  if (!preview || preview.kind !== "xlsx_sheet" || !Array.isArray(preview.rows) || !preview.rows.length) {
    return "";
  }

  const highlightIndex = preview.rows.findIndex((row) => Array.isArray(row) && row.some((cell) => Boolean(cell?.highlight)));
  const highlightRow = highlightIndex >= 0 ? preview.rows[highlightIndex] : null;
  const highlightCol = Array.isArray(highlightRow) ? highlightRow.findIndex((cell) => Boolean(cell?.highlight)) : -1;
  const start = highlightIndex >= 0 ? Math.max(0, highlightIndex - 2) : 0;
  const end = Math.min(preview.rows.length, start + 6);
  const rowsSlice = preview.rows.slice(start, end);
  const localHighlightIndex = highlightIndex >= 0 ? highlightIndex - start : -1;

  const totalCols = preview.rows.reduce((maxCount, row) => {
    if (!Array.isArray(row)) return maxCount;
    return Math.max(maxCount, row.length);
  }, 0);

  const selectedCols = [];
  if (highlightCol >= 0) {
    if (highlightCol > 0) selectedCols.push(highlightCol - 1);
    selectedCols.push(highlightCol);
    if (highlightCol === 0 && totalCols > 1) selectedCols.push(1);
  } else {
    for (let idx = 0; idx < Math.min(2, totalCols); idx += 1) selectedCols.push(idx);
  }
  const dedupedCols = [...new Set(selectedCols)].filter((idx) => idx >= 0 && idx < totalCols);

  const sanitizeCellValue = (value) => {
    const text = String(value ?? "").trim();
    if (!text) return "";
    const lowered = text.toLowerCase();
    if (lowered === "from borrower package" || lowered === "highlight target") return "";
    return text;
  };

  function renderRow(line, rowIndex, { pinnedTarget = false } = {}) {
    const rowClasses = [];
    if (rowIndex === localHighlightIndex) {
      rowClasses.push("dr-preview-row-target");
      if (mismatchFocus) rowClasses.push("dr-preview-row-mismatch");
      if (pinnedTarget) rowClasses.push("dr-preview-row-pinned");
    } else if (localHighlightIndex >= 0 && Math.abs(rowIndex - localHighlightIndex) <= 1) {
      rowClasses.push("dr-preview-row-context");
    }
    if (mismatchFocus && rowIndex !== localHighlightIndex) {
      rowClasses.push("dr-preview-row-muted");
    }

    const cells = dedupedCols
      .map((colIdx) => {
        const cell = Array.isArray(line) ? line[colIdx] : {};
        const classes = [];
        if (cell && cell.highlight) classes.push("dr-preview-hit");
        if (highlightCol >= 0 && colIdx === highlightCol) classes.push("dr-preview-target-col");
        return `<td class="${classes.join(" ")}">${escapeHtml(sanitizeCellValue(cell?.value))}</td>`;
      })
      .join("");
    return `<tr class="${rowClasses.join(" ")}">${cells}</tr>`;
  }

  if (mismatchFocus && localHighlightIndex >= 0) {
    const targetRow = rowsSlice[localHighlightIndex];
    const matchingRows = rowsSlice
      .map((line, rowIndex) => (rowIndex === localHighlightIndex ? "" : renderRow(line, rowIndex)))
      .join("");
    return `
      <div class="dr-evidence-preview dr-evidence-preview-conflict">
        <p class="dr-preview-focus-label">Conflicting row</p>
        <table class="dr-preview-sheet dr-preview-sheet-focus">
          <tbody>${renderRow(targetRow, localHighlightIndex, { pinnedTarget: true })}</tbody>
        </table>
        ${
          matchingRows
            ? `<details class="dr-preview-matching"><summary>Show matching rows</summary><table class="dr-preview-sheet"><tbody>${matchingRows}</tbody></table></details>`
            : ""
        }
      </div>
    `;
  }

  const rows = rowsSlice.map((line, rowIndex) => renderRow(line, rowIndex)).join("");

  return `
    <div class="dr-evidence-preview">
      <table class="dr-preview-sheet">
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderPdfPreview(previewPayload) {
  const preview = previewPayload?.preview || {};
  const text = String(preview.text || "").trim();
  if (!text) {
    return "";
  }
  const clipped = text.length > 1100 ? `${text.slice(0, 1100)}…` : text;
  return `
    <div class="dr-evidence-preview">
      <p class="dr-preview-note">${escapeHtml(clipped)}</p>
    </div>
  `;
}

function renderEvidencePreview(side, traceEvidenceCache, { mismatchFocus = false } = {}) {
  const previewUrl = String(side.preview_url || "").trim();
  if (!previewUrl) return "";

  const cached = traceEvidenceCache.get(previewUrl);
  if (!cached) return "";

  const payload = cached.evidence_preview || {};
  const docType = String(payload.doc_type || "").toUpperCase();

  if (docType === "XLSX") {
    return renderSheetPreview(payload, { mismatchFocus });
  }
  if (docType === "PDF") {
    return renderPdfPreview(payload);
  }
  return "";
}

function evidenceCard(sideEntry, item, traceEvidenceCache) {
  const sideKey = sideEntry.sideKey;
  const side = sideEntry.side || {};
  const anchorIndex = sideEntry.anchorIndex;
  const state = sideState(sideKey, item, side);
  const anchorValue = side.anchor_value_display || (sideKey === "Baseline" ? item.previous_value_display : item.current_value_display);
  const anchorValueText = String(anchorValue || "unresolved");
  const unresolvedAnchor = isUnresolvedDisplay(anchorValueText);
  const isRequirementSide = sideKey === "Requirement";
  const isConflictCase = ["investigation_conflict", "review_possible_source_conflict"].includes(String(item.case_mode || ""));

  const previewButton = side.preview_url
    ? `<button type="button" class="dr-link-button" data-preview-url="${escapeHtml(side.preview_url)}" data-side="${escapeHtml(
        sideKey
      )}">View in doc</button>`
    : "";
  const viewInDocFallback =
    !side.preview_url && side.download_url && side.locator
      ? `<a class="dr-link-button" href="${escapeHtml(side.download_url)}" target="_blank" rel="noreferrer noopener">View in doc</a>`
      : "";
  const downloadButton = side.download_url
    ? `<a class="dr-link-button" href="${escapeHtml(side.download_url)}" target="_blank" rel="noreferrer noopener">Download</a>`
    : "";

  const locatorLine = side.locator
    ? `<p class="dr-evidence-meta dr-numeric">${escapeHtml(side.locator)}</p>`
    : `<p class="dr-evidence-meta dr-numeric">locator unavailable</p>`;

  const snippet = compactSnippet(side.excerpt || "");
  const highlighted = snippet
    ? isRequirementSide
      ? { html: requirementSnippetMarkup(snippet, side.required_concept_label || item?.metric_label || ""), matched: true }
      : highlightSnippetText(snippet, anchorValueText, state.kind)
    : { html: "", matched: false };
  const preview = renderEvidencePreview(side, traceEvidenceCache, { mismatchFocus: isConflictCase });
  const showAnchorValue = !isRequirementSide && (!highlighted.matched || state.kind === "missing");
  const conflictConfirmButton =
    isConflictCase &&
    Number.isInteger(anchorIndex)
      ? `<button type="button" class="dr-secondary-button dr-inline-confirm" data-action-id="confirm_source_of_record" data-item-id="${escapeHtml(
          item.id
        )}" data-anchor-index="${anchorIndex}">Confirm this as source of record</button>`
      : "";

  return `
    <article class="dr-evidence-card">
      <header class="dr-evidence-head">
        <p class="dr-evidence-side">${escapeHtml(sideKey)}</p>
        <p class="dr-evidence-file">${escapeHtml(side.file_name || "Source unavailable")}</p>
      </header>
      <p class="dr-evidence-state dr-evidence-state-${escapeHtml(state.kind)}">${escapeHtml(state.text)}</p>
      ${isConflictCase ? '<p class="dr-evidence-focus">Mismatch focus</p>' : ""}
      ${preview}
      ${
        showAnchorValue
          ? `<p class="dr-evidence-value dr-numeric">
              <span class="dr-evidence-value-label">Anchor value</span>
              <strong class="${unresolvedAnchor ? "is-unresolved" : ""}">${escapeHtml(anchorValueText)}</strong>
            </p>`
          : ""
      }
      ${
        highlighted.html
          ? `<p class="dr-evidence-snippet">${highlighted.html}</p>`
          : '<p class="dr-evidence-empty">No source snippet available for this anchor.</p>'
      }
      ${locatorLine}
      ${(previewButton || viewInDocFallback || downloadButton) ? `<div class="dr-evidence-actions">${previewButton}${viewInDocFallback}${downloadButton}</div>` : ""}
      ${conflictConfirmButton ? `<div class="dr-evidence-confirm">${conflictConfirmButton}</div>` : ""}
    </article>
  `;
}

function requirementSnippetMarkup(snippet, conceptLabel) {
  let html = escapeHtml(snippet || "");
  const label = String(conceptLabel || "").trim();
  if (!label || !html) return html;
  const regex = new RegExp(escapeRegex(label), "ig");
  return html.replace(regex, '<mark class="dr-anchor-hit">$&</mark>');
}

function requirementProof(item, traceEvidenceCache) {
  const requirement = item?.requirement_anchor;
  if (!requirement || typeof requirement !== "object" || !requirement.grounded) return "";

  const locator = String(requirement.locator_display || "").trim() || "locator unavailable";
  const snippet = compactSnippet(requirement.source_snippet || "");
  const previewButton = requirement.preview_url
    ? `<button type="button" class="dr-link-button" data-preview-url="${escapeHtml(
        requirement.preview_url
      )}" data-side="Requirement">View in doc</button>`
    : "";
  const viewInDocFallback =
    !requirement.preview_url && requirement.download_url
      ? `<a class="dr-link-button" href="${escapeHtml(requirement.download_url)}" target="_blank" rel="noreferrer noopener">View in doc</a>`
      : "";
  const downloadButton = requirement.download_url
    ? `<a class="dr-link-button" href="${escapeHtml(requirement.download_url)}" target="_blank" rel="noreferrer noopener">Download</a>`
    : "";
  const preview = renderEvidencePreview(
    {
      preview_url: requirement.preview_url || "",
      download_url: requirement.download_url || "",
    },
    traceEvidenceCache
  );

  return `
    <article class="dr-requirement-proof">
      <header class="dr-evidence-head">
        <p class="dr-evidence-side">Requirement</p>
        <p class="dr-evidence-file">${escapeHtml(requirement.doc_name || "Requirement document")}</p>
      </header>
      <p class="dr-evidence-state dr-evidence-state-found">Grounded reporting requirement</p>
      ${preview}
      ${
        snippet
          ? `<p class="dr-evidence-snippet">${requirementSnippetMarkup(
              snippet,
              requirement.required_concept_label || item?.metric_label || ""
            )}</p>`
          : '<p class="dr-evidence-empty">Requirement snippet unavailable.</p>'
      }
      <p class="dr-evidence-meta dr-numeric">${escapeHtml(locator)}</p>
      ${(previewButton || viewInDocFallback || downloadButton) ? `<div class="dr-evidence-actions">${previewButton}${viewInDocFallback}${downloadButton}</div>` : ""}
    </article>
  `;
}

function renderHistory(traceId, historyCache) {
  if (!traceId) return "";
  const payload = historyCache.get(traceId);
  if (!payload) return "";
  const rows = payload.history || [];
  if (!rows.length) return "";

  const html = rows
    .map((entry) => {
      return `
        <article class="dr-history-item">
          <p><strong>${escapeHtml(entry.status_before || "unknown")} → ${escapeHtml(entry.status_after || "unknown")}</strong></p>
          <p>${escapeHtml(entry.resolver || "system")} • ${escapeHtml(entry.resolved_at || "n/a")}</p>
          <p>${escapeHtml(entry.note || "")}</p>
        </article>
      `;
    })
    .join("");

  return `
    <section class="dr-history">
      <h3 class="dr-history-title">Resolution history</h3>
      <div class="dr-history-list">${html}</div>
    </section>
  `;
}

function renderDetail(
  item,
  traceEvidenceCache,
  traceHistoryCache,
  { refreshing = false, activeDraft = null, activeAnalystNote = null, screenMode = "delta_review" } = {}
) {
  if (!item) {
    return `
      <div class="dr-empty">
        <p class="dr-empty-title">Select a decision object</p>
        <p class="dr-empty-copy">The case pane shows proof and action for the selected queue item.</p>
      </div>
    `;
  }

  const primary = item.primary_action || null;
  const actions = Array.isArray(item.available_actions) ? item.available_actions : [];
  const payloadSecondary = Array.isArray(item.secondary_actions) ? item.secondary_actions : [];
  const payloadOverflow = Array.isArray(item.overflow_actions) ? item.overflow_actions : [];
  const fallbackSecondary = actions.filter((action) => action.id !== primary?.id).slice(0, 1);
  const fallbackOverflow = actions.filter((action) => action.id !== primary?.id).slice(1);
  const secondaryActions = (payloadSecondary.length ? payloadSecondary : fallbackSecondary).slice(0, 1);
  const overflowActions = payloadOverflow.length ? payloadOverflow : fallbackOverflow;
  const isReviewCase = String(item.concept_maturity || "").trim().toLowerCase() === "review";
  const maturityClass = isReviewCase ? "is-review" : "is-grounded";
  const workspaceMode = String(item.workspace_mode || (isReviewCase ? "investigation_mode" : "decision_mode"));
  const isIntakeMode = screenMode === "first_package_intake";
  const workspaceLabel = isIntakeMode
    ? isReviewCase
      ? "Current package investigation"
      : "Current package intake"
    : workspaceMode === "investigation_mode"
      ? "Investigation mode"
      : "Delta review";
  const detailCaseLine =
    isReviewCase && !isIntakeMode
      ? `<p class="dr-detail-case">${escapeHtml(caseModeLabel(item.case_mode))}</p>`
      : "";
  const certaintyLabel = caseCertaintyLabel(item);
  const reason = concreteReviewReason(item);
  const reviewReasonLabel = reason ? reason.label : "";
  const reviewReasonDetail = reason ? reason.detail : "";
  const showWorkspaceLine = !(isIntakeMode && isReviewCase);
  const sides = proofSides(item, { screenMode });
  const requirementSide = requirementSideEntry(item);
  const requirementFirst =
    String(item.case_mode || "") === "investigation_missing_required_reporting" &&
    Boolean(item?.requirement_anchor?.grounded);
  const proofEntries = requirementSide
    ? requirementFirst
      ? [requirementSide, ...sides]
      : [...sides, requirementSide]
    : sides;
  const evidenceGridClasses = ["dr-evidence-grid"];
  if (proofEntries.length > 2) evidenceGridClasses.push("has-requirement");
  if (screenMode === "first_package_intake") evidenceGridClasses.push("is-intake");
  if (requirementFirst) evidenceGridClasses.push("requirement-first");
  const openDraft = activeDraft && activeDraft.itemId === item.id ? activeDraft : null;
  const openAnalystNote = activeAnalystNote && activeAnalystNote.itemId === item.id ? activeAnalystNote : null;

  const movement = movementMarkup(item, { forDetail: true, screenMode });
  const actionButtons = [
    primary
      ? `<button type="button" class="dr-primary-button ${isReviewCase ? "is-review" : "is-grounded"}" data-action-id="${escapeHtml(primary.id)}" data-item-id="${escapeHtml(
          item.id
        )}">${escapeHtml(primary.label || "Action")}</button>`
      : "",
    ...secondaryActions.map(
      (action, index) =>
        `<button type="button" class="dr-secondary-button ${index === 0 ? "is-moderate" : "is-tertiary"}" data-action-id="${escapeHtml(action.id)}" data-item-id="${escapeHtml(
          item.id
        )}">${escapeHtml(action.label || action.id)}</button>`
    ),
    overflowActions.length
      ? `<details class="dr-actions-overflow">
          <summary>More actions</summary>
          <div class="dr-actions-overflow-menu">
            ${overflowActions
              .map(
                (action) =>
                  `<button type="button" class="dr-secondary-button is-tertiary" data-action-id="${escapeHtml(action.id)}" data-item-id="${escapeHtml(
                    item.id
                  )}">${escapeHtml(action.label || action.id)}</button>`
              )
              .join("")}
          </div>
        </details>`
      : "",
  ]
    .filter(Boolean)
    .join("");

  const currentTrace = Array.isArray(item.trace_ids) && item.trace_ids.length ? String(item.trace_ids[0]) : "";

  return `
    <article class="dr-detail">
      <section class="dr-detail-head">
        ${refreshing ? '<div class="dr-loading-row"><span class="dr-spinner"></span>Refreshing case...</div>' : ""}
        <h2 class="dr-detail-headline">${escapeHtml(item.headline || "")}</h2>
        ${movement.movement}
        ${movement.deltaLine}
        ${showWorkspaceLine ? `<p class="dr-detail-workspace">${escapeHtml(workspaceLabel)}</p>` : ""}
        <p class="dr-detail-mode ${maturityClass}">${escapeHtml(certaintyLabel)}</p>
        ${
          reviewReasonLabel
            ? `<div class="dr-detail-review-reason">
                <p class="dr-detail-review-reason-label">Reason</p>
                <p class="dr-detail-review-reason-main">${escapeHtml(reviewReasonLabel)}</p>
                ${reviewReasonDetail ? `<p class="dr-detail-review-reason-detail">${escapeHtml(reviewReasonDetail)}</p>` : ""}
              </div>`
            : ""
        }
        ${detailCaseLine}
        <section class="dr-consequence">
          <p class="dr-consequence-label">Operational consequence</p>
          <p class="dr-consequence-copy">${escapeHtml(item.grounded_implication || item.why_it_matters || "")}</p>
        </section>
      </section>

      <section class="dr-evidence-section">
        <h3 class="dr-block-title">Proof</h3>
        <div class="${evidenceGridClasses.join(" ")}">
          ${proofEntries.map((entry) => evidenceCard(entry, item, traceEvidenceCache)).join("")}
        </div>
      </section>

      <section class="dr-actions">
        ${
          String(item.case_mode || "") === "investigation_conflict"
            ? '<p class="dr-actions-note">Choose a source in Proof, then confirm source of record.</p>'
            : String(item.case_mode || "") === "investigation_missing_required_reporting"
              ? '<p class="dr-actions-note">Request missing support or confirm an alternate source.</p>'
              : String(item.case_certainty || "") === "candidate_only"
                ? '<p class="dr-actions-note">Confirm source evidence before borrower follow-up.</p>'
              : String(item.concept_maturity || "") === "review"
                ? '<p class="dr-actions-note">Review signal: confirm evidence before escalation.</p>'
                : ""
        }
        ${actionButtons}
      </section>

      ${
        openDraft
          ? `
      <section class="dr-draft">
        <h3 class="dr-block-title">Borrower query draft</h3>
        <label class="dr-draft-label" for="draft-subject-${escapeHtml(item.id)}">Subject</label>
        <input id="draft-subject-${escapeHtml(item.id)}" class="dr-draft-input" type="text" data-draft-field="subject" data-item-id="${escapeHtml(item.id)}" value="${escapeHtml(openDraft.subject || "Draft query")}" />
        <label class="dr-draft-label" for="draft-text-${escapeHtml(item.id)}">Draft</label>
        <textarea id="draft-text-${escapeHtml(item.id)}" class="dr-draft-text" data-draft-field="text" data-item-id="${escapeHtml(item.id)}">${escapeHtml(openDraft.text || "")}</textarea>
        <div class="dr-draft-actions">
          <button type="button" class="dr-secondary-button" data-action-id="copy_borrower_draft" data-item-id="${escapeHtml(item.id)}">Copy draft</button>
          <button type="button" class="dr-secondary-button is-moderate" data-action-id="mark_follow_up_prepared" data-item-id="${escapeHtml(item.id)}">Mark borrower follow-up prepared</button>
          <button type="button" class="dr-secondary-button is-tertiary" data-action-id="close_draft_editor" data-item-id="${escapeHtml(item.id)}">Close draft editor</button>
        </div>
      </section>
      `
          : ""
      }

      ${
        openAnalystNote
          ? `
      <section class="dr-draft dr-analyst-note">
        <h3 class="dr-block-title">Analyst note</h3>
        <p class="dr-note-meta">
          Last updated ${escapeHtml(openAnalystNote.updatedAt || openAnalystNote.createdAt || "just now")}
          ${openAnalystNote.author ? ` • ${escapeHtml(openAnalystNote.author)}` : ""}
        </p>
        <label class="dr-draft-label" for="note-subject-${escapeHtml(item.id)}">Subject</label>
        <input id="note-subject-${escapeHtml(item.id)}" class="dr-draft-input" type="text" data-note-field="subject" data-item-id="${escapeHtml(item.id)}" value="${escapeHtml(openAnalystNote.subject || "")}" />
        <label class="dr-draft-label" for="note-text-${escapeHtml(item.id)}">Note</label>
        <textarea id="note-text-${escapeHtml(item.id)}" class="dr-draft-text" data-note-field="text" data-item-id="${escapeHtml(item.id)}">${escapeHtml(openAnalystNote.text || "")}</textarea>
        <div class="dr-note-flags">
          <label class="dr-note-flag">
            <input type="checkbox" data-note-field="memoReady" data-item-id="${escapeHtml(item.id)}" ${openAnalystNote.memoReady ? "checked" : ""} />
            Mark ready for memo
          </label>
          <label class="dr-note-flag">
            <input type="checkbox" data-note-field="exportReady" data-item-id="${escapeHtml(item.id)}" ${openAnalystNote.exportReady ? "checked" : ""} />
            Mark export-ready
          </label>
        </div>
        <div class="dr-draft-actions">
          <button type="button" class="dr-secondary-button is-moderate" data-action-id="save_analyst_note" data-item-id="${escapeHtml(item.id)}">Save note</button>
          <button type="button" class="dr-secondary-button is-tertiary" data-action-id="close_analyst_note" data-item-id="${escapeHtml(item.id)}">Close note</button>
        </div>
      </section>
      `
          : ""
      }

      ${renderHistory(currentTrace, traceHistoryCache)}
    </article>
  `;
}

function ensureSelectedItem(state) {
  const items = state.queuePayload?.items || [];
  if (!items.length) return "";
  const selected = items.find((item) => item.id === state.selectedItemId);
  if (selected) return selected.id;
  return items[0].id;
}

export function renderScreen(state, elements, handlers) {
  const {
    dealTitle,
    dealSwitcher,
    comparisonPair,
    comparisonNote,
    summaryCounts,
    periodRail,
    baselineSwitcher,
    baselineControl,
    includeResolved,
    queueContent,
    detailContent,
  } = elements;

  const queuePayload = state.queuePayload;
  const screenMode = String(queuePayload?.product_mode || queuePayload?.product_state?.screen_mode || "delta_review");
  const taxonomy = queuePayload?.screen_taxonomy || null;
  const selectedItemId = ensureSelectedItem(state);
  const items = queuePayload?.items || [];
  const selectedItem = items.find((item) => item.id === selectedItemId) || null;

  dealTitle.textContent = queuePayload?.deal?.name || "No deal selected";

  summaryCounts.innerHTML = renderCounts(queuePayload?.summary || {}, screenMode, taxonomy);
  comparisonPair.textContent =
    screenMode === "first_package_intake"
      ? queuePayload?.periods?.current
        ? `First package intake • ${queuePayload.periods.current.label}`
        : "First package intake"
      : queuePayload?.periods?.baseline && queuePayload?.periods?.current
        ? `${queuePayload.periods.current.label} vs ${queuePayload.periods.baseline.label}`
        : queuePayload?.periods?.current
          ? `${queuePayload.periods.current.label} (no baseline)`
          : "Comparison pending";

  comparisonNote.textContent =
    screenMode === "first_package_intake"
      ? "No prior processed package available yet. Focus is extraction and completeness."
      : queuePayload?.periods?.comparison_basis === "prior_processed_period"
        ? "Compared against prior processed period."
        : queuePayload?.periods?.comparison_basis === "explicit_baseline_period"
          ? "Baseline manually selected."
          : "";

  includeResolved.checked = Boolean(state.includeResolved);

  const options = state.deals || [];
  dealSwitcher.innerHTML = options
    .map((deal) => `<option value="${escapeHtml(deal.deal_id)}">${escapeHtml(deal.display_name || deal.deal_id)}</option>`)
    .join("");
  dealSwitcher.value = state.currentDealId || "";

  const periodOptions = queuePayload?.period_options || [];
  periodRail.innerHTML = periodOptions
    .map((period) => {
      const classes = ["dr-period-node"];
      const role = [];
      if (period.is_current) {
        classes.push("is-current");
        role.push("Current");
      }
      if (period.is_baseline) {
        classes.push("is-baseline");
        role.push("Baseline");
      }
      if (!role.length) role.push("Period");
      return `<button type="button" class="${classes.join(" ")}" data-period-id="${escapeHtml(period.id)}">
        <span class="dr-period-label">${escapeHtml(period.label)}</span>
        <span class="dr-period-role">${escapeHtml(role.join(" • "))}</span>
      </button>`;
    })
    .join("");

  const baselineChoices = periodOptions.filter((period) => period.id !== state.currentPeriodId);
  baselineSwitcher.innerHTML = [
    '<option value="">Auto baseline</option>',
    ...baselineChoices.map((period) => `<option value="${escapeHtml(period.id)}">${escapeHtml(period.label)}</option>`),
  ].join("");
  baselineSwitcher.value = state.baselinePeriodId || "";
  const isIntakeMode = screenMode === "first_package_intake";
  baselineSwitcher.disabled = isIntakeMode;
  baselineSwitcher.hidden = isIntakeMode;
  if (baselineControl) {
    baselineControl.hidden = isIntakeMode;
    if (baselineControl.style) {
      baselineControl.style.display = isIntakeMode ? "none" : "";
    }
  }

  if (state.errors.queue) {
    queueContent.innerHTML = `
      <div class="dr-empty">
        <p class="dr-empty-title">Queue request failed</p>
        <p class="dr-empty-copy">${escapeHtml(state.errors.queue)}</p>
      </div>
    `;
  } else if (state.loading.queue && !queuePayload) {
    queueContent.innerHTML = `
      <div class="dr-loading">
        <div class="dr-loading-row"><span class="dr-spinner"></span>Loading review queue...</div>
      </div>
    `;
  } else {
    queueContent.innerHTML = renderQueueGroups(items, selectedItemId, { screenMode, taxonomy });
  }

  if (!queuePayload && state.loading.queue) {
    detailContent.innerHTML = `
      <div class="dr-loading">
        <div class="dr-loading-row"><span class="dr-spinner"></span>Loading case...</div>
      </div>
    `;
  } else {
    detailContent.innerHTML = renderDetail(selectedItem, state.traceEvidenceCache, state.traceHistoryCache, {
      refreshing: state.loading.queue && Boolean(queuePayload),
      activeDraft: state.activeDraft || null,
      activeAnalystNote: state.activeAnalystNote || null,
      screenMode,
    });
  }

  for (const button of periodRail.querySelectorAll("[data-period-id]")) {
    button.addEventListener("click", () => {
      const periodId = button.getAttribute("data-period-id");
      if (periodId) handlers.onPeriodChange(periodId);
    });
  }

  for (const button of queueContent.querySelectorAll("[data-item-id]")) {
    button.addEventListener("click", () => {
      const itemId = button.getAttribute("data-item-id");
      if (itemId) handlers.onItemSelect(itemId);
    });
  }

  for (const button of detailContent.querySelectorAll("[data-action-id][data-item-id]")) {
    button.addEventListener("click", () => {
      const actionId = button.getAttribute("data-action-id");
      const itemId = button.getAttribute("data-item-id");
      const anchorIndexRaw = button.getAttribute("data-anchor-index");
      const anchorIndex = anchorIndexRaw === null ? null : Number(anchorIndexRaw);
      if (actionId && itemId) handlers.onAction(itemId, actionId, { anchorIndex });
    });
  }

  for (const button of detailContent.querySelectorAll("[data-preview-url]")) {
    button.addEventListener("click", () => {
      const previewUrl = button.getAttribute("data-preview-url");
      const side = button.getAttribute("data-side") || "";
      if (previewUrl) handlers.onOpenPreview(previewUrl, side);
    });
  }

  for (const field of detailContent.querySelectorAll("[data-draft-field][data-item-id]")) {
    field.addEventListener("input", () => {
      const draftField = field.getAttribute("data-draft-field");
      const itemId = field.getAttribute("data-item-id");
      if (!draftField || !itemId) return;
      handlers.onDraftChange(itemId, draftField, field.value);
    });
  }

  for (const field of detailContent.querySelectorAll("[data-note-field][data-item-id]")) {
    const noteField = field.getAttribute("data-note-field");
    field.addEventListener("input", () => {
      const itemId = field.getAttribute("data-item-id");
      if (!noteField || !itemId) return;
      const value =
        noteField === "memoReady" || noteField === "exportReady"
          ? Boolean(field.checked)
          : field.value;
      handlers.onAnalystNoteChange(itemId, noteField, value);
    });
    field.addEventListener("change", () => {
      const itemId = field.getAttribute("data-item-id");
      if (!noteField || !itemId) return;
      const value =
        noteField === "memoReady" || noteField === "exportReady"
          ? Boolean(field.checked)
          : field.value;
      handlers.onAnalystNoteChange(itemId, noteField, value);
    });
  }
}
