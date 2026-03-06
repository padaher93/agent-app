const state = {
  deals: [],
  selectedDealId: null,
  periodsByDeal: new Map(),
  deltaByPeriod: new Map(),
  packageManifestByPeriod: new Map(),
  packageEventsByPeriod: new Map(),
  traceEventsById: new Map(),
  traceEvidenceById: new Map(),
  activePeriodId: null,
  selectedTraceId: null,
  selectedRow: null,
  toast: "",
};

const conceptPriorityMap = {
  revenue_total: 13,
  ebitda_reported: 12,
  ebitda_adjusted: 11,
  operating_income_ebit: 10,
  interest_expense: 9,
  net_income: 8,
  cash_and_equivalents: 7,
  accounts_receivable_total: 6,
  inventory_total: 5,
  accounts_payable_total: 4,
  total_debt: 3,
  total_assets: 2,
  total_liabilities: 1,
};

const elements = {
  dealTree: document.getElementById("deal-tree"),
  periodsContainer: document.getElementById("periods-container"),
  evidenceContent: document.getElementById("evidence-content"),
  logsContent: document.getElementById("logs-content"),
  refreshButton: document.getElementById("refresh-button"),
};

let periodObserver = null;

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `request_failed_${response.status}`);
  }

  if (response.status === 204) {
    return {};
  }

  return response.json();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function parseNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed || trimmed.toUpperCase() === "N/A") return null;
  const cleaned = trimmed.replaceAll(/[^0-9.-]/g, "");
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatValue(value) {
  const num = parseNumber(value);
  if (num === null) {
    return value === null || value === undefined ? "N/A" : String(value);
  }
  return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function formatDate(value) {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value ?? "");
  return d.toISOString().slice(0, 10);
}

function emptyStateHtml(title, copy) {
  return `<div class="empty-state"><p class="empty-title">${escapeHtml(title)}</p><p class="empty-copy">${escapeHtml(copy)}</p></div>`;
}

function normalizeStatus(status) {
  if (!status) return "unresolved";
  return String(status);
}

function materialityForRow(row) {
  const priorValue = parseNumber(row.prior_value);
  const currentValue = parseNumber(row.current_value);
  const explicitAbsDelta = parseNumber(row.abs_delta);
  const explicitPctDelta = parseNumber(row.pct_delta);
  const fallbackMagnitude = parseNumber(row.normalized_value);

  const absDelta = explicitAbsDelta ?? (priorValue !== null && currentValue !== null ? Math.abs(currentValue - priorValue) : Math.abs(fallbackMagnitude ?? 0));
  const pctDelta = explicitPctDelta ?? (priorValue && currentValue !== null ? Math.abs((currentValue - priorValue) / priorValue) * 100 : 0);

  const absDeltaNorm = Math.min(1, Math.log10(Math.abs(absDelta) + 1) / 6);
  const pctDeltaNorm = Math.min(1, Math.abs(pctDelta) / 100);
  const conceptPriority = (conceptPriorityMap[row.concept_id] ?? 1) / 13;
  const confidence = Number.isFinite(Number(row.confidence)) ? Number(row.confidence) : 0;

  const score = 0.5 * absDeltaNorm + 0.3 * pctDeltaNorm + 0.2 * conceptPriority - 0.2 * (1 - confidence);

  return {
    score,
    confidence,
    absDelta,
  };
}

function sortedRows(rows) {
  return [...rows]
    .map((row) => ({ row, rank: materialityForRow(row) }))
    .sort((a, b) => {
      if (b.rank.score !== a.rank.score) return b.rank.score - a.rank.score;
      if (a.rank.confidence !== b.rank.confidence) return a.rank.confidence - b.rank.confidence;
      if (b.rank.absDelta !== a.rank.absDelta) return b.rank.absDelta - a.rank.absDelta;
      return String(a.row.concept_id).localeCompare(String(b.row.concept_id));
    });
}

function getPeriodsForSelectedDeal() {
  if (!state.selectedDealId) return [];
  return state.periodsByDeal.get(state.selectedDealId) || [];
}

function getRowsForPeriod(periodId) {
  const delta = state.deltaByPeriod.get(periodId);
  return delta?.rows || [];
}

function updatePeriodStatus(periodId, status) {
  const periods = getPeriodsForSelectedDeal();
  for (const period of periods) {
    if (period.package_id === periodId) {
      period.status = status;
    }
  }

  const deal = state.deals.find((item) => item.deal_id === state.selectedDealId);
  if (deal) {
    for (const period of deal.periods || []) {
      if (period.package_id === periodId) {
        period.status = status;
      }
    }
  }
}

async function loadDeals() {
  const payload = await api("/internal/v1/deals");
  state.deals = payload.deals || [];

  if (state.deals.length === 0) {
    state.selectedDealId = null;
    state.activePeriodId = null;
    state.selectedTraceId = null;
    state.selectedRow = null;
    renderLeftPanel();
    renderMiddlePanel();
    renderRightPanel();
    return;
  }

  const existing = state.deals.find((deal) => deal.deal_id === state.selectedDealId);
  state.selectedDealId = existing ? existing.deal_id : state.deals[0].deal_id;
  await loadDeal(state.selectedDealId);
}

async function loadDeal(dealId) {
  const payload = await api(`/internal/v1/deals/${encodeURIComponent(dealId)}/periods`);
  const periods = payload.periods || [];
  state.periodsByDeal.set(dealId, periods);

  await Promise.all(
    periods.map(async (period) => {
      const delta = await api(
        `/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(period.package_id)}/delta`
      );
      state.deltaByPeriod.set(period.package_id, delta);
    })
  );

  renderLeftPanel();
  renderMiddlePanel();

  if (!periods.length) {
    state.activePeriodId = null;
    state.selectedTraceId = null;
    state.selectedRow = null;
    renderRightPanel();
    return;
  }

  const currentStillExists = periods.some((period) => period.package_id === state.activePeriodId);
  const nextPeriodId = currentStillExists ? state.activePeriodId : periods[0].package_id;
  await setActivePeriod(nextPeriodId, { preserveSelection: currentStillExists });

  if (!currentStillExists) {
    scrollToPeriod(nextPeriodId);
  }
}

async function setActivePeriod(periodId, { preserveSelection = false } = {}) {
  state.activePeriodId = periodId;

  if (!preserveSelection) {
    state.selectedTraceId = null;
    state.selectedRow = null;
  }

  const [packageData, eventsData] = await Promise.all([
    api(`/internal/v1/packages/${encodeURIComponent(periodId)}?include_manifest=true`).catch(() => ({})),
    api(`/internal/v1/packages/${encodeURIComponent(periodId)}/events?limit=500`).catch(() => ({ events: [] })),
  ]);

  state.packageManifestByPeriod.set(periodId, packageData.package_manifest || null);
  state.packageEventsByPeriod.set(periodId, eventsData.events || []);

  renderLeftPanel();
  renderMiddlePanel();
  renderRightPanel();
}

function renderLeftPanel() {
  if (!state.deals.length) {
    elements.dealTree.innerHTML = emptyStateHtml("No deals", "Submit package manifests to populate this list.");
    return;
  }

  const periods = getPeriodsForSelectedDeal();
  const periodSet = new Set(periods.map((p) => p.package_id));

  const html = state.deals
    .map((deal) => {
      const isActiveDeal = deal.deal_id === state.selectedDealId;
      const activeClass = isActiveDeal ? "deal-item active" : "deal-item";
      const periodItems = (deal.periods || [])
        .filter((period) => !isActiveDeal || periodSet.has(period.package_id))
        .map((period) => {
          const isActivePeriod = period.package_id === state.activePeriodId;
          return `
            <button class="period-link ${isActivePeriod ? "active" : ""}" data-period-id="${escapeHtml(period.package_id)}">
              ${escapeHtml(formatDate(period.period_end_date))}
            </button>
          `;
        })
        .join("");

      return `
        <div class="${activeClass}" data-deal-id="${escapeHtml(deal.deal_id)}">
          <button class="deal-head" data-deal-id="${escapeHtml(deal.deal_id)}">${escapeHtml(deal.deal_id)}</button>
          ${isActiveDeal ? periodItems : ""}
        </div>
      `;
    })
    .join("");

  elements.dealTree.innerHTML = html;

  for (const button of elements.dealTree.querySelectorAll(".deal-head")) {
    button.addEventListener("click", async () => {
      const dealId = button.getAttribute("data-deal-id");
      if (!dealId || dealId === state.selectedDealId) return;
      state.selectedDealId = dealId;
      await loadDeal(dealId);
    });
  }

  for (const button of elements.dealTree.querySelectorAll(".period-link")) {
    button.addEventListener("click", async () => {
      const periodId = button.getAttribute("data-period-id");
      if (!periodId) return;
      await setActivePeriod(periodId);
      scrollToPeriod(periodId);
    });
  }
}

function renderMiddlePanel() {
  const periods = getPeriodsForSelectedDeal();
  if (!periods.length) {
    elements.periodsContainer.innerHTML = emptyStateHtml("No periods", "A deal appears here after package ingestion and processing.");
    return;
  }

  const html = periods
    .map((period) => {
      const rows = sortedRows(getRowsForPeriod(period.package_id));
      const tableRows = rows
        .map(({ row, rank }, index) => {
          const status = normalizeStatus(row.status);
          const selected = state.selectedTraceId && state.selectedTraceId === row.trace_id;
          return `
            <tr class="delta-row ${selected ? "selected" : ""}" data-period-id="${escapeHtml(period.package_id)}" data-trace-id="${escapeHtml(row.trace_id || "")}">
              <td>${index + 1}</td>
              <td>${escapeHtml(row.concept_id || "")}</td>
              <td>${escapeHtml(formatValue(row.current_value ?? row.normalized_value))}</td>
              <td>${escapeHtml(formatValue(row.abs_delta ?? "N/A"))}</td>
              <td><span class="status-pill status-${escapeHtml(status)}">${escapeHtml(status)}</span></td>
              <td>${escapeHtml((Number(row.confidence || 0)).toFixed(2))}</td>
              <td class="materiality-high">${rank.score.toFixed(3)}</td>
            </tr>
          `;
        })
        .join("");

      return `
        <section class="period-section" data-period-id="${escapeHtml(period.package_id)}">
          <div class="period-header">
            <div class="period-title">Period ${escapeHtml(formatDate(period.period_end_date))}</div>
            <div class="period-meta">${escapeHtml(period.status || "received")}</div>
          </div>
          <table class="delta-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Concept</th>
                <th>Current</th>
                <th>Abs Delta</th>
                <th>Status</th>
                <th>Conf.</th>
                <th>Materiality</th>
              </tr>
            </thead>
            <tbody>
              ${tableRows || `<tr><td colspan="7" class="inline-note">No rows in this period yet.</td></tr>`}
            </tbody>
          </table>
        </section>
      `;
    })
    .join("");

  elements.periodsContainer.innerHTML = html;

  for (const rowEl of elements.periodsContainer.querySelectorAll(".delta-row")) {
    rowEl.addEventListener("click", async () => {
      const traceId = rowEl.getAttribute("data-trace-id");
      const periodId = rowEl.getAttribute("data-period-id");
      if (!traceId || !periodId) return;
      await onRowSelected(periodId, traceId);
    });
  }

  attachPeriodObserver();
}

function scrollToPeriod(periodId) {
  const section = elements.periodsContainer.querySelector(`[data-period-id="${CSS.escape(periodId)}"]`);
  if (section) {
    section.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function attachPeriodObserver() {
  if (periodObserver) {
    periodObserver.disconnect();
  }

  periodObserver = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];

      if (!visible || visible.intersectionRatio < 0.6) {
        return;
      }

      const periodId = visible.target.getAttribute("data-period-id");
      if (!periodId || periodId === state.activePeriodId) {
        return;
      }

      setActivePeriod(periodId, { preserveSelection: false }).catch((error) => {
        state.toast = `Failed to switch period: ${error.message}`;
        renderRightPanel();
      });
    },
    {
      root: elements.periodsContainer,
      threshold: [0.6, 0.8],
    }
  );

  for (const section of elements.periodsContainer.querySelectorAll(".period-section")) {
    periodObserver.observe(section);
  }
}

function findRowByTrace(periodId, traceId) {
  const rows = getRowsForPeriod(periodId);
  for (const row of rows) {
    if (row.trace_id === traceId) return row;
  }
  return null;
}

async function onRowSelected(periodId, traceId) {
  if (state.activePeriodId !== periodId) {
    await setActivePeriod(periodId);
  }

  const localRow = findRowByTrace(periodId, traceId);
  if (!localRow) {
    return;
  }

  state.selectedTraceId = traceId;
  state.selectedRow = localRow;
  const [traceEvents, traceEvidence] = await Promise.all([
    api(`/internal/v1/traces/${encodeURIComponent(traceId)}/events?limit=500`).catch(() => ({ events: [] })),
    api(`/internal/v1/traces/${encodeURIComponent(traceId)}/evidence`).catch(() => ({ evidence_preview: null })),
  ]);
  state.traceEventsById.set(traceId, traceEvents.events || []);
  state.traceEvidenceById.set(traceId, traceEvidence.evidence_preview || null);

  renderMiddlePanel();
  renderRightPanel();
}

function docsForActivePeriod() {
  if (!state.activePeriodId) return [];

  const manifest = state.packageManifestByPeriod.get(state.activePeriodId);
  const rows = getRowsForPeriod(state.activePeriodId);
  const references = new Map();

  for (const row of rows) {
    const docId = row.evidence?.doc_id || "unknown_doc";
    if (!references.has(docId)) {
      references.set(docId, { doc_id: docId, row_count: 0, row_status_counts: {} });
    }
    const current = references.get(docId);
    current.row_count += 1;
    const status = normalizeStatus(row.status);
    current.row_status_counts[status] = (current.row_status_counts[status] || 0) + 1;
  }

  const files = manifest?.files || [];
  if (!files.length && !references.size) return [];

  const fileMap = new Map(files.map((file) => [file.file_id, file]));
  const docs = [];

  for (const [docId, ref] of references.entries()) {
    const file = fileMap.get(docId) || {};
    docs.push({
      doc_id: docId,
      filename: file.filename || docId,
      doc_type: file.doc_type || "DOC",
      pages_or_sheets: file.pages_or_sheets || null,
      storage_uri: file.storage_uri || "",
      row_count: ref.row_count,
      row_status_counts: ref.row_status_counts,
    });
  }

  for (const file of files) {
    if (references.has(file.file_id)) continue;
    docs.push({
      doc_id: file.file_id,
      filename: file.filename,
      doc_type: file.doc_type,
      pages_or_sheets: file.pages_or_sheets,
      storage_uri: file.storage_uri,
      row_count: 0,
      row_status_counts: {},
    });
  }

  return docs;
}

function renderDocGrid(docs) {
  if (!docs.length) {
    return emptyStateHtml("No evidence files", "Documents appear after rows are processed.");
  }

  return `
    <div class="doc-grid">
      ${docs
        .map((doc) => {
          const statuses = Object.entries(doc.row_status_counts)
            .map(([status, count]) => `${status}:${count}`)
            .join(" | ");

          return `
            <article class="doc-card">
              <div class="doc-name">${escapeHtml(doc.filename)}</div>
              <div class="doc-meta">${escapeHtml(doc.doc_type)}${doc.pages_or_sheets ? ` • ${escapeHtml(doc.pages_or_sheets)} pages/sheets` : ""}</div>
              <div class="doc-meta">Rows: ${escapeHtml(doc.row_count)}</div>
              <div class="doc-meta">${escapeHtml(statuses || "No linked rows")}</div>
            </article>
          `;
        })
        .join("")}
    </div>
  `;
}

function colLettersToIndex(text) {
  let result = 0;
  const letters = text.toUpperCase();
  for (let i = 0; i < letters.length; i += 1) {
    result = result * 26 + (letters.charCodeAt(i) - 64);
  }
  return result - 1;
}

function buildSheetGridFromPreview(preview) {
  if (!preview || preview.kind !== "xlsx_grid" || !Array.isArray(preview.rows) || !preview.rows.length) {
    return '<p class="inline-note">No XLSX grid preview available for this locator.</p>';
  }

  const bodyRows = preview.rows
    .map((row) => {
      const cells = row
        .map(
          (cell) =>
            `<td class="${cell.highlight ? "highlight" : ""}" title="${escapeHtml(cell.coordinate)}">${escapeHtml(
              cell.value === null || cell.value === undefined ? "" : String(cell.value)
            )}</td>`
        )
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");

  return `
    <div class="sheet-view">
      <table class="sheet-grid">
        <tbody>${bodyRows}</tbody>
      </table>
    </div>
  `;
}

function buildPdfPreview(preview) {
  if (!preview || preview.kind !== "pdf_text") {
    return '<p class="inline-note">No PDF text preview available for this locator.</p>';
  }
  const page = preview.page || "?";
  return `
    <div class="evidence-highlight">
      <div><strong>Page:</strong> ${escapeHtml(String(page))}</div>
      <div class="inline-note" style="margin-top:8px; white-space: pre-wrap;">${escapeHtml(preview.text || "")}</div>
    </div>
  `;
}

function candidateOptionsForRow(row) {
  const docs = docsForActivePeriod();
  const options = [];
  const evidence = row.evidence || {};

  if (evidence.doc_id || evidence.locator_value) {
    options.push({
      title: "Model candidate",
      evidence: {
        doc_id: evidence.doc_id || "",
        locator_type: evidence.locator_type || "paragraph",
        locator_value: evidence.locator_value || "",
      },
    });
  }

  for (const doc of docs) {
    options.push({
      title: `Review ${doc.filename}`,
      evidence: {
        doc_id: doc.doc_id,
        locator_type: "paragraph",
        locator_value: "manual_review",
      },
    });
  }

  const unique = new Map();
  for (const option of options) {
    const key = `${option.evidence.doc_id}|${option.evidence.locator_type}|${option.evidence.locator_value}`;
    if (!unique.has(key)) {
      unique.set(key, option);
    }
  }
  return [...unique.values()].slice(0, 8);
}

function renderFocusedEvidence(row) {
  const evidence = row.evidence || {};
  const docs = docsForActivePeriod();
  const doc = docs.find((item) => item.doc_id === evidence.doc_id);
  const docName = doc?.filename || evidence.doc_id || "Unknown source";
  const docType = (doc?.doc_type || "DOC").toUpperCase();
  const confidence = Number(row.confidence || 0).toFixed(2);
  const evidencePreview = state.traceEvidenceById.get(row.trace_id) || null;
  const previewPayload = evidencePreview?.preview || null;

  let previewBlock = '<p class="inline-note">No resolved evidence preview available.</p>';
  if (evidencePreview && evidencePreview.available === false) {
    previewBlock = `<p class="inline-note">Evidence preview unavailable: ${escapeHtml(evidencePreview.reason || "unknown_reason")}.</p>`;
  } else if (docType === "XLSX") {
    previewBlock = buildSheetGridFromPreview(previewPayload);
  } else if (docType === "PDF") {
    previewBlock = buildPdfPreview(previewPayload);
  }

  const core = `
    <div class="evidence-focus">
      <div class="evidence-head">
        <p class="evidence-title">${escapeHtml(row.concept_id || "concept")}</p>
        <p class="evidence-sub">status: ${escapeHtml(row.status || "unresolved")} | confidence: ${escapeHtml(confidence)} | trace: ${escapeHtml(row.trace_id || "")}</p>
      </div>
      <div class="evidence-highlight">
        <div><strong>Document:</strong> ${escapeHtml(docName)} (${escapeHtml(docType)})</div>
        <div><strong>Locator:</strong> ${escapeHtml(evidence.locator_type || "unknown")} = ${escapeHtml(evidence.locator_value || "")}</div>
        <div><strong>Current value:</strong> ${escapeHtml(formatValue(row.current_value ?? row.normalized_value))}</div>
      </div>
      ${previewBlock}
    </div>
  `;

  if (row.status === "verified") {
    return core;
  }

  const candidateButtons = candidateOptionsForRow(row)
    .map(
      (option, index) => `
      <button class="candidate-btn" type="button" data-candidate-index="${index}">
        <div class="candidate-name">${escapeHtml(option.title)}</div>
        <div class="candidate-meta">${escapeHtml(option.evidence.doc_id || "unknown_doc")}</div>
        <div class="candidate-meta">${escapeHtml(option.evidence.locator_type)}=${escapeHtml(option.evidence.locator_value)}</div>
      </button>
    `
    )
    .join("");

  return `${core}
    <div class="resolve-area">
      <p class="resolve-title">Resolve candidate to verified</p>
      <div class="candidate-grid">${candidateButtons}</div>
    </div>
  `;
}

function renderLogs(events, highlightedTraceId = null) {
  if (!events.length) {
    return emptyStateHtml("No log events", "Workflow events appear after package processing.");
  }

  return events
    .map((event) => {
      const isTraceHit = highlightedTraceId && event.trace_id === highlightedTraceId;
      return `
        <article class="log-item ${isTraceHit ? "trace-hit" : ""}" data-trace-id="${escapeHtml(event.trace_id || "")}">
          <div class="log-title">#${escapeHtml(event.sequence_id || "?")} ${escapeHtml(event.event_type || "event")}</div>
          <div class="log-line">phase: ${escapeHtml(event.phase || "")}</div>
          <div class="log-line">trace: ${escapeHtml(event.trace_id || "n/a")}</div>
          <div class="log-line">time: ${escapeHtml(event.timestamp || "")}</div>
        </article>
      `;
    })
    .join("");
}

async function resolveSelectedRow(optionIndex) {
  if (!state.selectedRow || state.selectedRow.status === "verified") {
    return;
  }

  const options = candidateOptionsForRow(state.selectedRow);
  const candidate = options[optionIndex];
  if (!candidate) {
    return;
  }

  const traceId = state.selectedRow.trace_id;
  const response = await api(`/internal/v1/traces/${encodeURIComponent(traceId)}:resolve`, {
    method: "POST",
    body: JSON.stringify({
      resolver: "operator_ui",
      selected_evidence: candidate.evidence,
      note: "Resolved from desktop shell",
    }),
  });

  state.toast = `Row ${traceId} resolved to verified.`;

  const delta = await api(
    `/internal/v1/deals/${encodeURIComponent(state.selectedDealId)}/periods/${encodeURIComponent(state.activePeriodId)}/delta`
  );
  state.deltaByPeriod.set(state.activePeriodId, delta);
  updatePeriodStatus(state.activePeriodId, response.package_status || "completed");

  const pkgEvents = await api(`/internal/v1/packages/${encodeURIComponent(state.activePeriodId)}/events?limit=500`).catch(() => ({ events: [] }));
  state.packageEventsByPeriod.set(state.activePeriodId, pkgEvents.events || []);

  const traceEvents = await api(`/internal/v1/traces/${encodeURIComponent(traceId)}/events?limit=500`).catch(() => ({ events: [] }));
  const traceEvidence = await api(`/internal/v1/traces/${encodeURIComponent(traceId)}/evidence`).catch(() => ({ evidence_preview: null }));
  state.traceEventsById.set(traceId, traceEvents.events || []);
  state.traceEvidenceById.set(traceId, traceEvidence.evidence_preview || null);

  state.selectedTraceId = traceId;
  state.selectedRow = findRowByTrace(state.activePeriodId, traceId);

  renderLeftPanel();
  renderMiddlePanel();
  renderRightPanel();
}

async function jumpToTrace(traceId) {
  if (!traceId) return;

  try {
    const trace = await api(`/internal/v1/traces/${encodeURIComponent(traceId)}`);

    if (trace.deal_id !== state.selectedDealId) {
      state.selectedDealId = trace.deal_id;
      await loadDeal(trace.deal_id);
    }

    if (trace.period_id !== state.activePeriodId) {
      await setActivePeriod(trace.period_id);
      scrollToPeriod(trace.period_id);
    }

    await onRowSelected(trace.period_id, trace.trace_id);
  } catch (error) {
    state.toast = `Unable to jump to trace ${traceId}: ${error.message}`;
    renderRightPanel();
  }
}

function attachLogHandlers() {
  for (const logItem of elements.logsContent.querySelectorAll(".log-item")) {
    logItem.addEventListener("click", async () => {
      const traceId = logItem.getAttribute("data-trace-id");
      if (!traceId) return;
      await jumpToTrace(traceId);
    });
  }
}

function attachCandidateHandlers() {
  for (const button of elements.evidenceContent.querySelectorAll(".candidate-btn")) {
    button.addEventListener("click", async () => {
      const idx = Number(button.getAttribute("data-candidate-index"));
      if (!Number.isFinite(idx)) return;
      await resolveSelectedRow(idx);
    });
  }
}

function renderRightPanel() {
  if (!state.activePeriodId) {
    elements.evidenceContent.innerHTML = emptyStateHtml("No active period", "Select a period to inspect evidence.");
    elements.logsContent.innerHTML = emptyStateHtml("No logs", "Log stream appears once processing starts.");
    return;
  }

  const docs = docsForActivePeriod();
  const packageEvents = state.packageEventsByPeriod.get(state.activePeriodId) || [];

  const toast = state.toast
    ? `<div class="toast">${escapeHtml(state.toast)}</div>`
    : "";
  state.toast = "";

  if (!state.selectedRow) {
    elements.evidenceContent.innerHTML = `${toast}${renderDocGrid(docs)}`;
    elements.logsContent.innerHTML = renderLogs(packageEvents);
    attachLogHandlers();
    return;
  }

  elements.evidenceContent.innerHTML = `${toast}${renderFocusedEvidence(state.selectedRow)}`;
  const traceEvents = state.traceEventsById.get(state.selectedTraceId) || [];
  elements.logsContent.innerHTML = renderLogs(traceEvents, state.selectedTraceId);

  attachCandidateHandlers();
  attachLogHandlers();
}

async function start() {
  elements.refreshButton.addEventListener("click", async () => {
    await loadDeals();
  });

  try {
    await loadDeals();
  } catch (error) {
    elements.dealTree.innerHTML = emptyStateHtml("Failed to load", `Error: ${error.message}`);
    elements.periodsContainer.innerHTML = emptyStateHtml("Unavailable", "Delta panel unavailable until API is reachable.");
    elements.evidenceContent.innerHTML = emptyStateHtml("Unavailable", "Evidence panel unavailable until API is reachable.");
    elements.logsContent.innerHTML = emptyStateHtml("Unavailable", "Logs panel unavailable until API is reachable.");
  }
}

start();
