const workspaceFromQuery = new URLSearchParams(window.location.search).get("workspace");
const workspaceId = workspaceFromQuery || window.localStorage.getItem("workspace_id") || "ws_default";
const persistedProofMode = String(window.localStorage.getItem("proof_mode") || "evidence").toLowerCase();
if (workspaceFromQuery) {
  window.localStorage.setItem("workspace_id", workspaceFromQuery);
}

const state = {
  workspaceId,
  sessionToken: window.localStorage.getItem("session_token") || "",
  user: null,
  deals: [],
  selectedDealId: null,
  periodsByDeal: new Map(),
  deltaByPeriod: new Map(),
  takeawaysByPeriod: new Map(),
  takeawaysErrorByPeriod: new Map(),
  takeawaysLoadingByPeriod: new Map(),
  packageManifestByPeriod: new Map(),
  packageEventsByPeriod: new Map(),
  traceEventsById: new Map(),
  traceEvidenceById: new Map(),
  xlsxWorkbookByTrace: new Map(),
  activeSheetByTrace: new Map(),
  activePeriodId: null,
  selectedTraceId: null,
  selectedRow: null,
  selectedWhatId: null,
  selectedTaskId: null,
  periodPickerOpenDealId: null,
  createPaneMode: "workspace",
  createDealBusy: false,
  createDealError: "",
  createDealMessage: "",
  createDraftName: "",
  createTemplateId: "tpl_fixed_starter_v1",
  createConceptDrafts: [],
  createResult: null,
  onboardingTasksByDeal: new Map(),
  proofMode: persistedProofMode === "audit" ? "audit" : "evidence",
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

const createTemplateOptions = [
  {
    templateId: "tpl_fixed_starter_v1",
    label: "Starter extraction variables (V1)",
  },
];

const createConceptDefaults = [
  { concept_id: "revenue_total", label: "Revenue (Total)" },
  { concept_id: "ebitda_reported", label: "EBITDA (Reported)" },
  { concept_id: "ebitda_adjusted", label: "EBITDA (Adjusted)" },
  { concept_id: "operating_income_ebit", label: "Operating Income (EBIT)" },
  { concept_id: "interest_expense", label: "Interest Expense" },
  { concept_id: "net_income", label: "Net Income" },
  { concept_id: "cash_and_equivalents", label: "Cash and Equivalents" },
  { concept_id: "accounts_receivable_total", label: "Accounts Receivable (Total)" },
  { concept_id: "inventory_total", label: "Inventory (Total)" },
  { concept_id: "accounts_payable_total", label: "Accounts Payable (Total)" },
  { concept_id: "total_debt", label: "Total Debt" },
  { concept_id: "total_assets", label: "Total Assets" },
  { concept_id: "total_liabilities", label: "Total Liabilities" },
];

const elements = {
  dealTree: document.getElementById("deal-tree"),
  whatContent: document.getElementById("what-content"),
  tasksContent: document.getElementById("tasks-content"),
  dataContent: document.getElementById("data-content"),
  proofCitations: document.getElementById("proof-citations"),
  evidenceContent: document.getElementById("evidence-content"),
  logsContent: document.getElementById("logs-content"),
  proofQualityChip: document.getElementById("proof-quality-chip"),
  proofViewerStack: document.getElementById("proof-viewer-stack"),
  proofModeEvidence: document.getElementById("proof-mode-evidence"),
  proofModeAudit: document.getElementById("proof-mode-audit"),
  refreshButton: document.getElementById("refresh-button"),
  logoutButton: document.getElementById("logout-button"),
  userPill: document.getElementById("user-pill"),
  portfolioUpdatedLabel: document.getElementById("portfolio-updated-label"),
  portfolioCoverageLabel: document.getElementById("portfolio-coverage-label"),
  createDealButton: document.getElementById("create-deal-button"),
  createDealStage: document.getElementById("create-deal-stage"),
  createDealStageContent: document.getElementById("create-deal-stage-content"),
  mainGrid: document.getElementById("main-grid"),
  authOverlay: document.getElementById("auth-overlay"),
  authCard: document.getElementById("auth-card"),
};

function defaultCreateConceptDrafts() {
  return createConceptDefaults.map((entry) => ({
    conceptId: entry.concept_id,
    label: entry.label,
  }));
}

state.createConceptDrafts = defaultCreateConceptDrafts();

function setProofMode(mode) {
  const normalized = mode === "audit" ? "audit" : "evidence";
  state.proofMode = normalized;
  window.localStorage.setItem("proof_mode", normalized);
  renderProofPanel();
}

function renderProofModeToggle() {
  const evidenceActive = state.proofMode !== "audit";
  if (elements.proofModeEvidence) {
    elements.proofModeEvidence.classList.toggle("active", evidenceActive);
    elements.proofModeEvidence.setAttribute("aria-selected", evidenceActive ? "true" : "false");
    elements.proofModeEvidence.setAttribute("aria-pressed", evidenceActive ? "true" : "false");
  }
  if (elements.proofModeAudit) {
    elements.proofModeAudit.classList.toggle("active", !evidenceActive);
    elements.proofModeAudit.setAttribute("aria-selected", evidenceActive ? "false" : "true");
    elements.proofModeAudit.setAttribute("aria-pressed", evidenceActive ? "false" : "true");
  }
}

async function api(path, options = {}) {
  const requestHeaders = {
    "Content-Type": "application/json",
    "X-Workspace-Id": state.workspaceId,
    ...(options.headers || {}),
  };
  if (state.sessionToken && !requestHeaders.Authorization) {
    requestHeaders.Authorization = `Bearer ${state.sessionToken}`;
  }

  const response = await fetch(path, {
    ...options,
    headers: requestHeaders,
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

async function authApi(path, options = {}) {
  const requestHeaders = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (state.sessionToken && !requestHeaders.Authorization) {
    requestHeaders.Authorization = `Bearer ${state.sessionToken}`;
  }

  const response = await fetch(path, {
    ...options,
    headers: requestHeaders,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `request_failed_${response.status}`);
  }

  if (response.status === 204) return {};
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

function shortTraceId(value) {
  const trace = String(value || "").trim();
  if (!trace) return "n/a";
  if (trace.length <= 16) return trace;
  return `${trace.slice(0, 8)}...${trace.slice(-6)}`;
}

function toTimeLabel(value) {
  const d = new Date(value || "");
  if (Number.isNaN(d.getTime())) return "updated at unavailable";
  return `updated at ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
}

function emptyStateHtml(title, copy) {
  return `<div class="empty-state"><p class="empty-title">${escapeHtml(title)}</p><p class="empty-copy">${escapeHtml(copy)}</p></div>`;
}

function normalizeApiErrorMessage(message) {
  const raw = String(message || "").trim();
  if (!raw) return "unknown_error";
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      const detail = parsed.detail;
      if (typeof detail === "string" && detail.trim()) {
        return detail.trim();
      }
    }
  } catch (_error) {
    // Fall through and return raw text.
  }
  return raw;
}

async function refreshTakeawaysForPeriod(dealId, periodId, { renderOnStart = true } = {}) {
  if (!dealId || !periodId) return;
  state.takeawaysLoadingByPeriod.set(periodId, true);
  state.takeawaysErrorByPeriod.delete(periodId);
  if (renderOnStart && state.activePeriodId === periodId) {
    renderWhatPanel();
  }

  const result = await api(
    `/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(periodId)}/takeaways`
  )
    .then((data) => ({ ok: true, data }))
    .catch((error) => ({
      ok: false,
      error: normalizeApiErrorMessage(error?.message || error),
    }));

  if (result.ok && result.data && typeof result.data === "object") {
    state.takeawaysByPeriod.set(periodId, result.data);
    state.takeawaysErrorByPeriod.delete(periodId);
  } else {
    state.takeawaysByPeriod.delete(periodId);
    state.takeawaysErrorByPeriod.set(periodId, result.error || "takeaways_fetch_failed");
  }
  state.takeawaysLoadingByPeriod.set(periodId, false);
  if (state.activePeriodId === periodId) {
    renderWhatPanel();
  }
}

function setHtmlWithFade(element, html) {
  if (!element) return;
  element.classList.remove("content-swap-in");
  element.innerHTML = html;
  window.requestAnimationFrame(() => {
    element.classList.add("content-swap-in");
  });
}

function setSession(token, user) {
  state.sessionToken = token || "";
  state.user = user || null;
  if (state.sessionToken) {
    window.localStorage.setItem("session_token", state.sessionToken);
  } else {
    window.localStorage.removeItem("session_token");
  }
  renderTopAuthState();
}

function clearSession() {
  setSession("", null);
}

function renderTopAuthState() {
  if (state.user?.email) {
    elements.userPill.textContent = `${state.user.email} · ${state.workspaceId}`;
    elements.userPill.classList.remove("hidden");
    elements.logoutButton.classList.remove("hidden");
  } else {
    elements.userPill.textContent = "";
    elements.userPill.classList.add("hidden");
    elements.logoutButton.classList.add("hidden");
  }
}

function showAuthOverlay(contentHtml) {
  elements.authCard.innerHTML = contentHtml;
  elements.authOverlay.classList.remove("hidden");
}

function hideAuthOverlay() {
  elements.authOverlay.classList.add("hidden");
}

function authErrorHtml(message) {
  if (!message) return "";
  return `<div class="auth-error">${escapeHtml(message)}</div>`;
}

function buildLoginCard(errorMessage = "") {
  const knownEmail = state.user?.email || "";
  return `
    <h2 class="auth-title">Sign In</h2>
    <p class="auth-copy">Login to access your parsed borrower packages.</p>
    <form id="login-form">
      <label class="auth-label" for="login-email">Email</label>
      <input class="auth-input" id="login-email" name="email" type="email" value="${escapeHtml(knownEmail)}" required />
      <label class="auth-label" for="login-password">Password</label>
      <input class="auth-input" id="login-password" name="password" type="password" required />
      <button class="auth-btn" type="submit">Login</button>
      ${authErrorHtml(errorMessage)}
    </form>
  `;
}

function buildPasswordSetupCard(magicToken, errorMessage = "") {
  return `
    <h2 class="auth-title">Set Password</h2>
    <p class="auth-copy">Your package was received. Set a password to activate your account.</p>
    <form id="password-setup-form" data-magic-token="${escapeHtml(magicToken)}">
      <label class="auth-label" for="setup-password">New Password</label>
      <input class="auth-input" id="setup-password" name="password" type="password" minlength="8" required />
      <button class="auth-btn" type="submit">Activate Account</button>
      ${authErrorHtml(errorMessage)}
    </form>
  `;
}

function attachLoginHandler() {
  const form = document.getElementById("login-form");
  if (!form) return;

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const email = String(formData.get("email") || "").trim();
    const password = String(formData.get("password") || "");
    try {
      const result = await authApi("/auth/v1/login", {
        method: "POST",
        body: JSON.stringify({ email, password }),
      });
      setSession(result.session_token, result.user);
      hideAuthOverlay();
      await loadDeals();
    } catch (error) {
      showAuthOverlay(buildLoginCard(error.message));
      attachLoginHandler();
    }
  });
}

function attachPasswordSetupHandler() {
  const form = document.getElementById("password-setup-form");
  if (!form) return;

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const magicToken = form.getAttribute("data-magic-token") || "";
    const formData = new FormData(form);
    const password = String(formData.get("password") || "");
    try {
      const result = await authApi("/auth/v1/magic-link/consume", {
        method: "POST",
        body: JSON.stringify({ token: magicToken, password }),
      });
      setSession(result.session_token, result.user);
      hideAuthOverlay();
      const url = new URL(window.location.href);
      url.searchParams.delete("magic_token");
      window.history.replaceState({}, "", url.toString());
      await loadDeals();
    } catch (error) {
      showAuthOverlay(buildPasswordSetupCard(magicToken, error.message));
      attachPasswordSetupHandler();
    }
  });
}

function normalizeStatus(status) {
  if (!status) return "unresolved";
  return String(status);
}

function titleCaseStatus(status) {
  const normalized = normalizeStatus(status).replaceAll("_", " ");
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function docTypeBadge(docType) {
  const normalized = String(docType || "DOC").toUpperCase();
  if (normalized === "PDF") return "PDF";
  if (normalized === "XLSX") return "XLSX";
  return normalized;
}

function materialityForRow(row) {
  const priorValue = parseNumber(row.prior_value);
  const currentValue = parseNumber(row.current_value);
  const explicitAbsDelta = parseNumber(row.abs_delta);
  const explicitPctDelta = parseNumber(row.pct_delta);
  const fallbackMagnitude = parseNumber(row.normalized_value);

  const absDelta =
    explicitAbsDelta ??
    (priorValue !== null && currentValue !== null ? Math.abs(currentValue - priorValue) : Math.abs(fallbackMagnitude ?? 0));
  const pctDelta =
    explicitPctDelta ??
    (priorValue && currentValue !== null ? Math.abs(((currentValue - priorValue) / priorValue) * 100) : 0);

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

function getRankedRowsForActivePeriod() {
  if (!state.activePeriodId) return [];
  return sortedRows(getRowsForPeriod(state.activePeriodId));
}

function getActivePeriod() {
  const periods = getPeriodsForSelectedDeal();
  return periods.find((period) => period.package_id === state.activePeriodId) || periods[0] || null;
}

function sortedPeriods(periods) {
  return [...(periods || [])].sort((a, b) => String(b.period_end_date).localeCompare(String(a.period_end_date)));
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

function findRowByTrace(periodId, traceId) {
  const rows = getRowsForPeriod(periodId);
  for (const row of rows) {
    if (row.trace_id === traceId) return row;
  }
  return null;
}

function toDealDotTone(deal) {
  const periods = sortedPeriods(state.periodsByDeal.get(deal.deal_id) || deal.periods || []);
  const latest = periods[0];
  const status = normalizeStatus(latest?.status || "received");
  if (status === "failed") return "critical";
  if (status === "needs_review") return "warning";
  if (status === "processing" || status === "received") return "blocked";
  return "ok";
}

function renderWorkspace() {
  renderLeftPanel();
  renderCreateDealSidebar();
  renderCreateDealStage();
  renderWhatPanel();
  renderTasksPanel();
  renderDataPanel();
  renderProofPanel();
}

function renderCreateDealSidebar() {
  if (!elements.createDealButton) return;
  const inCreateMode = state.createPaneMode === "create_deal";
  elements.createDealButton.disabled = state.createDealBusy || inCreateMode;
  elements.createDealButton.textContent = inCreateMode ? "Create flow open" : "Create new deal";
}

function resetCreateDealDraft() {
  state.createDraftName = "";
  state.createDealError = "";
  state.createDealMessage = "";
  state.createTemplateId = createTemplateOptions[0].templateId;
  state.createConceptDrafts = defaultCreateConceptDrafts();
}

function selectedCreateConceptCount() {
  return state.createConceptDrafts.length;
}

function renderCreateDealStage() {
  if (!elements.createDealStage || !elements.createDealStageContent || !elements.mainGrid) return;
  const inCreateMode = state.createPaneMode === "create_deal";
  elements.createDealStage.classList.toggle("hidden", !inCreateMode);
  elements.mainGrid.classList.toggle("hidden", inCreateMode);
  if (!inCreateMode) {
    elements.createDealStageContent.innerHTML = "";
    return;
  }

  if (state.createResult) {
    const forwardingAddress = state.createResult.forwarding_address || "inbound@patrici.us";
    const instruction =
      String(state.createResult.quick_instruction || "").trim() ||
      `Send your first borrower package to ${forwardingAddress} to start extraction.`;
    setHtmlWithFade(
      elements.createDealStageContent,
      `
        <div class="createDealCard">
          <header class="createDealHeader">
            <h2 class="createDealTitle">Inbound email ready</h2>
            <p class="createDealSubtitle">Share this address with the deal team and send the first borrower package.</p>
          </header>
          <div class="createDealSuccess">
            <p class="createDealSuccessTitle">Deal created: ${escapeHtml(state.createResult.display_name || state.createResult.deal_id)}</p>
            <p class="createDealSuccessCopy">${escapeHtml(instruction)}</p>
            <div class="createDealSuccessEmailRow">
              <code class="createDealSuccessEmail">${escapeHtml(forwardingAddress)}</code>
              <button type="button" class="settingsButton" data-create-action="copy-email">Copy</button>
            </div>
          </div>
          <div class="createDealActions">
            <button type="button" class="settingsButton" data-create-action="done">Done</button>
          </div>
          <p class="createDealMessage ${state.createDealMessage ? "" : "hidden"}">${escapeHtml(state.createDealMessage)}</p>
          <p class="createDealError ${state.createDealError ? "" : "hidden"}">${escapeHtml(state.createDealError)}</p>
        </div>
      `
    );
    const copyButton = elements.createDealStageContent.querySelector('[data-create-action="copy-email"]');
    if (copyButton) {
      copyButton.addEventListener("click", async () => {
        const email = String(state.createResult?.forwarding_address || "").trim();
        try {
          if (!email) {
            throw new Error("Inbound email is unavailable.");
          }
          if (!navigator?.clipboard?.writeText) {
            throw new Error("Clipboard is unavailable.");
          }
          await navigator.clipboard.writeText(email);
          state.createDealMessage = "Inbound email copied.";
          state.createDealError = "";
        } catch (error) {
          state.createDealError = error instanceof Error ? error.message : "Failed to copy inbound email.";
          state.createDealMessage = "";
        }
        renderCreateDealStage();
      });
    }
    const doneButton = elements.createDealStageContent.querySelector('[data-create-action="done"]');
    if (doneButton) {
      doneButton.addEventListener("click", async () => {
        await completeCreateDealFlow();
      });
    }
    return;
  }

  const selectedCount = selectedCreateConceptCount();
  const conceptRows = state.createConceptDrafts
    .map(
      (entry) => `
      <div class="createDealConceptRow createDealConceptRowSelected">
        <span class="createDealConceptToggle">
          <span class="createDealConceptDot"></span>
          <span class="createDealConceptName">${escapeHtml(entry.label)}</span>
        </span>
        <span class="createDealConceptId">
          ${escapeHtml(entry.conceptId)}
        </span>
      </div>
    `
    )
    .join("");

  const templateOptions = createTemplateOptions
    .map(
      (option) =>
        `<option value="${escapeHtml(option.templateId)}" ${option.templateId === state.createTemplateId ? "selected" : ""}>${escapeHtml(
          option.label
        )}</option>`
    )
    .join("");

  setHtmlWithFade(
    elements.createDealStageContent,
    `
      <form class="createDealCard" id="create-deal-form">
        <header class="createDealHeader">
          <h2 class="createDealTitle">Create new deal</h2>
          <p class="createDealSubtitle">Configure the deal and start extraction with the fixed V1 variable dictionary.</p>
        </header>
        <div class="createDealFields">
          <label class="createDealLabel" for="create-deal-name-input-stage">Deal name</label>
          <input
            id="create-deal-name-input-stage"
            class="createDealInput"
            type="text"
            placeholder="e.g. Atlas Capital Unitranche"
            value="${escapeHtml(state.createDraftName)}"
            ${state.createDealBusy ? "disabled" : ""}
            autofocus
          />
        </div>
        <div class="createDealFields">
          <label class="createDealLabel" for="create-deal-template-select">Extraction profile</label>
          <select
            id="create-deal-template-select"
            class="createDealInput createDealSelect"
            ${state.createDealBusy ? "disabled" : ""}
          >
            ${templateOptions}
          </select>
        </div>
        <div class="createDealFields">
          <label class="createDealLabel">Tracked variables (fixed for V1)</label>
          <p class="createDealSubtitle">The agent extracts and maps these variables automatically with evidence links.</p>
        </div>
        <div class="createDealConceptList">${conceptRows}</div>
        <p class="createDealHint" id="create-deal-selection-count">${selectedCount} variables tracked.</p>
        <div class="createDealActions">
          <button type="button" class="settingsButton" data-create-action="cancel" ${state.createDealBusy ? "disabled" : ""}>Cancel</button>
          <button type="submit" class="settingsButton" ${state.createDealBusy ? "disabled" : ""}>${
      state.createDealBusy ? "Creating..." : "Create deal & inbound email"
    }</button>
        </div>
        <p class="createDealError ${state.createDealError ? "" : "hidden"}" id="create-deal-stage-error">${escapeHtml(
      state.createDealError
    )}</p>
      </form>
    `
  );

  const form = document.getElementById("create-deal-form");
  const cancelButton = elements.createDealStageContent.querySelector('[data-create-action="cancel"]');
  const nameInput = document.getElementById("create-deal-name-input-stage");
  const templateSelect = document.getElementById("create-deal-template-select");
  const errorBlock = document.getElementById("create-deal-stage-error");

  const clearCreateError = () => {
    state.createDealError = "";
    if (errorBlock) {
      errorBlock.textContent = "";
      errorBlock.classList.add("hidden");
    }
  };

  if (form) {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      await submitCreateDeal();
    });
  }
  if (cancelButton) {
    cancelButton.addEventListener("click", () => {
      cancelCreateDealFlow();
    });
  }
  if (nameInput) {
    nameInput.addEventListener("input", (event) => {
      state.createDraftName = String(event.target.value || "");
      clearCreateError();
    });
  }
  if (templateSelect) {
    templateSelect.addEventListener("change", (event) => {
      state.createTemplateId = String(event.target.value || createTemplateOptions[0].templateId);
      clearCreateError();
    });
  }

  if (nameInput && !state.createDraftName) {
    nameInput.focus();
  }
}

function openCreateDealFlow() {
  state.createPaneMode = "create_deal";
  state.createDealBusy = false;
  state.createResult = null;
  resetCreateDealDraft();
  renderWorkspace();
}

function cancelCreateDealFlow() {
  state.createPaneMode = "workspace";
  state.createDealBusy = false;
  resetCreateDealDraft();
  renderWorkspace();
}

async function submitCreateDeal() {
  const displayName = String(state.createDraftName || "").trim();
  if (!displayName) {
    state.createDealError = "Deal name is required.";
    renderCreateDealStage();
    return;
  }

  if (!state.createConceptDrafts.length) {
    state.createDealError = "No extraction variables configured.";
    renderCreateDealStage();
    return;
  }

  const conceptOverrides = state.createConceptDrafts.map((concept) => ({
    concept_id: concept.conceptId,
    selected: true,
  }));

  state.createDealBusy = true;
  state.createDealError = "";
  state.createDealMessage = "";
  renderCreateDealStage();

  try {
    const created = await api("/internal/v1/deals", {
      method: "POST",
      body: JSON.stringify({
        display_name: displayName,
        template_id: state.createTemplateId,
        concept_overrides: conceptOverrides,
      }),
    });
    state.createResult = created;
    state.createDealBusy = false;
    state.selectedDealId = created.deal_id;
    await loadDeals();
    renderCreateDealStage();
  } catch (error) {
    state.createDealBusy = false;
    state.createDealError = `Create deal failed: ${error.message}`;
    renderCreateDealStage();
  }
}

function buildOnboardingTask(dealId, forwardingAddress) {
  const safeAddress = String(forwardingAddress || "inbound@patrici.us").trim() || "inbound@patrici.us";
  return {
    id: `task-send-package-${dealId}`,
    traceId: "",
    title: "Send first borrower package",
    reason: `Forward PDF/XLSX package to ${safeAddress}.`,
    chip: "Agent",
  };
}

async function completeCreateDealFlow() {
  if (!state.createResult) {
    return;
  }
  const dealId = String(state.createResult.deal_id || "").trim();
  if (!dealId) {
    state.createDealError = "Created deal ID is missing.";
    renderCreateDealStage();
    return;
  }

  state.onboardingTasksByDeal.set(dealId, buildOnboardingTask(dealId, state.createResult.forwarding_address));
  state.selectedDealId = dealId;
  state.selectedTaskId = `task-send-package-${dealId}`;
  state.activePeriodId = null;
  state.selectedTraceId = null;
  state.selectedRow = null;
  state.selectedWhatId = null;
  state.periodPickerOpenDealId = null;
  state.toast = `Deal ${state.createResult.display_name || dealId} created. Send package to start extraction.`;

  state.createPaneMode = "workspace";
  state.createDealBusy = false;
  state.createResult = null;
  resetCreateDealDraft();
  await loadDeals();
}

async function loadDeals() {
  const payload = await api("/internal/v1/deals");
  state.deals = payload.deals || [];

  if (state.deals.length === 0) {
    state.selectedDealId = null;
    state.activePeriodId = null;
    state.selectedTraceId = null;
    state.selectedRow = null;
    state.selectedWhatId = null;
    state.selectedTaskId = null;
    state.periodPickerOpenDealId = null;
    renderWorkspace();
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
      const [delta, packageData] = await Promise.all([
        api(`/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(period.package_id)}/delta`),
        api(`/internal/v1/packages/${encodeURIComponent(period.package_id)}?include_manifest=true`).catch(() => ({})),
      ]);
      state.deltaByPeriod.set(period.package_id, delta);
      state.packageManifestByPeriod.set(period.package_id, packageData.package_manifest || null);
    })
  );

  if (!periods.length) {
    state.activePeriodId = null;
    state.selectedTraceId = null;
    state.selectedRow = null;
    state.selectedWhatId = null;
    const onboardingTask = state.onboardingTasksByDeal.get(dealId);
    state.selectedTaskId = onboardingTask ? onboardingTask.id : null;
    renderWorkspace();
    return;
  }

  const periodList = sortedPeriods(periods);
  const currentStillExists = periodList.some((period) => period.package_id === state.activePeriodId);
  const nextPeriodId = currentStillExists ? state.activePeriodId : periodList[0].package_id;
  await setActivePeriod(nextPeriodId, { preserveSelection: currentStillExists });
}

async function setActivePeriod(periodId, { preserveSelection = false } = {}) {
  state.activePeriodId = periodId;

  if (!preserveSelection) {
    state.selectedTraceId = null;
    state.selectedRow = null;
    state.selectedWhatId = null;
    state.selectedTaskId = null;
  }

  const activeDealId = state.selectedDealId;
  void refreshTakeawaysForPeriod(activeDealId, periodId, { renderOnStart: true });
  renderWorkspace();

  const [packageData, eventsData] = await Promise.all([
    api(`/internal/v1/packages/${encodeURIComponent(periodId)}?include_manifest=true`).catch(() => ({})),
    api(`/internal/v1/packages/${encodeURIComponent(periodId)}/events?limit=500`).catch(() => ({ events: [] })),
  ]);

  state.packageManifestByPeriod.set(periodId, packageData.package_manifest || null);
  state.packageEventsByPeriod.set(periodId, eventsData.events || []);

  if (preserveSelection && state.selectedTraceId) {
    const found = findRowByTrace(periodId, state.selectedTraceId);
    if (!found) {
      state.selectedTraceId = null;
      state.selectedRow = null;
      state.selectedWhatId = null;
      state.selectedTaskId = null;
    } else {
      state.selectedRow = found;
    }
  }

  renderWorkspace();
}

async function onRowSelected(periodId, traceId, options = {}) {
  if (!periodId || !traceId) return;

  if (state.activePeriodId !== periodId) {
    await setActivePeriod(periodId);
  }

  const localRow = findRowByTrace(periodId, traceId);
  if (!localRow) {
    return;
  }

  state.selectedTraceId = traceId;
  state.selectedRow = localRow;
  state.selectedWhatId = traceId;
  state.selectedTaskId = traceId;

  const [traceEvents, traceEvidence] = await Promise.all([
    api(`/internal/v1/traces/${encodeURIComponent(traceId)}/events?limit=500`).catch(() => ({ events: [] })),
    api(`/internal/v1/traces/${encodeURIComponent(traceId)}/evidence`).catch(() => ({ evidence_preview: null })),
  ]);
  state.traceEventsById.set(traceId, traceEvents.events || []);
  const evidencePreview = traceEvidence.evidence_preview || null;
  state.traceEvidenceById.set(traceId, evidencePreview);

  if (evidencePreview?.doc_type === "XLSX" && evidencePreview?.download_url) {
    await ensureWorkbookLoaded(traceId, evidencePreview);
  }

  if (!options.keepPickerOpen) {
    state.periodPickerOpenDealId = null;
  }

  renderWorkspace();
}

function renderLeftPanel() {
  if (!state.deals.length) {
    elements.dealTree.innerHTML = emptyStateHtml("No deals", "Submit package manifests to populate this list.");
    elements.portfolioUpdatedLabel.textContent = "updated at unavailable";
    elements.portfolioCoverageLabel.textContent = "Deals 0/0 · Blockers 0";
    return;
  }

  const blockers = state.deals.filter((deal) => {
    const periods = sortedPeriods(state.periodsByDeal.get(deal.deal_id) || deal.periods || []);
    const latest = periods[0];
    const status = normalizeStatus(latest?.status || "received");
    return status === "needs_review" || status === "failed" || status === "processing";
  }).length;

  const latestUpdated = state.deals
    .map((deal) => {
      const periods = sortedPeriods(state.periodsByDeal.get(deal.deal_id) || deal.periods || []);
      return periods[0]?.created_at || periods[0]?.period_end_date || null;
    })
    .find(Boolean);

  elements.portfolioUpdatedLabel.textContent = toTimeLabel(latestUpdated);
  elements.portfolioCoverageLabel.textContent = `Deals ${state.deals.length}/${state.deals.length} · Blockers ${blockers}`;

  const html = state.deals
    .map((deal) => {
      const selected = deal.deal_id === state.selectedDealId;
      const periods = sortedPeriods(state.periodsByDeal.get(deal.deal_id) || deal.periods || []);
      const tone = toDealDotTone(deal);
      const activePeriod = selected
        ? periods.find((period) => period.package_id === state.activePeriodId) || periods[0] || null
        : periods[0] || null;
      const periodLabel = activePeriod ? formatDate(activePeriod.period_end_date) : "No period";
      const pickerOpen = selected && state.periodPickerOpenDealId === deal.deal_id;
      const oldestPeriodId = periods.length ? periods[periods.length - 1].package_id : null;

      const pickerHtml = selected
        ? `
          <div class="periodPickerBlock">
            <button class="periodPickerTrigger" type="button" data-period-toggle-deal-id="${escapeHtml(deal.deal_id)}">
              ${escapeHtml(periodLabel)}
            </button>
            <div class="periodPickerMenu ${pickerOpen ? "" : "hidden"}">
              ${
                periods.length
                  ? periods
                      .map((period) => {
                        const isSelected = period.package_id === state.activePeriodId;
                        const isBaseline = period.package_id === oldestPeriodId;
                        const label = `${formatDate(period.period_end_date)}${isBaseline ? " · baseline" : ""}`;
                        return `<button class="periodPickerOption ${isSelected ? "periodPickerOptionSelected" : ""}" type="button" data-period-id="${escapeHtml(
                          period.package_id
                        )}" data-period-deal-id="${escapeHtml(deal.deal_id)}">${escapeHtml(label)}</button>`;
                      })
                      .join("")
                  : `<p class="empty-state">No periods available.</p>`
              }
            </div>
          </div>
        `
        : "";

      return `
        <div class="dealListItem ${selected ? "dealListItemSelected" : ""}">
          <button class="dealOpenButton" type="button" data-deal-id="${escapeHtml(deal.deal_id)}">
            <span class="dealListRow">
              <span class="dealStatusDot ${
                tone === "critical"
                  ? "dealStatusDotCritical"
                  : tone === "warning"
                    ? "dealStatusDotWarning"
                    : tone === "blocked"
                      ? "dealStatusDotBlocked"
                      : "dealStatusDotOk"
              }"></span>
              <span class="dealListName">${escapeHtml(deal.display_name || deal.deal_id)}</span>
            </span>
            <span class="dealListMeta">${escapeHtml(periods.length ? `${periods.length} periods` : "No periods")}</span>
            <span class="dealListMeta">${escapeHtml(activePeriod ? `Latest ${formatDate(activePeriod.period_end_date)}` : "Waiting for package")}</span>
          </button>
          ${pickerHtml}
        </div>
      `;
    })
    .join("");

  elements.dealTree.innerHTML = html;

  for (const button of elements.dealTree.querySelectorAll(".dealOpenButton")) {
    button.addEventListener("click", async () => {
      const dealId = button.getAttribute("data-deal-id");
      if (!dealId || dealId === state.selectedDealId) return;
      state.selectedDealId = dealId;
      state.periodPickerOpenDealId = null;
      await loadDeal(dealId);
    });
  }

  for (const button of elements.dealTree.querySelectorAll(".periodPickerTrigger")) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const dealId = button.getAttribute("data-period-toggle-deal-id");
      if (!dealId) return;
      state.periodPickerOpenDealId = state.periodPickerOpenDealId === dealId ? null : dealId;
      renderLeftPanel();
    });
  }

  for (const button of elements.dealTree.querySelectorAll(".periodPickerOption")) {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      const periodId = button.getAttribute("data-period-id");
      const dealId = button.getAttribute("data-period-deal-id");
      if (!periodId || !dealId) return;

      if (dealId !== state.selectedDealId) {
        state.selectedDealId = dealId;
        await loadDeal(dealId);
      }
      state.periodPickerOpenDealId = null;
      await setActivePeriod(periodId);
    });
  }
}

function renderWhatPanel() {
  if (!state.activePeriodId) {
    setHtmlWithFade(elements.whatContent, emptyStateHtml("No takeaways", "Select a deal period to generate AI takeaways."));
    return;
  }

  const isLoading = state.takeawaysLoadingByPeriod.get(state.activePeriodId) === true;
  const summaryPayload = state.takeawaysByPeriod.get(state.activePeriodId);
  if (!summaryPayload) {
    if (isLoading) {
      setHtmlWithFade(
        elements.whatContent,
        `
          <div class="takeawaysCard">
            <div class="takeawaysLoadingRow">
              <span class="panelSpinner" aria-hidden="true"></span>
              <p class="takeawaysLoadingText">Generating AI takeaways...</p>
            </div>
            <p class="takeawaysLoadingSub">Other panels remain available while this summary runs.</p>
          </div>
        `
      );
      return;
    }
    const takeawaysError = state.takeawaysErrorByPeriod.get(state.activePeriodId);
    setHtmlWithFade(
      elements.whatContent,
      emptyStateHtml(
        "Takeaways failed",
        takeawaysError ? `AI generation error: ${takeawaysError}` : "AI takeaways could not be generated for this period."
      )
    );
    return;
  }

  const takeaways = summaryPayload.takeaways || {};
  const generator = summaryPayload.generator || {};
  const generatedAt = generator.generated_at ? formatDate(generator.generated_at) : "n/a";
  const sourceLabel =
    generator.type === "ai"
      ? `AI generated · ${generator.model || "model unavailable"}`
      : `Fallback summary · ${generator.reason || "generation unavailable"}`;
  const loadingBadge = isLoading
    ? `
      <span class="takeawaysLoadingBadge">
        <span class="panelSpinner" aria-hidden="true"></span>
        Updating
      </span>
    `
    : "";

  setHtmlWithFade(
    elements.whatContent,
    `
      <div class="takeawaysCard">
        <div class="takeawaysTop">
          <p class="takeawaysTitle">AI period summary</p>
          <div class="takeawaysTopRight">
            ${loadingBadge}
            <span class="takeawaysMeta">${escapeHtml(sourceLabel)}</span>
          </div>
        </div>
        <div class="takeawaysRows">
          <div class="takeawayRow">
            <p class="takeawayLabel">Top change</p>
            <p class="takeawayValue">${escapeHtml(takeaways.top_change || "No top-change summary available.")}</p>
          </div>
          <div class="takeawayRow">
            <p class="takeawayLabel">Primary risk</p>
            <p class="takeawayValue">${escapeHtml(takeaways.primary_risk || "No risk summary available.")}</p>
          </div>
          <div class="takeawayRow">
            <p class="takeawayLabel">Confidence note</p>
            <p class="takeawayValue">${escapeHtml(
              takeaways.confidence_note || "No confidence summary available."
            )}</p>
          </div>
          <div class="takeawayRow">
            <p class="takeawayLabel">Bottom line</p>
            <p class="takeawayValue">${escapeHtml(takeaways.bottom_line || "No bottom-line summary available.")}</p>
          </div>
        </div>
        <p class="takeawaysFootnote">Generated ${escapeHtml(generatedAt)} · period ${escapeHtml(
      summaryPayload.period_end_date || "n/a"
    )}</p>
      </div>
    `
  );
}

function buildTaskBuckets(rowsRanked) {
  const todo = [];
  const done = [];

  for (const { row, rank } of rowsRanked) {
    const status = normalizeStatus(row.status);
    const label = row.label || row.concept_id || "Concept";
    const docId = row.evidence?.doc_id || "unknown_doc";
    const locator = `${row.evidence?.locator_type || "locator"}=${row.evidence?.locator_value || "unknown"}`;
    const task = {
      id: row.trace_id || `${row.concept_id}-${rank.score}`,
      traceId: row.trace_id || null,
      title: `Resolve ${label}`,
      reason: `${titleCaseStatus(status)} candidate in ${docId} (${locator}).`,
      chip: "Agent",
    };

    if (status === "verified") {
      done.push(task);
    } else {
      todo.push(task);
    }
  }

  return { todo, done };
}

function renderTasksPanel() {
  if (!state.activePeriodId) {
    const onboardingTask = state.selectedDealId ? state.onboardingTasksByDeal.get(state.selectedDealId) : null;
    if (!onboardingTask) {
      setHtmlWithFade(elements.tasksContent, emptyStateHtml("No tasks", "Tasks appear after extraction runs."));
      return;
    }

    const selected = state.selectedTaskId === onboardingTask.id;
    setHtmlWithFade(
      elements.tasksContent,
      `
        <div class="tasksSection">
          <p class="sectionLabel">To do</p>
          <div class="tasksList">
            <div class="taskRow ${selected ? "taskRowSelected" : ""}" data-task-id="${escapeHtml(onboardingTask.id)}">
              <div class="taskTop">
                <span class="taskChip taskChipAgent">${escapeHtml(onboardingTask.chip)}</span>
                <div class="taskSummary">
                  <p class="taskSummaryTitle">${escapeHtml(onboardingTask.title)}</p>
                  <p class="taskSummaryReason">${escapeHtml(onboardingTask.reason)}</p>
                </div>
              </div>
            </div>
          </div>
          <p class="sectionLabel">Done</p>
          <div class="tasksList">
            <p class="empty-state">No completed rows yet.</p>
          </div>
        </div>
      `
    );
    return;
  }

  const rankedRows = getRankedRowsForActivePeriod();
  const buckets = buildTaskBuckets(rankedRows);

  if (!state.selectedTaskId && buckets.todo.length) {
    state.selectedTaskId = buckets.todo[0].id;
  }

  const todoHtml = buckets.todo
    .map((task) => {
      const selected = task.id === state.selectedTaskId;
      return `
        <div class="taskRow ${selected ? "taskRowSelected" : ""}" data-task-id="${escapeHtml(task.id)}" data-task-trace-id="${escapeHtml(
          task.traceId || ""
        )}">
          <div class="taskTop">
            <span class="taskChip taskChipAgent">${escapeHtml(task.chip)}</span>
            <div class="taskSummary">
              <p class="taskSummaryTitle">${escapeHtml(task.title)}</p>
              <p class="taskSummaryReason">${escapeHtml(task.reason)}</p>
            </div>
          </div>
        </div>
      `;
    })
    .join("");

  const doneHtml = buckets.done
    .slice(0, 8)
    .map(
      (task) => `
        <div class="taskRow" data-task-id="${escapeHtml(task.id)}" data-task-trace-id="${escapeHtml(task.traceId || "")}">
          <div class="taskTop">
            <span class="taskChip taskChipAgent">Agent</span>
            <div class="taskSummary">
              <p class="taskSummaryTitle">${escapeHtml(task.title)}</p>
              <p class="taskSummaryReason">Verified and evidence-linked.</p>
            </div>
          </div>
        </div>
      `
    )
    .join("");

  const doneCount = buckets.done.length;
  const doneSection = doneCount
    ? `
        <details class="tasksDoneDetails">
          <summary>Done (${doneCount})</summary>
          <div class="tasksDoneBody">
            <div class="tasksList">${doneHtml}</div>
          </div>
        </details>
      `
    : `
        <p class="sectionLabel">Done</p>
        <div class="tasksList"><p class="empty-state">No completed rows yet.</p></div>
      `;

  setHtmlWithFade(
    elements.tasksContent,
    `
      <div class="tasksSection">
        <p class="sectionLabel">To do</p>
        <div class="tasksList">${todoHtml || `<p class="empty-state">No unresolved rows.</p>`}</div>
        ${doneSection}
      </div>
    `
  );

  for (const row of elements.tasksContent.querySelectorAll(".taskRow")) {
    row.addEventListener("click", async () => {
      const taskId = row.getAttribute("data-task-id");
      const traceId = row.getAttribute("data-task-trace-id");
      state.selectedTaskId = taskId;
      if (!traceId) {
        renderTasksPanel();
        return;
      }
      await onRowSelected(state.activePeriodId, traceId, { keepPickerOpen: true });
    });
  }
}

function renderDataPanel() {
  if (!state.activePeriodId) {
    setHtmlWithFade(elements.dataContent, emptyStateHtml("No data", "Select a deal period to inspect delta rows."));
    return;
  }

  const activePeriod = getActivePeriod();
  const rows = getRankedRowsForActivePeriod();

  const tableRows = rows
    .map(({ row, rank }, index) => {
      const status = normalizeStatus(row.status);
      const selected = state.selectedTraceId && state.selectedTraceId === row.trace_id;
      return `
        <tr class="tableRow ${selected ? "tableRowSelected" : ""}" data-trace-id="${escapeHtml(row.trace_id || "")}">
          <td class="numeric">${index + 1}</td>
          <td>
            <div>${escapeHtml(row.label || row.concept_id || "Concept")}</div>
            <div class="inline-note">${escapeHtml(row.concept_id || "")}</div>
          </td>
          <td class="numeric">${escapeHtml(formatValue(row.current_value ?? row.normalized_value))}</td>
          <td class="numeric">${escapeHtml(formatValue(row.abs_delta ?? "N/A"))}</td>
          <td><span class="statusPill status-${escapeHtml(status)}">${escapeHtml(titleCaseStatus(status))}</span></td>
          <td class="numeric">${escapeHtml((Number(row.confidence || 0)).toFixed(2))}</td>
          <td class="numeric">${escapeHtml(rank.score.toFixed(3))}</td>
        </tr>
      `;
    })
    .join("");

  setHtmlWithFade(
    elements.dataContent,
    `
      <p class="dataLensLabel">Period ${escapeHtml(activePeriod ? formatDate(activePeriod.period_end_date) : "N/A")} · Relevance ranked</p>
      <div class="tableWrap">
        <table class="table">
          <thead>
            <tr>
              <th class="numeric">#</th>
              <th>Concept</th>
              <th class="numeric">Current</th>
              <th class="numeric">Abs Delta</th>
              <th>Status</th>
              <th class="numeric">Conf.</th>
              <th class="numeric">Materiality</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows || `<tr><td colspan="7"><p class="empty-state">No rows in this period yet.</p></td></tr>`}
          </tbody>
        </table>
      </div>
    `
  );

  for (const rowEl of elements.dataContent.querySelectorAll(".tableRow")) {
    rowEl.addEventListener("click", async () => {
      const traceId = rowEl.getAttribute("data-trace-id");
      if (!traceId) return;
      await onRowSelected(state.activePeriodId, traceId, { keepPickerOpen: true });
    });
  }
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
            .map(
              ([status, count]) =>
                `<span class="doc-status-chip status-${escapeHtml(normalizeStatus(status))}">${escapeHtml(titleCaseStatus(status))}: ${escapeHtml(
                  count
                )}</span>`
            )
            .join("");

          return `
            <article class="doc-card">
              <div class="doc-head">
                <div class="doc-name">${escapeHtml(doc.filename)}</div>
                <span class="doc-badge">${escapeHtml(docTypeBadge(doc.doc_type))}</span>
              </div>
              <div class="doc-meta">${doc.pages_or_sheets ? `${escapeHtml(doc.pages_or_sheets)} pages/sheets` : "No size metadata"}</div>
              <div class="doc-meta">Linked rows: <strong>${escapeHtml(doc.row_count)}</strong></div>
              <div class="doc-status-row">${statuses || `<span class="doc-status-chip">No linked rows</span>`}</div>
            </article>
          `;
        })
        .join("")}
    </div>
  `;
}

function parseSheetName(pageOrSheet) {
  if (!pageOrSheet) return "";
  return String(pageOrSheet).replace(/^Sheet:\s*/i, "").trim();
}

function parsePdfPage(locatorValue) {
  const match = String(locatorValue || "").match(/^p([0-9]+):/i);
  if (!match) return 1;
  const page = Number(match[1]);
  return Number.isFinite(page) && page > 0 ? page : 1;
}

async function ensureWorkbookLoaded(traceId, evidencePreview) {
  if (!traceId || !evidencePreview?.download_url || !window.XLSX) {
    return;
  }

  if (state.xlsxWorkbookByTrace.has(traceId)) {
    return;
  }

  const response = await fetch(evidencePreview.download_url, {
    headers: {
      "X-Workspace-Id": state.workspaceId,
    },
  });
  if (!response.ok) {
    return;
  }

  const bytes = await response.arrayBuffer();
  const workbook = window.XLSX.read(bytes, { type: "array" });
  state.xlsxWorkbookByTrace.set(traceId, workbook);
}

function buildSheetGridFromPreview(preview) {
  if (!preview || preview.kind !== "xlsx_sheet" || !Array.isArray(preview.rows) || !preview.rows.length) {
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

  const meta = `
    <div class="inline-note">
      Sheet: ${escapeHtml(preview.sheet || "unknown")} |
      Target: ${escapeHtml(preview.target || "unknown")} |
      Viewport rows ${escapeHtml(preview.viewport_start_row || 1)}-${escapeHtml(preview.viewport_end_row || 1)} /
      ${escapeHtml(preview.total_rows || preview.rows.length)}
      ${preview.truncated ? "| Truncated preview (cap applied)" : ""}
    </div>
  `;

  return `
    <div class="sheet-view">
      ${meta}
      <table class="sheet-grid">
        <tbody>${bodyRows}</tbody>
      </table>
    </div>
  `;
}

function buildWorkbookViewer(traceId, row) {
  const workbook = state.xlsxWorkbookByTrace.get(traceId);
  if (!workbook || !Array.isArray(workbook.SheetNames) || workbook.SheetNames.length === 0 || !window.XLSX) {
    return '<p class="inline-note">Workbook viewer loading or unavailable. Falling back to grid preview.</p>';
  }

  const targetSheet = parseSheetName(row.evidence?.page_or_sheet || "");
  const currentSheet = state.activeSheetByTrace.get(traceId) || targetSheet || workbook.SheetNames[0];
  const activeSheet = workbook.SheetNames.includes(currentSheet) ? currentSheet : workbook.SheetNames[0];
  state.activeSheetByTrace.set(traceId, activeSheet);

  const tabs = workbook.SheetNames.map((sheetName) => {
    const active = sheetName === activeSheet;
    return `
      <button class="sheet-tab-btn ${active ? "active" : ""}" type="button" data-trace-id="${escapeHtml(
      traceId
    )}" data-sheet-name="${escapeHtml(sheetName)}">${escapeHtml(sheetName)}</button>
    `;
  }).join("");

  const sheet = workbook.Sheets[activeSheet];
  const htmlTable = window.XLSX.utils.sheet_to_html(sheet, { editable: false });

  return `
    <div class="xlsx-workbook" data-trace-id="${escapeHtml(traceId)}" data-target-cell="${escapeHtml(
      row.evidence?.locator_value || ""
    )}">
      <div class="sheet-tabs">${tabs}</div>
      <div class="sheet-html">${htmlTable}</div>
    </div>
  `;
}

function buildPdfPreview(preview, locatorValue = "") {
  if (preview?.download_url) {
    const page = parsePdfPage(locatorValue);
    return `
      <div class="pdf-frame-wrap">
        <iframe class="pdf-frame" title="pdf evidence" src="${escapeHtml(preview.download_url)}#page=${page}&zoom=page-fit"></iframe>
      </div>
    `;
  }

  if (preview?.preview?.kind === "pdf_text") {
    const page = preview.preview.page || "?";
    return `
      <div class="evidence-highlight">
        <div><strong>Page:</strong> ${escapeHtml(String(page))}</div>
        <div class="inline-note" style="margin-top:8px; white-space: pre-wrap;">${escapeHtml(preview.preview.text || "")}</div>
      </div>
    `;
  }

  return '<p class="inline-note">No PDF preview available for this locator.</p>';
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
  const traceShort = shortTraceId(row.trace_id || "");
  const evidencePreview = state.traceEvidenceById.get(row.trace_id) || null;
  const previewPayload = evidencePreview?.preview || null;

  let previewBlock = '<p class="inline-note">No resolved evidence preview available.</p>';
  if (evidencePreview && evidencePreview.available === false) {
    previewBlock = `<p class="inline-note">Evidence preview unavailable: ${escapeHtml(evidencePreview.reason || "unknown_reason")}.</p>`;
  } else if (docType === "XLSX") {
    const workbookBlock = buildWorkbookViewer(row.trace_id, row);
    if (workbookBlock.includes("loading or unavailable")) {
      previewBlock = `${workbookBlock}${buildSheetGridFromPreview(previewPayload)}`;
    } else {
      previewBlock = workbookBlock;
    }
  } else if (docType === "PDF") {
    previewBlock = buildPdfPreview(evidencePreview, evidence.locator_value || "");
  }

  const core = `
    <div class="evidence-focus">
      <div class="evidence-head evidence-head-compact">
        <div class="evidence-title-row">
          <p class="evidence-title">${escapeHtml(row.label || row.concept_id || "concept")}</p>
          <span class="meta-pill">Trace ${escapeHtml(traceShort)}</span>
        </div>
        <p class="evidence-sub">${escapeHtml(row.concept_id || "")}</p>
        <div class="evidence-meta-row evidence-meta-row-compact">
          <span class="statusPill status-${escapeHtml(normalizeStatus(row.status || "unresolved"))}">${escapeHtml(
            titleCaseStatus(row.status || "unresolved")
          )}</span>
          <span class="meta-pill">Confidence ${escapeHtml(confidence)}</span>
          <span class="meta-pill">${escapeHtml(docTypeBadge(docType))}</span>
        </div>
      </div>
      <div class="evidence-viewer-shell">
        ${previewBlock}
      </div>
      <div class="evidence-highlight evidence-highlight-compact">
        <div><strong>Document:</strong> ${escapeHtml(docName)}</div>
        <div><strong>Locator:</strong> ${escapeHtml(evidence.locator_type || "unknown")} = ${escapeHtml(evidence.locator_value || "")}</div>
        <div><strong>Current value:</strong> ${escapeHtml(formatValue(row.current_value ?? row.normalized_value))}</div>
      </div>
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
      const traceId = typeof event.trace_id === "string" ? event.trace_id : "";
      const hasTraceTarget = Boolean(traceId && traceId !== "n/a");
      const isTraceHit = highlightedTraceId && traceId === highlightedTraceId;
      const payloadPreview = JSON.stringify(event.payload || {});
      return `
        <article class="log-item ${isTraceHit ? "trace-hit" : ""} ${hasTraceTarget ? "" : "no-trace"}" data-trace-id="${escapeHtml(
          hasTraceTarget ? traceId : ""
        )}">
          <div class="log-head">
            <div class="log-title">#${escapeHtml(event.sequence_id || "?")} ${escapeHtml(event.event_type || "event")}</div>
            <span class="log-phase">${escapeHtml(event.phase || "phase")}</span>
          </div>
          <div class="log-line">agent: ${escapeHtml(event.agent_id || "n/a")} · trace: ${escapeHtml(traceId || "n/a")}</div>
          <div class="log-line">time: ${escapeHtml(event.timestamp || "")}</div>
          <div class="log-payload">${escapeHtml(payloadPreview)}</div>
        </article>
      `;
    })
    .join("");
}

function selectDecisionEvent(events) {
  if (!Array.isArray(events) || !events.length) return null;
  const decisionTypes = new Set(["user_resolved", "verify_accepted", "verify_rejected"]);
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const event = events[i];
    if (decisionTypes.has(event.event_type)) {
      return event;
    }
  }
  return events[events.length - 1];
}

function renderDecisionEvent(event) {
  if (!event) return "";
  const phase = String(event.phase || "phase");
  return `
    <article class="decision-event" data-trace-id="${escapeHtml(event.trace_id || "")}">
      <div class="decision-header-row">
        <div class="decision-title">Decision</div>
        <div class="decision-phase">phase ${escapeHtml(phase)}</div>
      </div>
      <div class="decision-main">#${escapeHtml(event.sequence_id || "?")} ${escapeHtml(event.event_type || "event")}</div>
      <div class="log-line">time: ${escapeHtml(event.timestamp || "")}</div>
    </article>
  `;
}

function renderCompactAudit({
  decisionEvent = null,
  events = [],
  highlightedTraceId = null,
  summaryTitle = "Audit trail",
  summaryCopy = "Showing latest decision context for this evidence view.",
}) {
  const decisionBlock = decisionEvent
    ? renderDecisionEvent(decisionEvent)
    : `<article class="decision-event">
        <div class="decision-title">${escapeHtml(summaryTitle)}</div>
        <div class="log-line">${escapeHtml(summaryCopy)}</div>
      </article>`;

  return `
    <div class="auditCompact">
      ${decisionBlock}
      <details class="auditDetails">
        <summary>Show full agent logs (${events.length})</summary>
        <div class="auditDetailsBody">
          ${events.length ? renderLogs(events, highlightedTraceId) : `<p class="empty-state">No log events available.</p>`}
        </div>
      </details>
    </div>
  `;
}

function buildCitationEntries(rowsRanked) {
  return rowsRanked.slice(0, 12).map(({ row, rank }, index) => {
    const status = normalizeStatus(row.status);
    const label = row.label || row.concept_id || "Concept";
    const context = `${row.evidence?.doc_id || "unknown_doc"} · ${row.evidence?.locator_type || "locator"}=${
      row.evidence?.locator_value || "unknown"
    }`;
    return {
      pointerId: row.trace_id || `${row.concept_id}-${index}`,
      traceId: row.trace_id || null,
      index,
      label,
      quality: titleCaseStatus(status),
      context,
      rank,
    };
  });
}

function renderProofPanel() {
  renderProofModeToggle();
  const auditMode = state.proofMode === "audit";
  if (elements.proofViewerStack) {
    elements.proofViewerStack.classList.toggle("auditMode", auditMode);
  }
  if (!state.activePeriodId) {
    elements.proofQualityChip.textContent = "0/0 verified";
    setHtmlWithFade(elements.proofCitations, "");
    if (auditMode) {
      setHtmlWithFade(elements.evidenceContent, "");
      setHtmlWithFade(elements.logsContent, emptyStateHtml("No logs", "Log stream appears once processing starts."));
    } else {
      setHtmlWithFade(elements.evidenceContent, emptyStateHtml("No evidence", "Select a row to inspect source evidence."));
      setHtmlWithFade(
        elements.logsContent,
        renderCompactAudit({
          decisionEvent: null,
          events: [],
          summaryTitle: "Audit trail",
          summaryCopy: "Log stream appears once processing starts.",
        })
      );
    }
    const layout = elements.proofCitations?.closest(".proofLayout");
    if (layout) {
      layout.classList.toggle("viewerOnly", !auditMode);
      layout.classList.toggle("auditOnly", auditMode);
    }
    return;
  }

  const rankedRows = getRankedRowsForActivePeriod();
  const verifiedCount = rankedRows.filter(({ row }) => normalizeStatus(row.status) === "verified").length;
  elements.proofQualityChip.textContent = `${verifiedCount}/${rankedRows.length} verified`;

  const citationEntries = buildCitationEntries(rankedRows);
  const layout = elements.proofCitations?.closest(".proofLayout");
  if (layout) {
    layout.classList.toggle("viewerOnly", !auditMode);
    layout.classList.toggle("auditOnly", auditMode);
  }
  setHtmlWithFade(elements.proofCitations, "");

  const packageEvents = state.packageEventsByPeriod.get(state.activePeriodId) || [];
  const toast = state.toast ? `<div class="toast">${escapeHtml(state.toast)}</div>` : "";
  state.toast = "";

  if (auditMode) {
    const traceEvents = state.selectedTraceId ? state.traceEventsById.get(state.selectedTraceId) || [] : packageEvents;
    const decisionEvent = state.selectedTraceId ? selectDecisionEvent(traceEvents) : null;
    setHtmlWithFade(elements.evidenceContent, "");
    setHtmlWithFade(elements.logsContent, `${renderDecisionEvent(decisionEvent)}${renderLogs(traceEvents, state.selectedTraceId)}`);
    attachLogHandlers();
    return;
  }

  if (!state.selectedRow) {
    const topCitation = citationEntries[0] || null;
    const openTopButton = topCitation?.traceId
      ? `<button type="button" class="settingsButton" data-open-top-trace-id="${escapeHtml(topCitation.traceId)}">
           Open top materiality evidence: ${escapeHtml(topCitation.label)}
         </button>`
      : "";
    setHtmlWithFade(
      elements.evidenceContent,
      `
        ${toast}
        <div class="viewerEmptyState">
          <p class="viewerEmptyTitle">Select a row in Data to open exact evidence.</p>
          <p class="viewerEmptyCopy">The viewer will jump to the precise PDF/XLSX location for that variable.</p>
          ${openTopButton}
        </div>
      `
    );
    setHtmlWithFade(elements.logsContent, "");
    const openTop = elements.evidenceContent.querySelector("[data-open-top-trace-id]");
    if (openTop) {
      openTop.addEventListener("click", async () => {
        const traceId = openTop.getAttribute("data-open-top-trace-id");
        if (!traceId) return;
        await onRowSelected(state.activePeriodId, traceId, { keepPickerOpen: true });
      });
    }
    attachLogHandlers();
    return;
  }

  setHtmlWithFade(elements.evidenceContent, `${toast}${renderFocusedEvidence(state.selectedRow)}`);
  const traceEvents = state.traceEventsById.get(state.selectedTraceId) || [];
  const decisionEvent = selectDecisionEvent(traceEvents);
  setHtmlWithFade(elements.logsContent, "");

  attachCandidateHandlers();
  attachSheetTabHandlers();
  applyWorkbookHighlight();
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
  void refreshTakeawaysForPeriod(state.selectedDealId, state.activePeriodId, { renderOnStart: false });

  const pkgEvents = await api(`/internal/v1/packages/${encodeURIComponent(state.activePeriodId)}/events?limit=500`).catch(() => ({
    events: [],
  }));
  state.packageEventsByPeriod.set(state.activePeriodId, pkgEvents.events || []);

  const traceEvents = await api(`/internal/v1/traces/${encodeURIComponent(traceId)}/events?limit=500`).catch(() => ({
    events: [],
  }));
  const traceEvidence = await api(`/internal/v1/traces/${encodeURIComponent(traceId)}/evidence`).catch(() => ({
    evidence_preview: null,
  }));
  state.traceEventsById.set(traceId, traceEvents.events || []);
  const evidencePreview = traceEvidence.evidence_preview || null;
  state.traceEvidenceById.set(traceId, evidencePreview);
  if (evidencePreview?.doc_type === "XLSX" && evidencePreview?.download_url) {
    await ensureWorkbookLoaded(traceId, evidencePreview);
  }

  state.selectedTraceId = traceId;
  state.selectedRow = findRowByTrace(state.activePeriodId, traceId);
  state.selectedWhatId = traceId;
  state.selectedTaskId = traceId;

  renderWorkspace();
}

async function jumpToTrace(traceId) {
  if (!traceId) return;

  const previousState = {
    selectedDealId: state.selectedDealId,
    activePeriodId: state.activePeriodId,
    selectedTraceId: state.selectedTraceId,
    selectedRow: state.selectedRow,
    selectedWhatId: state.selectedWhatId,
    selectedTaskId: state.selectedTaskId,
  };

  try {
    const trace = await api(`/internal/v1/traces/${encodeURIComponent(traceId)}`);
    if (!trace?.deal_id || !trace?.period_id || !trace?.trace_id) {
      throw new Error("trace_context_incomplete");
    }

    if (trace.deal_id !== state.selectedDealId) {
      state.selectedDealId = trace.deal_id;
      await loadDeal(trace.deal_id);
    }

    if (trace.period_id !== state.activePeriodId) {
      await setActivePeriod(trace.period_id);
    }

    await onRowSelected(trace.period_id, trace.trace_id, { keepPickerOpen: true });
  } catch (error) {
    state.selectedDealId = previousState.selectedDealId;
    state.activePeriodId = previousState.activePeriodId;
    state.selectedTraceId = previousState.selectedTraceId;
    state.selectedRow = previousState.selectedRow;
    state.selectedWhatId = previousState.selectedWhatId;
    state.selectedTaskId = previousState.selectedTaskId;
    state.toast = `Unable to jump to trace ${traceId}: ${error.message}`;
    renderWorkspace();
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

function attachSheetTabHandlers() {
  for (const button of elements.evidenceContent.querySelectorAll(".sheet-tab-btn")) {
    button.addEventListener("click", () => {
      const traceId = button.getAttribute("data-trace-id");
      const sheetName = button.getAttribute("data-sheet-name");
      if (!traceId || !sheetName) return;
      state.activeSheetByTrace.set(traceId, sheetName);
      renderProofPanel();
    });
  }
}

function applyWorkbookHighlight() {
  for (const wrapper of elements.evidenceContent.querySelectorAll(".xlsx-workbook")) {
    const targetCell = (wrapper.getAttribute("data-target-cell") || "").toUpperCase();
    if (!targetCell) continue;

    const cells = wrapper.querySelectorAll("td");
    for (const cell of cells) {
      const id = (cell.getAttribute("id") || "").toUpperCase();
      if (!id) continue;
      if (id.endsWith(`-${targetCell}`) || id === targetCell || id.includes(targetCell)) {
        cell.classList.add("workbook-highlight");
        cell.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "nearest" });
        break;
      }
    }
  }
}

async function start() {
  elements.refreshButton.addEventListener("click", async () => {
    await loadDeals();
  });

  elements.createDealButton.addEventListener("click", () => {
    openCreateDealFlow();
  });
  if (elements.proofModeEvidence) {
    elements.proofModeEvidence.addEventListener("click", () => {
      setProofMode("evidence");
    });
  }
  if (elements.proofModeAudit) {
    elements.proofModeAudit.addEventListener("click", () => {
      setProofMode("audit");
    });
  }
  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && state.createPaneMode === "create_deal" && !state.createDealBusy) {
      event.preventDefault();
      cancelCreateDealFlow();
    }
  });

  elements.logoutButton.addEventListener("click", async () => {
    try {
      if (state.sessionToken) {
        await authApi("/auth/v1/logout", { method: "POST" });
      }
    } catch (_error) {
      // Ignore logout transport failures and clear local session.
    }

    clearSession();
    showAuthOverlay(buildLoginCard());
    attachLoginHandler();
  });

  renderTopAuthState();

  const url = new URL(window.location.href);
  const magicToken = url.searchParams.get("magic_token");
  if (magicToken) {
    showAuthOverlay(buildPasswordSetupCard(magicToken));
    attachPasswordSetupHandler();
    return;
  }

  if (!state.sessionToken) {
    showAuthOverlay(buildLoginCard());
    attachLoginHandler();
    return;
  }

  try {
    const me = await authApi("/auth/v1/me");
    setSession(state.sessionToken, me.user);
    hideAuthOverlay();
    await loadDeals();
  } catch (error) {
    clearSession();
    showAuthOverlay(buildLoginCard(error.message));
    attachLoginHandler();
  }
}

start();
