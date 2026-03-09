import { createApiClient } from "./delta-review-api.js";
import { attachKeyboardShortcuts } from "./delta-review-keyboard.js";
import { renderScreen } from "./delta-review-render.js";
import { createStore } from "./delta-review-state.js";

function parseQueryParams() {
  const params = new URLSearchParams(window.location.search);
  return {
    workspaceId: params.get("workspace") || window.localStorage.getItem("workspace_id") || "ws_default",
    dealId: params.get("deal_id") || "",
    periodId: params.get("period_id") || "",
    baselinePeriodId: params.get("baseline_period_id") || "",
  };
}

function syncQuery({ workspaceId, dealId, periodId, baselinePeriodId }) {
  const url = new URL(window.location.href);
  if (workspaceId) url.searchParams.set("workspace", workspaceId);
  else url.searchParams.delete("workspace");
  if (dealId) url.searchParams.set("deal_id", dealId);
  else url.searchParams.delete("deal_id");
  if (periodId) url.searchParams.set("period_id", periodId);
  else url.searchParams.delete("period_id");
  if (baselinePeriodId) url.searchParams.set("baseline_period_id", baselinePeriodId);
  else url.searchParams.delete("baseline_period_id");
  window.history.replaceState({}, "", url.toString());
}

function selectedItemFromState(state) {
  const items = state.queuePayload?.items || [];
  if (!items.length) return null;
  const selected = items.find((item) => item.id === state.selectedItemId);
  return selected || items[0];
}

function parseSideEvidenceForResolve(item) {
  const side = item?.evidence?.current || {};
  const locator = String(side.locator || "");
  const locatorMatch = locator.match(/([a-z_]+)=(.*)$/i);
  let locatorType = "paragraph";
  let locatorValue = "";
  if (locatorMatch) {
    locatorType = locatorMatch[1] || locatorType;
    locatorValue = locatorMatch[2] || "";
  }
  return {
    doc_id: side.file_id || "",
    locator_type: locatorType,
    locator_value: locatorValue,
  };
}

function parseAnchorEvidence(anchor) {
  return {
    doc_id: anchor?.doc_id || "",
    locator_type: anchor?.locator_type || "",
    locator_value: anchor?.locator_value || "",
    source_snippet: anchor?.source_snippet || "",
    raw_value_text: anchor?.raw_value_text || "",
    normalized_value: anchor?.normalized_value ?? null,
  };
}

function collectPreviewUrls(item) {
  const urls = new Set();
  const add = (value) => {
    const normalized = String(value || "").trim();
    if (normalized) urls.add(normalized);
  };

  const evidence = item?.evidence || {};
  add(evidence?.baseline?.preview_url);
  add(evidence?.current?.preview_url);

  if (Array.isArray(item?.competing_anchors)) {
    for (const anchor of item.competing_anchors) {
      add(anchor?.preview_url);
    }
  }

  add(item?.baseline_anchor?.preview_url);
  add(item?.current_candidate_anchor?.preview_url);
  add(item?.requirement_anchor?.preview_url);

  return Array.from(urls);
}

function analystNoteTemplate(item) {
  const note = item?.draft_analyst_note || {};
  const defaultSubject = note.subject || `Analyst note — ${item?.metric_label || "Item"}`;
  const defaultText = note.text || "";
  return {
    subject: defaultSubject,
    text: defaultText,
  };
}

async function start() {
  const query = parseQueryParams();
  window.localStorage.setItem("workspace_id", query.workspaceId);

  const elements = {
    dealTitle: document.getElementById("deal-title"),
    dealSwitcher: document.getElementById("deal-switcher"),
    comparisonPair: document.getElementById("comparison-pair"),
    comparisonNote: document.getElementById("comparison-note"),
    summaryCounts: document.getElementById("summary-counts"),
    periodRail: document.getElementById("period-rail"),
    baselineSwitcher: document.getElementById("baseline-switcher"),
    baselineControl: document.getElementById("baseline-control"),
    includeResolved: document.getElementById("include-resolved"),
    queueContent: document.getElementById("queue-content"),
    detailContent: document.getElementById("detail-content"),
  };

  const store = createStore({
    workspaceId: query.workspaceId,
    sessionToken: window.localStorage.getItem("session_token") || "",
    currentDealId: query.dealId,
    currentPeriodId: query.periodId,
    baselinePeriodId: query.baselinePeriodId,
  });

  const api = createApiClient({
    workspaceId: query.workspaceId,
    sessionTokenProvider: () => store.getState().sessionToken,
  });
  let queueRequestSeq = 0;

  function setMapCache(key, value, cacheKey) {
    store.mutate((state) => {
      const next = new Map(state[cacheKey]);
      next.set(key, value);
      return { [cacheKey]: next };
    });
  }

  async function ensurePreviewPayload(previewUrl) {
    const normalizedUrl = String(previewUrl || "").trim();
    if (!normalizedUrl) return null;

    const state = store.getState();
    const cached = state.traceEvidenceCache.get(normalizedUrl);
    if (cached) return cached;

    try {
      const payload = await api.getEvidencePreviewByUrl(normalizedUrl);
      setMapCache(normalizedUrl, payload, "traceEvidenceCache");
      return payload;
    } catch (_error) {
      return null;
    }
  }

  async function ensureSelectedHistoryLoaded() {
    const state = store.getState();
    const selected = selectedItemFromState(state);
    if (!selected || !Array.isArray(selected.trace_ids) || !selected.trace_ids.length) return;
    const traceId = String(selected.trace_ids[0] || "");
    if (!traceId || state.traceHistoryCache.has(traceId)) return;
    try {
      const history = await api.getTraceHistory(traceId);
      setMapCache(traceId, history, "traceHistoryCache");
    } catch (_error) {
      // Keep history optional.
    }
  }

  async function ensureSelectedProofLoaded() {
    const selected = selectedItemFromState(store.getState());
    if (!selected) return;

    const previewUrls = collectPreviewUrls(selected);
    if (!previewUrls.length) return;

    await Promise.all(previewUrls.map((previewUrl) => ensurePreviewPayload(previewUrl)));
  }

  async function submitDraftWorkflowEvent(item, eventType, { subject = "", draftText = "", metadata = {} } = {}) {
    const state = store.getState();
    await api.submitDraftWorkflowEvent({
      dealId: state.currentDealId,
      periodId: state.currentPeriodId,
      itemId: item?.id || "",
      eventType,
      subject,
      draftText,
      metadata,
    });
  }

  async function loadReviewQueue({ preserveSelection = true, allowBaselineResetRetry = true } = {}) {
    const state = store.getState();
    if (!state.currentDealId || !state.currentPeriodId) return;
    const requestSeq = ++queueRequestSeq;
    store.setState({ loading: { queue: true }, errors: { queue: "" } });
    try {
      const payload = await api.getReviewQueue({
        dealId: state.currentDealId,
        periodId: state.currentPeriodId,
        baselinePeriodId: state.baselinePeriodId,
        includeResolved: state.includeResolved,
      });
      if (requestSeq !== queueRequestSeq) return;

      const items = payload.items || [];
      const nextSelectedId =
        preserveSelection && items.some((item) => item.id === state.selectedItemId)
          ? state.selectedItemId
          : items[0]?.id || "";

      store.setState({
        queuePayload: payload,
        selectedItemId: nextSelectedId,
        loading: { queue: false },
      });
      syncQuery({
        workspaceId: store.getState().workspaceId,
        dealId: store.getState().currentDealId,
        periodId: store.getState().currentPeriodId,
        baselinePeriodId: store.getState().baselinePeriodId,
      });
      await Promise.all([ensureSelectedHistoryLoaded(), ensureSelectedProofLoaded()]);
    } catch (error) {
      if (requestSeq !== queueRequestSeq) return;
      const message = error instanceof Error ? error.message : "review_queue_failed";
      const shouldAutoResetBaseline =
        allowBaselineResetRetry &&
        Boolean(state.baselinePeriodId) &&
        (message === "baseline_period_cannot_match_current" || message === "baseline_period_not_found");

      if (shouldAutoResetBaseline) {
        store.setState({
          baselinePeriodId: "",
          errors: { queue: "" },
        });
        void loadReviewQueue({ preserveSelection: false, allowBaselineResetRetry: false });
        return;
      }

      store.setState({
        queuePayload: null,
        selectedItemId: "",
        activeDraft: null,
        activeAnalystNote: null,
        loading: { queue: false },
        errors: { queue: message },
      });
    }
  }

  async function loadPeriodsForDeal(dealId) {
    const payload = await api.listPeriods(dealId);
    return Array.isArray(payload.periods) ? payload.periods : [];
  }

  async function initializeDealSelection() {
    store.setState({ loading: { deals: true }, errors: { global: "" } });
    try {
      const dealsPayload = await api.listDeals();
      const deals = dealsPayload.deals || [];
      if (!deals.length) {
        store.setState({
          deals: [],
          currentDealId: "",
          currentPeriodId: "",
          queuePayload: { summary: {}, items: [], period_options: [], deal: null, periods: null },
          loading: { deals: false, queue: false },
        });
        return;
      }

      const state = store.getState();
      const requestedDeal = deals.find((deal) => deal.deal_id === state.currentDealId)?.deal_id || "";
      const currentDealId = requestedDeal || deals[0].deal_id;
      const periods = await loadPeriodsForDeal(currentDealId);
      const requestedPeriod = periods.find((period) => period.package_id === state.currentPeriodId)?.package_id || "";
      const currentPeriodId = requestedPeriod || periods[0]?.package_id || "";

      store.setState({
        deals,
        periods,
        currentDealId,
        currentPeriodId,
        loading: { deals: false },
      });
      if (currentPeriodId) {
        await loadReviewQueue({ preserveSelection: false });
      } else {
        store.setState({
          queuePayload: {
            deal: {
              id: currentDealId,
              name: deals.find((deal) => deal.deal_id === currentDealId)?.display_name || currentDealId,
            },
            periods: null,
            summary: { blockers: 0, material_changes: 0, verified: 0, resolved: 0, total: 0 },
            period_options: [],
            items: [],
          },
        });
      }
    } catch (error) {
      store.setState({
        loading: { deals: false, queue: false },
        errors: { global: error instanceof Error ? error.message : "initialization_failed" },
      });
    }
  }

  function onDealChange(dealId) {
    if (!dealId) return;
    (async () => {
      const periods = await loadPeriodsForDeal(dealId);
      const periodId = periods[0]?.package_id || "";
      store.setState({
        currentDealId: dealId,
        periods,
        currentPeriodId: periodId,
        baselinePeriodId: "",
        selectedItemId: "",
        activeDraft: null,
        activeAnalystNote: null,
      });
      if (periodId) {
        await loadReviewQueue({ preserveSelection: false });
      }
    })().catch((error) => {
      store.setState({
        errors: { global: error instanceof Error ? error.message : "deal_change_failed" },
      });
    });
  }

  function onPeriodChange(periodId) {
    if (!periodId) return;
    store.setState({
      currentPeriodId: periodId,
      baselinePeriodId: "",
      selectedItemId: "",
      activeDraft: null,
      activeAnalystNote: null,
    });
    void loadReviewQueue({ preserveSelection: false });
  }

  function onBaselineChange(periodId) {
    store.setState({
      baselinePeriodId: periodId || "",
      selectedItemId: "",
      activeDraft: null,
      activeAnalystNote: null,
    });
    void loadReviewQueue({ preserveSelection: false });
  }

  function onItemSelect(itemId) {
    const state = store.getState();
    const nextDraft = state.activeDraft && state.activeDraft.itemId === itemId ? state.activeDraft : null;
    const nextAnalystNote =
      state.activeAnalystNote && state.activeAnalystNote.itemId === itemId ? state.activeAnalystNote : null;
    store.setState({ selectedItemId: itemId, activeDraft: nextDraft, activeAnalystNote: nextAnalystNote });
    void Promise.all([ensureSelectedHistoryLoaded(), ensureSelectedProofLoaded()]);
  }

  async function runItemAction(itemId, actionId, meta = {}) {
    const state = store.getState();
    const item = (state.queuePayload?.items || []).find((entry) => entry.id === itemId);
    if (!item) return;
    const isReviewTier = String(item.concept_maturity || "").toLowerCase() === "review";

    async function submitReviewFeedback(outcome, note = "", metadata = {}) {
      if (!isReviewTier) return;
      await api.submitReviewFeedback({
        dealId: state.currentDealId,
        periodId: state.currentPeriodId,
        itemId,
        actionId,
        outcome,
        note,
        metadata,
      });
    }

    if (actionId === "view_source_evidence") {
      const anchors = Array.isArray(item.competing_anchors) ? item.competing_anchors : [];
      const anchorIndex = Number.isInteger(meta.anchorIndex) ? Number(meta.anchorIndex) : 0;
      const downloadUrl =
        anchors[anchorIndex]?.download_url || anchors[0]?.download_url || item?.evidence?.current?.download_url || "";
      if (downloadUrl) {
        window.open(downloadUrl, "_blank", "noopener,noreferrer");
      }
      return;
    }

    if (actionId === "view_review_history") {
      await ensureSelectedHistoryLoaded();
      return;
    }

    if (actionId === "draft_borrower_query" || actionId === "request_borrower_update" || actionId === "prepare_borrower_follow_up") {
      const draft = item?.draft_borrower_query;
      if (!draft?.text) return;
      store.setState({
        activeDraft: {
          itemId,
          subject: draft.subject || "",
          text: draft.text || "",
          initialSubject: draft.subject || "",
          initialText: draft.text || "",
          editedLogged: false,
        },
      });
      void submitDraftWorkflowEvent(item, "draft_opened", {
        subject: draft.subject || "",
        draftText: draft.text || "",
        metadata: { source_action: actionId },
      }).catch(() => {});
      return;
    }

    if (actionId === "draft_analyst_note") {
      const template = analystNoteTemplate(item);
      let persisted = null;
      try {
        const payload = await api.getAnalystNote({
          dealId: state.currentDealId,
          periodId: state.currentPeriodId,
          itemId,
        });
        persisted = payload?.note || null;
      } catch (_error) {
        persisted = null;
      }
      const nowIso = new Date().toISOString();
      store.setState({
        activeAnalystNote: {
          itemId,
          noteId: persisted?.note_id || null,
          author: persisted?.author || "operator_ui",
          createdAt: persisted?.created_at || nowIso,
          updatedAt: persisted?.updated_at || nowIso,
          subject: persisted?.subject || template.subject,
          text: persisted?.note_text || template.text,
          memoReady: Boolean(persisted?.memo_ready),
          exportReady: Boolean(persisted?.export_ready),
          dirty: false,
        },
      });
      void submitDraftWorkflowEvent(item, "draft_opened", {
        subject: persisted?.subject || template.subject || "",
        draftText: persisted?.note_text || template.text || "",
        metadata: { source_action: actionId, draft_kind: "analyst_note" },
      }).catch(() => {});
      return;
    }

    if (actionId === "save_analyst_note") {
      const openNote = state.activeAnalystNote;
      if (!openNote || openNote.itemId !== itemId) return;
      if (!String(openNote.text || "").trim()) return;
      store.setState({ loading: { action: true } });
      try {
        const payload = await api.upsertAnalystNote({
          dealId: state.currentDealId,
          periodId: state.currentPeriodId,
          itemId,
          actor: "operator_ui",
          subject: openNote.subject || "",
          noteText: openNote.text || "",
          memoReady: Boolean(openNote.memoReady),
          exportReady: Boolean(openNote.exportReady),
          metadata: { source_action: actionId },
        });
        const saved = payload?.note || null;
        const nowIso = new Date().toISOString();
        store.setState({
          loading: { action: false },
          activeAnalystNote: {
            itemId,
            noteId: saved?.note_id || openNote.noteId || null,
            author: saved?.author || openNote.author || "operator_ui",
            createdAt: saved?.created_at || openNote.createdAt || nowIso,
            updatedAt: saved?.updated_at || nowIso,
            subject: saved?.subject || openNote.subject || "",
            text: saved?.note_text || openNote.text || "",
            memoReady: Boolean(saved?.memo_ready),
            exportReady: Boolean(saved?.export_ready),
            dirty: false,
          },
        });
      } catch (error) {
        store.setState({
          loading: { action: false },
          errors: { queue: error instanceof Error ? error.message : "analyst_note_save_failed" },
        });
      }
      return;
    }

    if (actionId === "close_analyst_note") {
      store.setState({ activeAnalystNote: null });
      return;
    }

    if (actionId === "copy_borrower_query" || actionId === "copy_borrower_draft") {
      const activeDraft = state.activeDraft;
      const subject =
        activeDraft?.itemId === itemId
          ? activeDraft?.subject || ""
          : item?.draft_borrower_query?.subject || "";
      const text =
        activeDraft?.itemId === itemId
          ? activeDraft?.text
          : item?.draft_borrower_query?.text || "";
      if (!text) return;
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      }
      void submitDraftWorkflowEvent(item, "draft_copied", {
        subject,
        draftText: text,
        metadata: { source_action: actionId },
      }).catch(() => {});
      return;
    }

    if (actionId === "mark_follow_up_prepared") {
      const activeDraft = state.activeDraft;
      const subject =
        activeDraft?.itemId === itemId
          ? activeDraft?.subject || ""
          : item?.draft_borrower_query?.subject || "";
      const text =
        activeDraft?.itemId === itemId
          ? activeDraft?.text
          : item?.draft_borrower_query?.text || "";
      store.setState({ loading: { action: true } });
      try {
        if (isReviewTier) {
          await submitReviewFeedback("borrower_followup");
        }
        await submitDraftWorkflowEvent(item, "draft_prepared", {
          subject,
          draftText: text,
          metadata: { source_action: actionId },
        });
        store.setState({ loading: { action: false }, activeDraft: null, activeAnalystNote: null });
      } catch (error) {
        store.setState({
          loading: { action: false },
          errors: { queue: error instanceof Error ? error.message : "draft_workflow_failed" },
        });
      }
      return;
    }

    if (actionId === "close_draft_editor") {
      const activeDraft = state.activeDraft;
      const subject = activeDraft?.itemId === itemId ? activeDraft?.subject || "" : "";
      const text = activeDraft?.itemId === itemId ? activeDraft?.text || "" : "";
      void submitDraftWorkflowEvent(item, "draft_closed", {
        subject,
        draftText: text,
        metadata: { source_action: actionId },
      }).catch(() => {});
      store.setState({ activeDraft: null, activeAnalystNote: null });
      return;
    }

    if (actionId === "view_reporting_requirement") {
      const requirement = item?.requirement_anchor || {};
      const target = requirement.download_url || requirement.preview_url || "";
      if (target) {
        window.open(target, "_blank", "noopener,noreferrer");
      }
      return;
    }

    if (actionId === "review_possible_requirement") {
      const note = item?.draft_analyst_note || item?.draft_borrower_query;
      if (!note?.text) return;
      store.setState({
        activeDraft: {
          itemId,
          subject: note.subject || "",
          text: note.text || "",
          initialSubject: note.subject || "",
          initialText: note.text || "",
          editedLogged: false,
        },
      });
      void submitDraftWorkflowEvent(item, "draft_opened", {
        subject: note.subject || "",
        draftText: note.text || "",
        metadata: { source_action: actionId, draft_kind: "review_note" },
      }).catch(() => {});
      return;
    }

    if (actionId === "mark_expected_noise" || actionId === "dismiss_after_review") {
      const outcome = actionId === "mark_expected_noise" ? "expected_noise" : "dismissed";
      store.setState({ loading: { action: true } });
      try {
        await submitReviewFeedback(outcome);
        store.setState({ loading: { action: false }, activeDraft: null, activeAnalystNote: null });
        await loadReviewQueue({ preserveSelection: true });
      } catch (error) {
        store.setState({
          loading: { action: false },
          errors: { queue: error instanceof Error ? error.message : "review_feedback_failed" },
        });
      }
      return;
    }

    if (actionId === "confirm_alternate_source" || actionId === "mark_item_received") {
      const traceId = Array.isArray(item.trace_ids) && item.trace_ids.length ? String(item.trace_ids[0]) : "";
      if (!traceId) return;
      const candidate = item?.current_candidate_anchor;
      store.setState({ loading: { action: true } });
      try {
        await api.resolveTrace({
          traceId,
          selectedEvidence:
            actionId === "confirm_alternate_source" && candidate
              ? parseAnchorEvidence(candidate)
              : parseSideEvidenceForResolve(item),
        });
        store.setState({ loading: { action: false }, activeDraft: null, activeAnalystNote: null });
        await loadReviewQueue({ preserveSelection: true });
      } catch (error) {
        store.setState({
          loading: { action: false },
          errors: { queue: error instanceof Error ? error.message : "resolve_failed" },
        });
      }
      return;
    }

    if (actionId === "confirm_source_of_record") {
      const traceId = Array.isArray(item.trace_ids) && item.trace_ids.length ? String(item.trace_ids[0]) : "";
      const anchors = Array.isArray(item.competing_anchors) ? item.competing_anchors : [];
      if (String(item.case_mode || "") === "investigation_conflict" && !Number.isInteger(meta.anchorIndex)) {
        store.setState({
          errors: { queue: "Select Source A or Source B in Proof before confirming source of record." },
        });
        return;
      }
      const anchorIndex = Number.isInteger(meta.anchorIndex) ? Number(meta.anchorIndex) : 0;
      const selectedAnchor = anchors[anchorIndex] || anchors[0] || null;

      store.setState({ loading: { action: true } });
      try {
        if (traceId) {
          await api.resolveTrace({
            traceId,
            selectedEvidence: selectedAnchor ? parseAnchorEvidence(selectedAnchor) : parseSideEvidenceForResolve(item),
          });
        }
        if (isReviewTier) {
          await submitReviewFeedback("confirmed", "", {
            selected_anchor_index: Number.isInteger(meta.anchorIndex) ? Number(meta.anchorIndex) : null,
          });
        }
        store.setState({ loading: { action: false }, activeDraft: null, activeAnalystNote: null });
        await loadReviewQueue({ preserveSelection: true });
      } catch (error) {
        store.setState({
          loading: { action: false },
          errors: { queue: error instanceof Error ? error.message : "resolve_failed" },
        });
      }
    }
  }

  async function onOpenPreview(previewUrl) {
    const payload = await ensurePreviewPayload(previewUrl);
    if (!payload) return;
    const downloadUrl =
      String(payload?.download_url || "").trim() ||
      String(payload?.evidence_preview?.download_url || "").trim();
    if (downloadUrl) {
      window.open(downloadUrl, "_blank", "noopener,noreferrer");
    }
  }

  function onDraftChange(itemId, draftField, value) {
    const state = store.getState();
    const activeDraft = state.activeDraft;
    if (!activeDraft || activeDraft.itemId !== itemId) return;
    if (draftField !== "subject" && draftField !== "text") return;

    const nextDraft = {
      ...activeDraft,
      [draftField]: value,
    };
    store.setState({ activeDraft: nextDraft });

    const changed =
      String(nextDraft.subject || "") !== String(nextDraft.initialSubject || "") ||
      String(nextDraft.text || "") !== String(nextDraft.initialText || "");
    if (!changed || nextDraft.editedLogged) return;

    const item = (state.queuePayload?.items || []).find((entry) => entry.id === itemId);
    if (!item) return;

    store.setState({
      activeDraft: {
        ...nextDraft,
        editedLogged: true,
      },
    });
    void submitDraftWorkflowEvent(item, "draft_edited", {
      subject: nextDraft.subject || "",
      draftText: nextDraft.text || "",
      metadata: { source_field: draftField },
    }).catch(() => {});
  }

  function onAnalystNoteChange(itemId, noteField, value) {
    const state = store.getState();
    const activeAnalystNote = state.activeAnalystNote;
    if (!activeAnalystNote || activeAnalystNote.itemId !== itemId) return;
    if (!["subject", "text", "memoReady", "exportReady"].includes(noteField)) return;

    const next = {
      ...activeAnalystNote,
      [noteField]:
        noteField === "memoReady" || noteField === "exportReady"
          ? Boolean(value)
          : String(value ?? ""),
      dirty: true,
    };
    store.setState({ activeAnalystNote: next });
  }

  function onQueueMove(step) {
    const state = store.getState();
    const items = state.queuePayload?.items || [];
    if (!items.length) return;
    const idx = Math.max(
      0,
      items.findIndex((item) => item.id === state.selectedItemId)
    );
    const nextIdx = Math.max(0, Math.min(items.length - 1, idx + step));
    const nextId = items[nextIdx]?.id;
    if (nextId) {
      onItemSelect(nextId);
    }
  }

  function onPeriodMove(step) {
    const state = store.getState();
    const options = state.queuePayload?.period_options || [];
    if (!options.length) return;
    const idx = Math.max(
      0,
      options.findIndex((item) => item.id === state.currentPeriodId)
    );
    const nextIdx = Math.max(0, Math.min(options.length - 1, idx + step));
    const next = options[nextIdx];
    if (next && next.id !== state.currentPeriodId) {
      onPeriodChange(next.id);
    }
  }

  function onRunPrimaryAction() {
    const state = store.getState();
    const selected = selectedItemFromState(state);
    const actionId = selected?.primary_action?.id || "";
    if (!selected || !actionId) return;
    void runItemAction(selected.id, actionId, {});
  }

  const handlers = {
    onDealChange,
    onPeriodChange,
    onBaselineChange,
    onItemSelect,
    onAction: (itemId, actionId, meta) => {
      void runItemAction(itemId, actionId, meta);
    },
    onOpenPreview: (previewUrl) => {
      void onOpenPreview(previewUrl);
    },
    onDraftChange,
    onAnalystNoteChange,
  };

  store.subscribe((state) => {
    renderScreen(state, elements, handlers);
  });

  elements.dealSwitcher.addEventListener("change", () => onDealChange(elements.dealSwitcher.value));
  elements.baselineSwitcher.addEventListener("change", () => onBaselineChange(elements.baselineSwitcher.value));
  elements.includeResolved.addEventListener("change", () => {
    store.setState({
      includeResolved: elements.includeResolved.checked,
      selectedItemId: "",
      activeDraft: null,
      activeAnalystNote: null,
    });
    void loadReviewQueue({ preserveSelection: false });
  });
  attachKeyboardShortcuts({
    getState: () => store.getState(),
    onQueueMove,
    onPeriodMove,
    onRunPrimaryAction,
    focusDealSwitcher: () => elements.dealSwitcher.focus(),
  });

  renderScreen(store.getState(), elements, handlers);
  await initializeDealSelection();
}

void start();
