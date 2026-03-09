function normalizeError(text, status) {
  const raw = String(text || "").trim();
  if (!raw) return `request_failed_${status}`;
  try {
    const parsed = JSON.parse(raw);
    const detail = parsed && typeof parsed === "object" ? parsed.detail : "";
    if (typeof detail === "string" && detail.trim()) {
      return detail.trim();
    }
  } catch (_error) {
    // Keep raw text.
  }
  return raw;
}

export function createApiClient({ workspaceId, sessionTokenProvider }) {
  async function request(path, options = {}) {
    const headers = {
      "Content-Type": "application/json",
      "X-Workspace-Id": workspaceId,
      ...(options.headers || {}),
    };

    const sessionToken = typeof sessionTokenProvider === "function" ? sessionTokenProvider() : "";
    if (sessionToken && !headers.Authorization) {
      headers.Authorization = `Bearer ${sessionToken}`;
    }

    const response = await fetch(path, { ...options, headers });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(normalizeError(text, response.status));
    }
    if (response.status === 204) return {};
    return response.json();
  }

  return {
    async listDeals() {
      return request("/internal/v1/deals");
    },
    async listPeriods(dealId) {
      return request(`/internal/v1/deals/${encodeURIComponent(dealId)}/periods`);
    },
    async getReviewQueue({ dealId, periodId, baselinePeriodId = "", includeResolved = false }) {
      const query = new URLSearchParams();
      if (baselinePeriodId) query.set("baseline_period_id", baselinePeriodId);
      if (includeResolved) query.set("include_resolved", "true");
      const suffix = query.toString() ? `?${query.toString()}` : "";
      return request(
        `/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(periodId)}/review_queue${suffix}`
      );
    },
    async resolveTrace({ traceId, selectedEvidence }) {
      return request(`/internal/v1/traces/${encodeURIComponent(traceId)}:resolve`, {
        method: "POST",
        body: JSON.stringify({
          resolver: "operator_ui",
          selected_evidence: selectedEvidence || {},
          note: "Resolved from Delta Review v2",
        }),
      });
    },
    async getTraceEvidence(traceId) {
      return request(`/internal/v1/traces/${encodeURIComponent(traceId)}/evidence`);
    },
    async getEvidencePreviewByUrl(previewUrl) {
      return request(previewUrl);
    },
    async getTraceHistory(traceId) {
      return request(`/internal/v1/traces/${encodeURIComponent(traceId)}/history?limit=30`);
    },
    async submitReviewFeedback({ dealId, periodId, itemId, actionId, outcome, note = "", metadata = {} }) {
      return request(
        `/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(periodId)}/review_queue/items/${encodeURIComponent(itemId)}:feedback`,
        {
          method: "POST",
          body: JSON.stringify({
            action_id: actionId,
            outcome,
            actor: "operator_ui",
            note,
            metadata: metadata || {},
          }),
        }
      );
    },
    async submitDraftWorkflowEvent({
      dealId,
      periodId,
      itemId,
      eventType,
      subject = "",
      draftText = "",
      metadata = {},
    }) {
      return request(
        `/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(periodId)}/review_queue/items/${encodeURIComponent(itemId)}:draft_event`,
        {
          method: "POST",
          body: JSON.stringify({
            event_type: eventType,
            actor: "operator_ui",
            subject,
            draft_text: draftText,
            metadata: metadata || {},
          }),
        }
      );
    },
    async getAnalystNote({ dealId, periodId, itemId }) {
      return request(
        `/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(periodId)}/review_queue/items/${encodeURIComponent(itemId)}/analyst_note`
      );
    },
    async upsertAnalystNote({
      dealId,
      periodId,
      itemId,
      actor = "operator_ui",
      subject = "",
      noteText = "",
      memoReady = false,
      exportReady = false,
      metadata = {},
    }) {
      return request(
        `/internal/v1/deals/${encodeURIComponent(dealId)}/periods/${encodeURIComponent(periodId)}/review_queue/items/${encodeURIComponent(itemId)}/analyst_note`,
        {
          method: "PUT",
          body: JSON.stringify({
            actor,
            subject,
            note_text: noteText,
            memo_ready: Boolean(memoReady),
            export_ready: Boolean(exportReady),
            metadata: metadata || {},
          }),
        }
      );
    },
  };
}
