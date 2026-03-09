export function createStore(initialState = {}) {
  const listeners = new Set();
  let state = {
    workspaceId: "ws_default",
    sessionToken: "",
    deals: [],
    periods: [],
    currentDealId: "",
    currentPeriodId: "",
    baselinePeriodId: "",
    includeResolved: false,
    queuePayload: null,
    selectedItemId: "",
    loading: {
      deals: false,
      queue: false,
      action: false,
    },
    errors: {
      global: "",
      queue: "",
    },
    traceEvidenceCache: new Map(),
    traceHistoryCache: new Map(),
    activeDraft: null,
    activeAnalystNote: null,
    ...initialState,
  };

  function notify() {
    for (const listener of listeners) {
      listener(state);
    }
  }

  return {
    getState() {
      return state;
    },
    setState(patch) {
      state = {
        ...state,
        ...patch,
        loading: {
          ...state.loading,
          ...(patch.loading || {}),
        },
        errors: {
          ...state.errors,
          ...(patch.errors || {}),
        },
      };
      notify();
    },
    mutate(mutator) {
      const next = mutator(state);
      if (next) {
        state = {
          ...state,
          ...next,
          loading: {
            ...state.loading,
            ...(next.loading || {}),
          },
          errors: {
            ...state.errors,
            ...(next.errors || {}),
          },
        };
      }
      notify();
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}
