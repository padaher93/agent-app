function isTypingTarget(target) {
  if (!target) return false;
  const tag = String(target.tagName || "").toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") return true;
  return Boolean(target.isContentEditable);
}

export function attachKeyboardShortcuts({
  getState,
  onQueueMove,
  onPeriodMove,
  onRunPrimaryAction,
  focusDealSwitcher,
}) {
  function onKeyDown(event) {
    if (isTypingTarget(event.target)) return;
    const key = String(event.key || "");
    if (key === "j") {
      event.preventDefault();
      onQueueMove(1);
      return;
    }
    if (key === "k") {
      event.preventDefault();
      onQueueMove(-1);
      return;
    }
    if (key === "ArrowRight") {
      event.preventDefault();
      onPeriodMove(1);
      return;
    }
    if (key === "ArrowLeft") {
      event.preventDefault();
      onPeriodMove(-1);
      return;
    }
    if (key === "Enter") {
      const state = getState();
      if (state.loading.action) return;
      event.preventDefault();
      onRunPrimaryAction();
      return;
    }
    if (key === "/") {
      event.preventDefault();
      focusDealSwitcher();
    }
  }

  window.addEventListener("keydown", onKeyDown);
  return () => window.removeEventListener("keydown", onKeyDown);
}
