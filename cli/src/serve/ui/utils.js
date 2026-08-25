// Small shared helpers for the console UI.

/** Escape a string for safe interpolation into innerHTML. */
export function escapeHtml(s) {
  return String(s ?? "").replace(
    /[&<>"]/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c],
  );
}

/**
 * Render the minimal inline markdown used in connector descriptions — `**bold**`
 * and `` `code` `` — as safe HTML. The text is HTML-escaped first, then our own
 * tags are injected, so it never introduces markup from the source string.
 */
export function mdInline(s) {
  return escapeHtml(s)
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/`([^`]+)`/g, "<code>$1</code>");
}
