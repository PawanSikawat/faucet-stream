// Small shared helpers for the console UI.

/** Format an integer with thousands separators (e.g. 1500000 → "1,500,000").
 * Non-numbers pass through as their string form. Shared so every list/detail
 * renders counts identically. */
export function fmtInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toLocaleString("en-US") : String(v ?? "");
}

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
