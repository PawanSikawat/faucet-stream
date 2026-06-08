import { api, toast } from "../api.js";
import { navigate } from "../router.js";

const STATUSES = ["", "queued", "running", "completed", "failed", "cancelled"];

export async function renderRuns(container) {
  let cursor = null;
  let filters = { status: "", name: "", since: "", until: "" };
  let pollTimer = null;

  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <h1>Runs</h1>
        <button class="btn-primary" id="r-submit">+ Submit run</button>
      </div>
      <div class="filters">
        <select id="f-status">${STATUSES.map((s) => `<option value="${s}">${s || "all statuses"}</option>`).join("")}</select>
        <input id="f-name" placeholder="name" />
        <input id="f-since" type="datetime-local" />
        <input id="f-until" type="datetime-local" />
        <button class="btn-ghost" id="f-apply">Apply</button>
        <button class="btn-ghost" id="f-refresh">↻</button>
      </div>
      <div id="runs-list" class="runs-list"></div>
      <button class="btn-ghost" id="r-more" hidden>Load more</button>
    </div>`;

  const list = container.querySelector("#runs-list");
  container.querySelector("#r-submit").onclick = () => navigate("#/submit");

  function query(reset) {
    if (reset) cursor = null;
    const p = new URLSearchParams();
    if (filters.status) p.set("status", filters.status);
    if (filters.name) p.set("name", filters.name);
    if (filters.since) p.set("since", new Date(filters.since).toISOString());
    if (filters.until) p.set("until", new Date(filters.until).toISOString());
    p.set("limit", "50");
    if (cursor) p.set("cursor", cursor);
    return p.toString();
  }

  async function load(reset) {
    try {
      const data = await api(`/v1/runs?${query(reset)}`);
      if (reset) list.innerHTML = "";
      if (!data.runs.length && reset) list.innerHTML = `<div class="empty">No runs yet.</div>`;
      for (const r of data.runs) list.appendChild(row(r));
      cursor = data.next_cursor || null;
      container.querySelector("#r-more").hidden = !cursor;
      maybePoll(data.runs);
    } catch (e) {
      toast(e.message, "error");
    }
  }

  function maybePoll(runs) {
    const active = runs.some((r) => r.status === "running" || r.status === "queued");
    clearTimeout(pollTimer);
    if (active) pollTimer = setTimeout(() => load(true), 3000);
  }

  container.querySelector("#f-apply").onclick = () => {
    filters = {
      status: container.querySelector("#f-status").value,
      name: container.querySelector("#f-name").value.trim(),
      since: container.querySelector("#f-since").value,
      until: container.querySelector("#f-until").value,
    };
    load(true);
  };
  container.querySelector("#f-refresh").onclick = () => load(true);
  container.querySelector("#r-more").onclick = () => load(false);

  await load(true);
  return () => clearTimeout(pollTimer); // teardown
}

function row(r) {
  const el = document.createElement("div");
  el.className = "run-row";
  el.onclick = () => navigate(`#/runs/${r.run_id}`);
  const elapsed = r.elapsed_secs != null ? `${r.elapsed_secs.toFixed(1)}s` : "—";
  el.innerHTML = `
    <span class="pill pill-${r.status}">${r.status}</span>
    <span class="run-name">${r.name || r.run_id}</span>
    <span class="run-meta">${elapsed}</span>
    <span class="run-meta">${r.records_written ?? 0} rows</span>
    <span class="run-meta run-time">${fmtTime(r.submitted_at)}</span>`;
  return el;
}

export function fmtTime(s) {
  if (!s) return "—";
  try { return new Date(s).toLocaleString(); } catch { return s; }
}
