import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtInt } from "../utils.js";
import { attachDatePicker } from "./date-picker.js";
import { formatTs } from "../tz.js";

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
        <input id="f-since" class="date-input" type="text" readonly placeholder="from…" />
        <input id="f-until" class="date-input" type="text" readonly placeholder="to…" />
        <button class="btn-ghost" id="f-apply">Apply</button>
        <button class="btn-ghost" id="f-refresh">↻</button>
      </div>
      <table class="ds-table runs-table">
        <thead><tr>
          <th>status</th><th>name</th><th>duration</th><th>rows</th><th>submitted</th>
        </tr></thead>
        <tbody id="runs-list"></tbody>
      </table>
      <button class="btn-ghost load-more" id="r-more" hidden>Load more</button>
    </div>`;

  const list = container.querySelector("#runs-list");
  container.querySelector("#r-submit").onclick = () => navigate("#/submit");
  attachDatePicker(container.querySelector("#f-since"));
  attachDatePicker(container.querySelector("#f-until"));

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
      if (!data.runs.length && reset)
        list.innerHTML = `<tr><td colspan="5" class="empty">No runs yet.</td></tr>`;
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
      since: container.querySelector("#f-since").dataset.value || "",
      until: container.querySelector("#f-until").dataset.value || "",
    };
    load(true);
  };
  container.querySelector("#f-refresh").onclick = () => load(true);
  container.querySelector("#r-more").onclick = () => load(false);

  await load(true);
  return () => clearTimeout(pollTimer); // teardown
}

function row(r) {
  const el = document.createElement("tr");
  el.className = "ds-tr";
  el.onclick = () => navigate(`#/runs/${r.run_id}`);
  const elapsed = r.elapsed_secs != null ? `${r.elapsed_secs.toFixed(1)}s` : "—";
  el.innerHTML = `
    <td><span class="pill pill-${r.status}">${r.status}</span></td>
    <td class="ds-uri"><div class="ds-uri-in">${escapeHtml(r.name || r.run_id)}</div></td>
    <td class="ds-meta ds-num">${elapsed}</td>
    <td class="ds-meta ds-num">${fmtInt(r.records_written ?? 0)}</td>
    <td class="ds-meta ds-time">${fmtTime(r.submitted_at)}</td>`;
  return el;
}

export function fmtTime(s) {
  return formatTs(s); // formats in the user's selected display timezone (default local)
}
