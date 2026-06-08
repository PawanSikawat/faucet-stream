import { api, toast } from "../api.js";
import { streamLogs } from "../sse.js";
import { navigate } from "../router.js";
import { fmtTime } from "./runs.js";

const TERMINAL = ["completed", "failed", "cancelled"];

export async function renderDetail(container, { id }) {
  let pollTimer = null;
  let logCtrl = null;

  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <button class="btn-ghost" id="back">← Runs</button>
        <div class="detail-actions">
          <button class="btn-ghost" id="cancel" hidden>Cancel</button>
          <button class="btn-danger" id="delete" hidden>Delete</button>
        </div>
      </div>
      <div id="detail-head"></div>
      <h2>Invocations</h2>
      <div id="invocations"></div>
      <h2>Logs</h2>
      <pre id="logs" class="logs"></pre>
    </div>`;

  container.querySelector("#back").onclick = () => navigate("#/runs");

  const logsEl = container.querySelector("#logs");
  function appendLog(text, cls = "") {
    const atBottom = logsEl.scrollHeight - logsEl.scrollTop - logsEl.clientHeight < 40;
    const span = document.createElement("span");
    if (cls) span.className = cls;
    span.textContent = text + "\n";
    logsEl.appendChild(span);
    if (atBottom) logsEl.scrollTop = logsEl.scrollHeight;
  }

  async function load() {
    let rec;
    try {
      rec = await api(`/v1/runs/${encodeURIComponent(id)}`);
    } catch (e) {
      container.querySelector("#detail-head").innerHTML = `<div class="empty">${e.message}</div>`;
      return;
    }
    renderHead(rec);
    const live = !TERMINAL.includes(rec.status);
    container.querySelector("#cancel").hidden = !(rec.status === "running" || rec.status === "queued");
    container.querySelector("#delete").hidden = !TERMINAL.includes(rec.status);
    if (live) {
      clearTimeout(pollTimer);
      pollTimer = setTimeout(load, 3000);
    }
  }

  function renderHead(rec) {
    const errors = rec.error ? `<div class="error-box">${escapeHtml(rec.error)}</div>` : "";
    container.querySelector("#detail-head").innerHTML = `
      <div class="detail-grid">
        <div><span class="pill pill-${rec.status}">${rec.status}</span></div>
        <div><b>${rec.name || rec.run_id}</b></div>
        <div>submitted ${fmtTime(rec.submitted_at)}</div>
        <div>started ${fmtTime(rec.started_at)}</div>
        <div>finished ${fmtTime(rec.finished_at)}</div>
        <div>${rec.records_written ?? 0} rows</div>
        ${rec.idempotency_key ? `<div>idem: ${escapeHtml(rec.idempotency_key)}</div>` : ""}
      </div>${errors}`;
    const inv = container.querySelector("#invocations");
    inv.innerHTML =
      `<table class="tbl"><thead><tr><th>row</th><th>parent key</th><th>rows</th><th>error</th></tr></thead><tbody>` +
      (rec.invocations || [])
        .map(
          (i) =>
            `<tr><td>${escapeHtml(i.row_id)}</td><td>${escapeHtml(i.parent_record_key || "—")}</td><td>${i.records_written ?? 0}</td><td>${escapeHtml(i.error || "")}</td></tr>`,
        )
        .join("") +
      `</tbody></table>`;
  }

  container.querySelector("#cancel").onclick = async () => {
    try { await api(`/v1/runs/${encodeURIComponent(id)}/cancel`, { method: "POST" }); toast("cancel requested"); load(); }
    catch (e) { toast(e.message, "error"); }
  };
  container.querySelector("#delete").onclick = async () => {
    try { await api(`/v1/runs/${encodeURIComponent(id)}`, { method: "DELETE" }); navigate("#/runs"); }
    catch (e) { toast(e.message, "error"); }
  };

  logCtrl = streamLogs(id, {
    onLog: (l) => appendLog(l),
    onTruncated: (m) => appendLog(`— ${m} —`, "log-truncated"),
    onEnd: () => appendLog("— end of logs —", "log-end"),
    onExpired: () => appendLog("— logs expired —", "log-end"),
    onError: (e) => appendLog(`— log stream error: ${e.message} —`, "log-truncated"),
  });

  await load();
  return () => {
    clearTimeout(pollTimer);
    logCtrl?.abort();
  };
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
