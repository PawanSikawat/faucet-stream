import { api, toast } from "../api.js";
import { streamLogs } from "../sse.js";
import { navigate } from "../router.js";
import { fmtTime } from "./runs.js";
import { escapeHtml } from "../utils.js";

const TERMINAL = ["completed", "failed", "cancelled"];

// Location-driven DLQ panel: inspect / replay / discard envelopes at a
// server-local path. The DLQ is not run-scoped, so the location is entered
// explicitly. inspect → DlqRead (viewer); replay/discard → DlqManage (operator).
function wireDlqPanel(container) {
  const $ = (id) => container.querySelector(id);
  const resultEl = $("#dlq-result");
  const loc = () => $("#dlq-location").value.trim();
  const reason = () => $("#dlq-reason").value || undefined;

  function requireLocation() {
    if (loc()) return true;
    toast("enter a DLQ location", "error");
    return false;
  }

  $("#dlq-inspect").onclick = async () => {
    if (!requireLocation()) return;
    try {
      const s = await api("/v1/dlq/inspect", { method: "POST", body: { location: loc(), reason: reason(), limit: 5 } });
      renderInspect(resultEl, s);
    } catch (e) {
      resultEl.innerHTML = `<div class="error-box">${escapeHtml(e.message)}</div>`;
    }
  };

  $("#dlq-discard").onclick = async () => {
    if (!requireLocation()) return;
    const del = $("#dlq-delete").checked;
    if (!confirm(`${del ? "Delete" : "Archive"} matching envelopes at ${loc()}?`)) return;
    try {
      const o = await api("/v1/dlq/discard", { method: "POST", body: { location: loc(), reason: reason(), delete: del } });
      toast(`discarded ${o.discarded} envelope(s) across ${o.files_rewritten} file(s)`);
      resultEl.innerHTML = `<pre class="dlq-json">${escapeHtml(JSON.stringify(o, null, 2))}</pre>`;
    } catch (e) {
      toast(e.message, "error");
    }
  };

  $("#dlq-replay").onclick = async () => {
    if (!requireLocation()) return;
    const config = $("#dlq-config").value.trim();
    if (!config) { toast("paste a config to replay through", "error"); return; }
    const dry = $("#dlq-dryrun").checked;
    try {
      const o = await api("/v1/dlq/replay", {
        method: "POST",
        body: { config, config_format: "yaml", from: loc(), reason: reason(), dry_run: dry },
      });
      toast(dry ? `dry-run: ${o.candidates} candidate(s)` : `replayed ${o.records_written} record(s)`);
      resultEl.innerHTML = `<pre class="dlq-json">${escapeHtml(JSON.stringify(o, null, 2))}</pre>`;
    } catch (e) {
      toast(e.message, "error");
    }
  };
}

function renderInspect(el, s) {
  const rows = (obj) =>
    Object.entries(obj || {})
      .map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td>${v}</td></tr>`)
      .join("") || `<tr><td colspan="2">—</td></tr>`;
  const sample = (s.sample || [])
    .map(
      (e) =>
        `<li><span class="pill">${escapeHtml(e.reason || "?")}</span> ${escapeHtml(e.error_kind || "")}: ${escapeHtml(e.error_message || "")}<pre class="dlq-json">${escapeHtml(JSON.stringify(e.payload))}</pre></li>`,
    )
    .join("");
  el.innerHTML = `
    <div class="dlq-summary">
      <div>${s.total_envelopes} envelope(s) · ${s.files_read} file(s) · ${s.malformed} malformed · ${s.non_envelope} non-envelope</div>
      <div class="dlq-tables">
        <table class="tbl"><thead><tr><th>reason</th><th>count</th></tr></thead><tbody>${rows(s.by_reason)}</tbody></table>
        <table class="tbl"><thead><tr><th>error kind</th><th>count</th></tr></thead><tbody>${rows(s.by_error_kind)}</tbody></table>
      </div>
      <ul class="dlq-sample">${sample}</ul>
    </div>`;
}

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
      <h2>Dead-letter queue</h2>
      <div class="dlq-panel">
        <p class="dlq-hint">
          Inspect, replay, or discard DLQ envelopes at a server-local location
          (a <code>.jsonl</code> file, a directory of <code>*.jsonl</code>, or a glob).
        </p>
        <div class="dlq-row">
          <input id="dlq-location" type="text" placeholder="./dlq/dead-letters.jsonl" class="dlq-input" />
          <select id="dlq-reason">
            <option value="">any reason</option>
            <option value="partial">partial</option>
            <option value="dlq_all">dlq_all</option>
            <option value="quality">quality</option>
            <option value="schema_drift">schema_drift</option>
            <option value="contract">contract</option>
          </select>
          <button class="btn-ghost" id="dlq-inspect">Inspect</button>
          <button class="btn-ghost" id="dlq-discard">Discard</button>
          <label class="dlq-check"><input type="checkbox" id="dlq-delete" /> delete (no archive)</label>
        </div>
        <div id="dlq-result"></div>
        <details class="dlq-replay">
          <summary>Replay through a config</summary>
          <textarea id="dlq-config" rows="6" placeholder="paste the pipeline config (YAML) whose sink/transforms/quality/contract to replay through"></textarea>
          <div class="dlq-row">
            <label class="dlq-check"><input type="checkbox" id="dlq-dryrun" checked /> dry-run</label>
            <button class="btn-ghost" id="dlq-replay">Replay</button>
          </div>
        </details>
      </div>
    </div>`;

  container.querySelector("#back").onclick = () => navigate("#/runs");
  wireDlqPanel(container);

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
      container.querySelector("#detail-head").innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      // Keep polling through a transient (network / 5xx) failure; stop on 4xx (e.g. a deleted run).
      if (e.status === undefined || e.status >= 500) {
        clearTimeout(pollTimer);
        pollTimer = setTimeout(load, 3000);
      }
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
        <div><b>${escapeHtml(rec.name || rec.run_id)}</b></div>
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
