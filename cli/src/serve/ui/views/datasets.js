// Data Movement Catalog (#279): the Datasets browser — a filterable list of
// every dataset the server's pipelines have touched, plus a per-dataset
// detail view (schema timeline with diffs, recent volume, lineage edges).
//
// Also the control surface for local-output retention (#587/#588). Cleanup of
// *data artifacts* belongs next to the data artifacts, not on the Runs tab,
// which is about execution history. The model the UI has to convey:
// **data artifacts are disposable; run history is durable** — cleaning an output
// removes the file and leaves the run record alone, so a cleaned output renders
// as `expired`, never as a broken row.
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml } from "../utils.js";
import { fmtTime } from "./runs.js";

const CATALOG_MISSING =
  "The Data Movement Catalog endpoints are not available on this server " +
  "(faucet was built without the `catalog` feature).";

export function catalogUnavailable(e) {
  // A route that is not wired returns a bare 404 (no ApiError envelope code).
  return e && e.status === 404 && !e.code;
}

export async function renderDatasets(container) {
  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <h1>Datasets</h1>
        <button class="btn-ghost" id="d-lineage">Lineage graph →</button>
      </div>
      <div class="filters">
        <input id="f-kind" placeholder="kind (csv, postgres, …)" />
        <input id="f-q" placeholder="search URI" />
        <button class="btn-ghost" id="f-apply">Apply</button>
        <button class="btn-ghost" id="f-refresh">↻</button>
      </div>
      <div id="ds-list" class="runs-list"></div>
      <button class="btn-ghost" id="d-more" hidden>Load more</button>
      <div id="lo-section"></div>
    </div>`;

  const list = container.querySelector("#ds-list");
  container.querySelector("#d-lineage").onclick = () => navigate("#/lineage");
  let cursor = null;

  async function load(reset) {
    if (reset) cursor = null;
    const p = new URLSearchParams();
    const kind = container.querySelector("#f-kind").value.trim();
    const q = container.querySelector("#f-q").value.trim();
    if (kind) p.set("kind", kind);
    if (q) p.set("q", q);
    p.set("limit", "50");
    if (cursor) p.set("cursor", cursor);
    try {
      const data = await api(`/v1/catalog/datasets?${p}`);
      if (reset) list.innerHTML = "";
      if (!data.datasets.length && reset) {
        list.innerHTML = `<div class="empty">No datasets catalogued yet — run a pipeline first.</div>`;
      }
      for (const d of data.datasets) list.appendChild(row(d));
      cursor = data.next_cursor || null;
      container.querySelector("#d-more").hidden = !cursor;
    } catch (e) {
      if (catalogUnavailable(e)) list.innerHTML = `<div class="empty">${CATALOG_MISSING}</div>`;
      else toast(e.message, "error");
    }
  }

  container.querySelector("#f-apply").onclick = () => load(true);
  container.querySelector("#f-refresh").onclick = () => load(true);
  container.querySelector("#d-more").onclick = () => load(false);
  await load(true);
  await renderLocalOutputs(container.querySelector("#lo-section"), {});
}

// ── Local outputs (#587/#588) ───────────────────────────────────────────────

const LOCAL_OUTPUTS_MISSING =
  "Local-output retention is not available on this server " +
  "(faucet was built without the `catalog` feature).";

/** The confirm text for a scope that ignores retention windows. */
const CLEAN_ALL_PROMPT = (what) =>
  `Delete ${what}, including files still inside their retention window?\n\n` +
  "Only files faucet created are deleted. Run history, catalog entries, " +
  "and lineage are not touched.";

/** State labels + one-line meanings, so the UI never shows a bare word. */
const STATE_HINT = {
  present: "on disk",
  expired: "cleaned — the run record is kept",
  external: "faucet wrote this file but did not create it, so it is never cleaned",
};

/**
 * Render the "Local outputs" panel into `host`.
 *
 * `scope.datasetId` narrows it to one dataset (the detail view); omitted lists
 * everything (the browser). Destructive controls are rendered only when the
 * server says the caller holds the manage scope — a viewer sees the list and no
 * buttons, rather than buttons that can only 403.
 */
export async function renderLocalOutputs(host, scope = {}) {
  if (!host) return;
  const datasetId = scope.datasetId || null;
  let showExpired = false;

  host.innerHTML = `
    <h2 class="lo-head">Local outputs</h2>
    <div id="lo-body"><div class="empty">loading…</div></div>`;
  const body = host.querySelector("#lo-body");

  async function load() {
    const p = new URLSearchParams();
    if (datasetId) p.set("dataset_id", datasetId);
    if (showExpired) p.set("include_expired", "true");
    let data;
    try {
      data = await api(`/v1/local-outputs?${p}`);
    } catch (e) {
      body.innerHTML = `<div class="empty">${
        catalogUnavailable(e) ? LOCAL_OUTPUTS_MISSING : escapeHtml(e.message)
      }</div>`;
      return;
    }
    paint(data);
  }

  function paint(data) {
    const { outputs, retention_days: retention, gc_enabled: gcOn, can_manage: canManage } = data;
    body.innerHTML = `
      <div class="lo-bar">
        <span class="run-meta">${outputs.length} tracked${
          gcOn
            ? ` · auto-cleaned after ${retention} day${retention === 1 ? "" : "s"}`
            : " · automatic cleanup disabled"
        }</span>
        <label class="lo-toggle"><input type="checkbox" id="lo-expired" ${
          showExpired ? "checked" : ""
        } /> show cleaned</label>
        ${
          canManage
            ? `<span class="lo-purge">
                 <input type="number" id="lo-days" min="0" value="${retention}" />
                 <button class="btn-ghost" id="lo-purge-btn">Purge older than N days</button>
               </span>
               <button class="btn-danger" id="lo-all">${
                 datasetId ? "Clean this dataset's outputs" : "Clean all local outputs"
               }</button>`
            : ""
        }
        <button class="btn-ghost" id="lo-refresh">↻</button>
      </div>
      <div class="lo-list">${
        outputs.length
          ? outputs.map((o) => outputRow(o, canManage)).join("")
          : `<div class="empty">No local output files tracked${
              datasetId ? " for this dataset" : ""
            } yet — run a pipeline with a jsonl, csv, or parquet sink.</div>`
      }</div>`;

    body.querySelector("#lo-expired").onchange = (e) => {
      showExpired = e.target.checked;
      load();
    };
    body.querySelector("#lo-refresh").onclick = () => load();

    if (!canManage) return;
    body.querySelector("#lo-purge-btn").onclick = () => {
      const days = Number(body.querySelector("#lo-days").value);
      if (!Number.isFinite(days) || days < 0) {
        toast("Enter a number of days (0 or more).", "error");
        return;
      }
      // The typed window *is* the intent, so no dialog — except for 0, which
      // means "everything" and clears the same gate as "clean all".
      if (days === 0 && !confirm(CLEAN_ALL_PROMPT("every tracked local output"))) return;
      cleanup(
        { older_than_days: days, confirm: days === 0 },
        `purged outputs older than ${days} day(s)`,
      );
    };
    body.querySelector("#lo-all").onclick = () => {
      // "Clean all" removes files that are still inside their retention window,
      // so it is never a bare one-click.
      const what = datasetId ? "this dataset's tracked outputs" : "every tracked local output";
      if (!confirm(CLEAN_ALL_PROMPT(what))) return;
      // `confirm: true` is what the server's own gate requires for a scope that
      // can delete files still inside their retention window — the dialog above
      // is what earns it. A scripted caller has to opt in deliberately.
      cleanup(
        datasetId ? { dataset_id: datasetId } : { all: true, confirm: true },
        "cleaned local outputs",
      );
    };

    body.querySelectorAll(".lo-del").forEach((btn) => {
      btn.onclick = async () => {
        btn.disabled = true;
        try {
          report(await api(`/v1/local-outputs/${encodeURIComponent(btn.dataset.id)}`, {
            method: "DELETE",
          }));
          await load();
        } catch (e) {
          toast(e.message, "error");
          btn.disabled = false;
        }
      };
    });
  }

  async function cleanup(payload, ok) {
    try {
      report(await api("/v1/local-outputs/cleanup", { method: "POST", body: payload }), ok);
      await load();
    } catch (e) {
      toast(e.message, "error");
    }
  }

  /**
   * Turn a sweep report into one toast. A refusal is a successful request with
   * `deleted: 0`, so "nothing happened" must always come with the reason —
   * otherwise the button looks broken when it was in fact protecting a file.
   */
  function report(rep, fallback) {
    if (rep.deleted > 0) {
      toast(`${fallback || "cleaned"}: ${rep.deleted} file(s), ${fmtBytes(rep.bytes)} reclaimed`);
    }
    const skipped = rep.outputs.filter((o) => o.skipped);
    if (!rep.deleted && !skipped.length) {
      toast("Nothing to clean.");
    }
    for (const o of skipped.slice(0, 3)) {
      toast(`${o.path}: ${skipReason(o)}`, o.skipped === "delete_failed" ? "error" : "info");
    }
    if (skipped.length > 3) toast(`…and ${skipped.length - 3} more skipped.`);
  }

  await load();
}

function outputRow(o, canManage) {
  return `
    <div class="run-row lo-row lo-${escapeHtml(o.state)}">
      <span class="pill">${escapeHtml(o.kind)}</span>
      <span class="run-name mono" title="${escapeHtml(o.path)}">${escapeHtml(o.path)}</span>
      <span class="pill lo-state" title="${escapeHtml(STATE_HINT[o.state] || "")}">${escapeHtml(
        o.state,
      )}</span>
      <span class="run-meta">${escapeHtml(fmtAge(o.age_secs))}</span>
      <span class="run-meta run-time" title="last written">${fmtTime(o.last_written_at)}</span>
      ${
        canManage && o.state === "present"
          ? `<button class="btn-danger lo-del" data-id="${escapeHtml(o.id)}">Delete now</button>`
          : `<span class="run-meta"></span>`
      }
    </div>`;
}

/** Why a file was left alone, in the user's words rather than the enum's. */
function skipReason(o) {
  switch (o.skipped) {
    case "pre_existing":
      return "not deleted — faucet wrote this file but did not create it";
    case "already_deleted":
      return "already cleaned";
    case "not_on_disk":
      return "already gone from disk — marked cleaned";
    case "in_flight":
      return "a run is still writing it — will be retried";
    case "delete_failed":
      return `could not delete — ${o.error || "unknown error"}`;
    default:
      return "skipped";
  }
}

function fmtAge(secs) {
  if (secs < 60) return "just now";
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m old`;
  const hours = Math.floor(mins / 60);
  if (hours < 48) return `${hours}h old`;
  return `${Math.floor(hours / 24)}d old`;
}

function fmtBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GiB`;
}

function row(d) {
  const el = document.createElement("div");
  el.className = "run-row";
  el.onclick = () => navigate(`#/catalog/${d.id}`);
  el.innerHTML = `
    <span class="pill">${escapeHtml(d.kind)}</span>
    <span class="run-name mono">${escapeHtml(d.uri)}</span>
    <span class="run-meta">${escapeHtml(d.roles.join("+"))}</span>
    <span class="run-meta">${d.runs} run${d.runs === 1 ? "" : "s"}</span>
    <span class="run-meta">${d.last_records} rows</span>
    <span class="run-meta run-time" title="last success">${fmtTime(d.last_success)}</span>`;
  return el;
}

export async function renderDatasetDetail(container, params) {
  container.innerHTML = `<div class="empty">loading…</div>`;
  let d;
  try {
    d = await api(`/v1/catalog/datasets/${encodeURIComponent(params.id)}`);
  } catch (e) {
    container.innerHTML = `<div class="empty">${
      catalogUnavailable(e) ? CATALOG_MISSING : escapeHtml(e.message)
    }</div>`;
    return;
  }

  const maxRows = Math.max(1, ...d.stats.map((s) => s.records));
  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <h1 class="mono dataset-title">${escapeHtml(d.uri)}</h1>
      </div>
      <div class="detail-grid">
        <div><label>Kind</label><span class="pill">${escapeHtml(d.kind)}</span></div>
        <div><label>Roles</label>${escapeHtml(d.roles.join(", "))}</div>
        <div><label>Pipeline</label>${escapeHtml(d.pipeline)}</div>
        <div><label>Runs</label>${d.runs}</div>
        <div><label>Rows (last / total)</label>${d.last_records} / ${d.total_records}</div>
        <div><label>First seen</label>${fmtTime(d.first_seen)}</div>
        <div><label>Last success</label>${fmtTime(d.last_success)}</div>
        <div><label>Id</label><span class="mono">${escapeHtml(d.id)}</span></div>
      </div>

      <h2>Volume (recent runs)</h2>
      <div class="volume-bars">
        ${
          d.stats.length
            ? d.stats
                .slice()
                .reverse()
                .map(
                  (s) =>
                    `<div class="volume-bar" title="${escapeHtml(`${s.records} rows — ${fmtTime(s.recorded_at)} (run ${s.run_id})`)}"
                      style="height:${Math.max(4, Math.round((s.records / maxRows) * 64))}px"></div>`,
                )
                .join("")
            : `<div class="empty">no volume points yet</div>`
        }
      </div>

      <h2>Schema timeline</h2>
      <div id="timeline"></div>

      <div id="lo-section"></div>

      <h2>Lineage</h2>
      <div class="edge-lists">
        <div>
          <h3>Upstream</h3>
          ${edgeList(d.upstream, (e) => e.src_id, (e) => e.src_uri)}
        </div>
        <div>
          <h3>Downstream</h3>
          ${edgeList(d.downstream, (e) => e.dst_id, (e) => e.dst_uri)}
        </div>
      </div>
      <button class="btn-ghost" id="d-graph">View in lineage graph →</button>
    </div>`;

  container.querySelector("#d-graph").onclick = () => navigate(`#/lineage/${d.id}`);
  // This dataset's own local files, with the same controls scoped to it.
  await renderLocalOutputs(container.querySelector("#lo-section"), { datasetId: d.id });
  container.querySelectorAll(".edge-link").forEach((a) => {
    a.onclick = () => navigate(`#/catalog/${a.dataset.id}`);
  });

  const timeline = container.querySelector("#timeline");
  if (!d.schema_timeline.length) {
    timeline.innerHTML = `<div class="empty">no schema observed yet</div>`;
  }
  for (const v of d.schema_timeline.slice().reverse()) {
    timeline.appendChild(versionCard(v));
  }
}

function edgeList(edges, idOf, uriOf) {
  if (!edges.length) return `<div class="empty">none</div>`;
  return edges
    .map(
      (e) =>
        `<div class="run-row edge-link" data-id="${escapeHtml(idOf(e))}">
          <span class="run-name mono">${escapeHtml(uriOf(e))}</span>
          <span class="run-meta">${e.runs} run${e.runs === 1 ? "" : "s"}</span>
          <span class="run-meta">${e.last_records} rows</span>
        </div>`,
    )
    .join("");
}

function versionCard(v) {
  const el = document.createElement("details");
  el.className = "schema-version";
  const cols = Object.keys((v.schema && v.schema.properties) || {});
  el.innerHTML = `
    <summary>
      <b>v${v.version}</b>
      <span class="run-meta">${fmtTime(v.recorded_at)}</span>
      <span class="run-meta">${cols.length} column${cols.length === 1 ? "" : "s"}</span>
      ${diffBadges(v.diff)}
    </summary>
    <pre class="mono schema-json">${escapeHtml(JSON.stringify(v.schema, null, 2))}</pre>`;
  return el;
}

function diffBadges(diff) {
  if (!diff) return "";
  const name = (c) => (typeof c === "string" ? c : c.column);
  const badge = (cls, sym, items) =>
    (items || [])
      .map((c) => `<span class="pill diff-${cls}">${sym}${escapeHtml(name(c))}</span>`)
      .join("");
  return (
    badge("added", "+", diff.added) +
    badge("widened", "~", diff.widened) +
    badge("changed", "!", diff.changed) +
    badge("removed", "−", diff.removed)
  );
}
