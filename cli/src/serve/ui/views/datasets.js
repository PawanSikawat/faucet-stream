// Data Movement Catalog (#279): the Datasets browser — a filterable list of
// every dataset the server's pipelines have touched, plus a per-dataset
// detail view (schema timeline with diffs, recent volume, lineage edges).
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
