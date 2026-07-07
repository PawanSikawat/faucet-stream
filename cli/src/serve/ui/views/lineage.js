// Data Movement Catalog (#279): the Lineage graph — datasets as nodes laid
// out in topological layers (sources left, sinks right), edges as curves.
// Pure SVG, no dependencies. Clicking a node opens its dataset detail.
import { api } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml } from "../utils.js";
import { catalogUnavailable } from "./datasets.js";

export async function renderLineage(container, params = {}) {
  container.innerHTML = `<div class="empty">loading…</div>`;
  let data;
  try {
    const p = new URLSearchParams();
    if (params.root) {
      p.set("root", params.root);
      p.set("depth", "8");
    }
    data = await api(`/v1/catalog/lineage?${p}`);
  } catch (e) {
    container.innerHTML = `<div class="empty">${
      catalogUnavailable(e)
        ? "The Data Movement Catalog endpoints are not available on this server " +
          "(faucet was built without the `catalog` feature)."
        : escapeHtml(e.message)
    }</div>`;
    return;
  }

  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <h1>Lineage${params.root ? " (rooted)" : ""}</h1>
        <button class="btn-ghost" id="l-datasets">← Datasets</button>
        ${params.root ? `<button class="btn-ghost" id="l-all">Whole graph</button>` : ""}
      </div>
      <div id="graph" class="lineage-graph"></div>
    </div>`;
  container.querySelector("#l-datasets").onclick = () => navigate("#/catalog");
  const all = container.querySelector("#l-all");
  if (all) all.onclick = () => navigate("#/lineage");

  const graph = container.querySelector("#graph");
  if (!data.edges.length) {
    graph.innerHTML = `<div class="empty">No lineage edges recorded yet — run a pipeline first.</div>`;
    return;
  }
  graph.appendChild(buildSvg(layout(data.edges), params.root));
}

/** Compute topological layers: a node's layer is 1 + max layer of its inputs
 * (cycle-safe via bounded iteration). Returns { nodes, edges }. */
export function layout(edges) {
  const nodes = new Map(); // id → { id, uri, layer }
  for (const e of edges) {
    if (!nodes.has(e.src_id)) nodes.set(e.src_id, { id: e.src_id, uri: e.src_uri, layer: 0 });
    if (!nodes.has(e.dst_id)) nodes.set(e.dst_id, { id: e.dst_id, uri: e.dst_uri, layer: 0 });
  }
  // Relax layers |V| times max (cycles just stop improving).
  for (let i = 0; i < nodes.size; i++) {
    let changed = false;
    for (const e of edges) {
      const s = nodes.get(e.src_id);
      const d = nodes.get(e.dst_id);
      if (d.layer < s.layer + 1 && s.layer + 1 <= nodes.size) {
        d.layer = s.layer + 1;
        changed = true;
      }
    }
    if (!changed) break;
  }
  // Row index within each layer, ordered by URI for stability.
  const byLayer = new Map();
  for (const n of [...nodes.values()].sort((a, b) => a.uri.localeCompare(b.uri))) {
    const rows = byLayer.get(n.layer) || [];
    n.row = rows.length;
    rows.push(n);
    byLayer.set(n.layer, rows);
  }
  return { nodes, edges };
}

const NODE_W = 260;
const NODE_H = 44;
const GAP_X = 120;
const GAP_Y = 24;

function buildSvg({ nodes, edges }, rootId) {
  const ns = "http://www.w3.org/2000/svg";
  const layers = Math.max(...[...nodes.values()].map((n) => n.layer)) + 1;
  const rows = Math.max(...[...nodes.values()].map((n) => n.row)) + 1;
  const width = layers * NODE_W + (layers - 1) * GAP_X + 32;
  const height = rows * (NODE_H + GAP_Y) + 32;
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("class", "lineage-svg");

  const x = (n) => 16 + n.layer * (NODE_W + GAP_X);
  const y = (n) => 16 + n.row * (NODE_H + GAP_Y);

  for (const e of edges) {
    const s = nodes.get(e.src_id);
    const d = nodes.get(e.dst_id);
    const x1 = x(s) + NODE_W;
    const y1 = y(s) + NODE_H / 2;
    const x2 = x(d);
    const y2 = y(d) + NODE_H / 2;
    const mid = (x1 + x2) / 2;
    const path = document.createElementNS(ns, "path");
    path.setAttribute("d", `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`);
    path.setAttribute("class", "lineage-edge");
    const title = document.createElementNS(ns, "title");
    title.textContent = `${e.pipeline} (row ${e.row}) — ${e.runs} run(s), ${e.last_records} row(s) last`;
    path.appendChild(title);
    svg.appendChild(path);
  }

  for (const n of nodes.values()) {
    const g = document.createElementNS(ns, "g");
    g.setAttribute("class", `lineage-node${n.id === rootId ? " lineage-root" : ""}`);
    g.setAttribute("transform", `translate(${x(n)}, ${y(n)})`);
    g.style.cursor = "pointer";
    g.onclick = () => navigate(`#/catalog/${n.id}`);
    const rect = document.createElementNS(ns, "rect");
    rect.setAttribute("width", NODE_W);
    rect.setAttribute("height", NODE_H);
    rect.setAttribute("rx", 8);
    const label = document.createElementNS(ns, "text");
    label.setAttribute("x", 12);
    label.setAttribute("y", NODE_H / 2 + 4);
    label.textContent = shorten(n.uri, 34);
    const title = document.createElementNS(ns, "title");
    title.textContent = n.uri;
    g.appendChild(rect);
    g.appendChild(label);
    g.appendChild(title);
    svg.appendChild(g);
  }
  return svg;
}

function shorten(s, max) {
  return s.length <= max ? s : `…${s.slice(s.length - max + 1)}`;
}
