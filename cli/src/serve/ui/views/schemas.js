import { api, toast } from "../api.js";
import { escapeHtml } from "../utils.js";

export async function renderSchemas(container) {
  let catalog = { sources: [], sinks: [], transforms: [], state: [] };
  try { catalog = await api("/v1/schemas"); } catch (e) { toast(e.message, "error"); }

  const group = (title, kind, items) =>
    `<div class="schema-group"><h3>${title}</h3>${items
      .map((i) => `<button class="schema-item" data-kind="${kind}" data-name="${escapeHtml(i.name)}" title="${escapeHtml(i.description)}">${escapeHtml(i.name)}</button>`)
      .join("")}</div>`;

  container.innerHTML = `
    <div class="page schemas-page">
      <aside class="schema-list">
        ${group("Sources", "source", catalog.sources)}
        ${group("Sinks", "sink", catalog.sinks)}
        ${group("Transforms", "transform", catalog.transforms)}
        <div class="schema-group"><h3>State stores</h3>${catalog.state.map((s) => `<span class="tag">${escapeHtml(s)}</span>`).join("")}</div>
      </aside>
      <section id="schema-view" class="schema-view"><div class="empty">Select a connector to view its schema.</div></section>
    </div>`;

  const view = container.querySelector("#schema-view");
  container.querySelectorAll(".schema-item").forEach((btn) => {
    btn.onclick = async () => {
      container.querySelectorAll(".schema-item").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      view.innerHTML = `<div class="empty">loading…</div>`;
      try {
        const schema = await api(`/v1/schemas/${btn.dataset.kind}/${encodeURIComponent(btn.dataset.name)}`);
        view.innerHTML = `<h2>${escapeHtml(btn.dataset.kind)}: ${escapeHtml(btn.dataset.name)}</h2>` + fieldTable(schema) +
          `<details class="raw"><summary>raw JSON Schema</summary><pre>${escapeHtml(JSON.stringify(schema, null, 2))}</pre></details>`;
      } catch (e) {
        view.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      }
    };
  });
}

function fieldTable(schema) {
  const props = schema.properties || {};
  const req = new Set(schema.required || []);
  const rows = Object.entries(props)
    .map(([name, p]) => {
      const type = p.type || (p.$ref ? p.$ref.split("/").pop() : p.enum ? "enum" : p.oneOf || p.anyOf ? "union" : "object");
      const def = p.default !== undefined ? `<code>${escapeHtml(JSON.stringify(p.default))}</code>` : "";
      return `<tr><td>${escapeHtml(name)}${req.has(name) ? " *" : ""}</td><td>${escapeHtml(String(type))}</td><td>${def}</td><td>${escapeHtml(firstSentence(p.description || ""))}</td></tr>`;
    })
    .join("");
  return `<table class="tbl"><thead><tr><th>field</th><th>type</th><th>default</th><th>description</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function firstSentence(s) { return s ? s.split(/\.\s/)[0].slice(0, 160) : ""; }
