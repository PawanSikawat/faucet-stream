import { api, toast } from "../api.js";
import { renderSchemaForm } from "../schema-form.js";
import { navigate } from "../router.js";

export async function renderSubmit(container) {
  let catalog = { sources: [], sinks: [], transforms: [], state: [] };
  try { catalog = await api("/v1/schemas"); } catch (e) { toast(e.message, "error"); }

  container.innerHTML = `
    <div class="page">
      <div class="page-head"><h1>Submit a run</h1>
        <div class="mode-toggle">
          <button id="mode-guided" class="btn-ghost active">Guided</button>
          <button id="mode-editor" class="btn-ghost">Editor</button>
        </div>
      </div>
      <div id="guided" class="submit-mode"></div>
      <div id="editor" class="submit-mode" hidden>
        <textarea id="cfg" class="code" spellcheck="false" placeholder="version: 1
pipeline:
  source: { kind: rest, config: { ... } }
  sink: { kind: jsonl, config: { ... } }"></textarea>
      </div>
      <fieldset class="submit-opts">
        <label>name <input id="o-name" /></label>
        <label>format
          <select id="o-format"><option value="yaml">yaml</option><option value="json">json</option></select>
        </label>
        <label>timeout (s) <input id="o-timeout" type="number" /></label>
        <label><input id="o-doctor" type="checkbox" /> doctor first</label>
        <label>idempotency key <input id="o-idem" /></label>
      </fieldset>
      <div class="submit-actions">
        <button id="btn-check" class="btn-ghost">Check (doctor)</button>
        <button id="btn-run" class="btn-primary">Run</button>
      </div>
      <pre id="submit-out" class="submit-out" hidden></pre>
    </div>`;

  const guided = container.querySelector("#guided");
  const editor = container.querySelector("#editor");
  const cfgEl = container.querySelector("#cfg");
  const out = container.querySelector("#submit-out");

  // --- guided wizard ---
  let srcForm = null, sinkForm = null;
  const txForms = [];

  function selector(label, options) {
    return `<label>${label}<select>${options.map((o) => `<option value="${o.name}">${o.name}</option>`).join("")}</select></label>`;
  }

  guided.innerHTML = `
    <div class="wizard-step"><h3>Source</h3>${selector("kind", catalog.sources)}<div class="sf-host" id="src-form"></div></div>
    <div class="wizard-step"><h3>Sink</h3>${selector("kind", catalog.sinks)}<div class="sf-host" id="sink-form"></div></div>
    <div class="wizard-step"><h3>Transforms</h3><div id="tx-list"></div><button class="btn-ghost" id="tx-add">+ add transform</button></div>`;

  const srcSel = guided.querySelector("#guided .wizard-step:nth-child(1) select") || guided.querySelectorAll("select")[0];
  const sinkSel = guided.querySelectorAll("select")[1];

  async function loadForm(kind, name, host, set) {
    host.innerHTML = `<div class="empty">loading schema…</div>`;
    try {
      const schema = await api(`/v1/schemas/${kind}/${encodeURIComponent(name)}`);
      host.innerHTML = "";
      const form = renderSchemaForm(schema);
      host.appendChild(form.el);
      set(form);
    } catch (e) {
      host.innerHTML = `<div class="empty">${e.message}</div>`;
    }
  }

  if (catalog.sources.length) {
    srcSel.onchange = () => loadForm("source", srcSel.value, guided.querySelector("#src-form"), (f) => (srcForm = f));
    await loadForm("source", srcSel.value, guided.querySelector("#src-form"), (f) => (srcForm = f));
  }
  if (catalog.sinks.length) {
    sinkSel.onchange = () => loadForm("sink", sinkSel.value, guided.querySelector("#sink-form"), (f) => (sinkForm = f));
    await loadForm("sink", sinkSel.value, guided.querySelector("#sink-form"), (f) => (sinkForm = f));
  }

  guided.querySelector("#tx-add").onclick = async () => {
    const wrap = document.createElement("div");
    wrap.className = "wizard-tx";
    wrap.innerHTML = selector("transform", catalog.transforms);
    const host = document.createElement("div");
    host.className = "sf-host";
    wrap.appendChild(host);
    guided.querySelector("#tx-list").appendChild(wrap);
    const sel = wrap.querySelector("select");
    const entry = { kind: () => sel.value, form: null };
    const reload = () => loadForm("transform", sel.value, host, (f) => (entry.form = f));
    sel.onchange = reload;
    await reload();
    txForms.push(entry);
  };

  // Assemble the canonical config object from the wizard.
  function buildConfig() {
    const cfg = { version: 1, pipeline: {} };
    if (srcForm) cfg.pipeline.source = { kind: srcSel.value, config: srcForm.read() };
    if (sinkForm) cfg.pipeline.sink = { kind: sinkSel.value, config: sinkForm.read() };
    const tx = txForms
      .filter((t) => t.form)
      .map((t) => ({ type: t.kind(), ...t.form.read() }));
    if (tx.length) cfg.pipeline.transforms = tx;
    const name = container.querySelector("#o-name").value.trim();
    if (name) cfg.name = name;
    return cfg;
  }

  function toYaml(obj) {
    // Minimal, dependency-free YAML emitter for the preview/editor hand-off.
    const dump = (v, ind) => {
      const pad = "  ".repeat(ind);
      if (v === null || v === undefined) return "null";
      if (Array.isArray(v))
        return v.length ? "\n" + v.map((i) => `${pad}- ${dump(i, ind + 1).replace(/^\n/, "")}`).join("\n") : "[]";
      if (typeof v === "object")
        return "\n" + Object.entries(v).map(([k, val]) => `${pad}${k}: ${dump(val, ind + 1)}`).join("\n");
      if (typeof v === "string") return /[:#\n]/.test(v) ? JSON.stringify(v) : v;
      return String(v);
    };
    return Object.entries(obj).map(([k, v]) => `${k}: ${dump(v, 1)}`).join("\n").replace(/: \n/g, ":\n") + "\n";
  }

  // --- mode toggle ---
  const guidedBtn = container.querySelector("#mode-guided");
  const editorBtn = container.querySelector("#mode-editor");
  guidedBtn.onclick = () => { guided.hidden = false; editor.hidden = true; guidedBtn.classList.add("active"); editorBtn.classList.remove("active"); };
  editorBtn.onclick = () => {
    // Hand the assembled config to the raw editor for tweaks.
    if (!cfgEl.value.trim()) {
      cfgEl.value = toYaml(buildConfig());
      container.querySelector("#o-format").value = "yaml";
    }
    guided.hidden = true; editor.hidden = false; editorBtn.classList.add("active"); guidedBtn.classList.remove("active");
  };

  // --- request building ---
  function requestBody() {
    const isEditor = !editor.hidden;
    const format = container.querySelector("#o-format").value;
    const config = isEditor ? cfgEl.value : (format === "json" ? JSON.stringify(buildConfig()) : toYaml(buildConfig()));
    const body = { config, config_format: format };
    const name = container.querySelector("#o-name").value.trim();
    const timeout = container.querySelector("#o-timeout").value;
    const idem = container.querySelector("#o-idem").value.trim();
    if (name) body.name = name;
    if (timeout) body.timeout_secs = Number(timeout);
    if (container.querySelector("#o-doctor").checked) body.doctor_first = true;
    if (idem) body.idempotency_key = idem;
    return body;
  }

  container.querySelector("#btn-check").onclick = async () => {
    out.hidden = false;
    out.textContent = "running doctor…";
    const b = requestBody();
    try {
      const rep = await api("/v1/doctor", { method: "POST", body: { config: b.config, config_format: b.config_format } });
      out.textContent = "✓ all probes passed\n\n" + JSON.stringify(rep, null, 2);
    } catch (e) {
      out.textContent = `✗ ${e.message}\n\n` + (e.details ? JSON.stringify(e.details, null, 2) : "");
    }
  };

  container.querySelector("#btn-run").onclick = async () => {
    try {
      const resp = await api("/v1/runs", { method: "POST", body: requestBody() });
      toast(`run ${resp.run_id} ${resp.status}`);
      navigate(`#/runs/${resp.run_id}`);
    } catch (e) {
      out.hidden = false;
      const extra = e.retryAfter ? ` (retry in ${e.retryAfter}s)` : "";
      out.textContent = `✗ ${e.message}${extra}\n\n` + (e.details ? JSON.stringify(e.details, null, 2) : "");
      toast(e.message, "error");
    }
  };
}
