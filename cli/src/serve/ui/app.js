import { token, onUnauthorized, authOk } from "./api.js";
import { startRouter, navigate } from "./router.js";
import { renderRuns } from "./views/runs.js";
import { renderDetail } from "./views/detail.js";
import { renderSubmit } from "./views/submit.js";
import { renderSchemas } from "./views/schemas.js";
import { route } from "./router.js";

// --- theme ---
const THEME_KEY = "faucet.theme";
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  localStorage.setItem(THEME_KEY, t);
}
function initTheme() {
  applyTheme(localStorage.getItem(THEME_KEY) || "auto");
}

// --- token modal ---
function openTokenModal() {
  const modal = document.getElementById("token-modal");
  modal.classList.add("open");
  const input = document.getElementById("token-input");
  input.value = token.get();
  input.focus();
}
function closeTokenModal() {
  document.getElementById("token-modal").classList.remove("open");
}

function wireChrome() {
  document.getElementById("nav-runs").onclick = () => navigate("#/runs");
  document.getElementById("nav-submit").onclick = () => navigate("#/submit");
  document.getElementById("nav-schemas").onclick = () => navigate("#/schemas");
  document.getElementById("theme-toggle").onclick = () => {
    const cur = document.documentElement.dataset.theme;
    applyTheme(cur === "dark" ? "light" : cur === "light" ? "auto" : "dark");
  };
  document.getElementById("token-btn").onclick = openTokenModal;
  document.getElementById("token-save").onclick = () => {
    const v = document.getElementById("token-input").value.trim();
    if (v) token.set(v); else token.clear();
    closeTokenModal();
    location.reload();
  };
  document.getElementById("token-clear").onclick = () => {
    token.clear();
    closeTokenModal();
    location.reload();
  };
  onUnauthorized(openTokenModal);
}

function registerRoutes() {
  route("#/runs", renderRuns);
  route("#/runs/:id", renderDetail);
  route("#/submit", renderSubmit);
  route("#/schemas", renderSchemas);
}

async function main() {
  initTheme();
  wireChrome();
  registerRoutes();
  // If the server requires auth and we have no valid token, prompt first.
  if (!(await authOk())) openTokenModal();
  startRouter(document.getElementById("view"));
}
main();
