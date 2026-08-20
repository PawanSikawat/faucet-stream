// Interactive Beginner/Architect toggle for the "Learn the architecture" page.
// Loaded site-wide via book.toml `additional-js`. It is a no-op on every page
// that does not contain a [data-mode-toggle] control, so other chapters are
// unaffected. The chosen mode is remembered in localStorage across pages.
(function () {
  "use strict";
  var STORAGE_KEY = "faucet-learn-mode";

  function init() {
    var toggle = document.querySelector("[data-mode-toggle]");
    if (!toggle) return; // not the interactive page

    var buttons = Array.prototype.slice.call(
      toggle.querySelectorAll(".mode-switch button")
    );
    var contents = Array.prototype.slice.call(
      document.querySelectorAll(".mode-content")
    );
    if (!buttons.length || !contents.length) return;

    // Cache every mermaid diagram's source BEFORE mermaid renders it. This
    // listener is registered ahead of mermaid's own startOnLoad handler (the
    // load order in book.toml puts learn-toggle.js first), so `textContent` is
    // still the raw graph definition here, not the rendered SVG.
    Array.prototype.slice
      .call(document.querySelectorAll("pre.mermaid, .mermaid"))
      .forEach(function (el) {
        if (!el.getAttribute("data-src")) {
          el.setAttribute("data-src", el.textContent);
        }
      });

    // A diagram rendered while its section is display:none has no measurable
    // width and collapses to a tiny box. When a mode is first revealed by a
    // click, re-render its diagrams from the cached source so they size to the
    // now-visible column.
    var rendered = {};
    function renderMermaidIn(container) {
      if (!window.mermaid || !container) return;
      var nodes = Array.prototype.slice.call(
        container.querySelectorAll("pre.mermaid, .mermaid")
      );
      if (!nodes.length) return;
      nodes.forEach(function (el) {
        var src = el.getAttribute("data-src");
        if (src == null) return;
        el.removeAttribute("data-processed");
        el.innerHTML = src;
      });
      try {
        if (typeof mermaid.run === "function") {
          mermaid.run({ nodes: nodes });
        } else if (typeof mermaid.init === "function") {
          mermaid.init(undefined, nodes);
        }
      } catch (e) {}
    }

    function apply(mode, userAction) {
      buttons.forEach(function (b) {
        var on = b.getAttribute("data-mode") === mode;
        b.classList.toggle("active", on);
        b.setAttribute("aria-pressed", on ? "true" : "false");
      });
      var activeContent = null;
      contents.forEach(function (c) {
        var on = c.getAttribute("data-mode") === mode;
        c.classList.toggle("active", on);
        if (on) activeContent = c;
      });
      // On the first user-driven reveal of a mode, fix up any diagrams that
      // mermaid rendered while this section was hidden.
      if (userAction && !rendered[mode]) {
        rendered[mode] = true;
        renderMermaidIn(activeContent);
      }
      try { localStorage.setItem(STORAGE_KEY, mode); } catch (e) {}
    }

    buttons.forEach(function (b) {
      b.addEventListener("click", function () {
        apply(b.getAttribute("data-mode"), true);
      });
    });

    var saved = null;
    try { saved = localStorage.getItem(STORAGE_KEY); } catch (e) {}
    var initial = saved === "architect" ? "architect" : "beginner";
    // The initially-visible mode is rendered correctly by mermaid at load, so
    // mark it done — only the section that started hidden needs a re-render.
    rendered[initial] = true;
    apply(initial, false);
  }

  if (document.readyState !== "loading") {
    init();
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
