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

    function apply(mode) {
      buttons.forEach(function (b) {
        var on = b.getAttribute("data-mode") === mode;
        b.classList.toggle("active", on);
        b.setAttribute("aria-pressed", on ? "true" : "false");
      });
      contents.forEach(function (c) {
        c.classList.toggle("active", c.getAttribute("data-mode") === mode);
      });
      try { localStorage.setItem(STORAGE_KEY, mode); } catch (e) {}
    }

    buttons.forEach(function (b) {
      b.addEventListener("click", function () {
        apply(b.getAttribute("data-mode"));
      });
    });

    var saved = null;
    try { saved = localStorage.getItem(STORAGE_KEY); } catch (e) {}
    apply(saved === "architect" ? "architect" : "beginner");
  }

  if (document.readyState !== "loading") {
    init();
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
