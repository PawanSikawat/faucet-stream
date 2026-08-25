// "Ask AI" menu for the docs header. Injects a dropdown into mdBook's menu bar
// that opens the CURRENT docs page in a chosen AI assistant (Claude, ChatGPT,
// Perplexity, Grok, Gemini) with a prefilled prompt pointing at the page URL and
// the site's llms.txt. Loaded site-wide via book.toml `additional-js`. Styled
// with mdBook's own theme variables so it follows light/dark. A no-op if the
// menu bar is absent. Providers whose query param is unreliable (Gemini) open
// without a prefill.
(function () {
  "use strict";

  var SPARK =
    '<svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3l1.6 4.4L18 9l-4.4 1.6L12 15l-1.6-4.4L6 9l4.4-1.6L12 3Z"/><path d="M18.5 14.5l.7 1.8 1.8.7-1.8.7-.7 1.8-.7-1.8-1.8-.7 1.8-.7.7-1.8Z"/></svg>';
  var CARET =
    '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 9l6 6 6-6"/></svg>';

  var PROVIDERS = [
    { id: "claude",     label: "Claude",     url: "https://claude.ai/new",            param: "q",  icon: '<svg viewBox="0 0 24 24" fill="none" stroke="#d97757" stroke-width="2" stroke-linecap="round"><path d="M12 2.5v3.5M12 18v3.5M2.5 12H6M18 12h3.5M5.2 5.2l2.5 2.5M16.3 16.3l2.5 2.5M18.8 5.2l-2.5 2.5M7.7 16.3l-2.5 2.5"/></svg>' },
    { id: "chatgpt",    label: "ChatGPT",    url: "https://chatgpt.com/",             param: "q",  icon: '<svg viewBox="0 0 24 24" fill="none" stroke="#10a37f" stroke-width="1.5"><g transform="translate(12 12)"><ellipse rx="3" ry="7"/><ellipse rx="3" ry="7" transform="rotate(60)"/><ellipse rx="3" ry="7" transform="rotate(120)"/></g></svg>' },
    { id: "perplexity", label: "Perplexity", url: "https://www.perplexity.ai/search", param: "q",  icon: '<svg viewBox="0 0 24 24" fill="none" stroke="#20b8cd" stroke-width="1.7" stroke-linejoin="round"><path d="M12 3.2l7.3 4.4v8.8L12 20.8l-7.3-4.4V7.6L12 3.2z"/><path d="M12 3.2v17.6"/></svg>' },
    { id: "grok",       label: "Grok",       url: "https://grok.com/",                param: "q",  icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6.5 18L16 6M11 18l6.5-8.5"/></svg>' },
    { id: "gemini",     label: "Gemini",     url: "https://gemini.google.com/app",    param: null, icon: '<svg viewBox="0 0 24 24" fill="#4285f4"><path d="M12 2l2.1 7.9L22 12l-7.9 2.1L12 22l-2.1-7.9L2 12l7.9-2.1z"/></svg>' },
  ];

  function injectStyle() {
    if (document.getElementById("fs-askai-style")) return;
    var css =
      ".fs-askai{position:relative;display:inline-flex;vertical-align:middle;align-items:center}" +
      ".fs-askai-btn{display:inline-flex;align-items:center;gap:.45rem;height:2.4rem;padding:0 .9rem;border-radius:8px;border:1px solid var(--theme-popup-border);background:transparent;color:var(--icons);font-size:1.02rem;font-weight:500;line-height:1;white-space:nowrap;cursor:pointer;font-family:inherit}" +
      ".fs-askai-btn:hover{color:var(--fg);background:var(--theme-hover)}" +
      ".fs-askai-btn .fs-askai-caret{opacity:.7}" +
      ".fs-askai[data-open='true'] .fs-askai-caret{transform:rotate(180deg)}" +
      ".fs-askai-menu{position:absolute;top:calc(100% + .4rem);right:0;min-width:14.5rem;padding:.4rem;border:1px solid var(--theme-popup-border);border-radius:9px;background:var(--theme-popup-bg);box-shadow:0 8px 28px rgba(0,0,0,.18);z-index:200}" +
      ".fs-askai-menu[hidden]{display:none}" +
      ".fs-askai-head{margin:.2rem .5rem .3rem;font-size:.66rem;letter-spacing:.06em;text-transform:uppercase;opacity:.6}" +
      ".fs-askai-item{display:flex;align-items:center;gap:.55rem;padding:.5rem .5rem;border-radius:6px;font-size:.9rem;color:var(--fg);text-decoration:none}" +
      ".fs-askai-item:hover{background:var(--theme-hover)}" +
      ".fs-askai-ico{width:1.15em;height:1.15em;flex:none;display:inline-flex;align-items:center;justify-content:center;color:var(--fg)}" +
      ".fs-askai-ico svg{width:100%;height:100%;display:block}" +
      ".fs-askai-foot{margin:.35rem .5rem .1rem;padding-top:.4rem;border-top:1px solid var(--theme-popup-border);font-size:.7rem;line-height:1.5;opacity:.6}" +
      "@media(max-width:700px){.fs-askai-label{display:none}.fs-askai-btn{padding:0 .5rem}}";
    var s = document.createElement("style");
    s.id = "fs-askai-style";
    s.textContent = css;
    document.head.appendChild(s);
  }

  function buildPrompt() {
    var pageUrl = window.location.href.split("#")[0];
    var llmsUrl = window.location.origin + "/llms.txt";
    return (
      "I'm reading " + pageUrl + " — a page from the faucet-stream documentation " +
      "(faucet-stream is a fast, config-driven data-movement platform for Rust). " +
      "Please read that page (and the site guide at " + llmsUrl + ") and help me " +
      "understand it and answer my questions."
    );
  }

  function init() {
    var bar = document.getElementById("mdbook-menu-bar");
    if (!bar) return;
    var right = bar.querySelector(".right-buttons");
    if (!right) return;
    if (document.querySelector(".fs-askai")) return; // guard against double-init

    injectStyle();
    var prompt = buildPrompt();

    var wrap = document.createElement("div");
    wrap.className = "fs-askai";

    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "fs-askai-btn";
    btn.setAttribute("aria-haspopup", "menu");
    btn.setAttribute("aria-expanded", "false");
    btn.title = "Ask this page with an AI assistant";
    btn.innerHTML =
      SPARK + '<span class="fs-askai-label">Ask AI</span>' +
      '<span class="fs-askai-caret" style="display:inline-flex">' + CARET + "</span>";

    var menu = document.createElement("div");
    menu.className = "fs-askai-menu";
    menu.setAttribute("role", "menu");
    menu.hidden = true;

    var head = document.createElement("p");
    head.className = "fs-askai-head";
    head.textContent = "Open this page in…";
    menu.appendChild(head);

    PROVIDERS.forEach(function (p) {
      var a = document.createElement("a");
      a.className = "fs-askai-item";
      a.setAttribute("role", "menuitem");
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      if (p.param) {
        var u = new URL(p.url);
        u.searchParams.set(p.param, prompt);
        a.href = u.toString();
      } else {
        a.href = p.url;
      }
      var ico = document.createElement("span");
      ico.className = "fs-askai-ico";
      ico.setAttribute("aria-hidden", "true");
      ico.innerHTML = p.icon;
      a.appendChild(ico);
      a.appendChild(document.createTextNode(p.label));
      a.addEventListener("click", close);
      menu.appendChild(a);
    });

    var foot = document.createElement("p");
    foot.className = "fs-askai-foot";
    foot.textContent = "Sends a link to this page + llms.txt.";
    menu.appendChild(foot);

    wrap.appendChild(btn);
    wrap.appendChild(menu);
    right.insertBefore(wrap, right.firstChild);

    function open() {
      menu.hidden = false;
      wrap.setAttribute("data-open", "true");
      btn.setAttribute("aria-expanded", "true");
    }
    function close() {
      menu.hidden = true;
      wrap.removeAttribute("data-open");
      btn.setAttribute("aria-expanded", "false");
    }
    btn.addEventListener("click", function (e) {
      e.stopPropagation();
      if (menu.hidden) open();
      else close();
    });
    document.addEventListener("click", function (e) {
      if (!wrap.contains(e.target)) close();
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && !menu.hidden) {
        close();
        btn.focus();
      }
    });
  }

  if (document.readyState !== "loading") init();
  else document.addEventListener("DOMContentLoaded", init);
})();
