// "On this page" — a right-hand, per-page table of contents, in the spirit of
// the Pydantic / Material docs. Built client-side from the current page's
// <h2>/<h3> headings and kept in sync with the scroll position. Loaded
// site-wide via book.toml `additional-js`; it is a no-op on pages with fewer
// than two headings and on the landing page (which owns the full width for its
// hero), so other chapters are unaffected.
(function () {
  "use strict";

  function build() {
    var content = document.getElementById("mdbook-content");
    if (!content) return;
    var main = content.querySelector("main");
    if (!main) return;
    if (main.querySelector(".fs-hero")) return; // landing page keeps full width

    var heads = Array.prototype.slice.call(
      main.querySelectorAll("h2[id], h3[id]")
    );
    if (heads.length < 2) return;

    var nav = document.createElement("nav");
    nav.className = "page-toc";
    nav.setAttribute("aria-label", "On this page");

    var title = document.createElement("div");
    title.className = "page-toc-title";
    title.textContent = "On this page";
    nav.appendChild(title);

    var ul = document.createElement("ul");
    var links = [];
    heads.forEach(function (h) {
      var li = document.createElement("li");
      li.className = "page-toc-" + h.tagName.toLowerCase(); // page-toc-h2 / -h3
      var a = document.createElement("a");
      a.href = "#" + h.id;
      a.textContent = (h.textContent || "").trim();
      li.appendChild(a);
      ul.appendChild(li);
      links.push({ id: h.id, a: a });
    });
    nav.appendChild(ul);

    // Wrap the nav in a grid item that STRETCHES to the full row height, and
    // keep `position: sticky` on the inner nav. A sticky element only moves
    // within a containing block taller than itself, so the stretched wrapper is
    // what gives the nav room to stay pinned while the page scrolls. (Making the
    // grid item itself sticky fails — it shrinks to content and has nowhere to
    // stick.) Insert after <main> so the grid places it in the right column.
    var wrap = document.createElement("div");
    wrap.className = "page-toc-wrap";
    wrap.appendChild(nav);
    main.insertAdjacentElement("afterend", wrap);
    content.classList.add("has-page-toc");

    // Scroll-spy: mark the heading nearest the top of the viewport as active.
    var current = null;
    function setActive(id) {
      if (current === id) return;
      links.forEach(function (l) {
        l.a.classList.toggle("active", l.id === id);
      });
      current = id;
    }

    if ("IntersectionObserver" in window) {
      var visible = {};
      var obs = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (e) {
            visible[e.target.id] = e.isIntersecting;
          });
          for (var i = 0; i < heads.length; i++) {
            if (visible[heads[i].id]) {
              setActive(heads[i].id);
              return;
            }
          }
        },
        { rootMargin: "-8% 0px -72% 0px", threshold: 0 }
      );
      heads.forEach(function (h) {
        obs.observe(h);
      });
    }
    setActive(heads[0].id);
  }

  if (document.readyState !== "loading") build();
  else document.addEventListener("DOMContentLoaded", build);
})();
