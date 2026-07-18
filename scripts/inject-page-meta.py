#!/usr/bin/env python3
"""Inject per-page SEO metadata into a built mdBook.

mdBook's theme/head.hbs is the same fragment on every page, so Open Graph and
meta-description tags are otherwise site-wide (issue #337). This post-build step
rewrites, per page, the *content* of tags that already exist in <head>:

  - <meta name="description">        (mdBook emits one from book.toml [book].description)
  - <meta property="og:title">       (from head.hbs)
  - <meta property="og:description"> (from head.hbs)
  - <meta name="twitter:title">      (from head.hbs)
  - <meta name="twitter:description">(from head.hbs)

Per-page values come from the page's first <h1> (title) and first <p>
(description), taken from inside <main> so nav/sidebar chrome is ignored. Pages
without a usable <h1>/<p> keep the site-wide defaults. It only edits the content
attribute of tags that are already present, so it never adds duplicates and is
idempotent. No external dependencies (stdlib only).

Usage: python3 scripts/inject-page-meta.py docs/book/book
"""
import html
import pathlib
import re
import sys

MAX_DESC = 155
SKIP = {"404.html", "print.html"}


def clean(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", "", fragment)  # strip inline tags
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def truncate(text: str, limit: int = MAX_DESC) -> str:
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0].rstrip(".,;:—- ")
    return cut + "…"


def set_meta_content(head: str, attr: str, value: str, content: str) -> str:
    """Replace the content="..." of the first <meta {attr}="{value}" ...> tag."""
    escaped = html.escape(content, quote=True)
    pattern = re.compile(
        r'(<meta\s+[^>]*\b' + re.escape(attr) + r'="' + re.escape(value)
        + r'"[^>]*\bcontent=")[^"]*(")'
    )
    new, _ = pattern.subn(lambda m: m.group(1) + escaped + m.group(2), head, count=1)
    return new


def first_match(pattern: str, text: str):
    m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    return m.group(1) if m else None


def process(path: pathlib.Path) -> bool:
    text = path.read_text(encoding="utf-8")
    main = first_match(r"<main\b[^>]*>(.*?)</main>", text) or text
    h1 = first_match(r"<h1\b[^>]*>(.*?)</h1>", main)
    para = first_match(r"<p\b[^>]*>(.*?)</p>", main)

    title = clean(h1) if h1 else ""
    desc = truncate(clean(para)) if para else ""
    if not title and not desc:
        return False

    head_m = re.search(r"<head\b[^>]*>(.*?)</head>", text, re.DOTALL | re.IGNORECASE)
    if not head_m:
        return False
    head = original = head_m.group(1)

    if desc:
        head = set_meta_content(head, "name", "description", desc)
        head = set_meta_content(head, "property", "og:description", desc)
        head = set_meta_content(head, "name", "twitter:description", desc)
    if title:
        page_title = f"{title} · faucet-stream"
        head = set_meta_content(head, "property", "og:title", page_title)
        head = set_meta_content(head, "name", "twitter:title", page_title)

    if head == original:
        return False
    path.write_text(text[: head_m.start(1)] + head + text[head_m.end(1):], encoding="utf-8")
    return True


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: inject-page-meta.py <built-book-dir>", file=sys.stderr)
        return 2
    root = pathlib.Path(sys.argv[1])
    if not root.is_dir():
        print(f"error: {root} is not a directory", file=sys.stderr)
        return 2
    updated = sum(
        process(f) for f in sorted(root.rglob("*.html")) if f.name not in SKIP
    )
    print(f"inject-page-meta: updated {updated} pages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
