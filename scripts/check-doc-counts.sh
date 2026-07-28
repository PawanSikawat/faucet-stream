#!/usr/bin/env bash
# Guard against connector-/crate-count drift across the user-facing docs.
#
# Counts are a SINGLE SOURCE OF TRUTH derived from the crate directories on disk
# and rendered into `<!--COUNT:*-->N<!--/COUNT-->` sentinel spans throughout the
# docs by scripts/sync-doc-counts.py. This wrapper just runs its --check mode
# (kept for the CI `doc-counts` job and anyone with the old entrypoint in muscle
# memory). To fix drift, run: python3 scripts/sync-doc-counts.py
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 scripts/sync-doc-counts.py --check
