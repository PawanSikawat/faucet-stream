#!/usr/bin/env bash
# Guard against connector-count drift across user-facing docs.
#
# The number of source/sink connectors is stated in several places (README hero,
# the docs-site connector catalog, the docs intro). They have silently diverged
# before (issue #335). This asserts every one of them matches the actual number
# of connector crates on disk, so a connector added without updating the docs
# fails CI.
set -euo pipefail
cd "$(dirname "$0")/.."

S=$(find crates/source -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
K=$(find crates/sink -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
T=$((S + K))
echo "Connector crates on disk: ${S} sources, ${K} sinks, ${T} total"

fail=0
check() { # <file> <literal-substring>
  if ! grep -qF -- "$2" "$1"; then
    echo "  ✗ ${1}: expected to contain '${2}'"
    fail=1
  fi
}

check README.md "**${S} source**"
check README.md "**${K} sink**"
check README.md "**${T} in total**"
check docs/book/src/reference/connectors.md "**${S} sources**"
check docs/book/src/reference/connectors.md "**${K} sinks**"
check docs/book/src/introduction.md "**${S} source**"
check docs/book/src/introduction.md "**${K} sink**"

if [ "${fail}" -ne 0 ]; then
  echo ""
  echo "Connector-count drift detected. Update the file(s) above to ${S} sources / ${K} sinks / ${T} total."
  exit 1
fi
echo "OK: connector counts are consistent across docs."
