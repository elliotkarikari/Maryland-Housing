#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

errors=0

# Architecture should not claim old map.js size.
if rg -n "~450 LOC" docs/ARCHITECTURE.md >/dev/null; then
  echo "docs consistency: stale map.js LOC marker found in docs/ARCHITECTURE.md"
  errors=1
fi

# Methodology/limitations should not claim layers 2-6 are scaffolded only.
if rg -n "Layers 2-6 scaffolded but not ingesting data yet" docs/METHODOLOGY.md >/dev/null; then
  echo "docs consistency: stale layers scaffolded text found in docs/METHODOLOGY.md"
  errors=1
fi

if rg -n "Layers 2-6: ⚠️ Scaffolded but not ingesting data" docs/LIMITATIONS.md >/dev/null; then
  echo "docs consistency: stale layers scaffolded text found in docs/LIMITATIONS.md"
  errors=1
fi

# Data sources intro should not claim universal accessibility when failures are documented.
if rg -n "Every source listed is \*\*programmatically accessible\*\*" docs/architecture/DATA_SOURCES.md >/dev/null; then
  echo "docs consistency: absolute accessibility claim found in docs/architecture/DATA_SOURCES.md"
  errors=1
fi

if [[ "$errors" -ne 0 ]]; then
  exit 1
fi

echo "Docs consistency checks passed"
