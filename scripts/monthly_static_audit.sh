#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPORT_DIR="$ROOT/reports/monthly-audit"
TS="$(date +%Y-%m-%d)"
REPORT="$REPORT_DIR/monthly-audit-$TS.md"

mkdir -p "$REPORT_DIR"
cd "$ROOT"

{
  echo "# Monthly Engineering Static Audit ($TS)"
  echo
  echo "_Non-destructive checks only (read commands + report artifact)._"
  echo
  echo "## Repository State"
  echo '```text'
  git status --short || true
  echo '```'
  echo
  echo "## Source-of-Truth Docs Presence"
  echo '```text'
  ls -l \
    docs/ARCHITECTURE.md \
    docs/METHODOLOGY.md \
    docs/architecture/DATA_SOURCES.md \
    docs/LIMITATIONS.md \
    QUICKSTART.md \
    AGENTS.md \
    .env.example || true
  echo '```'
  echo
  echo "## CI Workflow Presence"
  echo '```text'
  find .github/workflows -maxdepth 2 -type f || true
  echo '```'
  echo
  echo "## Migration Inventory + Prefix Check"
  echo '```text'
  ls -1 migrations || true
  ./scripts/check_migration_prefixes.py || true
  echo '```'
  echo
  echo "## Hardcoded Year Risk Scan"
  echo '```text'
  ./scripts/check_year_literals.py || true
  echo '```'
  echo
  echo "## ETL Row-by-Row Hotspot Scan"
  echo '```text'
  rg -n "for _, row in .*iterrows\(|db\.execute\(" src/ingest src/processing src/export || true
  echo '```'
  echo
  echo "## Docs Consistency Scan"
  echo '```text'
  wc -l frontend/map.js || true
  ./scripts/check_docs_consistency.sh || true
  echo '```'
  echo
  echo "## Documentation Update Checklist"
  echo "- [ ] docs/ARCHITECTURE.md"
  echo "- [ ] docs/METHODOLOGY.md"
  echo "- [ ] docs/architecture/DATA_SOURCES.md"
  echo "- [ ] docs/LIMITATIONS.md"
  echo "- [ ] QUICKSTART.md"
  echo "- [ ] AGENTS.md"
  echo "- [ ] .env.example"
} > "$REPORT"

echo "Audit report written: $REPORT"
