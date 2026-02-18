# Monthly Engineering Audit

- Date: 2026-02-15
- Project: Maryland Growth & Family Viability Atlas
- Audit Scope: frontend, backend API, database/migrations, ETL/ingestion, export pipeline, runtime config, CI/CD, observability, docs
- Audit Mode: non-destructive static review

## 1. Executive Summary (Top 10 Actions)
1. Fix migration workflow drift: `make db-migrate` calls Alembic but no Alembic project files are tracked.
2. Enforce unique migration numbering; duplicate `011` prefixes create ordering ambiguity.
3. Align Make targets to v2 ingestion modules to remove runtime/operator drift.
4. Convert row-by-row DB writes to batched inserts/upserts in ingest and processing paths.
5. Replace Layer2 O(n^2) tract loops with indexed/vectorized proximity computation.
6. Centralize year policy and remove hardcoded year literals across pipeline/ingest/cron.
7. Split `frontend/map.js` monolith and gate Ask Atlas by backend capability.
8. Remove stale tracked frontend fallback GeoJSON and tighten fallback paths.
9. Harden CORS (`allow_origins=["*"]` with credentials is unsafe/incompatible).
10. Add CI workflows for lint/tests/static consistency checks and monthly audit automation.

## 2. Findings By Component

### Frontend

#### Finding F-01
- Severity: P2
- Component: frontend/docs
- Location: `frontend/map.js` (4367 LOC), `docs/ARCHITECTURE.md:321`
- Need: Yes
- Delete: Do not delete feature set; split by concern
- Simplify: Modularize map, API client, state, panel/chat components
- Speed Up: Reduce parse/eval cost and regression blast radius
- Automate: Add static complexity/size checks
- Reason: Single-file map logic increases change-risk; architecture doc is stale (`~450 LOC` claim)
- Action: Break into modules and update docs
- Safety: Verify map load, county click, filter, compare, chat UX
- Impact: 20-40% maintenance/iteration efficiency gain (confidence: Med)
- Doc Updates: `docs/ARCHITECTURE.md`, `QUICKSTART.md`

#### Finding F-02
- Severity: P2
- Component: frontend/export/etl
- Location: `frontend/map.js:42-45,1635-1687`, `frontend/md_counties_latest.geojson`
- Need: Fallback behavior is needed; tracked stale file is not
- Delete: Candidate `git rm frontend/md_counties_latest.geojson`
- Simplify: Single canonical export artifact in `exports/`
- Speed Up: Avoid stale data fetch path retries
- Automate: Optional post-export sync/checksum if local fallback retained
- Reason: Frontend fallback file is tracked and can drift from `exports/md_counties_latest.geojson`
- Action: Remove tracked fallback file and rely on API/export fallback strategy
- Safety: Ensure local dev still loads map when API unavailable
- Impact: Reduces stale-data regressions (confidence: High)
- Doc Updates: `docs/ARCHITECTURE.md`, `QUICKSTART.md`, `docs/LIMITATIONS.md`

#### Finding F-03
- Severity: P2
- Component: frontend/backend
- Location: `frontend/map.js:2494,3949`, `src/api/main.py:42`, `src/ai/providers/openai_provider.py:405`
- Need: Chat feature needed
- Delete: Do not delete chat
- Simplify: Add `metadata/capabilities` and gate UI controls
- Speed Up: Fewer failed chat requests when AI disabled
- Automate: API contract test for capabilities endpoint
- Reason: Ask Atlas UI is always presented while backend may return 503 when `AI_ENABLED=false`
- Action: Hide/disable chat UI based on backend capability
- Safety: Verify behavior for both `AI_ENABLED=true` and `false`
- Impact: Better UX reliability (confidence: High)
- Doc Updates: `docs/ARCHITECTURE.md`, `QUICKSTART.md`, `.env.example`

### Backend API

#### Finding B-01
- Severity: P1
- Component: backend/infra
- Location: `src/api/main.py:34-35`
- Need: CORS middleware needed
- Delete: Delete wildcard credentials pattern in production
- Simplify: Env-driven origin allowlist
- Speed Up: N/A
- Automate: Startup validation for invalid CORS config
- Reason: `allow_origins=["*"]` with credentials is unsafe and browser-restricted
- Action: Add `CORS_ALLOW_ORIGINS` setting and strict defaults
- Safety: Validate local + deployed origins
- Impact: High security/correctness gain (confidence: High)
- Doc Updates: `.env.example`, `docs/ARCHITECTURE.md`, `QUICKSTART.md`

#### Finding B-02
- Severity: P2
- Component: backend/docs
- Location: `src/api/routes.py:559-616`
- Need: Metadata endpoint needed
- Delete: Delete hardcoded source metadata literals
- Simplify: Load from structured registry file
- Speed Up: N/A
- Automate: CI check to detect source metadata drift
- Reason: Hardcoded values will drift from data source registry and settings
- Action: Create registry file and load at runtime
- Safety: Keep response model stable
- Impact: Eliminates stale metadata drift (confidence: High)
- Doc Updates: `docs/architecture/DATA_SOURCES.md`, `docs/ARCHITECTURE.md`

#### Finding B-03
- Severity: P3
- Component: backend
- Location: `config/database.py:75-88`, `src/api/routes.py:278,508,555`
- Need: DB dependency needed
- Delete: Delete manual close burden in route handlers
- Simplify: Yield-based dependency closure
- Speed Up: Minor
- Automate: Lint check for bad dependency pattern
- Reason: Current approach depends on manual route `db.close()` hygiene
- Action: Convert `get_db_session()` to generator dependency
- Safety: API endpoint smoke tests
- Impact: Lower session-leak risk (confidence: Med)
- Doc Updates: `docs/ARCHITECTURE.md`

### Database and Migrations

#### Finding D-01
- Severity: P1
- Component: db/infra/docs
- Location: `Makefile:55`, missing `alembic.ini` and `alembic/`
- Need: Migration mechanism needed
- Delete: Delete nonfunctional migration command path
- Simplify: SQL migration runner from `migrations/*.sql` with deterministic ordering
- Speed Up: Avoid full schema resets as workaround
- Automate: CI check for duplicate migration prefixes and missing migration runner consistency
- Reason: `make db-migrate` references Alembic without in-repo Alembic config
- Action: Implement script-based SQL migration runner and wire Make target
- Safety: Test on clean DB and existing DB
- Impact: High reliability gain (confidence: Med)
- Doc Updates: `AGENTS.md`, `QUICKSTART.md`, `docs/ARCHITECTURE.md`

#### Finding D-02
- Severity: P1
- Component: db
- Location: `migrations/011_layer3_school_directory_id_length.sql`, `migrations/011_layer5_demographic_equity_overhaul.sql`
- Need: Both migration intents needed
- Delete: Do not delete content
- Simplify: Unique monotonic migration numbering
- Speed Up: N/A
- Automate: Prefix uniqueness check script
- Reason: Duplicate prefixes create nondeterministic migration order
- Action: Renumber one migration and update docs
- Safety: Validate full migration sequence replay
- Impact: High schema safety gain (confidence: High)
- Doc Updates: `AGENTS.md`, `docs/ARCHITECTURE.md`

### ETL / Ingestion

#### Finding E-01
- Severity: P1
- Component: etl/docs
- Location: `Makefile:61-64,74,77,80,83`
- Need: Layer targets needed
- Delete: Delete legacy ingest module calls from primary workflow
- Simplify: Point all layer targets to v2 modules
- Speed Up: Removes accidental older path runs
- Automate: Static check for Makefile v2 mapping
- Reason: Makefile drifts from architecture/agents v2 defaults
- Action: Update `ingest-all` and `ingest-layer3..6`
- Safety: Layer-level smoke tests
- Impact: High operational correctness gain (confidence: High)
- Doc Updates: `QUICKSTART.md`, `AGENTS.md`, `docs/ARCHITECTURE.md`

#### Finding E-02
- Severity: P1
- Component: etl/perf
- Location: multiple `iterrows()` + `db.execute(...)` loops in ingest + processing modules
- Need: Writes needed
- Delete: Delete row-wise write loops where possible
- Simplify: Reusable bulk insert/upsert helper
- Speed Up: 2-8x expected write throughput
- Automate: Monthly perf snapshot script
- Reason: Row-wise DB writes are a primary ingest bottleneck
- Action: Introduce `src/utils/db_bulk.py` and refactor hotspots
- Safety: Row count/idempotency checks
- Impact: Large throughput improvement (confidence: High)
- Doc Updates: `docs/ARCHITECTURE.md`, `AGENTS.md`

#### Finding E-03
- Severity: P1
- Component: etl/perf
- Location: `src/ingest/layer2_accessibility.py:725,746`
- Need: Accessibility model needed
- Delete: Delete nested all-to-all tract loops
- Simplify: Vectorized pair-distance + threshold bands
- Speed Up: 5-20x expected depending on tract count
- Automate: Perf logging by pipeline stage
- Reason: Current algorithm scales O(n^2)
- Action: Replace nested loops with broadcasted matrix or indexed neighborhood calculations
- Safety: Score delta validation tolerance
- Impact: Major runtime reduction for Layer2 (confidence: Med)
- Doc Updates: `docs/METHODOLOGY.md`, `docs/ARCHITECTURE.md`, `docs/LIMITATIONS.md`

#### Finding E-04
- Severity: P1
- Component: etl/config/docs
- Location: hardcoded year literals across run/ingest/cron modules
- Need: Year handling needed
- Delete: Delete embedded `2021/2022/2024/2025` assumptions where avoidable
- Simplify: Shared year-policy utility
- Speed Up: Fewer reruns due wrong year assumptions
- Automate: Static check for banned hardcoded years
- Reason: Year logic currently fragmented and contradictory
- Action: Add `src/utils/year_policy.py` and adopt across modules
- Safety: Unit tests for observed/predicted year selection
- Impact: High reliability gain (confidence: Med)
- Doc Updates: `docs/METHODOLOGY.md`, `docs/architecture/DATA_SOURCES.md`, `AGENTS.md`, `.env.example`

### Exports

#### Finding X-01
- Severity: P2
- Component: export
- Location: `src/export/geojson_export.py:24-61`
- Need: Geometry source needed
- Delete: Delete pygris-first dependency for routine export path
- Simplify: Use DB geometries as primary source, pygris for explicit refresh task only
- Speed Up: Faster export and fewer source outages
- Automate: Scheduled geometry refresh/checksum workflow
- Reason: Export currently depends on external boundary fetch path despite DB geometry availability
- Action: Prefer DB geometry when present, fallback to pygris only if necessary
- Safety: Geometry count/topology checks
- Impact: Higher reliability (confidence: Med)
- Doc Updates: `docs/ARCHITECTURE.md`, `QUICKSTART.md`, `docs/LIMITATIONS.md`

### Infrastructure / CI / Observability

#### Finding I-01
- Severity: P2
- Component: infra/docs
- Location: `.env.example` missing multiple operational keys used in code/runtime
- Need: Env template needed
- Delete: Delete stale minimal assumptions
- Simplify: Expand `.env.example` to include operationally relevant keys
- Speed Up: Fewer setup failures
- Automate: Env parity static checker
- Reason: Settings/runtime rely on keys not documented in template
- Action: Add missing keys and guidance comments
- Safety: Fresh-clone bootstrap validation
- Impact: High onboarding/runtime stability (confidence: High)
- Doc Updates: `.env.example`, `QUICKSTART.md`, `AGENTS.md`

#### Finding I-02
- Severity: P2
- Component: observability
- Location: `config/settings.py` (`SENTRY_DSN`) but no init in runtime
- Need: Observability strategy needed
- Delete: Delete dead setting if not adopting Sentry
- Simplify: Either fully wire Sentry or remove references
- Speed Up: N/A
- Automate: Startup log of observability status
- Reason: Partial/unused instrumentation configuration
- Action: Implement Sentry init in API/pipeline entrypoints (or document intentional non-use)
- Safety: Confirm no sensitive payload leakage
- Impact: Better production diagnostics (confidence: Med)
- Doc Updates: `.env.example`, `docs/ARCHITECTURE.md`, `QUICKSTART.md`

#### Finding I-03
- Severity: P1
- Component: CI
- Location: missing `.github/workflows`
- Need: CI is needed
- Delete: N/A
- Simplify: Add baseline workflow plus static audit checks
- Speed Up: Earlier defect detection
- Automate: GitHub Actions
- Reason: No in-repo CI checks discovered
- Action: Add workflows for lint/test/static checks
- Safety: Keep deterministic and low-flake
- Impact: High quality/reliability gain (confidence: High)
- Doc Updates: `docs/ARCHITECTURE.md`, `AGENTS.md`, `QUICKSTART.md`

## 3. Delete Candidates

### Safe in this run
1. `git rm frontend/md_counties_latest.geojson`
   - Cleanup references in:
     - `frontend/map.js`
     - `src/ingest/layer6_risk.py`

### Deferred (higher risk)
1. Legacy ingest module purge (`src/ingest/layer3_schools.py`, `src/ingest/layer4_housing.py`, `src/ingest/layer5_demographics.py`, `src/ingest/layer6_risk.py`)
2. Legacy processing stack purge (`src/processing/normalization.py`, `src/processing/scoring.py`, `src/processing/classification.py`, `src/processing/feature_registry.py`)

## 4. Simplification Plan
1. Canonicalize migration path and numbering policy.
2. Canonicalize v2 ingestion entrypoints in Makefile and docs.
3. Centralize year selection policy in one utility.
4. Use shared bulk DB writes instead of repeated row loops.
5. Replace hardcoded metadata source list with registry file.
6. Split frontend map monolith into modules by function.

## 5. Performance Plan
1. Bulk write conversion for ingestion and processing storage layers.
2. Layer2 tract accessibility algorithm optimization.
3. Export geometry preference to DB-first.
4. Frontend module split and fallback cleanup.
5. Add stage-level timing logs in heavy ingest flows.

## 6. Automation Plan
1. CI workflow: lint + typing + tests.
2. CI static checks: migration prefix uniqueness, hardcoded year policy, docs consistency.
3. Monthly static audit script outputting markdown artifact.
4. Env parity check script comparing `.env.example` with `config/settings.py` usage.
5. PR template checklist requiring required docs update review.

## 7. Documentation Updates (Required Per Run)

| Doc Path | Section | Why | Proposed Delta |
| --- | --- | --- | --- |
| `docs/ARCHITECTURE.md` | Repo tree, ingest flow, migration list | Drift vs current code | Update map module structure, v2 ingest targets, full migration inventory |
| `docs/METHODOLOGY.md` | Version history + year policy | Outdated implementation status | Replace scaffolded-layer language; add observed vs modeled year rules |
| `docs/architecture/DATA_SOURCES.md` | Intro + status overview | Contradictory source accessibility claims | Add status matrix: available/intermittent/blocked/synthetic fallback + verification dates |
| `docs/LIMITATIONS.md` | Known debt | Stale limitations | Replace with current risks: source instability, year drift, CI gaps |
| `QUICKSTART.md` | Setup/run commands | Command drift | Align to v2 Make targets and runtime capability behavior |
| `AGENTS.md` | Workflow + migration policy | Need enforceable conventions | Add monotonic migration numbering policy and monthly static checks |
| `.env.example` | Runtime/env keys | Missing operational keys | Add CORS/runtime/year/API capability related keys |

## 8. 30/60/90-Day Roadmap

### 30 days
- Fix migration path and duplicate numbering policy.
- Align Makefile to v2 ingestion.
- Harden CORS and add API capabilities endpoint.
- Add baseline CI and monthly static audit automation.

### 60 days
- Land year-policy utility and remove hardcoded years.
- Bulk-write refactor for processing + ingest modules.
- Remove stale fallback GeoJSON and finalize fallback behavior.

### 90 days
- Complete Layer2 algorithm optimization.
- Complete frontend modularization.
- Reassess safe legacy file removals after compatibility tests.

## 9. Sample Monthly Cron Script (Static, Non-Destructive)

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/elliotkarikari/Dev Projects/Maryland Housing"
REPORT_DIR="$ROOT/reports/monthly-audit"
TS="$(date +%Y-%m-%d)"
REPORT="$REPORT_DIR/monthly-audit-$TS.md"

mkdir -p "$REPORT_DIR"
cd "$ROOT"

{
  echo "# Monthly Engineering Static Audit ($TS)"
  echo
  echo "## Repo State"
  echo '```text'
  git status --short || true
  echo '```'
  echo
  echo "## Source-of-Truth Docs"
  echo '```text'
  ls -l docs/ARCHITECTURE.md docs/METHODOLOGY.md docs/architecture/DATA_SOURCES.md docs/LIMITATIONS.md QUICKSTART.md AGENTS.md .env.example || true
  echo '```'
  echo
  echo "## CI Workflows"
  echo '```text'
  find .github/workflows -maxdepth 2 -type f || true
  echo '```'
  echo
  echo "## Migration Prefix Check Input"
  echo '```text'
  ls -1 migrations || true
  echo '```'
  echo
  echo "## Hardcoded Year Scan"
  echo '```text'
  rg -n "default=2025|2021\)|2022|2024|2025" src config railway.json || true
  echo '```'
  echo
  echo "## Docs Consistency Scan"
  echo '```text'
  wc -l frontend/map.js || true
  rg -n "~450 LOC|scaffolded but not ingesting|programmatically accessible" docs/ARCHITECTURE.md docs/METHODOLOGY.md docs/LIMITATIONS.md docs/architecture/DATA_SOURCES.md || true
  echo '```'
} > "$REPORT"

echo "Wrote $REPORT"
```

## Needs Validation
1. Confirm if Alembic configuration is intentionally external to this repo.
2. Confirm whether Sentry should be fully wired or explicitly removed.
3. Confirm accepted tolerance bounds for Layer2 optimized score deltas.

