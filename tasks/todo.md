# Tasks Todo

## 2026-02-17 - Elon Algorithm Codebase Pass
- [x] Bit 1 plan created (`tasks/plans/elon-algorithm-pass1.md`)
- [x] Baseline static audit run (`reports/monthly-audit/monthly-audit-2026-02-17.md`)
- [x] CI cycle-time acceleration (pip cache in CI + integration smoke workflows)
- [x] Bit 1 findings documented (`docs/audits/2026-02-elon-algorithm-pass1.md`)
- [x] Bit 2: deletion pass (remove/archive redundant docs with explicit canonical map)
- [x] Bit 3 (slice A): optimize scoring module (`src/processing/scoring.py`) with vectorized math + batched writes
- [x] Bit 3 (slice B): optimize classification store path (`src/processing/classification.py`) with batched writes
- [x] Bit 3 (slice C): optimize normalization store path (`src/processing/normalization.py`) with batched writes
- [x] Bit 3 (slice D): optimize layer2 row materialization (`src/ingest/layer2_accessibility.py`) by replacing `iterrows()` with record-based coercion helpers
- [x] Bit 3 (slice E): optimize layer3 storage row materialization (`src/ingest/layer3_education_accessibility.py`) with shared coercion helpers and record-based builders
- [x] Bit 3 (slice F): optimize layer4 storage row materialization (`src/ingest/layer4_housing_affordability.py`) with shared coercion helpers and record-based builders
- [x] Bit 3 (slice G): optimize layer5 v2 storage row materialization (`src/ingest/layer5_demographic_equity.py`) with shared coercion helpers and record-based builders
- [x] Bit 3 (slice H): optimize legacy layer5 storage writes (`src/ingest/layer5_demographics.py`) by replacing per-row executes with `execute_batch`
- [x] Bit 3 (slice I): optimize layer6 storage row materialization (`src/ingest/layer6_risk_vulnerability.py`) with reusable row-builder helper
- [x] Bit 3 (slice J): optimize export explainability row pass (`src/export/geojson_export.py`) with record-based builder helper
- [x] Bit 3 (slice K): optimize remaining Layer5 compute loops (`src/ingest/layer5_demographics.py`, `src/ingest/layer5_demographic_equity.py`) with vectorized year-anchored/join-based calculations
- [ ] Bit 3: simplify ingest write paths (replace highest-cost row-by-row inserts)
- [x] Bit 4 (slice A): accelerate CI/dev cycle time with workflow concurrency cancellation + archive/process-only path ignores + `make test-fast`
- [x] Bit 4 (slice B): automate performance regression checks for ingest/API hot paths (`scripts/performance_regression_check.py` + CI gate + `make perf-check`)
