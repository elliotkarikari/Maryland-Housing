# Elon Algorithm Pass 3 (Bit 3) - Scoring Write Path Simplification

## Goal
Reduce row-by-row overhead in the scoring pipeline by:
1) vectorizing per-layer score calculation, and
2) replacing per-row DB upserts with batched execution.

## Acceptance Criteria
- `src/processing/scoring.py` no longer performs row-by-row `db.execute(...)` in `store_layer_scores`.
- `calculate_layer_score` preserves existing score/coverage semantics with missing values.
- `tests/test_processing_scoring.py` includes coverage for batched write behavior and missing-value handling.
- Verification passes:
  - `./scripts/check_year_literals.py`
  - `./scripts/check_migration_prefixes.py`
  - `./scripts/check_docs_consistency.sh`
  - `.venv/bin/python -m pytest tests/test_processing_scoring.py -q`

## Edge Cases
- Partial feature availability per row (coverage < 1.0).
- All features missing for a county (score NaN, coverage 0.0).
- NaN/None SQL parameter handling for nullable score columns.

## Proposed File Changes
- `src/processing/scoring.py`
  - vectorized score/coverage compute
  - batched upsert via `src.utils.db_bulk.execute_batch`
- `tests/test_processing_scoring.py`
  - add tests for coverage with missing values
  - add test proving batched DB write path is used

## Alternatives (Ranked)
1. **Selected**: module-scoped change in scoring only (minimal blast radius).
2. Cross-module batch refactor for scoring + classification + normalization in one pass.
3. Introduce ORM bulk APIs everywhere (higher complexity, backend risk).
