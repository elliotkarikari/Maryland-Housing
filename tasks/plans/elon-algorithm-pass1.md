# Elon Algorithm Pass 1 (Codebase-Wide, Bit 1)

## Goal
Run a first full-repo pass using the sequence:
1) challenge requirements,
2) remove unnecessary process,
3) simplify/optimize,
4) accelerate cycle time,
5) automate remaining required checks.

## Acceptance Criteria (Testable)
- A written requirements-challenge ledger exists with decisions and owners.
- At least one low-risk cycle-time improvement is implemented.
- No production runtime behavior changes in this pass.
- Existing static guardrails pass:
  - `./scripts/check_migration_prefixes.py`
  - `./scripts/check_year_literals.py`
  - `./scripts/check_docs_consistency.sh`
- Working tree changes are limited to process/docs/workflow files.

## Edge Cases and Failure Modes
- CI optimization accidentally weakens dependency guarantees.
- Docs/process cleanup creates ambiguity about canonical sources.
- Over-deletion removes useful runbooks needed for incidents.
- Broad code optimization in one pass introduces regressions in ingest/API.

## Proposed File Changes (Diff-Style Summary)
- `tasks/todo.md`: Add phased Elon-algorithm execution checklist.
- `tasks/lessons.md`: Add dated preventive rules discovered in this pass.
- `.github/workflows/ci.yml`: Add pip cache to reduce repeated install time.
- `.github/workflows/integration-smoke.yml`: Add pip cache for smoke pipeline.
- `docs/audits/2026-02-elon-algorithm-pass1.md`: Record requirement challenges, delete candidates, and next-bit scope.

## Alternative Approaches (Ranked)
1. **Selected**: Process-first, low-risk workflow and docs cleanup, then hotspot code optimization by module.
2. Code-first: immediately optimize all `iterrows()`/row-by-row writes across ingest and processing.
3. Big-bang cleanup: delete/merge docs + refactor CI + refactor ingest in one PR.

## Required Verification Steps
1. Re-run static checks:
   - `./scripts/check_migration_prefixes.py`
   - `./scripts/check_year_literals.py`
   - `./scripts/check_docs_consistency.sh`
2. Validate workflows still parse:
   - `python -m pytest tests/test_api_endpoints.py -q` (smoke subset)
3. `git diff --stat` confirms minimal blast radius.

## Explicit Non-Goals (Pass 1)
- No schema migrations.
- No ingest formula/weighting changes.
- No API contract changes.
