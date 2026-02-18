# Elon Algorithm Pass 4 (Bit 4) - Cycle-Time Acceleration

## Goal
Reduce feedback-loop time for routine development by cutting redundant CI runs and adding a fast local verification path.

## Acceptance Criteria
- CI workflow cancels superseded in-progress runs on the same ref.
- CI workflow does not run for archive/process-only updates (`docs/archive/**`, `docs/audits/**`, `tasks/**`) when no code/config files changed.
- Local developer workflow includes a fast test target optimized for quick iteration (`-q --maxfail=1`).
- Verification passes:
  - `./scripts/check_year_literals.py`
  - `./scripts/check_migration_prefixes.py`
  - `./scripts/check_docs_consistency.sh`

## Edge Cases / Failure Modes
- Path filters must not suppress CI when code/config changes are present.
- Concurrency group must not collide across different workflows.
- Fast test target must remain optional and not replace full test coverage gates.

## Proposed File Changes
- `.github/workflows/ci.yml`
  - Add workflow concurrency cancelation.
  - Add `paths-ignore` filters for archive/process-only changes.
- `.github/workflows/integration-smoke.yml`
  - Add workflow concurrency cancelation.
- `Makefile`
  - Add `test-fast` target for short local loop.
  - Include target in `.PHONY` and help text.

## Alternatives (Ranked)
1. **Selected**: workflow-level concurrency + trigger filters + optional local fast target (minimal risk, immediate savings).
2. Split CI into multiple matrix jobs with aggressive parallelism (higher complexity).
3. Introduce test selection by changed files (more logic and maintenance burden).
