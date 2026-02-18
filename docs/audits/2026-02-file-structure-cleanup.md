# File Structure Cleanup Run (2026-02)

Date: 2026-02-15  
Scope: Organization-focused cleanup based on `.claude/prompts/cleanup.md`

## Summary

- Consolidated API docs to `docs/api/API_REFERENCE.md`
- Removed redundant legacy API package at repository root (`api/`)
- Updated stale setup script references to canonical API/export entrypoints
- Untracked committed cache artifacts under `data/cache/` (cache remains local and ignored)

## Changes Executed

### 1. API Directory Consolidation

- Removed root-level `api/` directory (`api/main.py`, `api/README.md`, `api/requirements.txt`, `api/start.sh`)
- Canonical API remains `src/api/`
- Added `docs/api/API_REFERENCE.md` as source-of-truth API doc
- Replaced `docs/API_IMPLEMENTATION.md` with a redirect note to `docs/api/API_REFERENCE.md`

### 2. Script Path Alignment

Updated `scripts/setup_api_demo.sh`:

- `python src/export/geojson.py` -> `python -m src.export.geojson_export`
- `./api/start.sh` -> `make serve`
- `uvicorn api.main:app` -> `uvicorn src.api.main:app`

### 3. Cache Tracking Cleanup

- Removed `data/cache/**` from Git index (kept locally)
- `.gitignore` already had `data/cache/` ignore rules, so no new rule was required

### 4. Documentation Link Normalization

Updated layer docs to point to canonical methods doc:

- `docs/layers/LAYER1_ECONOMIC.md`
- `docs/layers/LAYER2_MOBILITY.md`

`docs/ANALYSIS_METHODS.md` remains as a short high-level note; full methodology remains in `docs/architecture/ANALYSIS_METHODS.md`.

## Verification Performed

- Checked repo references to removed `api/` paths and updated active references
- Confirmed canonical runtime still points to `src.api.main:app` in `Makefile` and `Procfile`

## Follow-Up (Optional)

- Archive or refactor older planning docs that still reference pre-cleanup layout (`docs/PROJECT_CLEANUP_PLAN.md`)
- Add a small docs lint check to fail on stale path references like `api/main.py`
