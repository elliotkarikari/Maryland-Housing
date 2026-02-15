# AGENTS

## Project summary
Maryland Growth & Family Viability Atlas: a spatial analytics pipeline that ingests open government data across 6 layers, writes to PostGIS, and exports GeoJSON + API outputs for county-level growth signals.

## Source of truth docs
- Architecture: `docs/ARCHITECTURE.md`
- Methodology: `docs/METHODOLOGY.md`
- Data sources registry: `docs/architecture/DATA_SOURCES.md`
- Known limitations: `docs/LIMITATIONS.md`
- Quickstart: `QUICKSTART.md`

## Common workflows
- Install deps: `make install`
- Initialize DB: `make init-db` (or `make db-setup` then `make db-migrate`)
- Run full ingest: `make ingest-all`
- Run a single layer: `make ingest-layer1` ... `make ingest-layer6`
- Run pipeline + export: `make pipeline`
- Run pipeline for a year: `python src/run_pipeline.py --year 2024`
- Export GeoJSON only: `make export`
- Run API: `make serve`
- Tests: `make test`

## Ingestion updates (recurring task)
When adding or refreshing a data source:
- Update the relevant layer module in `src/ingest/`.
- Add or update env vars in `.env.example`.
- Document the source in `docs/architecture/DATA_SOURCES.md` (include URL, coverage, cadence, and format).
- If the data feeds into scoring, update `docs/METHODOLOGY.md` or `docs/ARCHITECTURE.md` as needed.
- Run the related `make ingest-layerX` and `make pipeline` to validate outputs.

When the ingest schema changes:
- Add a migration in `migrations/` to align table columns/precision with the new dataframe.
- Apply the migration before re-running `make ingest-all`.
- Do not rely on ad-hoc schema scripts to alter existing tables.

Optional source toggles (keep deterministic defaults):
- USPS vacancy: require a direct CSV/zip URL or a local file path in `.env`.
- FEMA NFHL / Maryland floodplain: set `FEMA_SKIP_NFHL=false` and confirm the NFHL URLs in `config/settings.py`.
- CIP AI: set `AI_ENABLED=true`, provide `OPENAI_API_KEY`, and ensure the expected input files are present.

## Preflight checks (recurring failure points)
- Confirm `.env` has required keys for the run: `DATABASE_URL`, `CENSUS_API_KEY`, `MAPBOX_ACCESS_TOKEN`.
- Confirm PostGIS extensions are enabled (`postgis`, `postgis_topology`) before ingest.
- Ensure `LODES_LATEST_YEAR`, `ACS_LATEST_YEAR`, and `PREDICT_TO_YEAR` in `config/settings.py` match available data.

## Layer2-5 v2 ingestion defaults (recurring)
- Layer2-5 v2 CLIs default to multi-year/full history; use `--single-year` for a single year.
- Use `--year` to set the end year; layer4 defaults to the latest available source year.
- Run directly when debugging: `python -m src.ingest.layer2_accessibility` (and layers 3-5 equivalents).

## Layer1 economic accessibility resume (recurring)
- For partial-year retries, use `INGEST_WRITE_MODE=append` so only missing tract rows are inserted.
- Databricks retries: prefer `DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=900` and `DATABRICKS_SOCKET_TIMEOUT_SECONDS=120`.
- Example resume command:
  `DATA_BACKEND=databricks INGEST_WRITE_MODE=append DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=900 DATABRICKS_SOCKET_TIMEOUT_SECONDS=120 .venv/bin/python -m src.ingest.layer1_economic_accessibility --year 2022`

## Layer3 education accessibility retry (recurring)
- For Databricks runs, keep `INGEST_WRITE_MODE=append` and avoid per-row retries by rerunning the module directly.
- Databricks retries: prefer `DATABRICKS_SOCKET_TIMEOUT_SECONDS=120`, `DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_COUNT=8`, and `DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=3600`.
- Example single-year rerun:
  `DATA_BACKEND=databricks INGEST_WRITE_MODE=append DATABRICKS_SOCKET_TIMEOUT_SECONDS=120 DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_COUNT=8 DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=3600 .venv/bin/python -m src.ingest.layer3_education_accessibility --year 2025 --single-year`

## Layer3 school quality expansion (MSDE + SchoolDigger)
- Treat NCES/MSDE as primary observed sources; SchoolDigger is a supplemental signal for gaps only.
- Stop observed writes at the latest truly observed year (currently 2024). Do not reuse NCES 2024 as 2025 observed data.
- Keep Layer3 forward prediction disabled by default; separate observed vs modeled outputs explicitly.
- Prefer Databricks-first runs for Layer3 scoring validation:
  `DATA_BACKEND=databricks .venv/bin/python -m src.ingest.layer3_education_accessibility --year 2024 --single-year`
- Direct MSDE proficiency pulls (no cache fallback):
  - Add config/env defaults: `MSDE_PROFICIENCY_ENABLED=true`, `MSDE_PROFICIENCY_MODE=direct`, `MSDE_PROFICIENCY_MANIFEST_PATH=config/msde_proficiency_manifest.json`, `MSDE_PROFICIENCY_TIMEOUT_SECONDS=120`, `MSDE_PROFICIENCY_RETRY_COUNT=4`, `MSDE_PROFICIENCY_ALLOW_CACHE_FALLBACK=false`.
  - Resolve MSDE URLs via the manifest and persist raw pulls for lineage.

## Prompt workflows
- Prompts live in `.claude/prompts/`.
- Use `make claude-list`, `make claude-run PROMPT=name`, `make claude-new NAME=name`.

## Data artifacts
- Cached inputs: `data/cache/`
- PostGIS schemas: `data/schemas/`
- Exports: `exports/`

## Testing + debugging tips
- Most ingestion failures are missing API keys or input files; start by diffing `.env` vs `.env.example`.
- Undefined column errors indicate a missing migration; add it in `migrations/` and run `make db-migrate`.
- If a layer fails, run its module directly (e.g. `python -m src.ingest.layer2_accessibility`).
- Avoid re-running the same failing step until the root cause is addressed; capture the stack trace + layer name first.

## Skills
- maryland-housing-ingest: Use for ingest runs, schema drift fixes, and source toggles. (file: `skills/maryland-housing-ingest/SKILL.md`)
- spreadsheet: Use for CSV/XLSX edits and validation. (file: `/Users/elliotkarikari/.codex/skills/spreadsheet/SKILL.md`)
- jupyter-notebook: Use for exploratory analysis or pipeline experiments. (file: `/Users/elliotkarikari/.codex/skills/jupyter-notebook/SKILL.md`)
