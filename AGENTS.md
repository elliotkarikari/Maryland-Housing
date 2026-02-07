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
