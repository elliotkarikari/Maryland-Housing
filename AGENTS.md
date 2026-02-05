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
- Run full ingest for a specific year: `INGEST_YEAR=2024 make ingest-all`
- Run pipeline + export: `make pipeline`
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
- Do not rely on `scripts/ensure_ingest_schema.py` to alter existing tables.

Optional source toggles (keep deterministic defaults):
- USPS vacancy: require a direct CSV/zip URL or a local file path configured in `src/ingest/settings.py`.
- FEMA NFHL / Maryland floodplain: set `FEMA_SKIP_NFHL=false` and provide a local shapefile path where supported.
- CIP AI: place the extracted CIP file in the configured path; missing files should no-op cleanly.

## Data artifacts
- Cached inputs: `data/cache/`
- PostGIS schemas: `data/schemas/`
- Exports: `exports/`

## Testing + debugging tips
- Most ingestion failures are missing API keys or input files; start with `.env` vs `.env.example`.
- If a layer fails, run its module directly (e.g. `python -m src.ingest.layer2_accessibility`).
- Avoid re-running the same failing step until the root cause is addressed; capture the stack trace + layer name first.

## Skills
- Use the `spreadsheet` skill for editing or validating CSV/XLSX data.
- Use the `jupyter-notebook` skill for exploratory analysis or pipeline experiments.
- Use the `maryland-housing-ingest` skill for ingest runs, schema drift fixes, and optional source configuration.
