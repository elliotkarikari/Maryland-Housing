---
name: maryland-housing-ingest
description: Run/debug the Maryland Housing ingest + pipeline, fix schema drift, and configure optional data sources.
metadata:
  short-description: Ingest workflow and fixes
---

# Maryland Housing Ingest

## When to use
- Running `make ingest-all` or a single layer.
- Fixing ingest failures (missing columns, missing data, missing dependencies).
- Updating optional data source toggles or data URLs.

## Quick start
1. Verify `.env` keys and `DATABASE_URL`.
2. Ensure PostGIS extensions are enabled and run `make db-migrate` after schema changes.
3. Run a single layer with `make ingest-layerX` before full ingest.

## Common fixes
- **Undefined column**: add an Alembic migration in `migrations/` and run `make db-migrate` before re-running ingest.
- **Parquet engine missing**: add `pyarrow` to `requirements.txt`, reinstall deps, then re-run.
- **Import errors when running scripts**: run modules via `python -m src.ingest.layerX` or `make ingest-layerX` from repo root.

## Optional sources
- USPS vacancy: set `USPS_VACANCY_DATA_URL` or `USPS_VACANCY_DATA_PATH` in `.env`.
- FEMA NFHL: set `FEMA_SKIP_NFHL=false` and confirm the NFHL URLs in `config/settings.py`.
- AI CIP extraction: set `AI_ENABLED=true`, provide `OPENAI_API_KEY`, and confirm expected input files per `src/ingest/policy_persistence.py`.

## Pipeline
- `make pipeline` or `python src/run_pipeline.py --year 2024` for a specific year.
- `make export` to regenerate GeoJSONs.

## References
- Data source registry: `docs/architecture/DATA_SOURCES.md`
- Architecture: `docs/ARCHITECTURE.md`
