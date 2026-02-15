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
- Resuming partial ingest runs for layer 1 with append mode.

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

## Layer1 economic accessibility resume
- Use `INGEST_WRITE_MODE=append` to insert only missing tract rows when a year is partially ingested.
- For Databricks retries, prefer `DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=900` and `DATABRICKS_SOCKET_TIMEOUT_SECONDS=120`.
- Example resume command:
  `DATA_BACKEND=databricks INGEST_WRITE_MODE=append DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=900 DATABRICKS_SOCKET_TIMEOUT_SECONDS=120 .venv/bin/python -m src.ingest.layer1_economic_accessibility --year 2022`

## Layer3 education accessibility retry
- For Databricks runs, keep `INGEST_WRITE_MODE=append` and rerun the module directly for the failing year.
- Recommended retry env vars: `DATABRICKS_SOCKET_TIMEOUT_SECONDS=120`, `DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_COUNT=8`, `DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=3600`.
- Example rerun:
  `DATA_BACKEND=databricks INGEST_WRITE_MODE=append DATABRICKS_SOCKET_TIMEOUT_SECONDS=120 DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_COUNT=8 DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=3600 .venv/bin/python -m src.ingest.layer3_education_accessibility --year 2025 --single-year`

## Layer3 school quality expansion (MSDE + SchoolDigger)
- Treat NCES/MSDE as primary observed sources; use SchoolDigger only to fill missing quality variation.
- Do not reuse NCES 2024 as 2025 observed data; stop observed writes at the latest observed year.
- Keep Layer3 forward prediction disabled by default to preserve observed vs modeled separation.
- Databricks-first validation:
  `DATA_BACKEND=databricks .venv/bin/python -m src.ingest.layer3_education_accessibility --year 2024 --single-year`
- Direct MSDE proficiency pulls (no cache fallback):
  - `MSDE_PROFICIENCY_ENABLED=true`
  - `MSDE_PROFICIENCY_MODE=direct`
  - `MSDE_PROFICIENCY_MANIFEST_PATH=config/msde_proficiency_manifest.json`
  - `MSDE_PROFICIENCY_TIMEOUT_SECONDS=120`
  - `MSDE_PROFICIENCY_RETRY_COUNT=4`
  - `MSDE_PROFICIENCY_ALLOW_CACHE_FALLBACK=false`
- Persist raw MSDE pulls for lineage and auditing.

## Pipeline
- `make pipeline` or `python src/run_pipeline.py --year 2024` for a specific year.
- `make export` to regenerate GeoJSONs.

## References
- Data source registry: `docs/architecture/DATA_SOURCES.md`
- Architecture: `docs/ARCHITECTURE.md`
