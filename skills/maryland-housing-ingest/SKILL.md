---
name: maryland-housing-ingest
description: Maintain and debug the Maryland Housing Atlas ingest pipeline, schema migrations, deterministic ingest runs, and optional data source configuration (USPS vacancy, FEMA NFHL, CIP AI). Use when tasks involve make ingest-all, layer ingest modules, ingest warnings/errors, or pipeline-related settings/env vars.
---

# Maryland Housing Ingest

## Quick start
- Confirm the target year and scope before running: `INGEST_YEAR=2024 make ingest-all` or `make ingest-layer3`.
- If a run fails, capture the exact stack trace and the layer name, then fix root cause before re-running the same step.

## Deterministic ingest checklist
- Verify `.env` values match `.env.example`, especially API keys and optional source paths/URLs.
- If the ingest schema changed, add a migration in `migrations/` and apply it before re-running.
- Run `python scripts/ensure_ingest_schema.py` only for fresh databases; do not rely on it to add columns to existing tables.

## Optional sources (turn on when files are present)
- USPS vacancy: use a direct CSV/zip URL or a local file path; wire it in `src/ingest/settings.py`.
- FEMA NFHL or Maryland floodplain: set `FEMA_SKIP_NFHL=false` and provide a local shapefile path when supported by the layer.
- CIP AI: place the extracted CIP file where the ingest layer expects it (check `src/ingest/layer4_housing.py` and `src/ingest/settings.py`).

## Schema drift playbook
- If an INSERT fails due to missing columns, add a migration that aligns the table schema to the current dataframe columns.
- Prefer widening numeric precision instead of capping values to avoid corrupting scores.

## Common cleanup fixes
- Pandas concat FutureWarning: filter empty/all-NA frames before `pd.concat`.
- Obsolete race filters in MSDE enrollment: remove or guard to avoid noisy logs when the source already provides totals.
