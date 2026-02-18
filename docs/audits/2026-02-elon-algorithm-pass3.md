# Elon Algorithm Audit - Pass 3 (2026-02-17)

## Scope
Bit 3 simplification/optimization slice focused on scoring pipeline internals.

## Changes Applied

### `src/processing/scoring.py`
- Replaced row-wise `iterrows()` layer-score computation with vectorized NumPy/Pandas operations.
- Replaced per-row `db.execute(...)` upserts in `store_layer_scores` with batched execution via `execute_batch(..., chunk_size=1000)`.
- Kept SQL upsert semantics unchanged (`ON CONFLICT (fips_code, data_year)`).

### `src/processing/classification.py`
- Replaced per-row `db.execute(...)` upserts in `store_classifications` with batched execution via `execute_batch(..., chunk_size=1000)`.
- Kept SQL upsert semantics unchanged (`ON CONFLICT (fips_code, data_year)`).

### `src/processing/normalization.py`
- Replaced nested row/feature `db.execute(...)` writes in `store_normalized_features` with long-format row construction + batched execution via `execute_batch(..., chunk_size=1000)`.
- Kept SQL upsert semantics unchanged (`ON CONFLICT (fips_code, data_year, feature_name)`).

### `src/ingest/layer2_accessibility.py`
- Replaced multiple `iterrows()` row materialization loops in storage functions with record-based iteration (`to_dict(orient="records")`) and centralized safe coercion helpers.
- Added `_build_county_accessibility_rows(...)` to keep row-shaping logic deterministic and testable.
- Preserved DB write semantics and existing `execute_batch(...)` usage.

### `src/ingest/layer3_education_accessibility.py`
- Replaced `iterrows()` row materialization loops in school/tract/county storage paths with record-based iteration (`to_dict(orient="records")`).
- Added shared coercion helpers for deterministic handling of missing values and bool/string coercion.
- Added `_build_county_education_rows(...)` to make county row shaping testable and consistent.
- Preserved DB write semantics and existing `execute_batch(...)` usage.

### `src/ingest/layer4_housing_affordability.py`
- Replaced `iterrows()` row materialization loops in tract/county storage paths with record-based iteration via row-builder helpers.
- Added shared coercion helpers and range-safe float handling (`_safe_float`) for deterministic missing/out-of-bounds behavior.
- Added `_build_tract_housing_rows(...)` and `_build_county_housing_update_rows(...)` to isolate row-shaping and opportunity-index composition.
- Preserved DB write semantics and existing `execute_batch(...)` usage.

### `src/ingest/layer5_demographic_equity.py`
- Replaced `iterrows()` row materialization loops in tract/county storage paths with record-based iteration (`to_dict(orient="records")`).
- Added shared coercion helpers and row-builder helpers (`_build_tract_demographic_rows(...)`, `_build_county_demographic_rows(...)`).
- Preserved DB write semantics and existing `execute_batch(...)` usage.
- Replaced row-wise segregation math loops in `compute_segregation_indices(...)` with county-aggregated vectorized calculations.
- Preserved segregation fallback semantics for non-computable counties (`dissimilarity=0`, `exposure=0.5`, `isolation=0.5`).

### `src/ingest/layer5_demographics.py`
- Replaced per-row `db.execute(...)` inserts in `store_demographic_data` with `execute_batch(...)`.
- Preserved delete-then-insert behavior and DB schema contract.
- Replaced row-wise vacancy prediction assignment loop with vectorized mask-based assignment per county.
- Added `_apply_momentum_features(...)` to compute year-anchored momentum fields via keyed joins (exact baseline-year match, no row-order assumptions).

### `src/ingest/layer6_risk_vulnerability.py`
- Replaced `iterrows()`-based row shaping in `store_risk_vulnerability_data` with `_build_risk_vulnerability_rows(...)` using record-based iteration.
- Centralized expected insert columns into a fixed list to ensure stable defaults for sparse upstream dataframes.
- Preserved delete-then-insert behavior and existing `execute_batch(...)` storage semantics.

### `src/export/geojson_export.py`
- Replaced `iterrows()`-based explainability generation with `_build_explainability_df(...)` using record-based iteration.
- Preserved export payload schema and explainability text behavior.

### `tests/test_processing_scoring.py`
- Added `test_calculate_layer_score_partial_coverage` to validate missing-data semantics.
- Added `test_store_layer_scores_uses_batched_execute` to verify batched DB write path and NaN->None sanitization via batch helper.

### `tests/test_classification.py`
- Added `test_store_classifications_uses_batched_execute` to verify batched DB write path and NaN->None sanitization via batch helper.

### `tests/test_processing_normalization.py`
- Added `test_store_normalized_features_uses_batched_execute` to verify batched DB write path and expected long-format feature rows.

### `tests/test_ingest/test_layer2_accessibility.py`
- Added `test_build_county_accessibility_rows_coerces_missing_values` to verify row coercion defaults/fallbacks.

### `tests/test_ingest/test_layer3_education_accessibility.py`
- Added `test_build_county_education_rows_coerces_missing_values` and `test_bool_or_default_handles_text_and_missing` to validate coercion/builder behavior.

### `tests/test_ingest/test_layer4_housing_affordability.py`
- Added `test_build_county_housing_update_rows_computes_opportunity_index` and `test_build_tract_housing_rows_coerces_missing_and_bounds`.

### `tests/test_ingest/test_layer5_demographic_equity.py`
- Added tests for tract/county row-builder coercion behavior.
- Added `test_compute_segregation_indices_handles_valid_and_invalid_counties` for county fallback and vectorized aggregation correctness.

### `tests/test_ingest/test_layer5_demographics.py`
- Added `test_store_demographic_data_uses_batched_execute` to verify batched insert path and NaN sanitization.
- Added `test_apply_vacancy_predictions_only_fills_eligible_future_rows` and `test_apply_momentum_features_requires_exact_reference_years`.

### `tests/test_ingest/test_layer6_risk_vulnerability_storage.py`
- Added `test_build_risk_vulnerability_rows_applies_defaults_and_expected_columns` for row-builder default/provenance coverage.

### `tests/test_export_geojson_export.py`
- Added `test_build_explainability_df_returns_expected_columns`.

## Why This Is Minimal and Safe
- Public API and output schema are unchanged.
- Existing score formulas and weighting policy are unchanged.
- Only implementation strategy changed (loop -> vectorized math, per-row writes -> batched writes).

## Verification
- `.venv/bin/python -m pytest tests/test_processing_scoring.py tests/test_classification.py -q`
- `.venv/bin/python -m black --check src/processing/scoring.py tests/test_processing_scoring.py`
- `.venv/bin/python -m isort --check-only src/processing/scoring.py tests/test_processing_scoring.py`
- `.venv/bin/python -m black --check src/processing/classification.py tests/test_classification.py`
- `.venv/bin/python -m isort --check-only src/processing/classification.py tests/test_classification.py`
- `.venv/bin/python -m pytest tests/test_processing_normalization.py -q`
- `.venv/bin/python -m black --check src/processing/normalization.py tests/test_processing_normalization.py`
- `.venv/bin/python -m isort --check-only src/processing/normalization.py tests/test_processing_normalization.py`
- `.venv/bin/python -m pytest tests/test_ingest/test_layer2_accessibility.py -q`
- `.venv/bin/python -m black --check src/ingest/layer2_accessibility.py tests/test_ingest/test_layer2_accessibility.py`
- `.venv/bin/python -m isort --check-only src/ingest/layer2_accessibility.py tests/test_ingest/test_layer2_accessibility.py`
- `.venv/bin/python -m pytest tests/test_ingest/test_layer3_education_accessibility.py -q`
- `.venv/bin/python -m black --check src/ingest/layer3_education_accessibility.py tests/test_ingest/test_layer3_education_accessibility.py`
- `.venv/bin/python -m isort --check-only src/ingest/layer3_education_accessibility.py tests/test_ingest/test_layer3_education_accessibility.py`
- `.venv/bin/python -m pytest tests/test_ingest/test_layer4_housing_affordability.py -q`
- `.venv/bin/python -m black --check src/ingest/layer4_housing_affordability.py tests/test_ingest/test_layer4_housing_affordability.py`
- `.venv/bin/python -m isort --check-only src/ingest/layer4_housing_affordability.py tests/test_ingest/test_layer4_housing_affordability.py`
- `.venv/bin/python -m pytest tests/test_ingest/test_layer5_demographic_equity.py tests/test_ingest/test_layer5_demographics.py -q`
- `.venv/bin/python -m black --check src/ingest/layer5_demographic_equity.py src/ingest/layer5_demographics.py tests/test_ingest/test_layer5_demographic_equity.py tests/test_ingest/test_layer5_demographics.py`
- `.venv/bin/python -m isort --check-only src/ingest/layer5_demographic_equity.py src/ingest/layer5_demographics.py tests/test_ingest/test_layer5_demographic_equity.py tests/test_ingest/test_layer5_demographics.py`
- `.venv/bin/python -m pytest tests/test_ingest/test_layer6_risk_vulnerability_storage.py tests/test_ingest/test_layer6_years.py -q`
- `.venv/bin/python -m black --check src/ingest/layer6_risk_vulnerability.py tests/test_ingest/test_layer6_risk_vulnerability_storage.py`
- `.venv/bin/python -m isort --check-only src/ingest/layer6_risk_vulnerability.py tests/test_ingest/test_layer6_risk_vulnerability_storage.py`
- `.venv/bin/python -m pytest tests/test_export_geojson_export.py -q`
- `.venv/bin/python -m black --check src/export/geojson_export.py tests/test_export_geojson_export.py`
- `.venv/bin/python -m isort --check-only src/export/geojson_export.py tests/test_export_geojson_export.py`
- `./scripts/check_year_literals.py`
- `./scripts/check_migration_prefixes.py`
- `./scripts/check_docs_consistency.sh`
