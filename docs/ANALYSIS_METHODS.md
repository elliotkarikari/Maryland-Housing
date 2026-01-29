# Maryland Growth & Family Viability Atlas — Analysis Methodology (V2)

This document explains how analysis is performed for each feature (layer), how layer scores are produced, how classifications are assigned, and how primary strengths/weaknesses and key trends are generated. All details reflect the current code paths used in the V2 multi-year pipeline.

## 1) Analysis Flow (End-to-End)

1. **Raw layer data** is ingested into layer tables (one table per layer) by year.
2. **Timeseries features** are computed per county and layer across a rolling window (default 5 years).
3. **Layer scores** are normalized and combined into a single score per layer (level + momentum + stability).
4. **Classification** assigns directional status, confidence, and a final grouping.
5. **Explainability** generates primary strengths/weaknesses and key trends for map display and API responses.

Key files:

- `src/processing/timeseries_features.py`
- `src/processing/multiyear_scoring.py`
- `src/processing/multiyear_classification.py`
- `src/export/geojson_export.py`
- `src/api/routes.py`

## 2) Feature Analysis (Timeseries Features)

Timeseries features are computed per county and per layer from the raw layer table. There is **no interpolation** of missing years; missing years reduce coverage and appear in `data_gaps`.

**Window size**

- Default window: 5 years (`DEFAULT_WINDOW_SIZE = 5`).
- A window is defined as `as_of_year - window_size + 1` through `as_of_year`.

**Coverage and data gaps**

- `coverage_years` = number of years with non-null values in the window.
- `data_gaps` = missing years in the window.

**Level (current position)**

- `level_latest`: most recent value in the window.
- `level_baseline`: earliest value in the window.

**Momentum (trend direction)**

- Requires at least 3 years (`MIN_YEARS_FOR_MOMENTUM = 3`).
- Uses Theil–Sen slope (robust to outliers):
  - `momentum_slope` from `stats.theilslopes(values, years)`.
  - `momentum_delta = level_latest - level_baseline`.
  - `momentum_percent_change = ((level_latest - level_baseline) / level_baseline) * 100` (if baseline != 0).
- If fewer than 3 years, slope is `NaN` and momentum is marked as insufficient.

**Stability (consistency)**

- Requires at least 3 years (`MIN_YEARS_FOR_STABILITY = 3`).
- Metrics computed:
  - `stability_volatility`: interquartile range (IQR).
  - `stability_cv`: coefficient of variation.
  - `stability_consistency`: fraction of year-over-year changes that are positive.
  - `stability_persistence`: maximum consecutive positive change streak.

**Source:** `src/processing/timeseries_features.py`

## 3) Layer Score Construction

Layer scores are normalized per layer across all counties and combined using weighted composition.

**Normalization**

- `layer_level_score` is the percentile rank (0–1) of `level_latest` within the layer.
- `layer_momentum_score` is the percentile rank (0–1) of `momentum_slope` within the layer (if available).
- `layer_stability_score` is **directly** the `stability_consistency` value (0–1).

**Weighted composition**

- Full evidence: `0.50*level + 0.30*momentum + 0.20*stability`.
- If momentum exists but stability is missing: `0.625*level + 0.375*momentum`.
- If only level exists: `1.00*level`.

**Missingness penalty**

- Penalizes scores when coverage is sparse.
- Coverage thresholds:
  - `>= 5 years`: no penalty.
  - `3–4 years`: small linear penalty.
  - `< 3 years`: larger penalty.
- Applied as a multiplicative reduction: `score * (1 - 0.5 * penalty)`.

**Source:** `src/processing/multiyear_scoring.py`

## 4) Classification Details

Classification is performed per county based on the layer scores and data coverage.

### 4.1 Directional Status

Outputs one of: `improving`, `stable`, `at_risk`.

**Inputs**

- Five growth layers: employment, mobility, schools, housing, demographics.
- Risk drag is also considered for at-risk checks.

**Thresholds**

- High score: `>= 0.60`.
- Low score: `< 0.30`.
- Improving requires at least 3 high layers and at most 1 low layer.
- Positive momentum evidence requires at least 2 momentum scores >= `0.55`.

**Logic**

- If fewer than 3 valid layer scores → `stable` (insufficient evidence).
- **Improving** if:
  - At least 3 high layer scores and <= 1 low score, and
  - Either momentum evidence is positive in at least 2 layers **or** no momentum data exists.
- **At risk** if:
  - At least 2 low scores, or
  - Risk drag is severe (>= 0.5) and at least 1 low score, or
  - Negative momentum in at least 2 layers (momentum < `1 - 0.55`).
- Otherwise → `stable`.

**Source:** `src/processing/multiyear_classification.py` (`classify_directional_status`).

### 4.2 Confidence Level

Outputs one of: `strong`, `conditional`, `fragile`.

**Inputs**

- Coverage across all available layers (`*_coverage` columns).

**Logic**

- `strong`: average coverage >= 5 and minimum coverage >= 3.
- `fragile`: average coverage < 3.
- `conditional`: otherwise (may include some sparse layers).

Uncertainty reasons are recorded (e.g., `sparse_coverage`, `some_layers_sparse`).

**Source:** `src/processing/multiyear_classification.py` (`classify_confidence_level`).

### 4.3 Final Synthesis Grouping

Outputs one of:

- `emerging_tailwinds`
- `conditional_growth`
- `stable_constrained`
- `at_risk_headwinds`
- `high_uncertainty`

**Logic**

1. `high_uncertainty` if confidence is `fragile` or there are 2+ uncertainty reasons.
2. `at_risk_headwinds` if directional status is `at_risk`.
3. If `improving`:
   - `emerging_tailwinds` when confidence is `strong`.
   - `conditional_growth` otherwise.
4. If `stable`: `stable_constrained`.

**Source:** `src/processing/multiyear_classification.py` (`determine_final_grouping`).

### 4.4 Composite County Score (Internal)

- Average of available layer scores across the 5 growth layers.
- Risk drag (if present) is applied as a multiplicative penalty: `composite * (1 - risk_drag_score)`.

**Source:** `src/processing/multiyear_classification.py` (`compute_composite_score`).

## 5) Primary Strengths & Weaknesses

Primary strengths/weaknesses are **explainability artifacts** derived from the layer scores (not stored as part of the classification).

**Method**

- Rank layer scores for the 5 growth layers.
- **Top 2** layers → `primary_strengths`.
- **Bottom 2** layers → `primary_weaknesses`.

**Layer display names**

- Employment Gravity
- Mobility Optionality
- School System Trajectory
- Housing Elasticity
- Demographic Momentum

**Source:** `src/export/geojson_export.py` (`_identify_top_strengths`, `_identify_top_weaknesses`, `_generate_explainability_payload`).

## 6) Key Trends

Key trends are derived from directional status, confidence, and risk drag.

**Trend signals**

- If `directional_status == improving`:
  - “Multiple reinforcing structural tailwinds present”
- If `directional_status == at_risk`:
  - “Structural headwinds constraining growth capacity”
- Otherwise:
  - “Balanced signals, mixed pressure directions”

**Confidence signals**

- If `confidence_level == strong`:
  - “High policy delivery reliability”
- If `confidence_level == fragile`:
  - “Low policy follow-through, high uncertainty”

**Risk signals**

- If `risk_drag_score >= 0.5`:
  - “Elevated environmental or infrastructure risk”

**Source:** `src/export/geojson_export.py` and `src/api/routes.py`.

## 7) Feature-to-Layer Mapping (Primary Metric Used for Trends)

The timeseries analysis uses one primary metric per layer as the core signal for level/momentum/stability.

| Layer                         | Table                         | Primary Metric                     | Notes                                    |
| ----------------------------- | ----------------------------- | ---------------------------------- | ---------------------------------------- |
| Employment Gravity (L1)       | `layer1_employment_gravity`   | `economic_opportunity_index`       | Used for multi-year trend and stability. |
| Mobility Optionality (L2)     | `layer2_mobility_optionality` | `mobility_optionality_index`       | Used for multi-year trend and stability. |
| School System Trajectory (L3) | `layer3_school_trajectory`    | `total_enrollment`                 | Uses total enrollment for timeseries.    |
| Housing Elasticity (L4)       | `layer4_housing_elasticity`   | `housing_elasticity_index`         | Used for multi-year trend and stability. |
| Demographic Momentum (L5)     | `layer5_demographic_momentum` | `demographic_momentum_score`       | Used for multi-year trend and stability. |
| Risk Drag (L6)                | `layer6_risk_drag`            | `risk_drag_index`                  | Used as risk penalty and at-risk signal. |

**Source:** `src/processing/timeseries_features.py` (`layer_configs`).

## 8) Summary of What Drives the Map

- **Map coloring** is driven by the **final synthesis grouping**.
- **Directional** and **confidence** classes are secondary map layers.
- **Strengths/weaknesses** and **key trends** are explainability fields derived after classification.
- **All multi-year behavior** depends on real data coverage; missing years increase uncertainty rather than being filled in.

If you want, I can extend this doc to include the specific raw input fields per layer (as defined in each ingestion script) and the exact data sources and vintages that feed each metric.

## 9) Raw Inputs per Layer (Current Ingestion Fields)

This section documents the raw fields written into each layer table. These are the inputs used to derive level/momentum/stability and the layer score.

### Layer 1 — Employment Gravity (`layer1_employment_gravity`)

**Version 2.0 (2026): Accessibility-Based Economic Opportunity**

Layer 1 was completely overhauled in January 2026 to use accessibility-based metrics instead of simple job counts. The new approach measures actual ability to reach high-wage jobs using proximity-based routing and LODES wage-segmented data.

**Primary sources:**
- LODES WAC (Workplace Area Characteristics): Employment by census tract with wage segments
- Census: Tract boundaries and population for weighting
- ACS: Tract-level population for aggregation

**Wage Segments (LODES):**

| Segment | Column | Definition |
|---------|--------|------------|
| Low Wage | SE01 | Jobs earning < $15,000/year |
| Mid Wage | SE02 | Jobs earning $15,000 - $40,000/year |
| High Wage | SE03 | Jobs earning > $40,000/year |

**Core Economic Accessibility Metrics (NEW in v2):**

| Metric | Description |
|--------|-------------|
| `high_wage_jobs_accessible_30min` | High-wage jobs (SE03) reachable within 30 minutes |
| `high_wage_jobs_accessible_45min` | High-wage jobs (SE03) reachable within 45 minutes |
| `total_jobs_accessible_45min` | All jobs reachable within 45 minutes |
| `pct_regional_high_wage_accessible` | Percentage of regional high-wage jobs reachable |
| `wage_quality_ratio` | Ratio of high-wage to low-wage accessible jobs |

**Normalized Scores (0-1 scale):**

| Score | Computation |
|-------|-------------|
| `economic_accessibility_score` | Percentile rank of high_wage_jobs_accessible_45min |
| `job_quality_index` | Weighted composite: 0.7×economic_accessibility + 0.3×wage_quality |

**Economic Accessibility Score Formula:**
```
economic_accessibility_score = percentile_rank(high_wage_jobs_accessible_45min)

job_quality_index =
    0.70 × economic_accessibility_score +
    0.30 × percentile_rank(wage_quality_ratio)
```

**Composite Opportunity Index (NEW):**
```
economic_opportunity_index =
    0.40 × employment_diversification_score +
    0.60 × economic_accessibility_score
```

This composite becomes the primary Layer 1 metric used for timeseries analysis.

**Sector Diversity Metrics:**

| Metric | Description |
|--------|-------------|
| `sector_diversity_shannon` | Shannon entropy across NAICS 2-digit sectors |
| `high_wage_sector_concentration` | HHI concentration of high-wage jobs by sector |

**Tract-Level Analysis:**

The v2 system computes economic accessibility at the census tract level first, then aggregates to county using population-weighted averages:

1. **Proximity-based gravity model**: Uses haversine distance with distance decay
2. **Distance thresholds**: 20km (~30 min), 35km (~45 min) approximations
3. **Population weighting**: County scores weighted by tract population
4. **No Java required**: Pure Python implementation using scipy

**Distance Decay Function:**
```python
# Gravity model: jobs weighted by inverse square of distance
weight = 1 / (1 + (distance_km / reference_km)²)
accessibility = Σ(jobs × weight)
```

**Why Wage-Weighted Accessibility > Raw Job Counts:**

The v1 approach counted total jobs and sector diversity, which correlates weakly with economic opportunity. The v2 approach measures what matters for family viability:

- **High-wage job access**: Jobs that support family formation (>$40k)
- **Job quality ratio**: Balance of high vs low-wage opportunities
- **Regional reach**: Access to the broader labor market

This makes `economic_opportunity_index` a more robust measure of economic opportunity, combining
local job-base strength with regional access to high-wage opportunities.

**V1 Fields (retained for backwards compatibility):**

- `total_jobs`
- `sector_diversity_entropy`
- `jobs_goods_producing`
- `jobs_trade_transport`
- `jobs_information`
- `jobs_financial`
- `jobs_professional`
- `jobs_education_health`
- `jobs_leisure`
- `jobs_other_services`
- `jobs_public_admin`
- `avg_weekly_wage`
- `qcew_total_establishments`
- `federal_awards_total`
- `federal_awards_5yr_avg`
- `federal_awards_volatility`
- `stable_sector_share`
- `employment_diversification_score` (v1 local strength composite)
- `economic_opportunity_index` (v1 + v2 combined)

**Source:** `src/ingest/layer1_economic_accessibility.py` (v2), `src/ingest/layer1_employment.py` (v1 fallback).

**Tract Table:** `layer1_economic_opportunity_tract` (new in v2)

### Layer 2 — Mobility Optionality (`layer2_mobility_optionality`)

**Version 2.0 (2026): Accessibility-Based Metrics**

Layer 2 was completely overhauled in January 2026 to use modern accessibility-based analysis instead of simple infrastructure counts. The new approach measures actual ability to reach jobs and opportunities using travel time routing.

**Primary sources:**
- OpenStreetMap: Maryland statewide extract (.osm.pbf) from Geofabrik
- GTFS feeds: MTA Maryland (local bus, light rail, metro, MARC, commuter bus), WMATA
- LODES: Workplace Area Characteristics (jobs by census tract)
- Census: Tract boundaries and population for weighting

**Computation Engine:**
- **R5py** (Conveyal R5 routing engine Python wrapper) for travel time matrix computation
- Fallback: Proximity-based gravity model if R5 is not available

**Core Accessibility Metrics (NEW in v2):**

| Metric | Description |
|--------|-------------|
| `jobs_accessible_transit_45min` | Jobs reachable by transit within 45 minutes |
| `jobs_accessible_transit_30min` | Jobs reachable by transit within 30 minutes |
| `jobs_accessible_walk_30min` | Jobs reachable by walking within 30 minutes |
| `jobs_accessible_bike_30min` | Jobs reachable by cycling within 30 minutes |
| `jobs_accessible_car_30min` | Jobs reachable by car within 30 minutes (baseline) |
| `pct_regional_jobs_by_transit` | Percentage of regional jobs reachable by transit |
| `transit_car_accessibility_ratio` | Transit competitiveness vs car access |

**Normalized Scores (0-1 scale):**

| Score | Computation |
|-------|-------------|
| `transit_accessibility_score` | Percentile rank of jobs_accessible_transit_45min |
| `walk_accessibility_score` | Percentile rank of jobs_accessible_walk_30min |
| `bike_accessibility_score` | Percentile rank of jobs_accessible_bike_30min |
| `multimodal_accessibility_score` | Weighted composite (see below) |

**Multimodal Accessibility Score Formula:**
```
multimodal_accessibility_score =
    0.60 × transit_accessibility_score +
    0.25 × walk_accessibility_score +
    0.15 × bike_accessibility_score
```

This composite becomes the new `mobility_optionality_index` used for timeseries analysis.

**Transit Quality Metrics:**

| Metric | Description |
|--------|-------------|
| `transit_stop_density` | Transit stops per square mile |
| `frequent_transit_area_pct` | Fraction of stops with ≤15 min headways |
| `average_headway_minutes` | Average service frequency |

**Infrastructure Metrics (retained from v1):**

| Metric | Description |
|--------|-------------|
| `highway_miles_total` | Total major highway miles in county |
| `interstate_exits` | Count of interstate exits |
| `major_highway_redundancy` | Boolean: multiple major routes available |
| `has_rail_service` | Boolean: rail transit available |
| `has_frequent_bus` | Boolean: frequent bus service available |
| `transit_stations_count` | Total transit stops |
| `mode_count` | Number of distinct modes (highway, rail, bus) |

**Tract-Level Analysis:**

The v2 system computes accessibility at the census tract level first, then aggregates to county using population-weighted averages. This provides:

1. **Finer spatial resolution**: Tract centroids serve as origins
2. **Population weighting**: County scores weighted by tract population
3. **Intra-county variation**: Can identify transit deserts vs hubs

**Data Provenance:**

| Field | Description |
|-------|-------------|
| `gtfs_feed_date` | Date of GTFS snapshot used |
| `osm_extract_date` | Date of OSM data used |
| `lodes_year` | Year of LODES job data |
| `accessibility_version` | `v1-infrastructure` or `v2-accessibility` |

**Why Accessibility > Infrastructure Counts:**

The v1 approach counted infrastructure (highway miles, transit stops) which correlates weakly with actual mobility. The v2 approach measures what matters:

- **Jobs you can actually reach** within a reasonable commute time
- **Transit competitiveness** vs driving (key for mode choice)
- **Regional job market access** (not just local infrastructure)

This makes `mobility_optionality_index` a true measure of economic opportunity access, not just infrastructure presence.

**Source:** `src/ingest/layer2_accessibility.py` (v2), `src/ingest/layer2_mobility.py` (v1 fallback).

**Tract Table:** `layer2_mobility_accessibility_tract` (new in v2)

### Layer 3 — School System Trajectory (`layer3_school_trajectory`)

Primary sources: NCES CCD Student Membership (052 series) by LEA.

Stored fields:

- `total_enrollment`
- `schools_total`
- `enrollment_3yr_change_pct` (derived)
- `enrollment_momentum_score` (derived; percentile by year)
- `capital_investment_score` (currently null)
- `capacity_strain_indicator` (currently null)

Source: `src/ingest/layer3_schools.py` (`store_school_data`).

### Layer 4 — Housing Elasticity (`layer4_housing_elasticity`)

Primary sources: ACS 5-year estimates (housing, income) and Census BPS permits (when available).

Stored fields:

- `permits_total`
- `permits_single_family`
- `permits_multifamily`
- `permits_per_1000_households`
- `permits_3yr_trend`
- `median_home_value`
- `median_household_income`
- `price_to_income_ratio`
- `price_to_income_5yr_change`
- `has_open_zoning_gis` (currently null)
- `zoning_capacity_indicator` (currently null)
- `supply_responsiveness_score` (derived)
- `housing_elasticity_index` (derived)

Source: `src/ingest/layer4_housing.py` (`store_housing_data`).

### Layer 5 — Demographic Momentum (`layer5_demographic_momentum`)

Primary sources: ACS 5-year estimates, IRS migration files.

Stored fields:

- `pop_total`
- `pop_age_25_44`
- `pop_age_25_44_pct` (derived)
- `households_total`
- `households_family`
- `households_family_with_children`
- `inflow_households`
- `outflow_households`
- `net_migration_households`
- `inflow_exemptions`
- `outflow_exemptions`
- `net_migration_persons`
- `total_addresses`
- `vacant_addresses`
- `vacancy_rate`
- `family_household_inflow_rate`
- `working_age_momentum`
- `household_formation_change`
- `demographic_momentum_score` (derived)

Source: `src/ingest/layer5_demographics.py` (`store_demographic_data`).

### Layer 6 — Risk Drag (`layer6_risk_drag`)

Primary sources: FEMA NFHL (flood zones), EPA EJScreen (air and proximity indicators), FHWA NBI (bridge condition).

Stored fields:

- `sfha_area_sq_mi`
- `sfha_pct_of_county`
- `sea_level_rise_exposure` (currently null)
- `extreme_heat_days_annual` (currently null)
- `pm25_avg`
- `ozone_avg`
- `proximity_hazwaste_score`
- `traffic_proximity_score`
- `bridges_total`
- `bridges_structurally_deficient`
- `bridges_deficient_pct`
- `risk_drag_index` (derived)

Source: `src/ingest/layer6_risk.py` (`store_risk_data`).

## 10) Data Coverage Caveats (Current Behavior)

The system is explicitly designed to avoid synthetic or imputed values. This creates predictable limitations when data coverage is partial.

1. **Minimum years for trend metrics**

- Momentum and stability require at least 3 years of real data.
- If fewer than 3 years are present, momentum and stability are set to `NaN`, and the layer score falls back to level-only or level+momentum.

2. **Missing years reduce confidence**

- Missing years are recorded in `data_gaps`.
- `missingness_penalty` reduces the layer score when coverage is below 5 years.
- Confidence is driven by coverage across layers; sparse coverage → `fragile` and `high_uncertainty` grouping.

3. **Layer-specific caveats**

- **Layer 1 (Economic Opportunity v2):** Uses LODES wage-segmented data with proximity-based accessibility model. LODES data lags ~2-3 years (latest typically 2021). High-wage job accessibility varies significantly between DC-adjacent counties and rural Western Maryland. The proximity model approximates travel time as straight-line distance, which may underestimate rural accessibility due to sparse road networks.
- **Layer 2 (Mobility v2):** Uses R5 routing engine for accessibility computation. Multi-year trend now possible via archived GTFS feeds from Mobility Database. Transit accessibility scores may vary significantly between urban (DC-adjacent) and rural counties. The fallback proximity model is less accurate but preserves basic accessibility signal.
- **Layer 3 (Schools):** depends on CCD membership file availability; some years can be missing, creating gaps in momentum and stability.
- **Layer 4 (Housing):** permits may be unavailable for some years; ACS availability is stable but lagged.
- **Layer 5 (Demographics):** IRS migration series may lag or change file formats; missing years reduce momentum confidence.
- **Layer 6 (Risk):** FEMA NFHL is occasionally unstable; EJScreen and NBI availability varies by year. If these inputs are missing, risk drag may be incomplete.

4. **Risk drag handling**

- Risk drag is applied as a penalty to the composite score; if risk is missing, the composite uses only growth layers.

5. **No interpolation, no synthetic fill**

- The pipeline never fabricates values or fills missing years. Any missingness flows through to the final uncertainty.
