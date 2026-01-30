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
| School System Trajectory (L3) | `layer3_school_trajectory`    | `education_opportunity_index`      | v1-v2 composite: 0.4×supply + 0.6×accessibility. |
| Housing Elasticity (L4)       | `layer4_housing_elasticity`   | `housing_opportunity_index`        | v1-v2 composite: 0.4×elasticity + 0.6×affordability. |
| Demographic Momentum (L5)     | `layer5_demographic_momentum` | `demographic_opportunity_index`    | v1-v3 composite: 0.3×static + 0.4×equity + 0.3×migration. |
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

**Source:** `src/ingest/layer1_economic_accessibility.py` (v2).

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

**Source:** `src/ingest/layer2_accessibility.py` (v2).

**Tract Table:** `layer2_mobility_accessibility_tract` (new in v2)

### Layer 3 — School System Trajectory (`layer3_school_trajectory`)

**Version 2.0 (2026): Accessibility-Based Education Opportunity**

Layer 3 was completely overhauled in January 2026 to use modern education accessibility standards. The new approach measures actual ability to reach high-quality schools rather than just enrollment counts.

**Primary sources:**
- NCES CCD: School directory with locations and characteristics
- MSDE Report Card: Proficiency rates, graduation rates by school
- ACS: School-age population (5-17) by census tract

**v1 Supply Metrics (retained):**

| Metric | Description |
|--------|-------------|
| `total_enrollment` | Total student enrollment in county |
| `schools_total` | Total number of schools |
| `enrollment_3yr_change_pct` | 3-year enrollment change percentage |
| `enrollment_momentum_score` | Percentile rank of enrollment trend |

**v2 Accessibility Metrics (NEW):**

| Metric | Description |
|--------|-------------|
| `schools_accessible_15min` | Schools reachable within 15 minutes |
| `schools_accessible_30min` | Schools reachable within 30 minutes |
| `high_quality_schools_15min` | Above-median proficiency schools within 15 min |
| `high_quality_schools_30min` | Above-median proficiency schools within 30 min |
| `top_quartile_schools_30min` | Top 25% proficiency schools within 30 min |
| `prek_programs_accessible_20min` | Pre-K programs within 20 minutes |

**School Quality Metrics (MSDE Report Card):**

| Metric | Description |
|--------|-------------|
| `avg_ela_proficiency` | Average ELA % proficient/advanced |
| `avg_math_proficiency` | Average Math % proficient/advanced |
| `avg_proficiency` | Combined average proficiency |
| `avg_graduation_rate` | 4-year graduation rate (high schools) |
| `frl_proficiency_gap` | Gap between FRL and non-FRL students |

**Quality Tiers:**

| Tier | Criteria |
|------|----------|
| `top_quartile` | Proficiency >= 75th percentile |
| `above_median` | Proficiency >= 50th percentile |
| `below_median` | Proficiency >= 25th percentile |
| `bottom_quartile` | Proficiency < 25th percentile |

**Normalized Scores (0-1 scale):**

| Score | Computation |
|-------|-------------|
| `school_supply_score` | v1: Normalized school density + pre-K availability |
| `education_accessibility_score` | v2: Percentile rank of high-quality schools reachable |
| `school_quality_score` | Average proficiency of accessible schools |
| `prek_accessibility_score` | Pre-K program accessibility |
| `equity_score` | Accessibility adjusted for FRL proficiency gaps |

**Education Accessibility Score Formula:**
```
education_accessibility_composite =
    0.50 × high_quality_access_score +
    0.30 × prek_accessibility_score +
    0.20 × equity_score
```

**Composite Opportunity Index (NEW):**
```
education_opportunity_index =
    0.40 × school_supply_score (v1) +
    0.60 × education_accessibility_composite (v2)
```

This composite becomes the primary Layer 3 metric used for timeseries analysis.

**Tract-Level Analysis:**

The v2 system computes education accessibility at the census tract level first:

1. **School accessibility**: Uses proximity-based model with distance thresholds (8km/15min, 20km/30min)
2. **Quality weighting**: Schools weighted by MSDE proficiency percentile
3. **Population weighting**: County scores weighted by school-age population (5-17)
4. **Equity adjustment**: Rewards access to schools serving all students well

**Why Accessibility > Enrollment Counts:**

The v1 approach measured enrollment trends and school counts, which indicates system size but not opportunity. The v2 approach measures what matters for family viability:

- **Quality access**: Can families reach high-performing schools?
- **Pre-K access**: Are early childhood programs available?
- **School choice**: How many quality options are reachable?
- **Equity**: Are high-quality schools accessible to all income levels?

**Data Provenance:**

| Field | Description |
|-------|-------------|
| `nces_year` | Year of NCES school directory |
| `msde_year` | Year of MSDE Report Card data |
| `acs_year` | Year of ACS population data |
| `education_version` | `v1-enrollment` or `v2-accessibility` |

**Source:** `src/ingest/layer3_education_accessibility.py` (v2), `src/ingest/layer3_schools.py` (v1).

**Tract Table:** `layer3_education_accessibility_tract` (new in v2)

### Layer 4 — Housing Elasticity (`layer4_housing_elasticity`)

**Version 2.0 (2026): Affordability-Based Housing Opportunity**

Layer 4 was completely overhauled in January 2026 to use modern housing affordability standards based on HUD CHAS methodology. The new approach measures actual affordability burden and housing quality rather than just supply metrics.

**Primary sources:**
- ACS 5-year estimates: Housing costs, tenure, income, housing quality indicators
- HUD CHAS data: Cost burden analysis by income bracket (when available)
- Census BPS permits: Supply-side metrics (v1, retained)

**Cost Burden Metrics (HUD Standard):**

| Metric | Description |
|--------|-------------|
| `cost_burdened_households` | Households paying >30% of income on housing |
| `severely_cost_burdened_households` | Households paying >50% of income on housing |
| `cost_burdened_pct` | Percentage of households that are cost burdened (0-1) |
| `severely_cost_burdened_pct` | Percentage severely burdened (0-1) |
| `owner_cost_burdened_pct` | Cost burden rate among homeowners |
| `renter_cost_burdened_pct` | Cost burden rate among renters |

**AMI Bracket Analysis (Income Segmentation):**

| Bracket | Column | Definition |
|---------|--------|------------|
| Extremely Low | `households_0_30_ami` | Households at 0-30% of Area Median Income |
| Very Low | `households_30_50_ami` | Households at 30-50% AMI |
| Low | `households_50_80_ami` | Households at 50-80% AMI |
| Moderate | `households_80_100_ami` | Households at 80-100% AMI |
| Above Moderate | `households_100_plus_ami` | Households above 100% AMI |

**Affordable Unit Supply:**

| Metric | Description |
|--------|-------------|
| `units_affordable_0_30_ami` | Units affordable to extremely low income |
| `units_affordable_30_50_ami` | Units affordable to very low income |
| `units_affordable_50_80_ami` | Units affordable to low income |
| `affordable_units_pct` | Percentage of units affordable to low-income households |

**Housing + Transportation (H+T) Burden:**

| Metric | Description |
|--------|-------------|
| `rent_to_income_ratio` | Monthly rent as fraction of monthly income |
| `avg_commute_time_minutes` | Average one-way commute time |
| `estimated_commute_cost_monthly` | Estimated monthly transportation cost |
| `housing_plus_transport_pct` | Combined H+T as percentage of income |

The H+T index concept recognizes that housing in a cheap but remote location may cost more overall when transportation costs are factored in.

**Housing Quality Metrics:**

| Metric | Description |
|--------|-------------|
| `housing_age_median_year` | Median year structure built |
| `pre_1950_housing_pct` | Percentage of housing built before 1950 |
| `crowded_units_pct` | Percentage with >1 person per room |
| `lacking_complete_plumbing_pct` | Percentage lacking complete plumbing |
| `lacking_complete_kitchen_pct` | Percentage lacking complete kitchen |

**Normalized Scores (0-1 scale):**

| Score | Computation |
|-------|-------------|
| `affordability_burden_score` | 1 - cost_burdened_pct (higher = better) |
| `affordable_stock_score` | Percentile rank of affordable_units_pct |
| `housing_quality_score` | Composite: age, crowding, facilities |
| `housing_affordability_score` | Weighted composite (see below) |

**Housing Affordability Score Formula:**
```
housing_affordability_score =
    0.50 × affordability_burden_score +
    0.30 × affordable_stock_score +
    0.20 × housing_quality_score
```

**Composite Opportunity Index (NEW):**
```
housing_opportunity_index =
    0.40 × housing_elasticity_index (v1 supply) +
    0.60 × housing_affordability_score (v2 affordability)
```

This composite becomes the primary Layer 4 metric used for timeseries analysis, combining supply-side responsiveness with demand-side affordability.

**Tract-Level Analysis:**

The v2 system computes affordability metrics at the census tract level first, then aggregates to county using population-weighted averages:

1. **Finer spatial resolution**: Tract-level cost burden varies significantly within counties
2. **Population weighting**: County scores weighted by tract population
3. **Intra-county variation**: Can identify affordability deserts vs affordable areas

**V1 Fields (retained for backwards compatibility):**

- `permits_total`
- `permits_single_family`
- `permits_multifamily`
- `permits_per_1000_households`
- `permits_3yr_trend`
- `median_home_value`
- `median_household_income`
- `price_to_income_ratio`
- `price_to_income_5yr_change`
- `supply_responsiveness_score`
- `housing_elasticity_index` (v1 supply composite)

**Why Affordability > Supply Metrics:**

The v1 approach measured permit activity and price-to-income ratios, which indicates supply responsiveness but not actual affordability. The v2 approach measures what matters for family viability:

- **Cost burden**: Are households struggling to pay for housing?
- **Affordable stock**: Are there units affordable to low/moderate income families?
- **Housing quality**: Is the affordable housing adequate?
- **H+T burden**: Is the total housing + transportation cost sustainable?

The combined `housing_opportunity_index` captures both supply responsiveness (can new housing be built?) and affordability outcomes (can families afford to live there?).

**Data Provenance:**

| Field | Description |
|-------|-------------|
| `acs_year` | Year of ACS data used |
| `chas_year` | Year of HUD CHAS data used (if different) |
| `affordability_version` | `v1-elasticity` or `v2-affordability` |

**Source:** `src/ingest/layer4_housing_affordability.py` (v2), `src/ingest/layer4_housing.py` (v1).

**Tract Table:** `layer4_housing_affordability_tract` (new in v2)

### Layer 5 — Demographic Momentum (`layer5_demographic_momentum`)

**Version 2.0 (2026): Equity-Based Demographic Opportunity with Migration Flows**

Layer 5 was completely overhauled in January 2026 to incorporate modern equity metrics and migration flow analysis. The new approach measures demographic health through integration, family viability, and community movement patterns.

**Primary sources:**
- ACS 5-year estimates: Demographics by tract (race, age, income, family structure)
- IRS SOI Migration: County-to-county migration flows (apportioned to tract)
- Census: Tract boundaries and population

**v1 Static Metrics (retained):**

| Metric | Description |
|--------|-------------|
| `pop_total` | Total population |
| `pop_age_25_44` | Prime working/family-forming age population |
| `households_total` | Total households |
| `households_family` | Family households |
| `inflow_households` | IRS tax returns moving into county |
| `outflow_households` | IRS tax returns moving out of county |
| `net_migration_households` | Inflow - outflow |

**v2 Equity Metrics (NEW):**

| Metric | Description |
|--------|-------------|
| `racial_diversity_index` | Shannon entropy-based diversity (0-1) |
| `dissimilarity_index` | Segregation: proportion that would need to move for even distribution (0-1) |
| `exposure_index` | Probability minority encounters majority (higher = more integrated) |
| `isolation_index` | Probability minority encounters minority |
| `family_viability_score` | Composite: income, poverty, single-parent rate |

**Family Viability Components:**

| Metric | Description |
|--------|-------------|
| `single_parent_pct` | Single-parent households / families with children |
| `poverty_rate` | Population below poverty line |
| `child_poverty_rate` | Children below poverty line |
| `median_family_income` | Median income for family households |

**v3 Migration Dynamics (NEW):**

| Metric | Description |
|--------|-------------|
| `net_migration_rate` | Net migration / population |
| `inflow_rate` | Inflows / population |
| `outflow_rate` | Outflows / population |
| `population_growth_rate` | Year-over-year population change |
| `top_inflow_counties` | Top 5 origin counties (JSON) |
| `top_outflow_counties` | Top 5 destination counties (JSON) |

**Normalized Scores (0-1 scale):**

| Score | Computation |
|-------|-------------|
| `static_demographic_score` | v1: working_age + family_household + diversity |
| `equity_score` | v2: (1-dissimilarity) + exposure + family_viability |
| `migration_dynamics_score` | v3: net_migration + (1-outflow) + growth |
| `demographic_opportunity_index` | Weighted composite (see below) |

**Static Score Formula:**
```
static_demographic_score =
    0.40 × working_age_pct_rank +
    0.30 × family_household_pct_rank +
    0.30 × racial_diversity_rank
```

**Equity Score Formula:**
```
equity_score =
    0.40 × (1 - dissimilarity_index) +
    0.30 × exposure_index_rank +
    0.30 × family_viability_score
```

**Migration Score Formula:**
```
migration_dynamics_score =
    0.50 × net_migration_rate_rank +
    0.20 × (1 - outflow_rate_rank) +
    0.30 × population_growth_rank
```

**Composite Opportunity Index (NEW):**
```
demographic_opportunity_index =
    0.30 × static_demographic_score +
    0.40 × equity_score +
    0.30 × migration_dynamics_score
```

This composite becomes the primary Layer 5 metric used for timeseries analysis.

**Tract-Level Analysis:**

The v2 system computes demographics at census tract level first:

1. **Equity metrics**: Segregation indices computed from tract racial composition within county
2. **Migration apportionment**: County IRS flows apportioned to tracts via population weights
3. **Population weighting**: County scores weighted by tract population

**Why Equity + Migration > Raw Counts:**

The v1 approach measured population trends and household formation, but missed critical signals:

- **Integration**: Are communities becoming more or less segregated?
- **Family conditions**: Can families thrive (income, poverty, structure)?
- **Movement patterns**: Are opportunity-seekers moving in or out?

The combined `demographic_opportunity_index` captures population health (static), community integration (equity), and dynamism (migration).

**Data Provenance:**

| Field | Description |
|-------|-------------|
| `acs_year` | Year of ACS demographic data |
| `demographic_version` | `v1-momentum` or `v2-equity` |

**Source:** `src/ingest/layer5_demographic_equity.py` (v2), `src/ingest/layer5_demographics.py` (v1).

**Tract Table:** `layer5_demographic_equity_tract` (new in v2)

### Layer 6 — Risk Drag (`layer6_risk_drag`)

**Version 2.0 (2026): Climate Vulnerability & Adaptive Capacity**

Layer 6 was completely overhauled in January 2026 to incorporate modern climate projections, social vulnerability assessment, and adaptive capacity metrics. The new approach measures actual climate risk exposure (sea level rise, extreme heat) combined with community vulnerability and resilience factors.

**Primary sources:**
- NOAA Sea Level Rise Viewer: SLR exposure at 1ft, 2ft, 3ft scenarios
- CDC HEAT: Extreme heat projections (RCP 4.5, 2050)
- CDC SVI: Social Vulnerability Index (4 themes)
- EPA EJScreen: Pollution burden indicators
- FEMA NFHL: Special Flood Hazard Areas
- FHWA NBI: Bridge structural condition

**v1 Static Metrics (retained):**

| Metric | Description |
|--------|-------------|
| `sfha_area_sq_mi` | Special Flood Hazard Area in county |
| `sfha_pct_of_county` | SFHA as percentage of county area |
| `pm25_avg` | Annual PM2.5 concentration |
| `ozone_avg` | Annual ozone concentration |
| `proximity_hazwaste_score` | Proximity to hazardous waste facilities |
| `traffic_proximity_score` | Traffic pollution exposure |
| `bridges_deficient_pct` | Structurally deficient bridges percentage |

**v2 Climate Projection Metrics (NEW):**

| Metric | Description |
|--------|-------------|
| `slr_exposure_1ft` | Land fraction exposed at 1ft SLR |
| `slr_exposure_2ft` | Land fraction exposed at 2ft SLR (2050 median) |
| `slr_exposure_3ft` | Land fraction exposed at 3ft SLR (2050 high) |
| `slr_risk_score` | Normalized SLR vulnerability (0-1) |
| `heat_days_above_95f_current` | Current annual days above 95F |
| `heat_days_above_95f_2050` | Projected 2050 days above 95F (RCP 4.5) |
| `heat_days_above_100f_2050` | Projected 2050 extreme heat days |
| `heat_wave_duration_2050` | Average consecutive hot days |
| `heat_vulnerability_score` | Normalized heat risk (0-1) |
| `urban_heat_island_intensity` | Degrees above rural baseline |
| `impervious_surface_pct` | Hard surface coverage |
| `tree_canopy_pct` | Tree cover (cooling effect) |

**v2 Social Vulnerability Metrics (CDC SVI):**

| Metric | Description |
|--------|-------------|
| `socioeconomic_vulnerability` | SVI Theme 1: poverty, unemployment, education |
| `household_vulnerability` | SVI Theme 2: age, disability, single-parent |
| `minority_language_vulnerability` | SVI Theme 3: minority status, limited English |
| `housing_transport_vulnerability` | SVI Theme 4: housing type, vehicle access |
| `social_vulnerability_index` | CDC SVI composite (0-1, higher = more vulnerable) |

**v2 Pollution Burden (expanded):**

| Metric | Description |
|--------|-------------|
| `diesel_pm_exposure` | Diesel particulate matter |
| `air_toxics_cancer_risk` | Cancer risk from air toxics |
| `lead_paint_indicator` | Pre-1960 housing proxy for lead paint |
| `proximity_superfund` | Distance to NPL Superfund sites |
| `proximity_rmp_facilities` | Distance to RMP facilities |
| `proximity_wastewater` | Distance to wastewater discharge |
| `pollution_burden_score` | Composite pollution (0-1) |

**v2 Infrastructure Resilience:**

| Metric | Description |
|--------|-------------|
| `road_flood_exposure_pct` | Roads in flood zone |
| `critical_facility_flood_risk` | Hospitals/fire/police in flood zone |
| `power_outage_risk_score` | Grid vulnerability |
| `broadband_access_pct` | Internet access for emergency info |
| `infrastructure_resilience_score` | Composite infrastructure health (0-1) |

**v2 Adaptive Capacity (positive factors):**

| Metric | Description |
|--------|-------------|
| `hospital_access_score` | Healthcare accessibility |
| `emergency_service_access_score` | Fire/EMS accessibility |
| `cooling_center_count` | Nearby cooling centers |
| `green_space_pct` | Parks/open space for cooling |
| `community_resilience_score` | Civic engagement proxy |
| `adaptive_capacity_index` | Composite adaptive capacity (0-1) |

**Normalized Scores (0-1 scale):**

| Score | Computation |
|-------|-------------|
| `static_risk_score` | v1: 0.35×flood + 0.35×pollution + 0.30×infrastructure |
| `climate_projection_score` | v2: 0.50×SLR + 0.50×heat |
| `vulnerability_score` | v2: social_vulnerability_index |
| `resilience_deficit_score` | 1 - adaptive_capacity_index |
| `modern_vulnerability_score` | Weighted v2 composite (see below) |

**Modern Vulnerability Score Formula:**
```
modern_vulnerability_score =
    0.40 × climate_projection_score +
    0.35 × vulnerability_score +
    0.25 × resilience_deficit_score
```

**Composite Risk Drag Index (NEW):**
```
risk_drag_index =
    0.40 × static_risk_score (v1) +
    0.60 × modern_vulnerability_score (v2)
```

This composite becomes the primary Layer 6 metric used for timeseries analysis and the risk penalty applied to composite county scores.

**Why Climate Vulnerability > Static Hazard Counts:**

The v1 approach measured current flood zones and pollution levels, but missed critical future risk signals:

- **Climate projections**: How will sea level rise and extreme heat affect this area by 2050?
- **Social vulnerability**: Which communities are least able to cope with hazards?
- **Adaptive capacity**: What resources exist for response and recovery?

The combined `risk_drag_index` captures current hazard exposure (static) plus future climate risk and community vulnerability (modern), providing a more complete picture of long-term risk.

**Data Provenance:**

| Field | Description |
|-------|-------------|
| `ejscreen_year` | Year of EPA EJScreen data |
| `svi_year` | Year of CDC SVI data |
| `climate_projection_source` | Climate data source (e.g., 'NOAA_SLR_CDC_HEAT') |
| `risk_version` | `v1-static` or `v2-vulnerability` |

**Source:** `src/ingest/layer6_risk_vulnerability.py` (v2), `src/ingest/layer6_risk.py` (v1).

**Tract Table:** `layer6_risk_vulnerability_tract` (new in v2)

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
- **Layer 3 (Schools v2):** Uses NCES school directory for locations and MSDE Report Card for quality metrics. School accessibility varies significantly between urban and rural areas. MSDE proficiency data may lag 1-2 years. The proximity model approximates travel time as straight-line distance. Pre-K availability is particularly variable across jurisdictions. Enrollment data (v1) may have gaps for some years.
- **Layer 4 (Housing v2):** Uses ACS 5-year estimates for cost burden and affordability metrics. HUD CHAS data provides more detailed AMI bracket analysis but lags 2-3 years. Tract-level affordability varies significantly within counties, especially in mixed urban/suburban areas. The H+T burden estimate uses commute time as a proxy for transportation costs. Permits data (v1) may be unavailable for some years.
- **Layer 5 (Demographics v2):** Uses ACS 5-year estimates for tract-level demographic composition and IRS SOI for county-level migration flows. Segregation indices are computed at the county level and assigned uniformly to tracts within that county. IRS migration data lags 1-2 years and is apportioned to tracts via population weights (not actual tract-level flows). Family viability relies on poverty and income data which may have larger margins of error in smaller tracts.
- **Layer 6 (Risk v2):** Combines multiple data sources with different update frequencies. NOAA SLR projections are based on 2022 intermediate scenarios. CDC HEAT projections use RCP 4.5 (moderate emissions) for 2050. CDC SVI data lags 1-2 years. Climate projections are county-level approximations based on published analyses; tract-level variation uses population-weighted aggregation. FEMA NFHL API is occasionally unstable. EJScreen and NBI data availability varies by year. If critical inputs are missing, the pipeline falls back to synthetic estimates based on county characteristics.

4. **Risk drag handling**

- Risk drag is applied as a penalty to the composite score; if risk is missing, the composite uses only growth layers.

5. **No interpolation, no synthetic fill**

- The pipeline never fabricates values or fills missing years. Any missingness flows through to the final uncertainty.
