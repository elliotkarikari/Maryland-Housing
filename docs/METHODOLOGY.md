# Methodology Documentation

**Maryland Growth & Family Viability Atlas**

**Version:** 1.1
**Last Updated:** 2026-02-15

---

## Overview

This document describes the analytical methodology used to classify Maryland counties by directional growth pressure and policy persistence confidence.

### Core Principle

This tool **does not predict** future outcomes. It identifies **directional pressure** under the assumption that **current policies and trends persist**.

Think of it as asking: "If nothing changes, which counties have structural tailwinds vs headwinds?"

---

## Analytical Framework

### Six Analytical Layers

Each layer measures a distinct dimension of structural advantage or constraint:

1. **Employment Gravity** - Income stability through sector diversification
2. **Mobility Optionality** - Job-change resilience via transportation redundancy
3. **School System Trajectory** - Family viability through enrollment & capital trends
4. **Housing Elasticity** - Growth absorption capacity
5. **Demographic Momentum** - Demand pressure from migration & household formation
6. **Risk Drag** - Long-term environmental & infrastructure constraints (PENALTY layer)

### Plus: Confidence Overlay

**Policy Persistence** - Modifies confidence in classifications based on delivery track record

---

## Data Processing Pipeline

### Stage 1: Data Ingestion

**Principle:** Only real, verifiable open data sources. No synthetic data, ever.

#### Layer 1: Employment Gravity

**Data Sources:**
- US Census LEHD/LODES (employment by sector, block-level aggregated to county)
- BLS QCEW (wages, establishments, quarterly)
- USASpending.gov (federal awards, 5-year window)

##### Layer 1 County Table (Databricks): `layer1_employment_gravity`

This is the county-level Layer 1 table used by the API fallback and multi-year scoring pipeline.
The table mixes observed inputs, derived metrics, and predicted continuity fields.

**Observed vs Predicted Coverage (Databricks snapshot as of 2026-02-15):**

| data_year | county rows | observed `economic_opportunity_index` | predicted flag (`economic_opportunity_index_predicted`) |
|-----------|-------------|----------------------------------------|---------------------------------------------------------|
| 2018 | 24 | 24 | 0 |
| 2019 | 24 | 24 | 0 |
| 2020 | 24 | 24 | 0 |
| 2021 | 24 | 24 | 0 |
| 2022 | 24 | 24 | 0 |
| 2023 | 24 | 0 | 24 |
| 2024 | 24 | 0 | 24 |
| 2025 | 24 | 0 | 24 |

**County Derived Columns and Methodology (current implementation):**

| Column | Type | Method (simple) | Formula/logic | Feeds final county index? |
|--------|------|------------------|---------------|---------------------------|
| `high_wage_jobs_accessible_45min` | Derived | For each tract, sum high-wage jobs reachable in 45-min threshold; county takes tract max | Haversine thresholded catchment, then county max over tracts | Indirectly (through `economic_accessibility_score`) |
| `high_wage_jobs_accessible_30min` | Derived | Same as above with tighter threshold | Same method, 30-min proxy distance | No (diagnostic context) |
| `total_jobs_accessible_45min` | Derived | Sum all reachable jobs within 45-min threshold; county takes tract max | Thresholded catchment, then county max | Indirectly (through `job_market_reach_score`) |
| `total_jobs_accessible_30min` | Derived | Same as above with tighter threshold | Thresholded catchment, then county max | No (diagnostic context) |
| `wage_quality_ratio` | Derived | Share of reachable jobs that are high-wage | `high_wage_jobs_accessible_45min / total_jobs_accessible_45min` | Indirectly (through `job_quality_index`) |
| `economic_accessibility_score` | Derived | Percentile rank (within Maryland) of high-wage reach | `rank_pct(high_wage_jobs_accessible_45min)` | Yes (primary direct term) |
| `job_market_reach_score` | Derived | Percentile rank (within Maryland) of total job reach | `rank_pct(total_jobs_accessible_45min)` | No (supporting metric) |
| `job_quality_index` | Derived | Blend access volume and wage mix quality | `0.7 * economic_accessibility_score + 0.3 * rank_pct(wage_quality_ratio)` | No (supporting metric) |
| `upward_mobility_score` | Derived | Current proxy for mobility | equals `economic_accessibility_score` | No (supporting metric) |
| `pct_regional_high_wage_accessible` | Derived | County share of Maryland high-wage jobs | `high_wage_jobs / statewide_high_wage_jobs` | No (diagnostic context) |
| `pct_regional_jobs_accessible` | Derived | County share of Maryland jobs | `total_jobs / statewide_total_jobs` | No (diagnostic context) |
| `high_wage_sector_concentration` | Derived | Concentration in high-wage sectors (risk of concentration) | Herfindahl-Hirschman Index on selected sectors | No (diagnostic context) |
| `qwi_hire_rate` | Derived | Hiring intensity from QWI | hires / employment (or provided rate) | Indirectly (through `qwi_net_job_growth_score`) |
| `qwi_separation_rate` | Derived | Separation intensity from QWI | separations / employment (or provided rate) | Indirectly (through `qwi_net_job_growth_score`) |
| `qwi_turnover_rate` | Derived | Labor churn | `qwi_hire_rate + qwi_separation_rate` (or provided) | No (supporting metric) |
| `qwi_net_job_growth_rate` | Derived | Net job dynamics | `qwi_hire_rate - qwi_separation_rate` (or `(hires-separations)/employment`) | Indirectly (ranked to `qwi_net_job_growth_score`) |
| `employment_diversification_score` | Derived/fallback | Legacy local strength reused when available | from prior v1 fields; optional fallback | Yes (if available) |
| `economic_opportunity_index` | Derived | Final observed county Layer 1 index | `base_index = 0.4 * local_strength + 0.6 * economic_accessibility_score`; if QWI score exists then `0.85 * base_index + 0.15 * qwi_net_job_growth_score` | Yes (final Layer 1 county metric) |
| `economic_opportunity_index_pred` | Predicted | Trend-based continuation of final index for future years | Theil-Sen trend fit per county, clipped to [0,1] | Used only for modeled years |
| `economic_opportunity_index_effective` | Derived wrapper | Single usable value column for consumers | `COALESCE(economic_opportunity_index, economic_opportunity_index_pred)` | Yes for cross-year continuity |

**Observed Input/Provenance Fields written to the county table:**
- LODES-derived observed counts: `high_wage_jobs`, `mid_wage_jobs`, `low_wage_jobs`
- QWI observed counts: `qwi_emp_total`, `qwi_hires`, `qwi_separations`
- Source-year lineage: `lodes_year`, `acs_year`, `qwi_year`
- Versioning: `accessibility_version='v2-accessibility'` for observed v2 rows

**Metrics Calculated:**
- **Sector Diversity Entropy**: Shannon entropy across 20 NAICS sectors
  - Formula: H = -Σ(p_i * log₂(p_i)) where p_i = share of employment in sector i
  - Range: 0 (single sector) to 4.32 (perfectly diversified across 20 sectors)
  - Hypothesis: Higher entropy → lower income volatility during sector-specific shocks

- **Stable Sector Share**: Employment in education, health care, and public administration
  - These sectors show lower cyclical volatility historically
  - Formula: (jobs_education + jobs_health + jobs_public_admin) / total_jobs

- **Federal Spending Volatility**: Coefficient of variation of USASpending awards (2020-2025)
  - CV = σ / μ
  - Lower CV indicates more predictable federal revenue stream
  - Used as inverse in scoring (1 - CV)

#### Policy Persistence (Confidence Modifier)

**Deterministic Component:**
- Federal spending year-over-year consistency (1 - CV of annual awards)

**AI-Extracted Component** (V1: partial coverage):
- CIP follow-through rate from Montgomery County (real PDF extraction)
  - Extracts: total capital budget, school capital, multi-year commitments
  - Validates against Pydantic schema
  - Stored with full provenance (model, prompt version, cost)

**Confidence Score Calculation:**
```python
if has_CIP_data:
    confidence = 0.4 * federal_consistency + 0.6 * cip_follow_through
else:
    confidence = 0.8 * federal_consistency  # Capped at 0.8 when missing CIP
```

**Classification Thresholds:**
- **Strong**: confidence ≥ 0.67
- **Conditional**: 0.34 ≤ confidence < 0.67
- **Fragile**: confidence < 0.34

**Implementation note (current pipelines):**
- The V1 confidence overlay is policy persistence–based (as above).
- The V2 multi-year pipeline also computes **coverage-based confidence** using
  years of available data. The production API **prefers** `final_synthesis_current`
  when populated, but now falls back to latest available layer-table snapshots
  for map/detail continuity during incremental ingest.

### Observed vs Modeled Year Policy (Current)

- The pipeline `--year` is the **as-of synthesis year**, not a guarantee that every input source has observed data in that same year.
- Observed source bounds are centralized in `src/utils/year_policy.py` and controlled by runtime settings:
  - `LODES_LATEST_YEAR` with `LODES_LAG_YEARS`
  - `ACS_LATEST_YEAR` and `ACS_GEOGRAPHY_MAX_YEAR`
  - `NCES_OBSERVED_MAX_YEAR`
  - `PREDICT_TO_YEAR`
- Layer ingestion modules clamp to the latest observed source year and explicitly separate observed from modeled/fallback behavior instead of relabeling future years as observed.

---

### Stage 2: Normalization

**Principle:** All normalization is **within Maryland only** (no national comparison).

**Methods:**

1. **Percentile Rank** (default for most features)
   - Robust to outliers
   - Produces 0-1 scores directly
   - Formula: rank(x) / n_counties
   - Used for: employment diversity, stable sector share, most composites

2. **Robust Z-Score** (for heavy-tailed distributions)
   - Uses median and IQR instead of mean/std
   - Formula: (x - median) / IQR, then clipped to ±3 IQR
   - Mapped to 0-1: (z + 3) / 6
   - Used for: migration flows, federal spending volatility

**Directionality:**
- **Positive features**: Higher raw value → higher score (e.g., employment diversity)
- **Negative features**: Lower raw value → higher score (e.g., flood risk, spending volatility)
  - Inverted: score = 1 - percentile_rank(x)

---

### Stage 3: Layer Scoring

**Principle:** Weighted aggregation within each layer, then risk drag applied as penalty.

**Within-Layer Aggregation:**
```python
layer_score = Σ(w_i * normalized_feature_i) / Σ(w_i)
```

Where weights (w_i) are defined in the feature registry. Example for Employment Gravity:
- employment_diversification_composite: weight = 2.0 (primary signal)
- sector_diversity_entropy: weight = 1.5
- stable_sector_share: weight = 1.0
- federal_spending_stability: weight = 0.8

**Where weights live (source of truth):**
- Feature weights: `src/processing/feature_registry.py`
- Temporal composition weights: `src/processing/multiyear_scoring.py`
  - Level: 0.50
  - Momentum: 0.30
  - Stability: 0.20

**Handling missing temporal components (current behavior):**
- If momentum is missing, weights are rebalanced to level-only or level+momentum.
- Scores receive a **missingness penalty** when coverage years are low.

**Risk Drag (Subtractive Only):**

Risk Drag is **not** averaged with positive layers. It is applied as a penalty:

```python
composite_raw = mean([employment_score, mobility_score, school_score,
                      housing_score, demographic_score])

composite_adjusted = composite_raw * (1 - risk_drag_score)
```

Example:
- If composite_raw = 0.8 and risk_drag = 0.3:
  - composite_adjusted = 0.8 * (1 - 0.3) = 0.56
- High risk drag (flood, infrastructure deficiency) reduces the ceiling of potential.

---

## Missing Data Handling (Current Behavior)

**Feature-level missingness:**
- Missing values remain NaN and are excluded from normalization.
- Normalization ranks only valid values; missing values stay missing.

**Layer-level missingness:**
- Layer scores are computed from available components only.
- A missingness penalty reduces the score when coverage is below thresholds
  (see `calculate_missingness_penalty()` in `multiyear_scoring.py`).

**Classification impact:**
- Sparse data (fewer than 2 valid layers) triggers **High Uncertainty**.
- Confidence is reduced when coverage is low (see Stage 4).

### API/Map Population Behavior (Databricks Runtime)

- `GET /api/v1/layers/counties/latest` serves county geometries from `md_counties`.
- If `final_synthesis_current` rows exist, synthesis values are used directly.
- If synthesis rows are missing, the API derives county properties from the latest available rows in:
  - `layer1_employment_gravity`
  - `layer2_mobility_optionality`
  - `layer3_school_trajectory`
  - `layer4_housing_elasticity`
  - `layer5_demographic_momentum`
  - `layer6_risk_drag`
- Derived values include:
  - `composite_score` (mean of available non-risk layers)
  - `directional_class`, `confidence_class`, and `synthesis_grouping` (deterministic fallback rules)
- Result: the map improves progressively as each ingest completes, rather than blocking on full synthesis refresh.

### Stage 4: Classification

**Directional Status** (describes structural tailwinds):

**Improving:**
- ≥3 layers with score ≥ 0.6 AND
- No layers with score < 0.3

**At Risk:**
- ≥2 layers with score < 0.3 OR
- Risk drag ≥ 0.7 AND ≥1 layer < 0.4

**Stable:**
- All other cases (neither clearly improving nor at risk)

**Thresholds are configurable** in `config/settings.py`:
```python
THRESHOLD_IMPROVING_MIN_LAYERS = 3
THRESHOLD_IMPROVING_HIGH = 0.6
THRESHOLD_IMPROVING_LOW = 0.3
THRESHOLD_AT_RISK_COUNT = 2
THRESHOLD_AT_RISK_LOW = 0.3
THRESHOLD_AT_RISK_WITH_DRAG = 0.4
```

**Why these thresholds?**
- 0.6 = top ~40% of Maryland counties (strong performers)
- 0.3 = bottom ~30% (weak performers)
- Requiring 3 layers prevents single-factor classifications
- Risk drag override recognizes that environmental/infrastructure constraints can dominate

---

## Explainability

Every county receives:

1. **Primary Strengths** (top 2 performing layers)
2. **Primary Weaknesses** (bottom 2 performing layers)
3. **Key Trends** (narrative summaries based on classification)

Example:
```json
{
  "fips_code": "24031",
  "county_name": "Montgomery County",
  "directional_class": "improving",
  "confidence_class": "strong",
  "primary_strengths": [
    "Employment Gravity",
    "School System Trajectory"
  ],
  "primary_weaknesses": [
    "Housing Elasticity"
  ],
  "key_trends": [
    "Multiple reinforcing structural tailwinds present",
    "High policy delivery reliability"
  ]
}
```

---

## Assumptions & Justifications

### Explicit Assumptions

1. **Employment diversification reduces income volatility**
   - Justification: Shannon entropy maximizes uncertainty, minimizing concentration risk
   - Empirical support: Diversified economies show lower GDP volatility (Dissart 2003)

2. **Modal redundancy increases job-change resilience**
   - Binary: has rail + highway vs not
   - Justification: Multiple commute options enable broader job search radius
   - Does NOT assume shorter commute = better (rich debate, not our claim)

3. **Enrollment growth signals family demand**
   - 3-year rolling average to smooth volatility
   - Justification: Families move to areas with school capacity growth
   - Correlation ≠ causation acknowledged

4. **Permit trends indicate supply responsiveness**
   - Permits per 1000 households (intensity measure)
   - Justification: Higher permit rates → more elastic supply (Saiz 2010)
   - Limitation: Permits ≠ completions

5. **Prime working-age inflow drives household formation**
   - Ages 25-44 from IRS migration data
   - Justification: Peak household formation years
   - Does NOT claim "more people = better" universally

6. **Flood risk is persistent**
   - FEMA SFHA (100-year floodplain) as proxy
   - Justification: Physical geography changes slowly
   - Limitation: Does not model future climate change adequately

### What This Model Does NOT Assume

- ❌ Growth is always good
- ❌ Higher scores = higher house prices
- ❌ Current trends will continue (we model IF they do)
- ❌ All counties want growth
- ❌ Policy makers will act rationally

---

## Sensitivity Analysis

**How robust are classifications to threshold changes?**

Tested scenarios:
1. Increase THRESHOLD_IMPROVING_HIGH from 0.6 to 0.7
   - Result: ~30% of "Improving" counties reclassified to "Stable"

2. Lower THRESHOLD_AT_RISK_LOW from 0.3 to 0.2
   - Result: ~15% of "At Risk" counties reclassified to "Stable"

3. Remove risk drag penalty
   - Result: Coastal counties shift up one category

**Conclusion:** Classifications are sensitive to thresholds, which is **transparent and adjustable**, unlike black-box models.

---

## Model Falsification Criteria

**What would prove this model wrong?**

1. **Counties with strong tailwinds showing persistent household decline**
   - If "Improving" counties lose population for 5+ years → model assumptions flawed

2. **High policy persistence + abandoned projects**
   - If "Strong" confidence counties abandon CIP projects → follow-through metric invalid

3. **Low risk drag + infrastructure failure**
   - If counties with low risk scores experience systemic failure → risk indicators insufficient

4. **High mobility optionality + wage stagnation despite job diversity**
   - If modal redundancy doesn't correlate with labor market flexibility → mechanism broken

---

## Planned Refinements (Not Yet Implemented)

- Add USPS vacancy, HUD FMR, and LIHTC data to strengthen Housing Elasticity.
- Add graduation rates and early childhood access to strengthen School Trajectory.
- Expand Census QWI longitudinal coverage and QA checks to improve employment timeliness.
- Publish a weights table auto-generated from the feature registry.
- Add a threshold sensitivity appendix (±0.05 thresholds, ±20% weights).
- Evaluate a capped risk-drag penalty and report its impact before changing the primary score.

## Version History

### V1.1 (2026-02-15)
- Layer ingestion for 1-6 is implemented and wired into default workflows.
- Multi-year scoring and classification are the canonical production path.
- Year selection policy is centralized (`src/utils/year_policy.py`) and exposed through runtime metadata.
- Confidence behavior remains coverage-aware, with policy-persistence overlays where available.
- AI extraction remains partial-coverage and capability-gated at runtime.

### Planned Next
- Expand AI CIP extraction to additional counties with verification workflow.
- Add tract-level and sub-county presentation where source coverage allows.
- Publish sensitivity appendices for threshold/weight perturbations.

---

## References

- Dissart, J. C. (2003). "Regional economic diversity and regional economic stability: Research results and agenda." *International Regional Science Review*, 26(4), 423-446.

- Saiz, A. (2010). "The geographic determinants of housing supply." *Quarterly Journal of Economics*, 125(3), 1253-1296.

- Shannon, C. E. (1948). "A mathematical theory of communication." *Bell System Technical Journal*, 27(3), 379-423.

---

**Maintained by:** Maryland Viability Atlas Team
**Questions?** See [LIMITATIONS.md](LIMITATIONS.md) for known constraints
