# Maryland Growth & Family Viability Atlas — Scoring Methodology

## How Scores Work (Plain English)

Each Maryland county gets a **composite score** (0–1) that answers one question:
> "How well-positioned is this county for sustainable family-oriented growth?"

Higher = stronger structural position. Lower = more headwinds.

The score is built in **three stages**:

```
Raw Data → 6 Layer Scores → Composite Score + Classification
```

---

## Stage 1: Six Analytical Layers

Each layer captures one dimension of viability. Every layer produces a score from 0 to 1.

| # | Layer | What It Measures | Key Data Sources |
|---|-------|-----------------|------------------|
| 1 | **Employment Gravity** | Access to good jobs and economic diversification | Census LODES, BLS QCEW, USAspending |
| 2 | **Mobility Optionality** | Transportation options beyond car dependence | OpenTripPlanner, GTFS, MDOT |
| 3 | **School Trajectory** | Quality school access and enrollment health | NCES CCD, MD Report Card, MSDE |
| 4 | **Housing Elasticity** | Can housing supply respond without pricing people out? | Census ACS, HUD FMR, LIHTC |
| 5 | **Demographic Momentum** | Population dynamics — are families moving in or out? | Census ACS, IRS Migration, ACS |
| 6 | **Risk Drag** *(penalty)* | Environmental and infrastructure liabilities | FEMA NFHL, EPA EJScreen, NBI |

### How Each Layer Score Is Calculated

1. **Collect features** — Each layer has 3–6 measurable indicators (e.g., "sector diversity entropy", "flood zone percentage")
2. **Normalize** — Convert raw values to 0–1 scale within Maryland using percentile rank (most features) or robust z-score (heavy-tailed distributions). This ensures a county at the 90th percentile in Maryland scores ~0.9
3. **Weight and average** — Each feature has a weight reflecting its importance. The layer score is the weighted average of all available features
4. **Handle missing data** — If a feature has no data for a county, it's skipped (the average uses only available features). Coverage is tracked separately

### Feature Details by Layer

#### Layer 1: Employment Gravity
| Feature | Source Column | Weight | Direction | What It Means |
|---------|-------------|--------|-----------|---------------|
| Economic Opportunity Index | `economic_opportunity_index` | 2.2 | Higher = better | Combined local job quality + regional accessibility |
| Sector Diversity | `sector_diversity_entropy` | 1.5 | Higher = better | Shannon entropy across 20 NAICS sectors |
| Stable Sector Employment | `stable_sector_share` | 1.0 | Higher = better | Share in education, health, government |
| Diversification Composite | `employment_diversification_score` | 0.8 | Higher = better | Legacy sector diversity composite |
| Federal Spending Stability | `federal_awards_volatility` | 0.8 | Lower = better | Federal funding consistency (CV) |
| Net Job Growth | `qwi_net_job_growth_rate` | 0.7 | Higher = better | Census QWI hires minus separations |

#### Layer 2: Mobility Optionality
| Feature | Source Column | Weight | Direction | What It Means |
|---------|-------------|--------|-----------|---------------|
| Mobility Index | `mobility_optionality_index` | 2.0 | Higher = better | Combined multimodal accessibility score |
| Transit Mode Count | `mode_count` | 1.5 | Higher = better | Count of available transportation modes |
| Highway Access | `interstate_exits` | 1.0 | Higher = better | Number of interstate highway exits |

#### Layer 3: School Trajectory
| Feature | Source Column | Weight | Direction | What It Means |
|---------|-------------|--------|-----------|---------------|
| Education Opportunity Index | `education_opportunity_index` | 2.0 | Higher = better | Combined supply + accessibility composite |
| Education Accessibility | `education_accessibility_score` | 1.5 | Higher = better | Quality schools reachable within commute distance |
| School Supply | `school_supply_score` | 1.0 | Higher = better | Enrollment density and school capacity |

#### Layer 4: Housing Elasticity
| Feature | Source Column | Weight | Direction | What It Means |
|---------|-------------|--------|-----------|---------------|
| Housing Opportunity Index | `housing_opportunity_index` | 2.0 | Higher = better | Combined affordability + elasticity composite |
| Affordability Score | `housing_affordability_score` | 1.5 | Higher = better | Cost burden and affordable stock availability |
| Housing Elasticity Index | `housing_elasticity_index` | 1.0 | Higher = better | Permit activity and supply responsiveness |

#### Layer 5: Demographic Momentum
| Feature | Source Column | Weight | Direction | What It Means |
|---------|-------------|--------|-----------|---------------|
| Demographic Opportunity Index | `demographic_opportunity_index` | 2.0 | Higher = better | Combined static + equity + migration composite |
| Equity Score | `equity_score` | 1.5 | Higher = better | Segregation and family viability equity |
| Static Demographics | `static_demographic_score` | 1.0 | Higher = better | Population structure and working-age share |
| Migration Dynamics | `migration_dynamics_score` | 1.2 | Higher = better | Net migration and household growth |

#### Layer 6: Risk Drag (Penalty Layer)
| Feature | Source Column | Weight | Direction | What It Means |
|---------|-------------|--------|-----------|---------------|
| Risk Drag Index | `risk_drag_index` | 2.0 | Lower = better | Combined static + modern vulnerability |
| Modern Vulnerability | `modern_vulnerability_score` | 1.5 | Lower = better | Climate projection + social vulnerability |
| Static Risk | `static_risk_score` | 1.0 | Lower = better | Flood, pollution, infrastructure deficiency |
| Flood Hazard | `sfha_pct_of_county` | 0.8 | Lower = better | FEMA 100-year floodplain exposure |

> **Note on negative direction:** For risk features, the normalization inverts the score so that *less risk = higher score*. A county with low flood exposure gets a high normalized score.

---

## Stage 2: Composite Score

```
composite = mean(Layer 1, Layer 2, Layer 3, Layer 4, Layer 5)  ×  risk_multiplier
```

### How It Works

1. **Average the five positive layers** (employment, mobility, schools, housing, demographics). Only layers with data are included
2. **Apply Risk Drag as a penalty multiplier:**
   - `risk_multiplier = max(1.0 - risk_drag_score, 0.5)`
   - This reduces the composite by up to 50% for high-risk counties
   - The floor of 0.5 prevents over-penalization

### Example

| County | Emp | Mob | Sch | Hsg | Dem | Avg | Risk | Multiplier | **Composite** |
|--------|-----|-----|-----|-----|-----|-----|------|------------|---------------|
| Montgomery | 0.72 | 0.65 | 0.80 | 0.45 | 0.60 | 0.644 | 0.20 | 0.80 | **0.515** |
| Allegany | 0.30 | 0.25 | 0.40 | 0.55 | 0.35 | 0.370 | 0.60 | 0.50 | **0.185** |

---

## Stage 3: Classification

Each county receives three labels based on the layer scores:

### Directional Status (improving / stable / at_risk)

This answers: *"Which way is this county trending?"*

| Status | Rule |
|--------|------|
| **Improving** | ≥3 layers score ≥0.6 AND ≤1 layer scores <0.3, with positive or no momentum data |
| **At-Risk** | ≥2 layers score <0.3, OR severe risk drag (≥0.5) + at least 1 layer <0.3, OR ≥2 layers with negative momentum |
| **Stable** | Everything else — balanced signals without clear trajectory |

### Confidence Level (strong / conditional / fragile)

This answers: *"How much data backs this classification?"*

| Level | Rule |
|-------|------|
| **Strong** | Average coverage ≥5 years AND minimum ≥3 years across all layers |
| **Conditional** | Average coverage ≥3 years but some layers have <3 years |
| **Fragile** | Average coverage <3 years |

### Final Synthesis Grouping

Combines directional + confidence into 5 intuitive categories:

| Grouping | Meaning | When Assigned |
|----------|---------|---------------|
| **Emerging Tailwinds** | Strong upside with reinforcing strengths | Improving + Strong confidence |
| **Conditional Growth** | Upside exists, but delivery risk matters | Improving + Conditional confidence |
| **Stable but Constrained** | Balanced conditions, limited upside | Stable direction |
| **At Risk / Headwinds** | Headwinds currently outweigh strengths | At-Risk direction |
| **High Uncertainty** | Too little data to classify confidently | Fragile confidence or ≥2 uncertainty reasons |

---

## Normalization Methods

All normalization is **within Maryland only** (24 counties). No national comparison.

| Method | When Used | How It Works |
|--------|----------|-------------|
| **Percentile rank** | Most features | Rank counties 1–24, divide by count. Robust to outliers |
| **Robust z-score** | Heavy-tailed distributions (e.g., migration flows) | `(value - median) / IQR`, clipped to ±3, mapped to 0–1 |
| **Min-max** | Rarely used | `(value - min) / (max - min)` |

---

## Data Pipeline

```
1. Ingest raw data       → layer1–6 tables (one per layer)
2. Normalize features    → percentile rank or robust z-score (0–1)
3. Score layers          → weighted average of normalized features per layer
4. Multi-year scoring    → combine level + momentum + stability across years
5. Classify counties     → directional status + confidence + final grouping
6. Export GeoJSON        → frontend-ready file with all scores and labels
```

### Robustness Measures

- **Missing features don't break scoring** — weighted average uses only available features
- **Coverage tracking** — each layer records how many years of data it has
- **Risk drag has a penalty floor** — can't reduce composite below 50%
- **Null score backfill** — if a layer score is null in the synthesis table, the API and GeoJSON export fall back to the layer table's primary composite column
- **Within-Maryland normalization** — avoids distortion from national outliers

---

## Quick Reference: What Each Score Means

| Score Range | Interpretation |
|------------|----------------|
| 0.70–1.00 | Strong structural position |
| 0.50–0.69 | Moderate — some strengths, some gaps |
| 0.35–0.49 | Constrained — meaningful headwinds |
| 0.00–0.34 | Weak — significant structural challenges |

These ranges apply to both individual layer scores and the composite score.
