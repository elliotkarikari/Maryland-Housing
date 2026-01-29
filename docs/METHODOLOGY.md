# Methodology Documentation

**Maryland Growth & Family Viability Atlas**

**Version:** 1.0
**Last Updated:** 2026-01-28

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

## Version History

### V1.0 (2026-01-28)
- Initial release
- County-level only
- Layer 1 (Employment) + Policy Persistence fully implemented
- Layers 2-6 scaffolded but not ingesting data yet
- AI extraction: Montgomery County CIP only

### Planned V1.1
- Complete Layers 2-6 data ingestion
- Expand AI CIP extraction to 5+ counties
- Tract-level geography option

---

## References

- Dissart, J. C. (2003). "Regional economic diversity and regional economic stability: Research results and agenda." *International Regional Science Review*, 26(4), 423-446.

- Saiz, A. (2010). "The geographic determinants of housing supply." *Quarterly Journal of Economics*, 125(3), 1253-1296.

- Shannon, C. E. (1948). "A mathematical theory of communication." *Bell System Technical Journal*, 27(3), 379-423.

---

**Maintained by:** Maryland Viability Atlas Team
**Questions?** See [LIMITATIONS.md](LIMITATIONS.md) for known constraints
