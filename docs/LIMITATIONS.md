# Limitations & Known Constraints

**Maryland Growth & Family Viability Atlas - V1.0**

**Last Updated:** 2026-01-28

---

## Purpose of This Document

This document explicitly catalogs what this system **does not do**, **cannot do**, and **does not claim** to do.

Transparency about limitations is not a weakness—it's a requirement for responsible analytical products.

---

## 1. What This Product Does NOT Do

### 1.1 Does NOT Predict Housing Prices

**Why not:**
- Housing prices depend on:
  - Interest rates (national/global, not modeled)
  - Investor sentiment (behavioral, not modeled)
  - Credit availability (financial system, not modeled)
  - National migration trends (beyond Maryland scope)

**What we DO:**
- Identify structural conditions that *could* support or constrain growth *if trends persist*

### 1.2 Does NOT Provide Investment Recommendations

**Why not:**
- Investment decisions require risk tolerance, time horizon, individual circumstances
- Our directional classifications ≠ "buy here, not there"
- Real estate is hyper-local; county-level is too coarse for investment

**What we DO:**
- Provide context for policy planning, not personal finance

### 1.3 Does NOT Forecast Specific Outcomes

**Why not:**
- No time-series forecasting models
- No regression predicting future population/employment/prices
- Directional pressure ≠ magnitude or timing

**What we DO:**
- Classify counties by whether current trends point toward growth tailwinds or headwinds

### 1.4 Does NOT Use Machine Learning for Scoring

**Why not:**
- ML models are black boxes (poor governance)
- Overfitting risk on small sample (24 counties)
- Threshold-based rules are transparent and auditable

**What we DO:**
- Rule-based classification with explicit, configurable thresholds

---

## 2. Geographic Limitations

### 2.1 County-Level Only (V1)

**Limitation:**
- Tract-level analysis requires significantly more complex data fusion
- County boundaries often obscure intra-county variation (e.g., Montgomery vs Baltimore County heterogeneity)

**Implication:**
- Classifications may not reflect neighborhood-level dynamics
- Urban vs suburban vs rural differences within county not captured

**Future Work:**
- V1.1: Tract-level option where data density allows

### 2.2 Maryland Only

**Limitation:**
- Border effects not modeled:
  - DC metro area dynamics (commuters crossing state lines)
  - Virginia/Pennsylvania labor market integration
- No comparison to other states

**Implication:**
- Cannot answer "Is Montgomery County improving relative to Fairfax County?"
- Maryland-only normalization may obscure national trends

**Future Work:**
- Regional expansion (DMV metro area) if demand exists

### 2.3 Baltimore City Separate Reporting

**Limitation:**
- Baltimore City often reported separately from Baltimore County in source data
- Some datasets aggregate city+county; others split

**Current Handling:**
- Baltimore City treated as separate entity (FIPS 24510)
- May have missing data in some layers due to reporting inconsistencies

---

## 3. Data Source Limitations

### 3.1 V1 Data Exclusions (Access Constraints)

The following indicators were **excluded from V1** because no programmatic data access exists:

#### County Zoning Capacity
- **Issue:** Only 8 of 24 Maryland counties have machine-readable zoning GIS layers
- **Impact:** Housing Elasticity layer relies on permits only (supply responsiveness proxy incomplete)
- **V1 Workaround:** Binary indicator (has zoning GIS = 1, else 0)
- **Future:** Manual data collection or county partnerships

#### Planning Commission Approvals
- **Issue:** No standardized API for zoning/development approvals
- **Impact:** Policy Persistence layer cannot measure approval consistency
- **V1 Workaround:** Excluded entirely
- **Future:** Web scraping of meeting minutes (labor-intensive)

#### County Budget Follow-Through
- **Issue:** Historical county budgets not centralized; each county maintains separately
- **Impact:** Cannot calculate budget vs actual spend divergence systematically
- **V1 Workaround:** Use CIP data where available (AI-extracted)
- **Future:** Partner with Maryland Association of Counties for standardized reporting

#### Leadership Tenure Data
- **Issue:** No authoritative API for county executive/council tenure
- **Impact:** Cannot model political continuity as covariate
- **V1 Workaround:** Excluded
- **Future:** Manual assembly from Ballotpedia + county clerk records

### 3.2 Temporal Lags

| Data Source | Latest Available | Reporting Lag | Update Frequency |
|-------------|------------------|---------------|------------------|
| LEHD/LODES | 2021 | 2 years | Annual |
| IRS Migration | 2021-2022 | 2 years | Annual |
| ACS 5-Year | 2019-2023 | 1 year | Annual |
| BLS QCEW | 2025 Q3 | 1 quarter | Quarterly |
| FEMA NFHL | Rolling | Varies by county | Rolling |

**Implication:**
- Employment Gravity layer uses 2021 LODES data (4-year lag as of 2026)
- Recent economic shocks (2022+) not reflected in LODES
- Demographic Momentum uses ACS 2019-2023 (pre-COVID + early COVID period)

**Mitigation:**
- BLS QCEW provides more recent employment trends (quarterly)
- Document lag explicitly in output metadata

### 3.3 Missing Data by County

Some counties have systematically missing data:

- **Garrett County** (24023): Limited transit data (rural, no GTFS feeds)
- **Somerset County** (24039): Sparse federal spending records (small population)
- **Kent County** (24029): Minimal development permits (low growth)

**Handling:**
- Missing features → NaN in normalization
- Layer scores calculated from available features only
- Classifications note reduced confidence when <70% feature coverage

---

## 4. Methodological Limitations

### 4.1 Correlation ≠ Causation

**We measure:**
- Employment diversity, enrollment growth, permit trends

**We do NOT claim:**
- "Diversifying employment CAUSES economic resilience"
- "Growing enrollment CAUSES family inflow"

**Why it matters:**
- These are **correlates**, not **causal mechanisms**
- Could be reverse causality (e.g., growth → school investment, not vice versa)

### 4.2 Assumes Policy Persistence

**Core assumption:**
- "Current policies and trends persist"

**Breaks down when:**
- Major policy shifts (new county executive with different priorities)
- External shocks (federal infrastructure bill, recession, pandemic)
- Zoning reform (sudden upzoning can change housing elasticity)

**Mitigation:**
- Policy Persistence layer explicitly models delivery reliability
- Confidence modifiers acknowledge political fragility

### 4.3 No Feedback Loops

**Limitation:**
- Model treats layers as independent
- Reality: school investment → family inflow → tax base → more school investment (positive feedback)

**Implication:**
- May underestimate acceleration in "Improving" counties
- May underestimate deterioration in "At Risk" counties

**Future:**
- System dynamics modeling (V2.0+ complexity)

### 4.4 Threshold Sensitivity

**Limitation:**
- Classification thresholds (0.3, 0.6, 0.67) are somewhat arbitrary
- Small score changes near thresholds cause classification flips

**Example:**
- County with composite score 0.59 = "Stable"
- County with composite score 0.61 = "Improving"
- But 0.02 difference may not be meaningful

**Mitigation:**
- Thresholds are configurable and documented
- Users can adjust in `config/settings.py` and re-run
- Future: Probabilistic classifications with uncertainty bands

---

## 5. AI Extraction Limitations

### 5.1 Limited Coverage (V1)

**Current Status:**
- CIP extraction: **1 county** (Montgomery County)
- Remaining 23 counties: No AI-extracted CIP data

**Why:**
- Requires accessible, machine-readable CIP PDFs
- Each county formats CIPs differently (no standardization)
- Manual verification needed before production use

**Impact:**
- Policy Persistence layer has reduced confidence for 23/24 counties
- Confidence capped at 0.8 (vs 1.0 max) when CIP data missing

### 5.2 Hallucination Risk

**Issue:**
- LLMs can "hallucinate" facts not present in documents
- Structured output + validation reduces but does not eliminate risk

**Mitigation:**
- Low-confidence extractions (< 0.6) flagged for manual review
- Budget values validated for internal consistency (school budget ≤ total budget)
- Outliers (>3 std dev from county mean) flagged
- Full provenance tracking (model, prompt version, SHA256 of source doc)

### 5.3 PDF Quality Dependency

**Issue:**
- Text extraction from PDF depends on:
  - Not being image-scanned (OCR required)
  - Proper text encoding (some PDFs have garbled text)
  - Table structure preservation (often broken)

**Example Failure Mode:**
- CIP budget table in PDF → extracted as continuous text → numbers misaligned

**Mitigation:**
- Validation checks for implausible values
- Manual review queue for failed extractions
- Store raw PDF hash for re-processing if extraction logic improves

### 5.4 Cost Constraints

**V1 Budget:**
- $5.00 per pipeline run (max)
- Montgomery County CIP extraction: ~$0.50 per run (GPT-4 Turbo)
- 10 counties at current rate = $5.00 (budget limit)

**Implication:**
- Cannot extract all 24 counties in single run without increasing budget
- Trade-off: coverage vs cost

**Mitigation:**
- Caching: same document hash → skip re-extraction
- Cost tracking logged in database
- Alert if cost limit exceeded

---

## 6. Risk Drag Layer Limitations

### 6.1 Climate Change Modeling

**Limitation:**
- FEMA SFHA represents historical 100-year floodplain
- Does NOT model:
  - Sea level rise (accelerating)
  - Increased precipitation intensity (climate change)
  - Future flood risk (only current)

**Impact:**
- Coastal counties (Anne Arundel, Calvert, etc.) may be under-penalized
- Risk Drag may not reflect 2040-2050 conditions

**Mitigation:**
- NOAA Sea Level Rise Viewer used for coastal counties (binary flag)
- Document that risk is "current snapshot, not future projection"

### 6.2 Infrastructure Data Granularity

**Limitation:**
- Bridge condition from National Bridge Inventory (point data)
- Road quality, sewer systems, water systems not systematically tracked

**Impact:**
- Infrastructure deficiency may be underestimated
- County-level aggregation obscures localized failures

### 6.3 Environmental Justice Weighting

**Limitation:**
- EPA EJScreen includes both environmental exposure AND demographic weighting
- We use environmental exposure only (PM2.5, ozone, proximity to hazwaste)
- Do NOT use demographic/socioeconomic components

**Why:**
- This tool is not an environmental justice tool
- Conflating exposure with demographics would introduce circularity

---

## 7. Update Cadence Constraints

### 7.1 Not Real-Time

**Limitation:**
- Data updates: monthly to annual (depending on source)
- Classifications may lag market conditions by 3-12 months

**Example:**
- Employment shock in Jan 2026 → QCEW data available Apr 2026 → next pipeline run May 2026
- Total lag: 4 months

### 7.2 Irregular Source Updates

**Issue:**
- ACS 5-year estimates: annual release (December)
- LEHD/LODES: annual release (unpredictable, 18-24 month lag)
- CIP documents: published on county fiscal year calendars (varies)

**Implication:**
- Cannot guarantee all layers use same reference year
- "Data year 2023" may include 2021 LODES + 2023 ACS + 2025 QCEW

**Mitigation:**
- Metadata tracks source year per layer
- Export includes `last_updated` timestamp per feature

---

## 8. Jurisdictional & Political Limitations

### 8.1 County Boundaries Are Arbitrary

**Issue:**
- Labor markets, housing markets, school catchments cross county lines
- Counties are administrative units, not economic units

**Example:**
- Montgomery County + Prince George's County + DC = integrated metro labor market
- But model treats them as independent

### 8.2 No Policy Recommendations

**Limitation:**
- We classify, we do not prescribe
- "At Risk" does not mean "invest in X"
- "Improving" does not mean "do nothing"

**Why:**
- Policy is normative (values-driven)
- This tool is descriptive (evidence-driven)
- Conflating the two → advocacy, not analysis

---

## 9. Known Bugs & Technical Debt (V1)

### 9.1 Layers 2-6 Not Fully Implemented

**Status:**
- Layer 1 (Employment): ✅ Fully functional
- Layers 2-6: ⚠️ Scaffolded but not ingesting data

**Impact:**
- V1 scores based primarily on Employment Gravity
- Classifications may be premature until all layers operational

**Timeline:**
- V1.1 (target Q2 2026): Complete Layers 2-6

### 9.2 Tract-Level Not Yet Supported

**Status:**
- Schema designed for tract-level
- Ingestion pipelines county-level only

**Impact:**
- Cannot serve neighborhood-level queries
- GeoJSON export county-level only

### 9.3 No Historical Time Series

**Status:**
- Database stores multiple years
- No trend analysis or year-over-year comparison UI

**Impact:**
- Cannot answer "Is Montgomery improving faster than Howard?"
- Only current snapshot available via API

---

## 10. Transparency Commitments

### What We Promise

1. **No Hidden Data:**
   - All data sources documented in [DATA_SOURCES.md](DATA_SOURCES.md)
   - No proprietary datasets

2. **No Black Boxes:**
   - All scoring logic in [METHODOLOGY.md](METHODOLOGY.md)
   - Thresholds configurable in code

3. **No Fabrication:**
   - If data unavailable → documented as "missing"
   - Never filled with synthetic/imputed values

4. **Versioning:**
   - All exports include version ID + timestamp
   - Schema changes tracked in git history

5. **Cost Transparency:**
   - AI extraction costs logged in database
   - Monthly spend reports available

### What We Cannot Promise

1. **Accuracy:**
   - Data sources may have errors (garbage in, garbage out)
   - We validate for plausibility, not ground truth

2. **Completeness:**
   - Some counties have sparse data
   - Some features unavailable for all geographies

3. **Stability:**
   - Classifications may change as data updates
   - Threshold changes reclassify counties

4. **Actionability:**
   - Analysis ≠ recommendations
   - Use at your own risk

---

## 11. Future Enhancements (Roadmap)

### V1.1 (Q2 2026)
- [ ] Complete Layers 2-6 data ingestion
- [ ] Expand AI CIP extraction to 5+ counties
- [ ] Add historical time series API

### V1.2 (Q3 2026)
- [ ] Tract-level geography option
- [ ] Interactive frontend (Mapbox GL JS)
- [ ] Uncertainty bands on classifications

### V2.0 (2027+)
- [ ] Regional expansion (DMV metro area)
- [ ] Scenario analysis ("What if zoning reform?")
- [ ] Feedback loop modeling (system dynamics)

---

## 12. How to Report Issues

### Data Errors
- If you find a factual error in our data (e.g., wrong FIPS code, incorrect source URL):
  - Open GitHub issue with evidence
  - Tag: `data-quality`

### Methodology Concerns
- If you disagree with our analytical approach:
  - Open GitHub issue with alternative methodology
  - Tag: `methodology`

### Feature Requests
- If you want additional data layers or geographies:
  - Open GitHub issue with use case
  - Tag: `enhancement`

**We commit to:**
- Responding within 7 days
- Transparent decision-making on methodology changes
- Public changelog for all updates

---

## Conclusion

This product is **imperfect by design**.

Perfect data does not exist. Perfect models do not exist. Perfect predictions do not exist.

What exists:
- Real data with known gaps
- Transparent logic with documented assumptions
- Directional classifications with explicit uncertainty

Use accordingly.

---

**Maintained by:** Maryland Viability Atlas Team
**Last Reviewed:** 2026-01-28
**Next Review:** 2026-04-30
