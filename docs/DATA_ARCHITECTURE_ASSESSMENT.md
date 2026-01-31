Data Architecture Assessment & Improvement Plan
Maryland Growth & Family Viability Atlas

Last Updated: 2026-01-31

Purpose
This document captures the current data architecture, identifies known gaps, and lists prioritized improvements. It is intentionally practical: what is already in place, what is missing, and what we should do next.

Current State (Summary)
- Data sources: Fully open, verifiable sources across 6 analytical layers (see docs/architecture/DATA_SOURCES.md).
- Pipeline: Ingest → timeseries features → normalization → layer scores → classification → GeoJSON/API.
- Transparency: All scores traceable to specific features and data sources.
- Momentum: Theil-Sen slope for trend detection (robust to outliers).
- Risk drag: Penalized multiplicatively, not averaged.
- Confidence: Policy persistence scoring (and in V2 pipeline, coverage-based confidence).

Strengths
- Open data priority: provenance and reproducibility are strong.
- Temporal signals: momentum and stability add depth beyond snapshots.
- Explicit weights: feature weights are defined in src/processing/feature_registry.py.
- Clear separation of layers: maintainable and extensible.

Known Gaps (by layer)
- Employment Gravity: solid; consider more timely labor-market data.
- Mobility Optionality: solid; could add observed commute flows.
- School Trajectory: weakest (quality proxies limited, CIP coverage partial).
- Housing Elasticity: weakest (zoning coverage limited, vacancy data partial).
- Demographic Momentum: strong but could add births/deaths and education trends.
- Risk Drag: strong but can expand to food access and heat islands.
- Policy Persistence: partial coverage; CIP extraction only in a few counties.

Priority Improvements (Next 1–2 Sprints)
1) Housing Elasticity upgrades
   - HUD USPS Vacancy (apply for access)
   - HUD Fair Market Rents (FMR) time series
   - LIHTC project database
   - Expand ACS housing tenure/cost burden indicators
2) School Trajectory upgrades
   - Graduation rates, proficiency (NCES / MSDE)
   - Early childhood access (Head Start/pre-K enrollment)
   - Higher-ed proximity (distance to colleges)
3) Employment upgrades
   - Census QWI (job creation/destruction, turnover)
   - BEA county GDP / personal income growth
4) Data governance clarity
   - Explicit missing-data handling docs
   - Threshold sensitivity note in methodology
   - Weights table auto-generated from feature registry

Medium-Term Improvements
- CIP extraction across all counties (automated ingestion + validation)
- Zoning coverage: direct county partnerships + standardized schema
- Validation study: correlate classifications with outcomes (population, income)
- Add comparison view (layer heatmap + “choose your weights” mode)

Long-Term Enhancements
- Tract-level pipeline (where data density allows)
- Uncertainty bands and probabilistic classification
- Scenario analysis (policy simulator)

Implementation Plan (Executable)
- Stage A: Documentation and transparency
  - Publish missing-data handling and weighting logic in METHODOLOGY.md
  - Add weights/feature table summary (generated from feature_registry)
- Stage B: Data source expansion
  - Add HUD FMR + LIHTC + USPS vacancy
  - Add NCES graduation rates and early childhood access
  - Add Census QWI
- Stage C: Methodology refinements
  - Sensitivity analysis (thresholds ±0.05, weight perturbations)
  - Risk drag penalty cap experiment (report-only at first)

Success Criteria
- Housing and School layers move from “partial” to “strong.”
- Documentation explains missing data handling clearly.
- At least one validation check against real-world outcomes.
