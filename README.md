# Maryland Growth & Family Viability Atlas

**A production-grade spatial analytics system for Maryland directional growth signals**

## What This Product Does

This system analyzes **structural tailwinds** across Maryland counties by combining six analytical layers:

1. **Employment Gravity** - Income stability through sector diversification
2. **Mobility Optionality** - Job-change resilience via transit/highway redundancy
3. **School System Trajectory** - Family viability through enrollment & capital trends
4. **Housing Elasticity** - Growth absorption capacity
5. **Demographic Momentum** - Demand pressure signals
6. **Risk Drag** - Long-term constraints (flood, climate, infrastructure)

Plus a **Policy Persistence** confidence overlay that modifies certainty (not scores).

## What This Product Does NOT Do

- ❌ Predict housing prices
- ❌ Generate point forecasts
- ❌ Recommend investment decisions
- ❌ Use machine learning or black-box models
- ❌ Rely on test scores as primary indicators

## Core Question

> "Which places in Maryland have stacked structural tailwinds **if current policies and trends persist**, and how confident should we be that those tailwinds will continue?"

## Output Classification

### Directional Pressure
- **Improving** - Multiple reinforcing tailwinds
- **Stable** - Balanced or neutral signals
- **At Risk** - Structural headwinds present

### Confidence Modifier (from Policy Persistence layer)
- **Strong** - High capital follow-through, consistent delivery
- **Conditional** - Mixed track record, administrative dependency
- **Fragile** - Low follow-through, high political volatility

## Real Data Sources Only

This system uses **exclusively verifiable open data**:

### Layer 1: Employment Gravity
- US Census LEHD/LODES (Longitudinal Employer-Household Dynamics)
- BLS QCEW (Quarterly Census of Employment and Wages)
- USASpending.gov API (Federal spending patterns)

### Layer 2: Mobility Optionality
- OpenStreetMap (highway network)
- GTFS feeds (MARC, WMATA, MTA)
- Maryland DOT open data portal

### Layer 3: School System Trajectory
- Maryland State Department of Education
- NCES (National Center for Education Statistics)
- County Capital Improvement Plans (schools only)

### Layer 4: Housing Elasticity
- HUD Building Permits Survey
- County zoning GIS layers
- ACS (price-to-income trends)

### Layer 5: Demographic Momentum
- ACS migration & age cohorts
- IRS county-to-county migration data
- USPS vacancy data (if accessible)

### Layer 6: Risk Drag
- FEMA National Flood Hazard Layer
- NOAA climate projections
- EPA EJScreen
- Maryland infrastructure condition reports

### Policy Persistence Layer
- County Capital Improvement Plans (historical)
- State & county budget archives
- USASpending repeat awards
- Planning commission meeting records

## Update Cadence

| Layer | Frequency | Source Refresh |
|-------|-----------|----------------|
| Employment Gravity | Quarterly | BLS QCEW |
| Mobility Optionality | Annual | GTFS updates |
| School System Trajectory | Annual | MSDE releases |
| Housing Elasticity | Monthly | HUD permits |
| Demographic Momentum | Annual | ACS 5-year |
| Risk Drag | Annual | FEMA/NOAA |
| Policy Persistence | Quarterly | Manual scrape |

## Deployment

**Platform:** Railway
**Database:** PostgreSQL 15+ with PostGIS 3.3+
**API:** FastAPI (read-only)
**Scheduling:** Railway cron jobs

### Required Environment Variables

```bash
DATABASE_URL=postgresql://user:pass@host:port/db
MAPBOX_ACCESS_TOKEN=pk.xxxxx
CENSUS_API_KEY=your_key_here
BLS_API_KEY=your_key_here  # optional, increases rate limits
```

### Deployment Steps

```bash
# 1. Install Railway CLI
npm i -g @railway/cli

# 2. Login and link project
railway login
railway link

# 3. Set environment variables
railway variables set DATABASE_URL=...
railway variables set MAPBOX_ACCESS_TOKEN=...

# 4. Deploy
railway up
```

## Local Development

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up PostGIS database
make db-setup

# 3. Run initial data ingestion
make ingest-all

# 4. Run V2 pipeline + export
python src/run_pipeline.py --year 2025

# 5. Start API server
make serve
```

## API Endpoints

### County-Level GeoJSON
```
GET /api/v1/layers/counties/latest
```

Returns Maryland counties with:
- Directional classification
- Confidence modifier
- Layer scores
- Explanation fields

### Individual Layer Data
```
GET /api/v1/areas/{fips}
```

### Versioned Snapshots
```
GET /api/v1/layers/counties/{version}
```

## Architecture

```
┌─────────────────┐
│  Data Sources   │ (LEHD, BLS, FEMA, etc.)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Ingest Layer   │ (src/ingest/*.py)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │ (Raw tables, versioned)
│   + PostGIS     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Processing    │ (Normalization, scoring)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Classification  │ (Directional + confidence)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  GeoJSON Export │ (Map-ready outputs)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   FastAPI       │ (Read-only endpoints)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Mapbox GL JS  │ (Frontend visualization)
└─────────────────┘
```

## Known Limitations

### Current Exclusions (Data Access Constraints)
- **County zoning capacity**: Many counties lack machine-readable zoning GIS layers
- **Planning commission approvals**: No standardized API; requires manual scraping
- **Leadership tenure**: No consolidated Maryland source; would need manual assembly

### Methodological Constraints
- **County-level only**: Tract-level requires significantly more complex data fusion
- **Directional, not predictive**: Outputs indicate pressure direction, not magnitude
- **Policy persistence is retrospective**: Cannot anticipate political regime changes
- **No real-time updates**: Depends on source agency refresh schedules

### Geographic Scope
- **Maryland only**: Border effects (DC, PA, VA) not modeled
- **Excludes Baltimore City**: Often reported separately; requires custom handling

## Analytical Assumptions

All assumptions are **explicit and documented**:

1. **Employment diversification reduces income volatility** (Shannon entropy on NAICS sectors)
2. **Modal redundancy increases job-change resilience** (binary: has rail + highway)
3. **Enrollment growth signals family demand** (3-year rolling average)
4. **Permit trends indicate supply responsiveness** (permits per 1000 households, trend)
5. **Prime working-age inflow drives household formation** (ages 25-44 net migration)
6. **Flood risk is persistent** (FEMA Special Flood Hazard Area % by county)

See [docs/METHODOLOGY.md](docs/METHODOLOGY.md) for full technical documentation.

## What Would Falsify This Model?

1. **Strong tailwinds + persistent decline** in household formation
2. **High policy persistence + abandoned projects** in consecutive CIP cycles
3. **Low risk drag + systemic infrastructure failure** in "safe" counties
4. **High mobility optionality + sustained wage stagnation** despite job diversity

## Contributing

This is a **read-only analytical product**. Data corrections and methodology critiques welcome via issues.

## License

MIT License - See LICENSE file

## Contact

For data source questions: See [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md)
For methodology questions: See [docs/METHODOLOGY.md](docs/METHODOLOGY.md)
For deployment issues: Open a GitHub issue

---

**Built with:** Python, PostgreSQL/PostGIS, FastAPI, Mapbox GL JS
**Deployed on:** Railway
**Last updated:** 2026-01-28
