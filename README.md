# Maryland Growth & Family Viability Atlas

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Databricks SQL](https://img.shields.io/badge/Databricks-SQL%20Warehouse-orange.svg)](https://www.databricks.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109+-009688.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Railway Deployment](https://img.shields.io/badge/Deploy-Railway-blueviolet.svg)](https://railway.app/)

**A production-grade spatial analytics system for Maryland directional growth signals**

---

## Table of Contents

- [Overview](#overview)
- [What This Does (and Does NOT Do)](#what-this-does-and-does-not-do)
- [Core Question](#core-question)
- [The 6 Analytical Layers](#the-6-analytical-layers)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [API Reference](#api-reference)
- [Data Sources](#data-sources)
- [Output Classification](#output-classification)
- [Known Limitations](#known-limitations)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

The Maryland Growth & Family Viability Atlas analyzes **structural tailwinds and headwinds** across Maryland's 24 counties by combining six analytical layers of open government data. It provides directional pressure indicators—not predictions—to help understand which areas have favorable conditions for growth under current policies.

### Key Value Proposition

> **"Which places in Maryland have stacked structural tailwinds if current policies and trends persist, and how confident should we be that those tailwinds will continue?"**

This system uses **verifiable open data where available**, with explicit fallback flags when upstream sources are intermittent or blocked.

---

## What This Does (and Does NOT Do)

### What It Does

- Identifies **structural pressure direction** (improving, stable, at risk)
- Measures **confidence levels** based on policy delivery track record
- Provides **explainable classifications** with factor breakdowns
- Uses **multi-year evidence** for robust trend detection
- Serves a **live Databricks-backed county GeoJSON feed** for map visualization
- Progressively improves map/detail output as new layer tables are ingested

### What It Does NOT Do

- Predict housing prices
- Generate point forecasts
- Recommend investment decisions
- Use machine learning or black-box models
- Rely on test scores as primary education indicators
- Make claims about causation

---

## Core Question

> **"If nothing changes in policy and economic trends, which Maryland counties have structural tailwinds vs headwinds, and how certain can we be?"**

---

## The 6 Analytical Layers

| Layer | Name | Measures | Key Data Sources |
|-------|------|----------|------------------|
| **1** | Employment Gravity | Income stability via sector diversification | LEHD/LODES, BLS QCEW, USASpending |
| **2** | Mobility Optionality | Job-change resilience via transit redundancy | OSM, GTFS, LODES |
| **3** | School System Trajectory | Family viability via enrollment & capital trends | MSDE, NCES, County CIPs |
| **4** | Housing Elasticity | Growth absorption capacity | HUD BPS, ACS, County Zoning |
| **5** | Demographic Momentum | Demand pressure from migration | ACS, IRS Migration, USPS |
| **6** | Risk Drag | Environmental & infrastructure constraints | FEMA NFHL, NOAA, EPA EJScreen |

**Plus:** Policy Persistence (confidence modifier based on delivery track record)

---

## Technology Stack

### Backend
| Component | Technology |
|-----------|------------|
| API Framework | FastAPI 0.109+ |
| Database | Databricks SQL Warehouse (primary ingest) + PostgreSQL/PostGIS fallback |
| ORM | SQLAlchemy 2.0 + GeoAlchemy2 |
| Python | 3.12+ |

### Data Processing
| Component | Technology |
|-----------|------------|
| Geospatial | GeoPandas, Shapely, Fiona, PyProj |
| Analysis | Pandas, NumPy, SciPy |
| Census Data | pygris, census library |
| Transit | gtfs-kit |

### Frontend
| Component | Technology |
|-----------|------------|
| Mapping | Mapbox GL JS v3.0+ |
| UI | Vanilla JavaScript + HTML5/CSS3 |

### DevOps
| Component | Technology |
|-----------|------------|
| Deployment | Railway (PaaS) |
| Container | Docker + Docker Compose |
| Automation | Makefile (30+ tasks) |
| Testing | pytest with coverage |
| Linting | Black, isort, mypy |

---

## Quick Start

### Prerequisites

- Python 3.12+
- Databricks SQL Warehouse credentials (`DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_ACCESS_TOKEN`)
- Census API key (free): [api.census.gov](https://api.census.gov/data/key_signup.html)
- Mapbox token: [mapbox.com](https://www.mapbox.com/)

### Installation

```bash
# 1. Clone repository
git clone https://github.com/your-org/maryland-housing.git
cd maryland-housing

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your API keys

# 5. (Optional) Initialize local Postgres fallback database
# Skip when using DATA_BACKEND=databricks
make init-db

# 6. Run data ingestion
make ingest-all

# 7. Run V2 pipeline
make pipeline

# 8. Start API server
make serve
```

API available at: `http://localhost:8000`
Interactive docs: `http://localhost:8000/docs`

See [QUICKSTART.md](QUICKSTART.md) for detailed setup instructions.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  LEHD/LODES │ BLS QCEW │ FEMA │ GTFS │ OSM │ ACS │ USASpending  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                             │
│  src/ingest/layer1_*.py  │  layer2_*.py  │ ... │  layer6_*.py   │
│  ─────────────────────────────────────────────────────────────  │
│  • Fetches from external APIs                                    │
│  • Computes raw metrics per county/tract                         │
│  • Handles caching and rate limiting                             │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│      Databricks SQL Warehouse (primary) + PostgreSQL fallback    │
│  ─────────────────────────────────────────────────────────────  │
│  layer1_employment_gravity  │  layer2_mobility_optionality      │
│  layer3_school_trajectory   │  layer4_housing_elasticity        │
│  layer5_demographic_momentum│  layer6_risk_drag                 │
│  ─────────────────────────────────────────────────────────────  │
│  Versioned by data_year  │  Tract + County levels               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYER                              │
│  src/processing/                                                 │
│  ─────────────────────────────────────────────────────────────  │
│  timeseries_features.py  →  Level, Momentum, Stability          │
│  normalization.py        →  Percentile rank (0-1)               │
│  multiyear_scoring.py    →  Weighted layer aggregation          │
│  multiyear_classification.py → Directional + Confidence         │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     CLASSIFICATION                               │
│  ─────────────────────────────────────────────────────────────  │
│  Directional Status: Improving │ Stable │ At Risk               │
│  Confidence Level:   Strong │ Conditional │ Fragile             │
│  ─────────────────────────────────────────────────────────────  │
│  Synthesis Grouping (5 categories):                              │
│  • Emerging Tailwinds      • At Risk / Headwinds                │
│  • Conditional Growth      • High Uncertainty                   │
│  • Stable but Constrained                                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       API + EXPORT LAYER                         │
│  ─────────────────────────────────────────────────────────────  │
│  src/api/main.py + routes.py   →  live Databricks county feed   │
│  src/export/geojson_export.py  →  optional versioned snapshots  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      FRONTEND                                    │
│  frontend/index.html + map.js                                    │
│  ─────────────────────────────────────────────────────────────  │
│  Mapbox GL JS  │  5-color synthesis map  │  County detail panel │
└─────────────────────────────────────────────────────────────────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed system design.

---

## API Reference

### Base URL

```
http://localhost:8000/api/v1
```

### Core Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/layers/counties/latest` | Live county GeoJSON from Databricks (`md_counties` + latest layer scores, or `final_synthesis_current` when present) |
| `GET` | `/layers/counties/{version}` | Versioned GeoJSON snapshot (YYYYMMDD) |
| `GET` | `/areas/{fips}` | Detailed county analysis (scores, strengths, trends) |
| `GET` | `/areas/{fips}/layers/{layer}` | Layer-specific factor breakdown |
| `GET` | `/metadata/refresh` | Latest data refresh status |
| `GET` | `/metadata/sources` | Data source documentation |
| `GET` | `/metadata/capabilities` | Runtime feature flags + year policy metadata |
| `GET` | `/metadata/classifications` | Classification threshold definitions |
| `POST` | `/chat` | Ask Atlas endpoint (enabled when capabilities report chat enabled) |
| `GET` | `/counties` | List all 24 MD counties with FIPS codes |
| `GET` | `/health` | Health check endpoint |

### Example Response

```bash
curl http://localhost:8000/api/v1/areas/24031 | jq
```

```json
{
  "fips_code": "24031",
  "county_name": "Montgomery County",
  "synthesis_grouping": "conditional_growth",
  "directional_class": "improving",
  "confidence_class": "strong",
  "composite_score": 0.7917,
  "layer_scores": {
    "employment_gravity": 0.85,
    "mobility_optionality": 0.72,
    "school_trajectory": 0.81,
    "housing_elasticity": 0.54,
    "demographic_momentum": 0.68,
    "risk_drag": 0.15
  },
  "primary_strengths": ["Employment Gravity", "School System Trajectory"],
  "primary_weaknesses": ["Housing Elasticity"],
  "key_trends": [
    "Multiple reinforcing structural tailwinds present",
    "High policy delivery reliability"
  ]
}
```

Interactive API documentation: `http://localhost:8000/docs`

### Runtime Note

- Map polygons and county properties are served live from Databricks-backed API queries.
- `final_synthesis_current` is preferred when populated.
- If synthesis rows are missing, the API derives county detail from the latest available layer tables so the map remains usable during incremental ingest.

---

## Data Sources

Data-source availability is mixed by provider and run window (`available`, `intermittent`, `blocked`, `synthetic fallback`).

### Update Cadence

| Layer | Frequency | Source |
|-------|-----------|--------|
| Employment Gravity | Quarterly | BLS QCEW releases |
| Mobility Optionality | Annual | GTFS feed updates |
| School System Trajectory | Annual | MSDE releases |
| Housing Elasticity | Monthly | HUD Building Permits Survey |
| Demographic Momentum | Annual | ACS 5-year estimates |
| Risk Drag | Annual | FEMA/NOAA updates |
| Policy Persistence | Quarterly | Manual + USASpending |

See [docs/architecture/DATA_SOURCES.md](docs/architecture/DATA_SOURCES.md) for complete source documentation.

---

## Output Classification

### Directional Status

| Status | Criteria | Meaning |
|--------|----------|---------|
| **Improving** | ≥3 layers scoring ≥0.6, none <0.3, positive momentum | Multiple reinforcing tailwinds |
| **Stable** | Balanced signals, no clear direction | Neutral structural position |
| **At Risk** | ≥2 layers <0.3 OR severe risk drag | Structural headwinds present |

### Confidence Level

| Level | Criteria | Meaning |
|-------|----------|---------|
| **Strong** | Average data coverage ≥5 years | High certainty in assessment |
| **Conditional** | Average coverage ≥3 years | Moderate certainty |
| **Fragile** | Average coverage <3 years | Low certainty, needs more data |

### Synthesis Grouping (Primary Map Layer)

| Grouping | Color | Criteria |
|----------|-------|----------|
| Emerging Tailwinds | Deep Green | Improving + Strong confidence |
| Conditional Growth | Light Green | Improving + Conditional/Fragile |
| Stable but Constrained | Yellow | Stable + No severe risk |
| At Risk / Headwinds | Orange | At Risk OR severe risk + weak policy |
| High Uncertainty | Gray | Fragile confidence OR sparse data |

---

## Known Limitations

### Data Access Constraints

- **County zoning**: Many counties lack machine-readable zoning GIS layers
- **Planning approvals**: No standardized API; requires manual scraping
- **Leadership tenure**: No consolidated Maryland source

### Methodological Constraints

- **County-level only**: Tract-level requires more complex data fusion
- **Directional, not predictive**: Outputs indicate pressure, not magnitude
- **Policy persistence is retrospective**: Cannot anticipate political changes
- **No real-time updates**: Depends on source agency refresh schedules

### Geographic Scope

- **Maryland only**: Border effects (DC, PA, VA, DE, WV) not modeled
- **24 jurisdictions**: 23 counties + Baltimore City

See [docs/LIMITATIONS.md](docs/LIMITATIONS.md) for full discussion.

---

## Documentation

| Document | Description |
|----------|-------------|
| [QUICKSTART.md](QUICKSTART.md) | Step-by-step setup guide |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design and data flow |
| [docs/METHODOLOGY.md](docs/METHODOLOGY.md) | Analytical methodology |
| [docs/LIMITATIONS.md](docs/LIMITATIONS.md) | Known constraints |
| [docs/architecture/DATA_SOURCES.md](docs/architecture/DATA_SOURCES.md) | Data source documentation |
| [docs/architecture/SYNTHESIS_GROUPING.md](docs/architecture/SYNTHESIS_GROUPING.md) | Classification logic |
| [docs/development/CONTRIBUTING.md](docs/development/CONTRIBUTING.md) | Contribution guidelines |
| [docs/development/DEPLOYMENT_GUIDE.md](docs/development/DEPLOYMENT_GUIDE.md) | Deployment instructions |

---

## Makefile Commands

```bash
make help           # Show all available commands
make install        # Install Python dependencies
make init-db        # Initialize local Postgres fallback database (optional)
make db-migrate     # Run numbered SQL migrations (scripts/run_sql_migrations.py)
make ingest-all     # Run all 6 layer ingestion pipelines
make ingest-layer1  # Ingest Economic Opportunity data
make process        # Run multi-year scoring + classification
make pipeline       # Complete V2 pipeline (live API feed + optional GeoJSON snapshots)
make export         # Generate optional GeoJSON snapshots only
make serve          # Start FastAPI server (port 8000)
make frontend       # Start frontend dev server (port 3000)
make smoke          # Run fast smoke tests
make eval           # Run Agent Lightning eval checks
make test           # Run pytest with coverage
make lint           # Run Black, isort, mypy
make format         # Auto-format code
make clean          # Remove cache and temp files
```

### Runtime Configuration Notes

- CORS is origin-scoped via `CORS_ALLOW_ORIGINS` (comma-separated).
- Year policy is runtime-configurable with `LODES_LATEST_YEAR`, `LODES_LAG_YEARS`,
  `ACS_LATEST_YEAR`, `ACS_GEOGRAPHY_MAX_YEAR`, `NCES_OBSERVED_MAX_YEAR`, and `PREDICT_TO_YEAR`.
- Frontend Ask Atlas visibility is gated by `/api/v1/metadata/capabilities`.

---

## Contributing

This is a **read-only analytical product**. Contributions welcome for:

- Data corrections
- Methodology critiques
- Documentation improvements
- Bug fixes

See [docs/development/CONTRIBUTING.md](docs/development/CONTRIBUTING.md) for guidelines.

---

## License

MIT License - See [LICENSE](LICENSE) file.

---

## What Would Falsify This Model?

1. **Strong tailwinds + persistent decline** in household formation
2. **High policy persistence + abandoned projects** in consecutive CIP cycles
3. **Low risk drag + systemic infrastructure failure** in "safe" counties
4. **High mobility optionality + sustained wage stagnation** despite job diversity

---

**Built with:** Python, Databricks SQL + PostgreSQL/PostGIS fallback, FastAPI, Mapbox GL JS
**Deployed on:** Railway
**Last updated:** 2026-02-15
