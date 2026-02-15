# System Architecture

**Maryland Growth & Family Viability Atlas**

**Version:** 2.0
**Last Updated:** 2026-02-15

---

## Table of Contents

- [Overview](#overview)
- [System Components](#system-components)
- [Data Flow](#data-flow)
- [Directory Structure](#directory-structure)
- [Database Schema](#database-schema)
- [Processing Pipeline](#processing-pipeline)
- [API Layer](#api-layer)
- [Frontend Architecture](#frontend-architecture)
- [Configuration Management](#configuration-management)
- [Deployment Architecture](#deployment-architecture)
- [Security Considerations](#security-considerations)

---

## Overview

The Maryland Viability Atlas is a **multi-layer spatial analytics system** that processes open government data to generate directional growth classifications for Maryland counties. The architecture follows a clear separation between data ingestion, processing, storage, and presentation layers.

### Design Principles

1. **Transparency**: All calculations are explicit and auditable
2. **Data Provenance**: Every data point traceable to source
3. **No Interpolation**: Missing data reduces confidence, not filled in
4. **Read-Only API**: No mutations through API endpoints
5. **Explainability First**: Every classification includes reasoning

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL DATA SOURCES                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌────────┐ │
│  │ LEHD/   │ │  BLS    │ │  FEMA   │ │  GTFS   │ │   OSM   │ │  ACS   │ │
│  │ LODES   │ │  QCEW   │ │  NFHL   │ │  Feeds  │ │ Network │ │ Census │ │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └───┬────┘ │
└───────┼──────────┼──────────┼──────────┼──────────┼─────────────┼───────┘
        │          │          │          │          │             │
        └──────────┴──────────┴──────────┴──────────┴─────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           INGESTION LAYER                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
│  │ Layer Ingestors │  │   Caching       │  │   Rate Limiting         │  │
│  │ (src/ingest/)   │  │   (data/cache/) │  │   & Error Handling      │  │
│  └────────┬────────┘  └────────┬────────┘  └────────────┬────────────┘  │
└───────────┼────────────────────┼────────────────────────┼───────────────┘
            │                    │                        │
            └────────────────────┴────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│   Databricks SQL Warehouse (primary) + PostgreSQL/PostGIS (fallback)    │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐   │
│  │  Raw Layer Data  │  │  Timeseries      │  │  Synthesis &         │   │
│  │  (layer1_*, etc) │  │  Features        │  │  Classifications     │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          PROCESSING LAYER                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
│  │ Timeseries      │  │ Normalization & │  │ Classification &        │  │
│  │ Features        │  │ Scoring         │  │ Synthesis               │  │
│  └────────┬────────┘  └────────┬────────┘  └────────────┬────────────┘  │
└───────────┼────────────────────┼────────────────────────┼───────────────┘
            │                    │                        │
            └────────────────────┴────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        API + OUTPUT LAYER                                │
│  ┌─────────────────────────┐         ┌─────────────────────────────┐    │
│  │  Optional GeoJSON       │         │     FastAPI REST API        │    │
│  │  snapshots (exports/)   │         │ live county feed from DB    │    │
│  └─────────────────────────┘         └─────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          PRESENTATION LAYER                              │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Mapbox GL JS Frontend                         │    │
│  │                    (frontend/index.html + map.js)                │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## System Components

### 1. Data Ingestion Layer

**Location:** `src/ingest/`

Each analytical layer has a dedicated ingestion module:

| Module | Purpose | Data Sources |
|--------|---------|--------------|
| `layer1_economic_accessibility.py` | Employment & economic data | LEHD/LODES, BLS QCEW, USASpending |
| `layer2_accessibility.py` | Transportation accessibility | OSM, GTFS, Census |
| `layer3_education_accessibility.py` | Education system data | NCES, MSDE, County CIPs |
| `layer4_housing_affordability.py` | Housing market data | ACS, HUD BPS |
| `layer5_demographic_equity.py` | Population dynamics | ACS, IRS Migration |
| `layer6_risk_vulnerability.py` | Environmental risk | FEMA, EPA, NOAA |
| `policy_persistence.py` | Policy delivery tracking | USASpending, CIP documents |

**Responsibilities:**
- Fetch data from external APIs
- Handle rate limiting and retries
- Transform raw data to normalized format
- Store in Databricks SQL tables (Postgres fallback supported)
- Manage local caching

### 2. Processing Layer

**Location:** `src/processing/`

| Module | Purpose |
|--------|---------|
| `feature_registry.py` | Single source of truth for all features |
| `timeseries_features.py` | Compute level, momentum, stability |
| `normalization.py` | Percentile rank and robust Z-score |
| `multiyear_scoring.py` | Weighted layer aggregation |
| `multiyear_classification.py` | Directional status + confidence |
| `scoring.py` | Legacy single-year scoring |
| `classification.py` | Legacy classification logic |

### 3. Export Layer (Optional Artifacts)

**Location:** `src/export/`

| Module | Purpose |
|--------|---------|
| `geojson_export.py` | Generate optional map snapshot GeoJSON files |

**Primary map runtime source:** `GET /api/v1/layers/counties/latest` (live DB-backed feed)

**Optional output files:**
- `exports/md_counties_latest.geojson` - Latest snapshot artifact
- `exports/md_counties_YYYYMMDD.geojson` - Versioned archive snapshots

### 4. API Layer

**Location:** `src/api/`

| Module | Purpose |
|--------|---------|
| `main.py` | FastAPI application initialization |
| `routes.py` | Endpoint definitions, metadata registry loading, and capabilities endpoint |

### 5. Configuration Layer

**Location:** `config/`

| Module | Purpose |
|--------|---------|
| `settings.py` | Pydantic-based configuration management (CORS + year/runtime policy) |
| `database.py` | SQLAlchemy backend routing (Databricks primary, Postgres fallback) |

### 6. Frontend Layer

**Location:** `frontend/`

| File | Purpose |
|------|---------|
| `index.html` | Main UI structure |
| `map.js` | Mapbox GL JS orchestration and UI event wiring |
| `modules/capabilities.js` | Runtime capability loader for feature gating |
| `serve.py` | Development server with CORS |

---

## Data Flow

### Complete Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: DATA INGESTION                                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  External API ──▶ Fetch ──▶ Transform ──▶ Validate ──▶ Store            │
│                      │                                    │             │
│                      ▼                                    ▼             │
│                   Cache                    Databricks/Postgres          │
│              (data/cache/)                   (layer*_* tables)          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: TIMESERIES FEATURE COMPUTATION                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Multi-Year Raw Data ──▶ Level (latest) ──▶ Momentum (Theil-Sen)        │
│                               │                    │                    │
│                               └────────────────────┼─────────▶ Stability│
│                                                    │              │     │
│                                                    ▼              ▼     │
│                                            layer_timeseries_features    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: NORMALIZATION & SCORING                                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Timeseries Features ──▶ Percentile Rank ──▶ Weighted Aggregation       │
│                               │                       │                 │
│                               ▼                       ▼                 │
│                         0-1 Normalized        Layer Composite Scores    │
│                                                       │                 │
│                                                       ▼                 │
│                                            layer_summary_scores         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: CLASSIFICATION                                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Layer Scores ──▶ Directional Status ──▶ Confidence Level               │
│                        │                       │                        │
│                        ▼                       ▼                        │
│                   improving              strong/conditional/fragile     │
│                   stable                                                │
│                   at_risk                      │                        │
│                        │                       │                        │
│                        └───────────┬───────────┘                        │
│                                    ▼                                    │
│                         Synthesis Grouping                              │
│                    (5 final categories)                                 │
│                                    │                                    │
│                                    ▼                                    │
│                        final_synthesis_current                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ STAGE 5: LIVE MAP FEED + OPTIONAL EXPORT                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  County Boundaries + Layer/Synthesis Tables ──▶ API GeoJSON Assembly    │
│          │                                       │                      │
│          ▼                                       ▼                      │
│       md_counties + layer*_*          /api/v1/layers/counties/latest    │
│     (final_synthesis preferred, progressive fallback supported)          │
│                               Optional: exports/md_counties_*.geojson   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Canonical V2 Runtime Flow

1. `make db-migrate` (executes all numbered SQL migrations with `scripts/run_sql_migrations.py`)
2. `make ingest-all` (layers 1-6 + policy persistence ingestion)
3. `make pipeline` (multi-year features, scoring, classification, optional export)
4. `make serve` + `make frontend` (API + UI runtime)

Map runtime note:
- Map/feed endpoints remain available during partial ingest by deriving county properties from latest layer tables when `final_synthesis_current` is sparse.

---

## Directory Structure

```
maryland-housing/
│
├── src/                              # Core application code
│   ├── api/                          # FastAPI application
│   │   ├── __init__.py
│   │   ├── main.py                   # App initialization, middleware
│   │   └── routes.py                 # Endpoint definitions + metadata APIs
│   │
│   ├── ingest/                       # Data ingestion pipelines
│   │   ├── __init__.py
│   │   ├── layer1_economic_accessibility.py    # ~1400 LOC
│   │   ├── layer2_accessibility.py             # ~1500 LOC
│   │   ├── layer3_education_accessibility.py   # ~1750 LOC
│   │   ├── layer4_housing_affordability.py     # ~1400 LOC
│   │   ├── layer5_demographic_equity.py        # ~1700 LOC
│   │   ├── layer6_risk_vulnerability.py        # ~1700 LOC
│   │   └── policy_persistence.py               # ~300 LOC
│   │
│   ├── processing/                   # Scoring and classification
│   │   ├── __init__.py
│   │   ├── feature_registry.py       # Feature definitions
│   │   ├── timeseries_features.py    # Multi-year feature computation
│   │   ├── normalization.py          # Percentile/Z-score normalization
│   │   ├── multiyear_scoring.py      # V2 scoring logic
│   │   ├── multiyear_classification.py  # V2 classification
│   │   ├── scoring.py                # Legacy V1 scoring
│   │   └── classification.py         # Legacy V1 classification
│   │
│   ├── export/                       # Output generation
│   │   ├── __init__.py
│   │   └── geojson_export.py         # GeoJSON file generation
│   │
│   ├── utils/                        # Shared utilities
│   │   ├── __init__.py
│   │   ├── logging.py                # Logging configuration
│   │   ├── data_sources.py           # Data source metadata helpers
│   │   ├── db_bulk.py                # Shared batched DB writes
│   │   └── year_policy.py            # Central year selection rules
│   │
│   ├── ai/                           # Optional AI subsystem
│   │   ├── pipeline/                 # AI extraction pipeline
│   │   └── providers/                # LLM provider integrations
│   │
│   ├── run_pipeline.py               # V2 pipeline orchestration
│   └── run_multiyear_pipeline.py     # Multi-year pipeline runner
│
├── config/                           # Configuration management
│   ├── __init__.py
│   ├── settings.py                   # Pydantic settings
│   └── database.py                   # SQLAlchemy setup
│
├── frontend/                         # Web UI
│   ├── index.html                    # Main HTML
│   ├── map.js                        # Mapbox GL JS app orchestration
│   ├── modules/
│   │   └── capabilities.js           # Runtime feature capability loader
│   ├── serve.py                      # Development server
│   └── README.md                     # Frontend documentation
│
├── migrations/                       # Database schema evolution (numeric, append-only)
│   ├── 006_layer2_accessibility_overhaul.sql
│   ├── 007_layer1_economic_accessibility_overhaul.sql
│   ├── 008_layer1_economic_opportunity_index.sql
│   ├── 009_layer4_housing_affordability_overhaul.sql
│   ├── 010_layer3_education_accessibility_overhaul.sql
│   ├── 011_layer5_demographic_equity_overhaul.sql
│   ├── 012_layer6_risk_vulnerability_overhaul.sql
│   ├── 013_layer1_qwi_layer4_hud_additions.sql
│   ├── 014_layer5_low_vacancy_prediction.sql
│   ├── 015_layer1_prediction.sql
│   ├── 016_layer2_prediction.sql
│   ├── 017_layer3_prediction.sql
│   ├── 018_layer4_prediction.sql
│   ├── 019_layer6_prediction.sql
│   ├── 020_layer5_prediction.sql
│   └── 021_layer3_school_directory_id_length.sql
│
├── data/                             # Data storage
│   ├── cache/                        # Downloaded data cache (gitignored)
│   ├── schemas/                      # SQL schema definitions
│   └── README.md
│
├── exports/                          # GeoJSON output files
│   ├── md_counties_latest.geojson
│   └── md_counties_YYYYMMDD.geojson
│
├── tests/                            # Test suite
│   ├── __init__.py
│   ├── conftest.py                   # Pytest fixtures
│   └── test_classification.py
│
├── docs/                             # Documentation
│   ├── architecture/                 # Architecture docs
│   ├── development/                  # Developer guides
│   ├── layers/                       # Layer-specific docs
│   └── archive/                      # Historical docs
│
├── docker/                           # Container definitions
│   └── agent_lightning/
│       └── Dockerfile
│
├── scripts/                          # Automation scripts
│   ├── setup_database.sh
│   ├── setup_env.sh
│   └── cleanup.sh
│
├── logs/                             # Application logs (gitignored)
│
├── devtools/                         # Development utilities
│
├── .env.example                      # Environment template
├── .gitignore                        # Git ignore rules
├── docker-compose.agent-lightning.yml
├── LICENSE                           # MIT license
├── Makefile                          # Build automation
├── Procfile                          # Railway process definition
├── pyproject.toml                    # Python project config
├── railway.json                      # Railway deployment config
├── README.md                         # Project README
├── QUICKSTART.md                     # Quick start guide
└── requirements.txt                  # Python dependencies
```

---

## Database Schema

### Core Tables

#### Layer Data Tables (Versioned by Year)

```sql
-- Layer 1: Employment Gravity
CREATE TABLE layer1_employment_gravity (
    fips_code VARCHAR(5),
    data_year INTEGER,
    economic_opportunity_index FLOAT,
    employment_diversification_score FLOAT,
    sector_diversity_entropy FLOAT,
    federal_awards_volatility FLOAT,
    high_wage_jobs INTEGER,
    mid_wage_jobs INTEGER,
    low_wage_jobs INTEGER,
    economic_accessibility_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fips_code, data_year)
);

-- Similar structure for layers 2-6
CREATE TABLE layer2_mobility_optionality (...);
CREATE TABLE layer3_school_trajectory (...);
CREATE TABLE layer4_housing_elasticity (...);
CREATE TABLE layer5_demographic_momentum (...);
CREATE TABLE layer6_risk_drag (...);
```

#### Tract-Level Detail Tables

```sql
-- Layer 1 tract-level (for detailed analysis)
CREATE TABLE layer1_economic_opportunity_tract (
    tract_geoid VARCHAR(11),
    fips_code VARCHAR(5),
    data_year INTEGER,
    jobs_total INTEGER,
    jobs_high_wage INTEGER,
    economic_accessibility_score FLOAT,
    PRIMARY KEY (tract_geoid, data_year)
);
```

#### Processing Output Tables

```sql
-- Timeseries features (multi-year evidence)
CREATE TABLE layer_timeseries_features (
    fips_code VARCHAR(5),
    layer_key VARCHAR(50),
    feature_key VARCHAR(100),
    level FLOAT,              -- Latest value
    momentum FLOAT,           -- Theil-Sen slope
    stability FLOAT,          -- Volatility measure
    coverage_years INTEGER,   -- Number of years with data
    computed_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fips_code, layer_key, feature_key)
);

-- Normalized layer scores
CREATE TABLE layer_summary_scores (
    fips_code VARCHAR(5),
    layer_key VARCHAR(50),
    score FLOAT,              -- 0-1 normalized
    momentum FLOAT,
    coverage_years INTEGER,
    computed_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fips_code, layer_key)
);

-- Final synthesis (current snapshot)
CREATE TABLE final_synthesis_current (
    fips_code VARCHAR(5) PRIMARY KEY,
    county_name VARCHAR(100),
    synthesis_grouping VARCHAR(30),
    directional_class VARCHAR(20),
    confidence_class VARCHAR(20),
    composite_score FLOAT,
    primary_strengths TEXT[],
    primary_weaknesses TEXT[],
    key_trends TEXT[],
    computed_at TIMESTAMP DEFAULT NOW()
);
```

#### Audit Tables

```sql
-- Data refresh log
CREATE TABLE data_refresh_log (
    id SERIAL PRIMARY KEY,
    layer_key VARCHAR(50),
    data_year INTEGER,
    records_processed INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);
```

### Entity-Relationship Diagram

```
┌───────────────────┐       ┌───────────────────┐
│ layer1_employment │       │ layer2_mobility   │
│ _gravity          │       │ _optionality      │
├───────────────────┤       ├───────────────────┤
│ fips_code (PK)    │       │ fips_code (PK)    │
│ data_year (PK)    │       │ data_year (PK)    │
│ metrics...        │       │ metrics...        │
└─────────┬─────────┘       └─────────┬─────────┘
          │                           │
          │                           │
          ▼                           ▼
┌─────────────────────────────────────────────────┐
│              layer_timeseries_features          │
├─────────────────────────────────────────────────┤
│ fips_code (PK)                                  │
│ layer_key (PK)                                  │
│ feature_key (PK)                                │
│ level, momentum, stability, coverage_years      │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│              layer_summary_scores               │
├─────────────────────────────────────────────────┤
│ fips_code (PK)                                  │
│ layer_key (PK)                                  │
│ score, momentum, coverage_years                 │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│            final_synthesis_current              │
├─────────────────────────────────────────────────┤
│ fips_code (PK)                                  │
│ county_name                                     │
│ synthesis_grouping                              │
│ directional_class                               │
│ confidence_class                                │
│ composite_score                                 │
│ primary_strengths, primary_weaknesses           │
│ key_trends                                      │
└─────────────────────────────────────────────────┘
```

---

## Processing Pipeline

### Stage 1: Timeseries Feature Computation

**Module:** `src/processing/timeseries_features.py`

```python
def compute_timeseries_features(fips_code: str, layer_key: str) -> dict:
    """
    Compute multi-year evidence metrics for a county-layer combination.

    Returns:
        level: Latest available value
        momentum: Theil-Sen robust slope across years
        stability: 1 - coefficient_of_variation (lower volatility = higher stability)
        coverage_years: Number of years with valid data
    """
```

**Key Design Decisions:**
- **Minimum 3 years** required for momentum calculation
- **Theil-Sen estimator** used for robustness to outliers
- **No interpolation** - missing years reduce coverage, not filled

### Stage 2: Normalization

**Module:** `src/processing/normalization.py`

**Methods:**

1. **Percentile Rank** (default)
   - Robust to outliers
   - Directly produces 0-1 scores
   - Formula: `rank(x) / n_counties`

2. **Robust Z-Score** (for heavy-tailed distributions)
   - Uses median and IQR
   - Formula: `(x - median) / IQR`, clipped to ±3
   - Mapped to 0-1: `(z + 3) / 6`

**Directionality Handling:**
- Positive features: higher raw → higher score
- Negative features: lower raw → higher score (inverted)

### Stage 3: Multi-Year Scoring

**Module:** `src/processing/multiyear_scoring.py`

```python
def compute_layer_score(fips_code: str, layer_key: str) -> float:
    """
    Aggregate normalized features into layer composite score.

    Formula:
        layer_score = Σ(weight_i * normalized_feature_i) / Σ(weight_i)

    Weights defined in feature_registry.py
    """
```

**Risk Drag Treatment:**
- Risk Drag is **subtractive only** (penalty)
- Not averaged with positive layers
- Applied as: `composite * (1 - risk_drag_score)`

### Stage 4: Classification

**Module:** `src/processing/multiyear_classification.py`

**Directional Status:**

| Status | Criteria |
|--------|----------|
| Improving | ≥3 layers ≥0.6 AND ≤1 layer <0.3 AND positive momentum |
| At Risk | ≥2 layers <0.3 OR severe risk drag (≥0.5) |
| Stable | All other cases |

**Confidence Level:**

| Level | Criteria |
|-------|----------|
| Strong | Average coverage ≥5 years |
| Conditional | Average coverage ≥3 years |
| Fragile | Average coverage <3 years |

**Synthesis Grouping:**

| Grouping | Criteria |
|----------|----------|
| Emerging Tailwinds | Improving + Strong |
| Conditional Growth | Improving + Conditional/Fragile |
| Stable but Constrained | Stable + No severe risk |
| At Risk / Headwinds | At Risk OR severe risk + weak policy |
| High Uncertainty | Fragile OR sparse data (<2 layers) |

---

## API Layer

### FastAPI Application Structure

```python
# src/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Maryland Viability Atlas API",
    version="2.0.0",
    description="Read-only API for Maryland county classifications"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_allow_origins(settings.CORS_ALLOW_ORIGINS),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
```

### Endpoint Design

**Base Path:** `/api/v1`

| Endpoint | Response Model | Purpose |
|----------|---------------|---------|
| `GET /layers/counties/latest` | GeoJSON | Live county data from Databricks tables (`final_synthesis_current` preferred; latest-layer fallback) |
| `GET /layers/counties/{version}` | GeoJSON | Versioned snapshot |
| `GET /areas/{fips}` | AreaDetail | County detail (live + progressive fallback) |
| `GET /areas/{fips}/layers/{layer}` | LayerDetail | Layer breakdown |
| `GET /metadata/refresh` | RefreshStatus | Data refresh audit |
| `GET /metadata/sources` | DataSource[] | Source documentation |
| `GET /metadata/capabilities` | CapabilitiesResponse | Runtime feature and year policy flags |
| `GET /metadata/classifications` | dict | Threshold definitions |
| `POST /chat` | ChatResponse | Ask Atlas AI response (gated by runtime capability) |
| `GET /counties` | County[] | County list |
| `GET /health` | HealthStatus | Health check |

### Response Models

```python
class AreaDetail(BaseModel):
    fips_code: str
    county_name: str
    synthesis_grouping: str
    directional_class: str
    confidence_class: str
    composite_score: float
    layer_scores: dict[str, float]
    primary_strengths: list[str]
    primary_weaknesses: list[str]
    key_trends: list[str]

class LayerDetail(BaseModel):
    layer_key: str
    layer_name: str
    score: float
    momentum: float
    coverage_years: int
    factors: list[LayerFactor]

class LayerFactor(BaseModel):
    name: str
    value: float
    trend: str  # "increasing", "stable", "decreasing"
    weight: float
```

---

## Frontend Architecture

### Technology Stack

- **Mapbox GL JS v3.0+** - Vector tile mapping
- **Vanilla JavaScript** - No framework dependencies
- **HTML5/CSS3** - Semantic markup and styling

### Component Structure

```
frontend/
├── index.html          # Main document structure
│   ├── <header>        # Title and subtitle
│   ├── <div#map>       # Mapbox container
│   ├── <div#legend>    # Color legend
│   ├── <div#panel>     # Detail side panel
│   └── <div#loading>   # Loading overlay
│
└── map.js              # Application logic
    ├── initMap()       # Map initialization
    ├── loadData()      # GeoJSON fetching
    ├── addLayers()     # Map layer configuration
    ├── setupHandlers() # Click/hover events
    └── showDetail()    # Panel population
└── modules/
    └── capabilities.js
        ├── loadCapabilities()   # Fetches /api/v1/metadata/capabilities
        ├── isChatEnabled()      # Determines Ask Atlas visibility
        └── getYearPolicy()      # Exposes runtime year policy for diagnostics
```

### Data Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Page Load      │────▶│  Fetch GeoJSON  │────▶│  Render Map     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Show Detail    │◀────│  Fetch County   │◀────│  User Click     │
│  Panel          │     │  API            │     │  Event          │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Color Scheme

```javascript
const SYNTHESIS_COLORS = {
    'emerging_tailwinds': '#2d5016',    // Deep Green
    'conditional_growth': '#7cb342',     // Light Green
    'stable_constrained': '#fdd835',     // Yellow
    'at_risk_headwinds': '#f4511e',      // Orange
    'high_uncertainty': '#757575'        // Gray
};
```

---

## Configuration Management

### Environment Variables

**Required:**
```bash
DATABASE_URL=postgresql://user:pass@host:port/db
CENSUS_API_KEY=your_census_key
MAPBOX_ACCESS_TOKEN=pk.your_token
```

**Optional:**
```bash
BLS_API_KEY=your_bls_key
OPENAI_API_KEY=sk-your_key
AI_ENABLED=false
CORS_ALLOW_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
ATLAS_API_BASE_URL=
LODES_LATEST_YEAR=2022
LODES_LAG_YEARS=2
ACS_LATEST_YEAR=2024
ACS_GEOGRAPHY_MAX_YEAR=2022
NCES_OBSERVED_MAX_YEAR=2024
PREDICT_TO_YEAR=2025
SENTRY_DSN=https://...
ENVIRONMENT=development|production
LOG_LEVEL=DEBUG|INFO|WARNING|ERROR
```

### Settings Module

**Location:** `config/settings.py`

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database
    DATABASE_URL: str

    # API Keys
    CENSUS_API_KEY: str
    MAPBOX_ACCESS_TOKEN: str
    BLS_API_KEY: str | None = None

    # Classification Thresholds
    THRESHOLD_IMPROVING_HIGH: float = 0.6
    THRESHOLD_IMPROVING_LOW: float = 0.3
    THRESHOLD_AT_RISK_LOW: float = 0.3
    THRESHOLD_AT_RISK_COUNT: int = 2

    # Runtime policy
    CORS_ALLOW_ORIGINS: str = "http://localhost:3000,http://127.0.0.1:3000"
    LODES_LATEST_YEAR: int = 2022
    LODES_LAG_YEARS: int = 2
    ACS_LATEST_YEAR: int = 2024
    ACS_GEOGRAPHY_MAX_YEAR: int = 2022
    NCES_OBSERVED_MAX_YEAR: int = 2024
    PREDICT_TO_YEAR: int = 2025

    model_config = SettingsConfigDict(env_file=".env")
```

---

## Deployment Architecture

### Railway Deployment

```
┌─────────────────────────────────────────────────────────────────┐
│                          Railway                                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐     ┌─────────────────────────────────┐   │
│  │  Databricks SQL │     │  FastAPI Service                │   │
│  │  Warehouse      │◀───▶│  (from Procfile)                │   │
│  │                 │     │  uvicorn src.api.main:app       │   │
│  └─────────────────┘     └─────────────────────────────────┘   │
│                                     │                           │
│                                     ▼                           │
│                          ┌─────────────────┐                   │
│                          │  Public URL     │                   │
│                          │  *.railway.app  │                   │
│                          └─────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

### Scheduled Jobs

```json
// railway.json
{
  "cron": [
    {
      "schedule": "0 2 1 * *",
      "command": "python src/run_pipeline.py"
    },
    {
      "schedule": "0 5 * * *",
      "command": "python -m src.export.geojson_export"
    }
  ]
}
```

### Health Checks

**Endpoint:** `GET /health`

**Checks:**
1. Database connectivity
2. County boundary availability in `md_counties`
3. Live county feed readiness

---

## Security Considerations

### API Security

- **Read-only endpoints** - No mutation operations
- **CORS configured** - Controlled origin access
- **No authentication** - Public data only
- **Rate limiting** - Via upstream proxy (Railway/Cloudflare)

### Data Security

- **No PII stored** - County-level aggregates only
- **Open data sources** - All data publicly available
- **Audit logging** - Data refresh operations logged

### Secret Management

- **Environment variables** - Never committed to git
- **`.env` file** - Local development only
- **Railway variables** - Production secrets

### Input Validation

- **Pydantic models** - Type validation on responses
- **FIPS code validation** - Must match Maryland patterns
- **SQL parameterization** - Prevents injection

---

**Last updated:** 2026-02-15
**Maintainer:** Maryland Viability Atlas Team
