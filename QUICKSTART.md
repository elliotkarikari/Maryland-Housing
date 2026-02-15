# Quick Start Guide

Get the Maryland Viability Atlas running locally in under 15 minutes.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Local Setup](#local-setup)
- [Railway Deployment](#railway-deployment)
- [Verify Installation](#verify-installation)
- [Troubleshooting](#troubleshooting)
- [Next Steps](#next-steps)

---

## Prerequisites

### Required

| Requirement | Version | Get It |
|-------------|---------|--------|
| Python | 3.12+ | [python.org](https://www.python.org/downloads/) |
| Databricks SQL Warehouse | - | [databricks.com](https://www.databricks.com/) |
| Census API Key | - | [api.census.gov](https://api.census.gov/data/key_signup.html) (free) |
| Mapbox Token | - | [mapbox.com](https://account.mapbox.com/auth/signup/) (free tier) |

### Optional

| Requirement | Purpose |
|-------------|---------|
| PostgreSQL + PostGIS | Local fallback backend (`DATA_BACKEND=postgres`) |
| BLS API Key | Higher rate limits for employment data |
| OpenAI API Key | AI-powered CIP document extraction |
| Docker | Container-based development |

---

## Local Setup

### Step 1: Clone and Setup Environment

```bash
# Clone repository
git clone https://github.com/your-org/maryland-housing.git
cd maryland-housing

# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows PowerShell
# .venv\Scripts\activate.bat     # Windows CMD
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

**Expected output:** ~70 packages installed including FastAPI, GeoPandas, SQLAlchemy.

### Step 3: Configure Environment

```bash
# Copy example environment file
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# Required
DATA_BACKEND=databricks
DATABRICKS_SERVER_HOSTNAME=adb-xxxx.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxx
DATABRICKS_ACCESS_TOKEN=dapi_xxxx
DATABRICKS_CATALOG=maryland_atlas
DATABRICKS_SCHEMA=default

# Optional postgres fallback (used only when DATA_BACKEND=postgres)
DATABASE_URL=postgresql://localhost/maryland_atlas

CENSUS_API_KEY=your_census_key_here
MAPBOX_ACCESS_TOKEN=pk.your_mapbox_token_here

# Optional (for enhanced features)
BLS_API_KEY=your_bls_key_here
OPENAI_API_KEY=sk-your_openai_key_here
AI_ENABLED=false

# Environment settings
ENVIRONMENT=development
LOG_LEVEL=INFO
CORS_ALLOW_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
ATLAS_API_BASE_URL=

# Year/runtime policy (keep aligned with available source vintages)
LODES_LATEST_YEAR=2022
LODES_LAG_YEARS=2
ACS_LATEST_YEAR=2024
ACS_GEOGRAPHY_MAX_YEAR=2022
NCES_OBSERVED_MAX_YEAR=2024
PREDICT_TO_YEAR=2025
```

### Step 4: Initialize Database (Postgres Fallback Only)

Skip this step for Databricks-backed ingest (`DATA_BACKEND=databricks`).

```bash
# Create database
createdb maryland_atlas

# Enable PostGIS extension
psql maryland_atlas -c "CREATE EXTENSION postgis;"
psql maryland_atlas -c "CREATE EXTENSION postgis_topology;"

# Initialize schema
make init-db
```

**Expected output:**
```
Initializing database...
Creating PostGIS extensions... done
Loading schema... done
Loading county boundaries... 24 counties loaded
Verification complete
```

### Step 5: Run Data Ingestion

#### Option A: All Layers (Full Setup)

```bash
make ingest-all
```

This runs all 6 layer ingestion pipelines sequentially. By default, writes go to Databricks (`DATA_BACKEND=databricks`).

| Layer | Data Source | Expected Time |
|-------|-------------|---------------|
| Layer 1 | LEHD/LODES, BLS, USASpending | 2-5 min |
| Layer 2 | OSM, GTFS feeds | 5-10 min |
| Layer 3 | NCES, State DoE | 2-5 min |
| Layer 4 | ACS, HUD BPS | 2-5 min |
| Layer 5 | IRS Migration, ACS | 2-5 min |
| Layer 6 | FEMA, EPA, NOAA | 3-5 min |

**Total estimated time:** 15-35 minutes

#### Option B: Single Layer (Quick Test)

```bash
# Just Layer 1 (Economic Opportunity)
make ingest-layer1
```

This is sufficient for testing the system. Other layers can be added later.

### Step 6: Run V2 Pipeline

```bash
make pipeline
```

**Pipeline stages:**
1. **Timeseries Features** - Computes level, momentum, stability across years
2. **Multi-Year Scoring** - Normalizes and aggregates layer scores
3. **Multi-Year Classification** - Generates directional status + confidence
4. **GeoJSON Export** - Creates map-ready output files

**Expected output:**
```
Running multi-year pipeline and export...
Computing timeseries features... 24 counties processed
Running multi-year scoring... done
Running classification... done
Exporting GeoJSON... exports/md_counties_latest.geojson created
Pipeline complete
```

### Step 7: Start Servers

#### API Server

```bash
make serve
```

**Access points:**
- API Base: http://localhost:8000
- Interactive Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health
- Runtime Capabilities: http://localhost:8000/api/v1/metadata/capabilities

#### Frontend Server (separate terminal)

```bash
# Activate virtual environment first
source .venv/bin/activate

# Start frontend
make frontend
# Or: python frontend/serve.py
```

**Access:** http://localhost:3000

---

## Railway Deployment

### Step 1: Install Railway CLI

```bash
npm i -g @railway/cli
```

### Step 2: Login and Initialize

```bash
railway login
railway init
```

### Step 3: Provision PostgreSQL

```bash
railway add --database postgres
```

This automatically sets `DATABASE_URL` in Railway environment.

### Step 4: Enable PostGIS

```bash
# Connect to Railway shell
railway run psql $DATABASE_URL -c "CREATE EXTENSION postgis;"
railway run psql $DATABASE_URL -c "CREATE EXTENSION postgis_topology;"
```

### Step 5: Set Environment Variables

```bash
railway variables set CENSUS_API_KEY=your_key_here
railway variables set MAPBOX_ACCESS_TOKEN=pk.your_token
railway variables set ENVIRONMENT=production
railway variables set LOG_LEVEL=INFO

# Optional
railway variables set BLS_API_KEY=your_key
railway variables set OPENAI_API_KEY=sk-your_key
railway variables set AI_ENABLED=true
```

### Step 6: Deploy

```bash
railway up
```

Railway will:
- Install dependencies from `requirements.txt`
- Start FastAPI server via `Procfile`
- Set up scheduled jobs from `railway.json`

### Step 7: Initialize Database (One-Time)

```bash
railway run python scripts/init_db.py
```

### Step 8: Run Initial Pipeline

```bash
# Run ingestion + pipeline
railway run make ingest-all
railway run make pipeline
```

### Step 9: Verify Deployment

```bash
# Check logs
railway logs

# Test health endpoint
curl https://your-app.up.railway.app/health
```

---

## Verify Installation

### Quick Health Check

```bash
# 1. Database connection
python -c "from config.database import test_connection; test_connection()"

# 2. API health
curl http://localhost:8000/health

# 3. GeoJSON export exists
ls -la exports/md_counties_*.geojson
```

### Data Verification

```bash
# Check county count
psql $DATABASE_URL -c "SELECT COUNT(DISTINCT fips_code) FROM layer1_employment_gravity;"
# Expected: 24

# Check layer data
psql $DATABASE_URL -c "SELECT fips_code, data_year, economic_opportunity_index FROM layer1_employment_gravity LIMIT 5;"

# Check classifications
psql $DATABASE_URL -c "SELECT fips_code, synthesis_grouping, directional_class, confidence_class FROM final_synthesis_current LIMIT 5;"
```

### API Verification

```bash
# Get county list
curl http://localhost:8000/api/v1/counties | jq '.[0:3]'

# Get specific county detail
curl http://localhost:8000/api/v1/areas/24031 | jq

# Get latest GeoJSON
curl http://localhost:8000/api/v1/layers/counties/latest | jq '.features | length'
# Expected: 24

# Check runtime capabilities and year policy
curl http://localhost:8000/api/v1/metadata/capabilities | jq
# Use chat_enabled/ai_enabled to verify Ask Atlas UI should be visible
```

### Success Checklist

- [ ] Virtual environment activated
- [ ] `.env` file configured with API keys
- [ ] PostGIS extensions enabled
- [ ] Database schema initialized (24 counties)
- [ ] At least Layer 1 data ingested
- [ ] Pipeline completed (GeoJSON exists)
- [ ] API responds to `/health`
- [ ] API returns county data at `/api/v1/areas/24031`
- [ ] Frontend loads map at `http://localhost:3000`

---

## Troubleshooting

### Database Issues

#### "Database connection failed"

**Check connection string:**
```bash
echo $DATABASE_URL
psql $DATABASE_URL -c "SELECT version();"
```

**Common fixes:**
- Ensure PostgreSQL is running: `pg_isready`
- Check credentials in `.env`
- Verify database exists: `psql -l | grep maryland_atlas`

#### "PostGIS extension not found"

```bash
# Install PostGIS (macOS with Homebrew)
brew install postgis

# Enable in database
psql maryland_atlas -c "CREATE EXTENSION postgis;"
```

#### "Permission denied"

```bash
# Grant permissions
psql -c "GRANT ALL ON DATABASE maryland_atlas TO your_user;"
```

### API Key Issues

#### "CENSUS_API_KEY not set"

**Get free key:** https://api.census.gov/data/key_signup.html

**Verify:**
```bash
echo $CENSUS_API_KEY
# Or check .env file
```

#### "Census API rate limit exceeded"

Wait 24 hours or use a different API key. Cached data will be used for previously fetched requests.

#### "Mapbox token invalid"

Verify token starts with `pk.` and is valid at https://account.mapbox.com/

### Ingestion Issues

#### "No data found for layer X"

1. Check layer ingestion ran successfully:
   ```bash
   psql $DATABASE_URL -c "SELECT COUNT(*) FROM layer1_employment_gravity;"
   ```

2. Verify API keys are set
3. Check network connectivity to data sources
4. Review logs: `tail -f logs/api.log`

#### "HTTP 429 - Too many requests"

Data source rate limiting. Wait and retry, or use API keys for higher limits.

#### "Connection timeout to external API"

Network issue. Check:
```bash
curl -I https://api.census.gov/
curl -I https://api.bls.gov/
```

### Pipeline Issues

#### "No classifications generated"

**Cause:** Multi-year pipeline requires data from multiple years.

**Check data years:**
```bash
psql $DATABASE_URL -c "SELECT DISTINCT data_year FROM layer1_employment_gravity ORDER BY data_year;"
```

**Fix:** Run ingestion for multiple years, then run pipeline with policy defaults:
```bash
python src/run_pipeline.py
# or explicitly pin as-of year:
python src/run_pipeline.py --year 2025
```

#### "Export failed - GeoJSON empty"

**Cause:** Classification or synthesis not completed.

**Fix:**
```bash
# Run full pipeline
make pipeline

# Or run steps individually
python -m src.run_multiyear_pipeline
python -m src.export.geojson_export
```

#### "Timeseries features require 3+ years"

V2 requires multi-year data for momentum/stability calculations. With limited data:
- Counties will show "High Uncertainty"
- This is correct behavior - the system is honest about data limitations

### Frontend Issues

#### "Map doesn't load"

1. Check API server is running on port 8000
2. Verify GeoJSON file exists: `ls exports/md_counties_latest.geojson`
3. Check browser console (F12) for errors
4. Verify CORS is not blocking requests

#### "Counties show as gray"

All gray indicates "High Uncertainty" - this is expected if:
- Only 1-2 layers have data
- Multi-year coverage is sparse

Run more layer ingestions to improve coverage.

#### "Side panel doesn't open on click"

1. Check API is accessible
2. Look for CORS errors in browser console
3. Verify county FIPS exists in database

#### "Ask Atlas chat is hidden or disabled"

1. Check capabilities endpoint:
   ```bash
   curl http://localhost:8000/api/v1/metadata/capabilities | jq
   ```
2. Confirm `AI_ENABLED=true` and `OPENAI_API_KEY` are set if chat should be enabled.
3. If capabilities report `chat_enabled=false`, frontend hiding is expected behavior.

### Logs

**Application logs:**
```bash
tail -f logs/api.log
```

**Pipeline logs:**
```bash
tail -f pipeline_output.log
```

**Database logs (PostgreSQL):**
```bash
tail -f /var/log/postgresql/postgresql-15-main.log
```

---

## Next Steps

### After Basic Setup

1. **Add more layers:**
   ```bash
   make ingest-layer2
   make ingest-layer3
   make ingest-layer4
   make ingest-layer5
   make ingest-layer6
   make pipeline
   ```

2. **View interactive map:**
   Open http://localhost:3000 in browser

3. **Explore API documentation:**
   Open http://localhost:8000/docs

### Customization

#### Adjust Classification Thresholds

Edit `config/settings.py`:
```python
THRESHOLD_IMPROVING_HIGH = 0.6  # Raise to 0.7 for stricter "Improving"
THRESHOLD_AT_RISK_LOW = 0.3     # Lower to 0.2 for stricter "At Risk"
```

Then re-run classification:
```bash
python -m src.run_multiyear_pipeline --skip-timeseries --skip-scoring
make export
```

#### Add Data for Specific Year

```bash
python -m src.ingest.layer1_economic_accessibility --year 2023
python src/run_pipeline.py
```

### Documentation

| Topic | Document |
|-------|----------|
| Full methodology | [docs/METHODOLOGY.md](docs/METHODOLOGY.md) |
| System architecture | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| Data sources | [docs/architecture/DATA_SOURCES.md](docs/architecture/DATA_SOURCES.md) |
| Known limitations | [docs/LIMITATIONS.md](docs/LIMITATIONS.md) |
| Contributing | [docs/development/CONTRIBUTING.md](docs/development/CONTRIBUTING.md) |

### Getting Help

- **Documentation:** See `docs/` folder
- **API Reference:** http://localhost:8000/docs
- **Issues:** Open a GitHub issue with:
  - Error message
  - Command you ran
  - Environment (OS, Python version)
  - Relevant log snippets

---

**Last updated:** 2026-02-15
