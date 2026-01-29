# Quick Start Guide

Get the Maryland Viability Atlas running in **under 10 minutes**.

---

## Prerequisites

- Python 3.10+
- PostgreSQL 15+ with PostGIS 3.3+
- Census API key (free): https://api.census.gov/data/key_signup.html
- Optional: OpenAI API key (for CIP extraction)

---

## Local Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# Required
DATABASE_URL=postgresql://user:pass@localhost:5432/maryland_atlas
CENSUS_API_KEY=your_census_key_here
MAPBOX_ACCESS_TOKEN=pk.your_mapbox_token

# Optional (for AI extraction)
OPENAI_API_KEY=sk-your_openai_key
AI_ENABLED=true
```

### 3. Initialize Database

```bash
# Create database
createdb maryland_atlas

# Enable PostGIS
psql maryland_atlas -c "CREATE EXTENSION postgis;"

# Initialize schema and load county boundaries
python scripts/init_db.py
```

Expected output:
```
✓ Database connection successful
✓ Schema initialized
✓ County boundaries loaded
✓ Verification complete
```

### 4. Run Data Ingestion (Layer 1 Employment)

```bash
# Run just employment layer (fastest test)
python -m src.ingest.layer1_employment
```

This fetches real data from:
- US Census LEHD/LODES
- BLS QCEW
- USASpending.gov

**Expected time:** 2-5 minutes

### 5. Run Full Pipeline

```bash
# Full pipeline with AI extraction
python src/run_pipeline.py --level county --run-ai auto

# Or skip AI for faster testing
python src/run_pipeline.py --level county --run-ai false
```

Pipeline stages:
1. Data ingestion (Layer 1)
2. AI extraction (if enabled)
3. Normalization
4. Scoring
5. Classification
6. GeoJSON export

**Expected time:** 5-10 minutes (without AI), 10-15 minutes (with AI)

### 6. Start API Server

```bash
make serve

# Or directly:
uvicorn src.api.main:app --reload --port 8000
```

Test endpoints:
- http://localhost:8000/health
- http://localhost:8000/docs (interactive API docs)
- http://localhost:8000/api/v1/layers/counties/latest

### 7. View Results

**GeoJSON Output:**
```bash
ls exports/
# md_counties_latest.geojson
# md_counties_20260128.geojson
```

**Open in QGIS, Mapbox Studio, or any GIS tool.**

**API Query Example:**
```bash
# Get Montgomery County detail
curl http://localhost:8000/api/v1/areas/24031 | jq
```

---

## Railway Deployment

### 1. Install Railway CLI

```bash
npm i -g @railway/cli
```

### 2. Login and Initialize

```bash
railway login
railway init
```

### 3. Provision PostgreSQL

```bash
railway add --database postgres
```

This automatically sets `DATABASE_URL` environment variable.

### 4. Set Environment Variables

```bash
railway variables set CENSUS_API_KEY=your_key_here
railway variables set MAPBOX_ACCESS_TOKEN=pk.your_token
railway variables set OPENAI_API_KEY=sk-your_key  # Optional
railway variables set AI_ENABLED=true  # Optional
```

### 5. Deploy

```bash
railway up
```

Railway will:
- Install dependencies from `requirements.txt`
- Run database migrations
- Start FastAPI server (via `Procfile`)
- Schedule cron jobs (from `railway.json`)

### 6. Initialize Database (One-Time)

```bash
# Connect to Railway shell
railway run python scripts/init_db.py
```

### 7. Trigger First Pipeline Run

```bash
# Manual trigger (or wait for monthly cron)
railway run python src/run_pipeline.py --level county --run-ai auto
```

### 8. Monitor

```bash
# View logs
railway logs

# Check health
curl https://your-app.up.railway.app/health
```

---

## Cron Schedule (Automatic Updates)

Railway runs these automatically:

| Job | Schedule | Command |
|-----|----------|---------|
| Monthly pipeline | 1st of month, 2 AM | Full ingestion + processing |
| Weekly AI extraction | Every Monday, 3 AM | CIP document updates |
| Daily export refresh | Every day, 5 AM | Regenerate GeoJSON |

**Manual override:**
```bash
railway run python src/run_pipeline.py --export-only
```

---

## Troubleshooting

### "Database connection failed"

**Check:**
```bash
echo $DATABASE_URL
psql $DATABASE_URL -c "SELECT version();"
```

**Fix:** Ensure PostgreSQL is running and DATABASE_URL is correct.

### "CENSUS_API_KEY not set"

**Get free key:** https://api.census.gov/data/key_signup.html

**Set:**
```bash
export CENSUS_API_KEY=your_key
# Or add to .env file
```

### "No module named 'openai'"

**Fix:**
```bash
pip install openai PyPDF2 tiktoken
```

### "AI extraction failed"

**Check:**
```bash
echo $OPENAI_API_KEY
echo $AI_ENABLED
```

**Skip AI:**
```bash
python src/run_pipeline.py --run-ai false
```

### "No data found for layer X"

**Expected for V1:**
- Layers 2-6 are scaffolded but not ingesting yet
- Only Layer 1 (Employment) is fully implemented

**This is normal** - system runs with partial data.

### "Export failed - no classifications"

**Cause:** Pipeline didn't complete processing stage.

**Fix:**
```bash
# Run full pipeline first
python src/run_pipeline.py --level county

# Then export
python src/run_pipeline.py --export-only
```

---

## Next Steps

### Test the System

```bash
# 1. Verify database
python -c "from config.database import test_connection; test_connection()"

# 2. Check data coverage
psql $DATABASE_URL -c "SELECT COUNT(*) FROM layer1_employment_gravity;"

# 3. Test API
curl http://localhost:8000/api/v1/metadata/refresh | jq
```

### Customize Thresholds

Edit `config/settings.py`:

```python
THRESHOLD_IMPROVING_HIGH = 0.6  # Raise to 0.7 for stricter "Improving"
THRESHOLD_AT_RISK_LOW = 0.3     # Lower to 0.2 for stricter "At Risk"
```

Re-run classification:
```bash
python -m src.processing.classification
python src/run_pipeline.py --export-only
```

### Add More Counties to AI Extraction

Edit `src/ai/pipeline/cip_extractor.py`:

```python
CIP_SOURCES = {
    "24031": {...},  # Montgomery (existing)
    "24027": {       # Add Howard County
        "name": "Howard County",
        "url": "https://verified_url_here.pdf",
        "title": "Howard County FY25 CIP",
        "published_date": date(2024, 6, 1)
    }
}
```

### Build Frontend

Use Mapbox GL JS:

```javascript
map.addSource('counties', {
  type: 'geojson',
  data: 'http://localhost:8000/api/v1/layers/counties/latest'
});

map.addLayer({
  id: 'county-fills',
  type: 'fill',
  source: 'counties',
  paint: {
    'fill-color': [
      'match',
      ['get', 'directional_class'],
      'improving', '#2ecc71',
      'stable', '#f39c12',
      'at_risk', '#e74c3c',
      '#95a5a6'
    ],
    'fill-opacity': 0.6
  }
});
```

---

## Success Checklist

- [ ] Database initialized with 24 Maryland counties
- [ ] Layer 1 employment data ingested
- [ ] API server responds to /health
- [ ] GeoJSON export exists in `exports/` directory
- [ ] Can query county detail: `/api/v1/areas/24031`
- [ ] Logs show no critical errors

**If all checked:** You have a working Maryland Viability Atlas! 🎉

---

## Getting Help

- **Documentation:** See `docs/` folder
- **Methodology:** `docs/METHODOLOGY.md`
- **Limitations:** `docs/LIMITATIONS.md`
- **Data Sources:** `docs/DATA_SOURCES.md`

**Questions?** Open an issue on GitHub with:
- Error message
- Command you ran
- Your environment (OS, Python version)
- Relevant logs
