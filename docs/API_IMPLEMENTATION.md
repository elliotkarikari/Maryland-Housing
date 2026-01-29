# API Implementation Summary

## What Was Built

A complete FastAPI backend to serve the Maryland Viability Atlas map and enable county detail panels.

### Components Created

1. **[api/main.py](../api/main.py)** - FastAPI application with 4 endpoints
2. **[api/requirements.txt](../api/requirements.txt)** - Python dependencies
3. **[api/start.sh](../api/start.sh)** - Quick start script
4. **[api/README.md](../api/README.md)** - Complete API documentation
5. **[scripts/setup_api_demo.sh](../scripts/setup_api_demo.sh)** - One-command setup

### Frontend Integration

Updated **frontend/map.js** to use API endpoints instead of static files.

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
# One command to set up everything
./scripts/setup_api_demo.sh

# Then start the API
./api/start.sh
```

### Option 2: Manual Setup

```bash
# 1. Install API dependencies
pip install -r api/requirements.txt

# 2. Run multi-year pipeline (creates final_synthesis table)
python src/run_multiyear_pipeline.py --year 2021

# 3. Export GeoJSON
python src/export/geojson.py

# 4. Start API
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

### 1. Health Check
```bash
curl http://localhost:8000/api/v1/health
# {"status":"healthy","database":"connected"}
```

### 2. GeoJSON Layer
```bash
curl http://localhost:8000/api/v1/layers/counties/latest
# Returns full GeoJSON with all counties
```

### 3. County Detail (NEW!)
```bash
curl http://localhost:8000/api/v1/areas/24003
```

**Response**:
```json
{
  "fips_code": "24003",
  "county_name": "Anne Arundel County",
  "data_year": 2021,
  "synthesis_grouping": "stable_constrained",
  "directional_class": "stable",
  "confidence_class": "strong",
  "composite_score": 0.523,
  "classification_version": "v2.0-multiyear",
  "layer_scores": {
    "employment_gravity": 0.612,
    "mobility_optionality": 0.745,
    "school_trajectory": 0.423,
    "housing_elasticity": 0.356,
    "demographic_momentum": 0.534,
    "risk_drag": 0.467
  },
  "primary_strengths": [
    "Strong employment base",
    "Excellent mobility options"
  ],
  "primary_weaknesses": [
    "Declining school enrollment",
    "High housing costs"
  ],
  "key_trends": [
    "Stable employment growth"
  ],
  "last_updated": "2026-01-29T14:30:00"
}
```

## How It Works

### Architecture

```
Browser Request
    ↓
Frontend (map.js)
    ↓
FastAPI (api/main.py)
    ↓
PostgreSQL (final_synthesis table)
    ↓
JSON Response
    ↓
Interactive Map Panel
```

### Data Flow

1. **Map loads**: Fetches GeoJSON from `/api/v1/layers/counties/latest`
2. **User clicks county**: Triggers request to `/api/v1/areas/{fips_code}`
3. **API queries database**: Pulls from `final_synthesis` table
4. **Response formatted**: Converts to Pydantic model (type-safe)
5. **Frontend renders**: Displays in side panel with scores, trends, etc.

### Database Schema

The API reads from the `final_synthesis` table created by the multi-year pipeline:

```sql
final_synthesis:
  - geoid (fips_code)
  - county_name
  - synthesis_grouping (emerging_tailwinds | stable_constrained | etc.)
  - directional_class (improving | stable | at_risk)
  - confidence_class (strong | conditional | fragile)
  - composite_score (0-1)
  - employment_gravity_score
  - mobility_optionality_score
  - school_trajectory_score
  - housing_elasticity_score
  - demographic_momentum_score
  - risk_drag_score
  - primary_strengths (array)
  - primary_weaknesses (array)
  - key_trends (array)
  - classification_version
  - last_updated
```

## Features

### ✅ Implemented

- [x] RESTful API with 4 endpoints
- [x] GeoJSON serving for map
- [x] County detail endpoint
- [x] Type-safe responses (Pydantic models)
- [x] Health check endpoint
- [x] CORS middleware for local development
- [x] Interactive API docs (FastAPI auto-generates)
- [x] Database connection pooling
- [x] Error handling (404, 500, etc.)
- [x] Auto-reload for development

### 📚 Interactive API Documentation

FastAPI automatically generates interactive docs:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

You can **test endpoints directly** in the browser!

## Frontend Changes

### Before (Static File)
```javascript
const GEOJSON_PATH = 'md_counties_latest.geojson';
```

### After (API Endpoint)
```javascript
const GEOJSON_PATH = `${API_BASE_URL}/layers/counties/latest`;
```

The county detail panel now works when clicking counties!

## Testing

### 1. Start Everything

**Terminal 1: API Server**
```bash
python -m uvicorn api.main:app --reload
```

**Terminal 2: Frontend (optional, API serves GeoJSON)**
```bash
cd frontend
python -m http.server 8080
```

### 2. Test in Browser

**Map**: http://localhost:8080 (or just open frontend/index.html)

**API Docs**: http://localhost:8000/docs

### 3. Test Endpoints

```bash
# Health
curl http://localhost:8000/api/v1/health

# GeoJSON
curl http://localhost:8000/api/v1/layers/counties/latest | jq '.features | length'
# Should return: 24

# County Detail - Anne Arundel
curl http://localhost:8000/api/v1/areas/24003 | jq '.synthesis_grouping'
# Should return: "stable_constrained" (or similar)

# County Detail - Baltimore City
curl http://localhost:8000/api/v1/areas/24510 | jq '.county_name'
# Should return: "Baltimore City"
```

## Example Use Cases

### 1. Get All Counties at Risk

```python
import requests

resp = requests.get('http://localhost:8000/api/v1/layers/counties/latest')
geojson = resp.json()

at_risk = [
    f["properties"]["county_name"]
    for f in geojson["features"]
    if f["properties"]["synthesis_grouping"] == "at_risk_headwinds"
]

print(f"Counties at risk: {', '.join(at_risk)}")
```

### 2. Compare Layer Scores

```python
import requests

def get_county_scores(fips):
    resp = requests.get(f'http://localhost:8000/api/v1/areas/{fips}')
    data = resp.json()
    return data['layer_scores']

anne_arundel = get_county_scores('24003')
baltimore_city = get_county_scores('24510')

print(f"Employment: AA={anne_arundel['employment_gravity']:.2f}, BC={baltimore_city['employment_gravity']:.2f}")
```

### 3. Export to CSV

```python
import requests
import csv

resp = requests.get('http://localhost:8000/api/v1/layers/counties/latest')
geojson = resp.json()

with open('counties.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['FIPS', 'County', 'Grouping', 'Confidence', 'Score'])

    for feature in geojson['features']:
        p = feature['properties']
        writer.writerow([
            p['fips_code'],
            p['county_name'],
            p['synthesis_grouping'],
            p['confidence_class'],
            p['composite_score']
        ])
```

## Production Considerations

For production deployment, consider:

### 1. Environment Configuration
```bash
# .env
DATABASE_URL=postgresql://user:pass@prod-host/db
ALLOWED_ORIGINS=https://yourdomain.com
API_KEY_SECRET=...
```

### 2. CORS Restrictions
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Not "*"
    ...
)
```

### 3. ASGI Server
```bash
gunicorn api.main:app \
  -w 4 \
  -k uvicorn.workers.UvicornWorker \
  -b 0.0.0.0:8000
```

### 4. Reverse Proxy (nginx)
```nginx
location /api {
    proxy_pass http://localhost:8000;
    proxy_set_header Host $host;
}
```

### 5. HTTPS
Use Let's Encrypt or Cloudflare for SSL/TLS.

## Troubleshooting

### Problem: "relation 'final_synthesis' does not exist"

**Solution**: Run the multi-year pipeline first:
```bash
python src/run_multiyear_pipeline.py --year 2021
```

### Problem: Database connection failed

**Solution**: Check `.env` file and PostgreSQL:
```bash
psql $DATABASE_URL -c "SELECT COUNT(*) FROM final_synthesis"
```

### Problem: Port 8000 already in use

**Solution**: Kill existing process or use different port:
```bash
lsof -ti:8000 | xargs kill -9
# OR
python -m uvicorn api.main:app --port 8001
```

### Problem: Frontend shows "Error loading map data"

**Solution**: Check API is running and accessible:
```bash
curl http://localhost:8000/api/v1/health
```

## Next Steps

### Potential Enhancements

1. **Caching**: Add Redis for frequently accessed counties
2. **Pagination**: For bulk data exports
3. **Filtering**: Query parameters like `?grouping=at_risk_headwinds`
4. **Comparisons**: Endpoint to compare multiple counties
5. **Historical**: Time-series data for trend charts
6. **Authentication**: API keys for rate limiting
7. **WebSockets**: Real-time updates when pipeline runs
8. **Export Formats**: CSV, Excel, PDF reports

### Example: Add Filtering

```python
@app.get("/api/v1/areas")
async def list_counties(
    grouping: Optional[str] = None,
    confidence: Optional[str] = None,
    min_score: Optional[float] = None
):
    """List counties with optional filters."""
    # Build dynamic SQL query based on filters
    ...
```

## Summary

You now have a **fully functional REST API** that:

- ✅ Serves GeoJSON for map visualization
- ✅ Provides detailed county information
- ✅ Enables interactive county panels
- ✅ Auto-generates documentation
- ✅ Handles errors gracefully
- ✅ Ready for production deployment

The map is now **fully interactive** with clickable counties showing detailed analysis!

---

**Documentation Version**: 1.0
**Last Updated**: January 29, 2026
