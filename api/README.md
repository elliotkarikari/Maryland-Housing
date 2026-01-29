# Maryland Viability Atlas API

FastAPI backend serving county synthesis data and map visualizations.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r api/requirements.txt
```

### 2. Run Multi-Year Pipeline (First Time Only)

The API requires the `final_synthesis` table to be populated. Run the pipeline:

```bash
python src/run_multiyear_pipeline.py --year 2021
python src/export/geojson.py
```

Or use the automated setup script:

```bash
./scripts/setup_api_demo.sh
```

### 3. Start the API

```bash
./api/start.sh
```

Or manually:

```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at:
- **Base URL**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## API Endpoints

### GET `/`
API root with endpoint information.

**Response**:
```json
{
  "name": "Maryland Viability Atlas API",
  "version": "2.0.0",
  "endpoints": {
    "counties": "/api/v1/layers/counties/latest",
    "county_detail": "/api/v1/areas/{fips_code}"
  }
}
```

### GET `/api/v1/health`
Health check endpoint.

**Response**:
```json
{
  "status": "healthy",
  "database": "connected"
}
```

### GET `/api/v1/layers/counties/latest`
Returns GeoJSON with all Maryland counties and their synthesis classifications.

**Response**: GeoJSON FeatureCollection

**Example**:
```bash
curl http://localhost:8000/api/v1/layers/counties/latest
```

### GET `/api/v1/areas/{fips_code}`
Get detailed synthesis data for a specific county.

**Parameters**:
- `fips_code` (path): 5-digit FIPS code (e.g., "24003" for Anne Arundel)

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
    "Excellent mobility options",
    "Low environmental risk"
  ],
  "primary_weaknesses": [
    "Declining school enrollment",
    "High housing costs"
  ],
  "key_trends": [
    "Stable employment growth",
    "Increasing housing affordability challenges"
  ],
  "last_updated": "2026-01-29T14:30:00"
}
```

**Example**:
```bash
# Anne Arundel County
curl http://localhost:8000/api/v1/areas/24003

# Baltimore City
curl http://localhost:8000/api/v1/areas/24510

# Montgomery County
curl http://localhost:8000/api/v1/areas/24031
```

## Maryland County FIPS Codes

| FIPS | County |
|------|--------|
| 24001 | Allegany |
| 24003 | Anne Arundel |
| 24005 | Baltimore County |
| 24009 | Calvert |
| 24011 | Caroline |
| 24013 | Carroll |
| 24015 | Cecil |
| 24017 | Charles |
| 24019 | Dorchester |
| 24021 | Frederick |
| 24023 | Garrett |
| 24025 | Harford |
| 24027 | Howard |
| 24029 | Kent |
| 24031 | Montgomery |
| 24033 | Prince George's |
| 24035 | Queen Anne's |
| 24037 | St. Mary's |
| 24039 | Somerset |
| 24041 | Talbot |
| 24043 | Washington |
| 24045 | Wicomico |
| 24047 | Worcester |
| 24510 | Baltimore City |

## Frontend Integration

The frontend map is configured to use these API endpoints:

```javascript
const API_BASE_URL = 'http://localhost:8000/api/v1';

// Load GeoJSON
const response = await fetch(`${API_BASE_URL}/layers/counties/latest`);

// Load county detail
const detail = await fetch(`${API_BASE_URL}/areas/24003`);
```

## Development

### Running with Auto-Reload

```bash
python -m uvicorn api.main:app --reload
```

The `--reload` flag enables auto-restart when code changes.

### Environment Variables

The API uses the same `.env` file as the main application:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/md_housing
```

### Testing

```bash
# Health check
curl http://localhost:8000/api/v1/health

# Get GeoJSON
curl http://localhost:8000/api/v1/layers/counties/latest | jq '.features | length'

# Get county detail
curl http://localhost:8000/api/v1/areas/24003 | jq '.synthesis_grouping'
```

## CORS Configuration

The API currently allows all origins for local development:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

For production, restrict origins to your domain.

## Troubleshooting

### "relation 'final_synthesis' does not exist"

Run the multi-year pipeline to create and populate the synthesis table:

```bash
python src/run_multiyear_pipeline.py --year 2021
```

### Database Connection Issues

Check your `.env` file and ensure PostgreSQL is running:

```bash
# Test connection
psql $DATABASE_URL -c "SELECT 1"
```

### Port Already in Use

Stop existing processes on port 8000:

```bash
lsof -ti:8000 | xargs kill -9
```

Or use a different port:

```bash
python -m uvicorn api.main:app --port 8001
```

## Architecture

```
api/
├── main.py           # FastAPI application
├── requirements.txt  # Python dependencies
├── start.sh         # Startup script
└── README.md        # This file

Database Tables:
├── final_synthesis           # County classifications
├── layer_multiyear_scores   # Layer scores
└── layer_timeseries_features # Temporal features
```

## Production Deployment

For production deployment:

1. **Use a production ASGI server**:
   ```bash
   gunicorn api.main:app -w 4 -k uvicorn.workers.UvicornWorker
   ```

2. **Configure CORS** to allow only your domain

3. **Add authentication** if needed

4. **Use environment variables** for configuration

5. **Enable HTTPS** with a reverse proxy (nginx, Caddy)

6. **Monitor** with logging and metrics

## License

Same as main project.
