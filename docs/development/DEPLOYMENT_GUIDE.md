# Production Deployment Guide

**Maryland Growth & Family Viability Atlas**

Complete guide for deploying this system to production on Railway.

---

## Pre-Deployment Checklist

### Required Accounts & Keys

- [ ] Railway account (https://railway.app)
- [ ] Databricks workspace + SQL Warehouse access token
- [ ] Census API key (https://api.census.gov/data/key_signup.html)
- [ ] Mapbox account + token (https://account.mapbox.com/)
- [ ] Optional: OpenAI API key (https://platform.openai.com/api-keys)
- [ ] Optional: Sentry DSN for error tracking

### Repository Preparation

```bash
# 1. Initialize git repository
git init
git add .
git commit -m "Initial commit: Maryland Viability Atlas V1.0"

# 2. Create GitHub repository
gh repo create maryland-viability-atlas --public --source=. --remote=origin

# 3. Push to GitHub
git push -u origin main
```

---

## Railway Deployment

### Step 1: Install Railway CLI

```bash
npm install -g @railway/cli
railway login
```

### Step 2: Create New Project

```bash
# From project root
railway init

# Project name: maryland-viability-atlas
# Environment: production
```

### Step 3: Configure Databricks SQL Warehouse (Primary Backend)

```bash
railway variables set DATA_BACKEND="databricks"
railway variables set DATABRICKS_SERVER_HOSTNAME="dbc-xxxx.cloud.databricks.com"
railway variables set DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxx"
railway variables set DATABRICKS_ACCESS_TOKEN="dapi..."
railway variables set DATABRICKS_CATALOG="maryland_atlas"
railway variables set DATABRICKS_SCHEMA="default"
```

Optional local fallback mode only:

```bash
railway add --database postgres
railway variables set DATA_BACKEND="postgres"
```

### Step 4: Set Environment Variables

```bash
# Required
railway variables set CENSUS_API_KEY="your_census_key_here"
railway variables set MAPBOX_ACCESS_TOKEN="pk.your_mapbox_token"

# Optional: AI extraction
railway variables set OPENAI_API_KEY="sk-your_openai_key"
railway variables set AI_ENABLED="true"

# Optional: Error tracking
railway variables set SENTRY_DSN="https://your_sentry_dsn"

# Application settings
railway variables set ENVIRONMENT="production"
railway variables set LOG_LEVEL="INFO"
railway variables set DEBUG="false"
railway variables set CORS_ALLOW_ORIGINS="https://your-frontend-domain.com"
```

**Verify variables:**
```bash
railway variables
```

### Step 5: Deploy Application

```bash
railway up
```

Railway will:
1. Detect Python app from `requirements.txt`
2. Install dependencies
3. Use `Procfile` to start FastAPI server
4. Expose public URL

**Monitor deployment:**
```bash
railway logs
```

### Step 6: Validate Databricks Connectivity

```bash
# Confirm backend connection from app runtime
railway run python -c "from config.database import test_connection; print('db_connected=', test_connection())"
```

Expected output includes:
```
db_connected= True
```

### Step 7: Run Initial Data Ingestion

```bash
# Full pipeline run (may take 10-15 minutes)
railway run python src/run_pipeline.py --level county --run-ai auto
```

**Monitor progress:**
```bash
railway logs --follow
```

### Step 8: Verify Deployment

```bash
# Get your Railway URL
railway status

# Test health endpoint
curl https://your-app.up.railway.app/health

# Test API
curl https://your-app.up.railway.app/api/v1/layers/counties/latest | jq '.features | length'
curl -i https://your-app.up.railway.app/api/v1/areas/24031 | head -n 20
```

---

## Cron Jobs Configuration

Railway automatically configures cron jobs from `railway.json`:

| Job | Schedule | Purpose |
|-----|----------|---------|
| `monthly-pipeline-run` | 1st @ 2 AM EST | Full ingestion + processing |
| `weekly-ai-cip-extraction` | Monday @ 3 AM EST | AI document extraction |
| `daily-export-refresh` | Daily @ 5 AM EST | Optional GeoJSON snapshot refresh (map runtime stays API-first) |

**Manual trigger:**
```bash
railway run python src/run_pipeline.py --level county --run-ai auto
```

**View cron logs:**
```bash
railway logs --filter "cron"
```

---

## Monitoring & Maintenance

### Health Checks

Railway monitors `/health` endpoint automatically.

**Manual check:**
```bash
curl https://your-app.up.railway.app/health
```

Expected response:
```json
{
  "status": "healthy",
  "database": "connected",
  "county_boundaries": "available",
  "county_count": 24,
  "geojson_export": "available",
  "environment": "production"
}
```

### Log Monitoring

```bash
# Real-time logs
railway logs --follow

# Filter by severity
railway logs --filter "ERROR"

# Export logs
railway logs --json > logs.json
```

### Database Monitoring

```bash
# Databricks connectivity from app runtime
railway run python -c "from config.database import test_connection; print('db_connected=', test_connection())"

# Live county feed coverage (expect 24)
curl https://your-app.up.railway.app/api/v1/layers/counties/latest | jq '.features | length'

# Detail endpoint should return 200 for valid FIPS
curl -i https://your-app.up.railway.app/api/v1/areas/24031 | head -n 20
```

### Cost Monitoring

**Railway costs:**
- Depends on service plan and runtime usage.

**Databricks costs:**
- Driven by SQL Warehouse uptime/query volume.

**AI extraction costs:**
- Tracked in `ai_extraction` table
- Monthly limit: $5.00 (configurable in `config/settings.py`)

**Query current month's AI spending:**
```sql
SELECT
  SUM(cost_estimate) as total_cost_usd,
  COUNT(*) as extraction_count
FROM ai_extraction
WHERE created_at >= date_trunc('month', CURRENT_DATE);
```

---

## Scaling Considerations

### Current Capacity (Hobby Plan)

- **Database:** Databricks SQL Warehouse managed outside Railway
- **API:** Single instance (handles ~100 req/sec)
- **Cron:** 3 jobs (monthly, weekly, daily)

### When to Upgrade

Upgrade to Pro plan if:
- API response times > 1 second
- Need more than 3 cron jobs
- Want staging environment

### Horizontal Scaling

**API layer:**
```json
// railway.json
{
  "deploy": {
    "numReplicas": 3  // Scale to 3 instances
  }
}
```

**Database:** Scale SQL Warehouse size/concurrency in Databricks.

---

## Backup & Disaster Recovery

### Automated Backups

Railway covers application deployment rollback/history. Primary analytical data resides in Databricks and should follow Databricks workspace backup/versioning policy.

Optional fallback-mode backup (only when `DATA_BACKEND=postgres`):
```bash
# Export database
railway run pg_dump $DATABASE_URL > backup_$(date +%Y%m%d).sql

# Upload to S3/Google Cloud Storage
aws s3 cp backup_*.sql s3://your-bucket/backups/
```

### Export Versioning

GeoJSON exports are versioned automatically:

```bash
# List versions
railway run ls -lh exports/

# Download specific version
railway run cat exports/md_counties_20260128.geojson > local_copy.geojson
```

### Recovery Procedure

```bash
# 1. Confirm Databricks/API connectivity
curl https://your-app.up.railway.app/health

# 2. Re-run pipeline to repopulate synthesis and snapshots
railway run python src/run_pipeline.py --level county

# 3. Re-verify live endpoints
curl https://your-app.up.railway.app/api/v1/layers/counties/latest | jq '.features | length'
```

---

## CI/CD Pipeline (GitHub Actions)

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to Railway

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install Railway CLI
        run: npm i -g @railway/cli

      - name: Deploy to Railway
        env:
          RAILWAY_TOKEN: ${{ secrets.RAILWAY_TOKEN }}
        run: railway up --detach

      - name: Run tests
        run: |
          railway run python -m pytest tests/ -v
```

**Setup:**
```bash
# Get Railway token
railway tokens

# Add to GitHub secrets
# Settings > Secrets > New repository secret
# Name: RAILWAY_TOKEN
# Value: <your_token>
```

---

## Security Hardening

### Environment Variables

**Never commit:**
- API keys
- Database credentials
- Tokens

**Use Railway secrets:**
```bash
railway variables set --sensitive OPENAI_API_KEY="sk-..."
```

### CORS Configuration

Edit `src/api/main.py`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://your-frontend-domain.com",
        "https://mapbox-app.netlify.app"
    ],  # Specific origins only
    allow_credentials=True,
    allow_methods=["GET"],  # Read-only
    allow_headers=["*"],
)
```

### Rate Limiting

Add to `src/api/main.py`:

```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.get("/api/v1/areas/{geoid}")
@limiter.limit("10/minute")
async def get_area_detail(request: Request, geoid: str):
    ...
```

### Database Security

- Use a dedicated Databricks service principal/token for API access.
- Scope warehouse permissions to read access on required schemas/tables.
- Rotate Databricks access tokens on a fixed schedule.
- If running Postgres fallback mode, apply standard least-privilege SQL grants.

---

## Troubleshooting Production Issues

### "Database connection pool exhausted"

**Cause:** Too many concurrent connections.

**Fix:**
```python
# config/database.py
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=20,  # Increase pool size
    max_overflow=10
)
```

### "AI extraction cost limit exceeded"

**Check:**
```bash
railway run python -c "from sqlalchemy import text; from config.database import engine; c=engine.connect(); print(c.execute(text(\"SELECT COALESCE(SUM(cost_estimate),0) FROM ai_extraction\")).scalar()); c.close()"
```

**Fix:** Increase limit in `config/settings.py` or disable AI:
```bash
railway variables set AI_ENABLED="false"
```

### "Cron job not running"

**Verify schedule:**
```bash
railway cron list
```

**Manual trigger:**
```bash
railway run python src/run_pipeline.py --level county
```

### "Live county feed missing"

**Check logs:**
```bash
railway logs --filter "layers/counties/latest"
```

**Validate source tables via app health and rerun pipeline:**
```bash
curl https://your-app.up.railway.app/health
railway run python src/run_pipeline.py --level county
```

---

## Rolling Back a Deployment

```bash
# View deployment history
railway status --history

# Rollback to previous version
railway rollback

# Rollback to specific deployment
railway rollback --to <deployment_id>
```

---

## Performance Optimization

### Database Indexes

For Databricks, optimize table layout/warehouse sizing in Databricks workspace settings.  
Postgres fallback mode can verify indexes with:

```sql
-- Check missing indexes
SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename;
```

### API Response Caching

Add Redis:
```bash
railway add --database redis
```

Update API:
```python
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

@app.on_event("startup")
async def startup():
    redis = aioredis.from_url("redis://localhost")
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")

@app.get("/api/v1/areas/{geoid}")
@cache(expire=3600)  # Cache for 1 hour
async def get_area_detail(geoid: str):
    ...
```

---

## Maintenance Schedule

| Task | Frequency | Owner |
|------|-----------|-------|
| Review error logs | Daily | DevOps |
| Check AI costs | Weekly | Data team |
| Update dependencies | Monthly | Engineering |
| Review classifications | Quarterly | Analysts |
| Backup verification | Monthly | DevOps |

---

## Support & Escalation

**Production issues:**
1. Check `/health` endpoint
2. Review Railway logs
3. Check `GET /api/v1/metadata/refresh` and `GET /api/v1/layers/counties/latest`
4. Open GitHub issue with tag `production`

**Data quality issues:**
1. Check source availability (DATA_SOURCES.md)
2. Verify API keys haven't expired
3. Review ingestion logs
4. Open issue with tag `data-quality`

---

## Success Metrics

Track these KPIs:

- **Uptime:** Target 99.5% (Railway SLA)
- **API latency:** p95 < 500ms
- **Data freshness:** Updated within 24h of source release
- **AI extraction success rate:** > 90%
- **Cost per month:** < $30 (Railway + AI)

**Monitor with:**
```bash
# Uptime
railway status

# Latency
curl -w "@curl-format.txt" https://your-app.up.railway.app/health

# Data freshness
curl https://your-app.up.railway.app/api/v1/metadata/refresh | jq
```

---

**Deployment complete!** 🚀

Your Maryland Viability Atlas is now running in production with automated updates, full observability, and graceful degradation.
