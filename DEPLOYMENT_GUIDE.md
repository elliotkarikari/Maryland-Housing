# Production Deployment Guide

**Maryland Growth & Family Viability Atlas**

Complete guide for deploying this system to production on Railway.

---

## Pre-Deployment Checklist

### Required Accounts & Keys

- [ ] Railway account (https://railway.app)
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

### Step 3: Provision PostgreSQL with PostGIS

```bash
railway add --database postgres
```

Railway creates `DATABASE_URL` automatically.

**Enable PostGIS extension:**

```bash
# Connect to Railway database
railway connect postgres

# In psql shell:
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
\q
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

### Step 6: Initialize Database

```bash
# Run initialization script
railway run python scripts/init_db.py
```

Expected output:
```
✓ Database connection successful
✓ Schema initialized
✓ County boundaries loaded (24 counties)
✓ Verification complete
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
curl https://your-app.up.railway.app/api/v1/metadata/refresh | jq
```

---

## Cron Jobs Configuration

Railway automatically configures cron jobs from `railway.json`:

| Job | Schedule | Purpose |
|-----|----------|---------|
| `monthly-pipeline-run` | 1st @ 2 AM EST | Full ingestion + processing |
| `weekly-ai-cip-extraction` | Monday @ 3 AM EST | AI document extraction |
| `daily-export-refresh` | Daily @ 5 AM EST | Regenerate GeoJSON |

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
# Connect to database
railway connect postgres

# Check table sizes
\dt+

# Check recent refreshes
SELECT layer_name, refresh_date, status
FROM data_refresh_log
ORDER BY refresh_date DESC
LIMIT 10;

# Check AI extraction costs
SELECT * FROM v_ai_cost_summary
ORDER BY extraction_date DESC;
```

### Cost Monitoring

**Railway costs:**
- Hobby plan: $5/month (includes 500MB PostgreSQL)
- Pro plan: $20/month (includes 8GB PostgreSQL + priority support)

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

- **Database:** 500MB (sufficient for ~5 years of data)
- **API:** Single instance (handles ~100 req/sec)
- **Cron:** 3 jobs (monthly, weekly, daily)

### When to Upgrade

Upgrade to Pro plan if:
- Database exceeds 400MB (check with `\l+` in psql)
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

**Database:** Use Railway's managed PostgreSQL autoscaling.

---

## Backup & Disaster Recovery

### Automated Backups

Railway provides automatic daily backups (retained 7 days on Hobby, 30 days on Pro).

**Manual backup:**
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
# 1. Restore database from backup
railway connect postgres < backup_20260128.sql

# 2. Reinitialize schema (if needed)
railway run python scripts/init_db.py

# 3. Re-run pipeline
railway run python src/run_pipeline.py --level county
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

```sql
-- Create read-only API user
CREATE ROLE api_user WITH LOGIN PASSWORD 'strong_password';
GRANT CONNECT ON DATABASE maryland_atlas TO api_user;
GRANT USAGE ON SCHEMA public TO api_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_user;

-- Revoke write permissions
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM api_user;
```

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
railway run psql $DATABASE_URL -c "SELECT SUM(cost_estimate) FROM ai_extraction WHERE created_at >= date_trunc('month', CURRENT_DATE);"
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

### "GeoJSON export missing"

**Check logs:**
```bash
railway logs --filter "geojson_export"
```

**Regenerate:**
```bash
railway run python src/run_pipeline.py --export-only
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

Already configured in `schema.sql`, but verify:

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
3. Query `data_refresh_log` table
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
psql $DATABASE_URL -c "SELECT MAX(refresh_date) FROM data_refresh_log;"
```

---

**Deployment complete!** 🚀

Your Maryland Viability Atlas is now running in production with automated updates, full observability, and graceful degradation.
