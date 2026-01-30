# System Status Dashboard

**Maryland Growth & Family Viability Atlas**

**Document Type:** Runtime Status Dashboard
**Last Updated:** 2026-01-30
**Version:** 2.0

---

## Quick Status Overview

| Component | Status | Notes |
|-----------|--------|-------|
| API Server | `check_api()` | FastAPI on port 8000 |
| Database | `check_db()` | PostgreSQL + PostGIS |
| Frontend | `check_frontend()` | Port 3000 |
| GeoJSON Export | `check_export()` | Latest file present |

---

## Data Layer Status

### Layer Coverage Matrix

| Layer | Description | v2 Status | Data Years | Last Refresh |
|-------|-------------|-----------|------------|--------------|
| Layer 1 | Employment Gravity | Operational | 2019-2024 | `check_layer1()` |
| Layer 2 | Mobility Optionality | Operational | 2020-2024 | `check_layer2()` |
| Layer 3 | School System Trajectory | Operational | 2019-2024 | `check_layer3()` |
| Layer 4 | Housing Elasticity | Operational | 2019-2024 | `check_layer4()` |
| Layer 5 | Demographic Momentum | Operational | 2018-2023 | `check_layer5()` |
| Layer 6 | Risk Drag | Operational | 2020-2024 | `check_layer6()` |
| Policy | Policy Persistence | Partial | 2020-2025 | `check_policy()` |

### Check Data Status

```bash
# Check layer data counts
psql $DATABASE_URL -c "
SELECT 'Layer 1' as layer, COUNT(*) as records, COUNT(DISTINCT data_year) as years
FROM layer1_employment_gravity
UNION ALL
SELECT 'Layer 2', COUNT(*), COUNT(DISTINCT data_year)
FROM layer2_mobility_optionality
UNION ALL
SELECT 'Layer 3', COUNT(*), COUNT(DISTINCT data_year)
FROM layer3_school_trajectory
UNION ALL
SELECT 'Layer 4', COUNT(*), COUNT(DISTINCT data_year)
FROM layer4_housing_elasticity
UNION ALL
SELECT 'Layer 5', COUNT(*), COUNT(DISTINCT data_year)
FROM layer5_demographic_momentum
UNION ALL
SELECT 'Layer 6', COUNT(*), COUNT(DISTINCT data_year)
FROM layer6_risk_drag;
"
```

---

## Pipeline Status

### V2 Pipeline Stages

| Stage | Module | Status | Last Run |
|-------|--------|--------|----------|
| Timeseries Features | `timeseries_features.py` | `check_timeseries()` | |
| Multi-Year Scoring | `multiyear_scoring.py` | `check_scoring()` | |
| Classification | `multiyear_classification.py` | `check_classification()` | |
| GeoJSON Export | `geojson_export.py` | `check_export()` | |

### Check Pipeline Status

```bash
# Check timeseries features
psql $DATABASE_URL -c "
SELECT COUNT(DISTINCT fips_code) as counties,
       COUNT(DISTINCT layer_key) as layers,
       MAX(computed_at) as last_computed
FROM layer_timeseries_features;
"

# Check classifications
psql $DATABASE_URL -c "
SELECT synthesis_grouping, COUNT(*) as count
FROM final_synthesis_current
GROUP BY synthesis_grouping
ORDER BY count DESC;
"

# Check export file
ls -la exports/md_counties_latest.geojson
```

---

## Classification Distribution

### Current Synthesis Groupings

```bash
psql $DATABASE_URL -c "
SELECT
    synthesis_grouping,
    COUNT(*) as counties,
    ROUND(COUNT(*) * 100.0 / 24, 1) as percentage
FROM final_synthesis_current
GROUP BY synthesis_grouping
ORDER BY
    CASE synthesis_grouping
        WHEN 'emerging_tailwinds' THEN 1
        WHEN 'conditional_growth' THEN 2
        WHEN 'stable_constrained' THEN 3
        WHEN 'at_risk_headwinds' THEN 4
        WHEN 'high_uncertainty' THEN 5
    END;
"
```

### Expected Distribution (Healthy System)

| Grouping | Expected Range | Color |
|----------|---------------|-------|
| Emerging Tailwinds | 2-5 counties | Deep Green |
| Conditional Growth | 4-8 counties | Light Green |
| Stable but Constrained | 6-10 counties | Yellow |
| At Risk / Headwinds | 2-5 counties | Orange |
| High Uncertainty | 0-4 counties | Gray |

**Note:** If all counties show "High Uncertainty", this indicates insufficient multi-year data coverage.

---

## Confidence Level Distribution

```bash
psql $DATABASE_URL -c "
SELECT
    confidence_class,
    COUNT(*) as counties
FROM final_synthesis_current
GROUP BY confidence_class
ORDER BY
    CASE confidence_class
        WHEN 'strong' THEN 1
        WHEN 'conditional' THEN 2
        WHEN 'fragile' THEN 3
    END;
"
```

### Confidence Requirements

| Level | Requirement | Interpretation |
|-------|-------------|----------------|
| Strong | ≥5 years avg coverage | High certainty |
| Conditional | ≥3 years avg coverage | Moderate certainty |
| Fragile | <3 years avg coverage | Low certainty |

---

## API Health Checks

### Health Endpoint

```bash
curl -s http://localhost:8000/health | jq
```

**Expected Response:**
```json
{
  "status": "healthy",
  "database": "connected",
  "postgis": "available",
  "export_file": "present",
  "timestamp": "2026-01-30T12:00:00Z"
}
```

### Endpoint Availability

```bash
# Test all endpoints
echo "Testing API endpoints..."

# Health
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health
# Expected: 200

# Counties list
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/v1/counties
# Expected: 200

# GeoJSON
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/v1/layers/counties/latest
# Expected: 200

# County detail (Montgomery)
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/v1/areas/24031
# Expected: 200

# Metadata
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/v1/metadata/refresh
# Expected: 200
```

---

## Data Refresh Log

### Recent Refresh Operations

```bash
psql $DATABASE_URL -c "
SELECT
    layer_key,
    data_year,
    status,
    records_processed,
    completed_at
FROM data_refresh_log
ORDER BY completed_at DESC
LIMIT 10;
"
```

### Refresh Schedule

| Operation | Schedule | Command |
|-----------|----------|---------|
| Full Pipeline | 1st of month, 2 AM | `make pipeline` |
| Export Only | Daily, 5 AM | `make export` |
| Layer 1 Refresh | Quarterly | `make ingest-layer1` |
| Layer 2-6 Refresh | Annual | `make ingest-all` |

---

## System Health Metrics

### Database Size

```bash
psql $DATABASE_URL -c "
SELECT
    pg_size_pretty(pg_database_size(current_database())) as db_size;
"
```

### Table Row Counts

```bash
psql $DATABASE_URL -c "
SELECT
    relname as table_name,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY n_live_tup DESC;
"
```

### GeoJSON File Size

```bash
ls -lh exports/md_counties_latest.geojson
# Expected: ~300-500 KB
```

---

## Troubleshooting Quick Reference

### Common Issues

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| All counties gray | Sparse data coverage | Run more layer ingestions |
| API returns 500 | Database connection issue | Check `DATABASE_URL` |
| GeoJSON empty | Pipeline not run | `make pipeline` |
| No classifications | Multi-year data missing | Verify data years |
| Frontend blank | API not running | `make serve` |

### Diagnostic Commands

```bash
# Check database connection
python -c "from config.database import test_connection; test_connection()"

# Check PostGIS
psql $DATABASE_URL -c "SELECT PostGIS_Version();"

# Check data coverage
psql $DATABASE_URL -c "
SELECT layer_key, MIN(coverage_years), MAX(coverage_years), AVG(coverage_years)
FROM layer_timeseries_features
GROUP BY layer_key;
"

# Check classification thresholds
python -c "from config.settings import settings; print(f'Improving: {settings.threshold_improving_high}')"

# Verify export
python -c "
import json
with open('exports/md_counties_latest.geojson') as f:
    data = json.load(f)
    print(f'Features: {len(data[\"features\"])}')
    print(f'First county: {data[\"features\"][0][\"properties\"][\"county_name\"]}')
"
```

---

## Recovery Procedures

### Reset and Rebuild

```bash
# 1. Clear existing data
psql $DATABASE_URL -c "TRUNCATE layer_timeseries_features, layer_summary_scores, final_synthesis_current;"

# 2. Re-run ingestion
make ingest-all

# 3. Re-run pipeline
make pipeline

# 4. Verify
curl http://localhost:8000/health
```

### Partial Recovery (Single Layer)

```bash
# Re-ingest specific layer
make ingest-layer1

# Re-run pipeline (will use existing data for other layers)
make pipeline
```

### Export Only Recovery

```bash
# If classifications exist but GeoJSON is missing
make export

# Verify
ls -la exports/md_counties_*.geojson
```

---

## Monitoring Checklist

### Daily Checks

- [ ] API health endpoint returns 200
- [ ] GeoJSON export file exists and is recent
- [ ] No error logs in `logs/api.log`

### Weekly Checks

- [ ] Database connection stable
- [ ] All 24 counties present in classifications
- [ ] Frontend loads without errors

### Monthly Checks

- [ ] Data refresh completed successfully
- [ ] Classification distribution looks reasonable
- [ ] Coverage years increasing for multi-year features

### Quarterly Checks

- [ ] Layer 1 (Employment) refreshed with latest BLS data
- [ ] Policy persistence data updated
- [ ] Documentation still accurate

---

## Contact & Escalation

### Log Locations

| Log | Location | Purpose |
|-----|----------|---------|
| API | `logs/api.log` | Request handling |
| Pipeline | `pipeline_output.log` | Pipeline execution |
| Database | PostgreSQL logs | Query performance |

### Support Resources

- **Documentation:** `docs/` folder
- **Methodology:** `docs/METHODOLOGY.md`
- **Architecture:** `docs/ARCHITECTURE.md`
- **API Docs:** http://localhost:8000/docs

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0 | 2026-01-30 | V2 multi-year pipeline, synthesis groupings |
| 1.1 | 2026-01-28 | Added synthesis grouping classification |
| 1.0 | 2026-01-15 | Initial release with Layer 1 |

---

**Status:** Operational
**Environment:** Development / Production
**Last Verified:** 2026-01-30
