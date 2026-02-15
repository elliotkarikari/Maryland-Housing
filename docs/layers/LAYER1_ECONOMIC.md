# Layer 1 v2 Economic Accessibility - Quick Start Guide

## Overview

Layer 1 was completely overhauled in January 2026 to use **wage-segmented accessibility metrics** instead of simple job counts. The new approach measures actual ability to reach high-wage jobs using LODES employment data and proximity-based routing.

## What's New

### Key Improvements
- **High-wage job accessibility**: Measures jobs >$40k/year you can reach, not just total employment
- **Wage quality analysis**: Ratio of high-wage to low-wage accessible jobs
- **Tract-level resolution**: Computes at census tract level, aggregates to county
- **Population weighting**: County scores weighted by where people actually live
- **Composite opportunity index**: Combines local strength (v1) with regional access (v2)

### Metrics Produced
| Metric | Description |
|--------|-------------|
| `high_wage_jobs_accessible_45min` | High-wage jobs (>$40k) reachable within 45 min |
| `economic_accessibility_score` | Percentile rank of high-wage accessibility |
| `wage_quality_ratio` | Ratio of high-wage to low-wage accessible jobs |
| `economic_opportunity_index` | Composite: 40% local + 60% accessibility |

## Usage

### Basic Usage (No Special Dependencies)

```bash
# Run for latest available year (single year)
python src/ingest/layer1_economic_accessibility.py

# Run for specific year
python src/ingest/layer1_economic_accessibility.py --year 2021

# Run multi-year window (default: 5 years)
python src/ingest/layer1_economic_accessibility.py --year 2021
```

**Note**: Script automatically uses LODES data from 2 years prior (e.g., 2021 analysis uses 2019 LODES).

### Multi-Year Ingestion

```bash
# Ingest 5-year window ending in 2021 (years 2017-2021)
python src/ingest/layer1_economic_accessibility.py --year 2021

# Single year only
python src/ingest/layer1_economic_accessibility.py --year 2021 --single-year
```

### Dry Run (Test Without Storing)

```bash
# Test computation without database writes
python src/ingest/layer1_economic_accessibility.py --year 2021 --dry-run --single-year
```

## Data Sources

### Automatically Downloaded
- **LODES WAC**: Workplace employment with wage segments (SE01/SE02/SE03)
- **Census Tracts**: 2020 tract boundaries and population
- **ACS**: Tract-level demographics (population, working age, labor force)

### Wage Segments (LODES)
| Segment | Column | Definition |
|---------|--------|------------|
| Low Wage | SE01 | Jobs earning < $15,000/year |
| Mid Wage | SE02 | Jobs earning $15,000 - $40,000/year |
| High Wage | SE03 | Jobs earning > $40,000/year |

### Cache Location
All downloaded data is cached in `data/cache/economic_v2/`:
- LODES: `lodes/md_wac_segments_{year}.csv`
- Tracts: `md_tract_centroids_{year}.csv`
- ACS: `acs/md_acs_demo_{year}.csv`

**Caches never expire** - LODES/ACS are stable archives.

## Performance Notes

### Processing Time
- **~10-15 minutes** for all 1,461 tracts (single year)
- **~45-60 minutes** for 5-year window (2017-2021)

### Method
- Proximity-based gravity model using haversine distance
- Distance decay function weights jobs by inverse-square distance
- No Java required (pure Python with pandas/numpy)

### Distance Approximations
- 30 min commute ≈ 20 km (12.4 mi)
- 45 min commute ≈ 35 km (21.7 mi)

Assumes blended car/transit mix (~70% drive, ~30% transit).

## Troubleshooting

### Error: "No module named 'pygris'"
**Solution**: Install census data dependencies:
```bash
pip install pygris census
```

### Error: "LODES download failed"
**Solution**: Check internet connection and LEHD server status:
```bash
# Test LODES availability
curl -I https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_S000_JT00_2019.csv.gz
```

### Script is Slow
**Solutions**:
1. Run with `--dry-run` first to test without database writes
2. Use `--single-year` to process only one year
3. Check `data/cache/economic_v2/` - if caches exist, subsequent runs are faster

### "Employment diversification score is NULL"
**Explanation**: Requires existing v1 data for composite score.
- First run creates accessibility metrics only
- Second run (after v1 data exists) creates composite index
- Or run full pipeline: `src/run_pipeline.py`

## Output

### Tract Table: `layer1_economic_opportunity_tract`
- **Records**: 1,461 (all Maryland census tracts per year)
- **Columns**: 20+ metrics per tract
- **Uses**: Detailed spatial analysis, identifying job access deserts

### County Table: `layer1_employment_gravity`
- **Records**: 24 (Maryland counties per year)
- **Columns**: Population-weighted aggregates + composite index
- **Uses**: Integration with multi-year pipeline, county-level scoring

## Integration with Existing Pipeline

The new `economic_opportunity_index` becomes the primary Layer 1 metric for timeseries analysis.

**Formula**:
```
economic_opportunity_index =
    0.40 × employment_diversification_score (v1 local strength) +
    0.60 × economic_accessibility_score (v2 regional access)
```

**Existing pipeline works as-is** - no changes needed to:
- `src/processing/timeseries_features.py` (uses `economic_opportunity_index`)
- `src/processing/multiyear_scoring.py`
- `src/processing/multiyear_classification.py`

## Example Workflow

```bash
# 1. Run Layer 1 accessibility analysis (5-year window)
python src/ingest/layer1_economic_accessibility.py --year 2021

# 2. Run multi-year pipeline to update classifications
python src/run_multiyear_pipeline.py --year 2021

# 3. Export updated GeoJSON
python -c "
from src.export.geojson_export import run_geojson_export
run_geojson_export()
"

# 4. Restart API to serve new data
# (API auto-loads from database)
```

## Advanced: Historical Multi-Year Analysis

To build true multi-year timeseries for Layer 1:

```bash
# Ingest multiple years sequentially
for year in 2017 2018 2019 2020 2021; do
    python src/ingest/layer1_economic_accessibility.py --year $year --single-year
done
```

This enables true economic momentum and stability analysis across the full 5-year window.

## Comparison: v1 vs v2

### Version 1 (Deprecated)
- **Method**: Count total jobs in county
- **Metric**: `employment_diversification_score` (sector entropy)
- **Problem**: Doesn't measure accessibility or wage quality

### Version 2 (Current)
- **Method**: Measure high-wage jobs accessible via proximity model
- **Metric**: `economic_accessibility_score` + `wage_quality_ratio`
- **Advantage**: Policy-relevant, captures regional labor market access

### Why v2 is Better
A county with 10,000 local jobs but no access to regional high-wage employment has worse opportunity than a county with 5,000 local jobs but access to 500,000 high-wage jobs within 45 minutes.

**v2 measures what matters for family viability: access to good jobs.**

## Top Counties by Economic Opportunity

Based on 2021 analysis (LODES 2019 data):

| County | Opportunity Index | High-Wage Accessible | Local Strength |
|--------|-------------------|----------------------|----------------|
| Howard (24027) | 0.949 | 984,588 jobs | High diversity |
| Baltimore City (24510) | 0.727 | 701,523 jobs | Urban core |
| Anne Arundel (24003) | 0.701 | 949,873 jobs | Regional hub |
| Baltimore County (24005) | 0.605 | 764,100 jobs | Suburban |
| Prince George's (24033) | 0.575 | 939,448 jobs | DC access |

## Questions?

See `docs/architecture/ANALYSIS_METHODS.md` Section 9 (Layer 1) for detailed methodology.

For issues or questions:
- Check cache directory: `data/cache/economic_v2/`
- Review logs for specific error messages
- Verify LODES data availability at https://lehd.ces.census.gov/

## Key Differences from Layer 2

| Aspect | Layer 1 (Economic) | Layer 2 (Mobility) |
|--------|-------------------|-------------------|
| Focus | Jobs accessible | Transit performance |
| Primary metric | High-wage job count | Multimodal accessibility |
| Data source | LODES employment | OSM + GTFS |
| Routing | Proximity model | R5 engine (optional) |
| Distance basis | Haversine (km) | Travel time (min) |
| Java required | No | Optional (fallback available) |

Both use tract-level computation with county aggregation for consistency.
