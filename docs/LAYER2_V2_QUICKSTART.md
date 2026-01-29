# Layer 2 v2 Accessibility - Quick Start Guide

## Overview

Layer 2 was completely overhauled in January 2026 to use **accessibility-based metrics** instead of simple infrastructure counts. The new approach measures actual ability to reach jobs and opportunities using travel time routing.

## What's New

### Key Improvements
- **Jobs-based accessibility**: Measures how many jobs you can reach, not just infrastructure presence
- **Multimodal analysis**: Transit, walk, bike, and car accessibility
- **Tract-level resolution**: Computes at census tract level, aggregates to county
- **Population weighting**: County scores weighted by where people actually live
- **Transit competitiveness**: Ratio of transit vs car accessibility

### Metrics Produced
| Metric | Description |
|--------|-------------|
| `jobs_accessible_transit_45min` | Jobs reachable by transit in 45 min |
| `multimodal_accessibility_score` | Weighted composite (0.6×transit + 0.25×walk + 0.15×bike) |
| `transit_car_accessibility_ratio` | Transit competitiveness vs driving |
| `transit_stop_density` | Stops per square mile |

## Usage

### Basic Usage (Fallback Mode - No Java Required)

```bash
# Use fallback proximity model (no R5/Java needed)
python src/ingest/layer2_accessibility.py --year 2026 --no-r5
```

**Note**: Fallback mode is slower (~10-20 minutes for all tracts) but requires no additional dependencies.

### Advanced Usage (R5 Routing - Requires Java 11+)

```bash
# Install Java 11 or higher first
brew install openjdk@11  # macOS
# or download from https://adoptium.net/

# Then run with R5 (much faster, more accurate)
python src/ingest/layer2_accessibility.py --year 2026
```

**Note**: R5 mode is faster (~5 minutes) and more accurate but requires Java.

### Dry Run (Test Without Storing)

```bash
# Test computation without database writes
python src/ingest/layer2_accessibility.py --year 2026 --no-r5 --dry-run
```

## Data Sources

### Automatically Downloaded
- **OSM**: Maryland extract from Geofabrik (~50 MB)
- **GTFS**: MTA Maryland feeds (local bus, light rail, metro, MARC, commuter bus)
- **LODES**: 2021 workplace job counts by census tract (~5 MB)
- **Census**: Tract boundaries and population

### Cache Location
All downloaded data is cached in `data/cache/mobility_v2/`:
- OSM: `osm/maryland-latest.osm.pbf`
- GTFS: `gtfs/*.zip`
- LODES: `lodes/md_wac_2021.csv`
- Tracts: `md_tracts_2020.geojson`

**Caches expire**: OSM (30 days), GTFS (7 days), other (never)

## Performance Notes

### Fallback Mode (--no-r5)
- **Time**: ~15-25 minutes for all 1,465 tracts
- **Method**: Straight-line distance + gravity model
- **Accuracy**: Estimates only, but captures basic accessibility patterns
- **Requirements**: Python packages only (no Java)

### R5 Mode (default)
- **Time**: ~5-8 minutes for all tracts
- **Method**: Actual routing on street/transit networks
- **Accuracy**: Realistic travel times
- **Requirements**: Java 11+ installed

## Troubleshooting

### Error: "Unable to locate a Java Runtime"
**Solution**: Use fallback mode with `--no-r5` flag, or install Java:
```bash
brew install openjdk@11  # macOS
sudo apt install openjdk-11-jdk  # Ubuntu/Debian
```

### Error: "WMATA GTFS 404"
**Solution**: This is expected - WMATA feed requires API key. The script continues with MTA feeds only.

### Script is Slow
**Solutions**:
1. Use R5 mode (requires Java) - much faster
2. Run with `--dry-run` first to test without database writes
3. Check `data/cache/mobility_v2/` - if caches exist, subsequent runs are faster

### "Cached as CSV (parquet not available)"
**Explanation**: Optional optimization. Install pyarrow for faster caching:
```bash
pip install pyarrow
```

## Output

### Tract Table: `layer2_mobility_accessibility_tract`
- **Records**: 1,465 (all Maryland census tracts)
- **Columns**: 25+ metrics per tract
- **Uses**: Detailed spatial analysis, identifying transit deserts

### County Table: `layer2_mobility_optionality`
- **Records**: 24 (Maryland counties)
- **Columns**: Population-weighted aggregates
- **Uses**: Integration with multi-year pipeline, county-level scoring

## Integration with Existing Pipeline

The new `multimodal_accessibility_score` becomes the `mobility_optionality_index` used by the timeseries pipeline.

**Existing pipeline works as-is** - no changes needed to:
- `src/processing/timeseries_features.py`
- `src/processing/multiyear_scoring.py`
- `src/processing/multiyear_classification.py`

## Example Workflow

```bash
# 1. Run Layer 2 accessibility analysis (fallback mode)
python src/ingest/layer2_accessibility.py --year 2026 --no-r5

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

To build true multi-year timeseries for Layer 2, you would need:

1. **Archived GTFS feeds** from Mobility Database or transit agencies
2. **Historical LODES data** for different years (2017-2021 available)
3. **Run analysis per year**:
   ```bash
   for year in 2019 2020 2021 2022 2023; do
       python src/ingest/layer2_accessibility.py --year $year --no-r5
   done
   ```

This would enable true mobility momentum and stability analysis.

## Questions?

See `docs/ANALYSIS_METHODS.md` Section 9 (Layer 2) for detailed methodology.

For issues or questions, check:
- https://github.com/r5py/r5py (R5 routing engine)
- https://github.com/OneBusAway/onebusaway-gtfs-modules (GTFS tools)
