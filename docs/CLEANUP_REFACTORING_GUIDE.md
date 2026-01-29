# Maryland Housing Atlas - Cleanup & Refactoring Guide

**Date**: January 2026
**Status**: Post Layer 1/2 v2 Overhaul

## Executive Summary

After implementing Layer 1 v2 (Economic Accessibility) and Layer 2 v2 (Mobility Accessibility), the codebase has accumulated redundant files, deprecated code, and untracked cache data. This document identifies what should be cleaned up, consolidated, or deleted.

**Current Cache Size**: ~280 MB (not in git, but should be explicitly ignored)
**Redundant Code**: ~2 deprecated ingestion scripts
**Migration Files**: 3 active, 0 deprecated

---

## 1. CRITICAL: Git Ignore Updates

### Problem
The `data/cache/` directory (280 MB) is NOT in `.gitignore` and could accidentally be committed.

### Action Required
Add to `.gitignore`:
```gitignore
# Data cache (auto-downloaded, never commit)
data/cache/
data/cache/**/*

# Scratchpad and temp data
data/temp/
data/raw/
scratchpad/
```

### Verification
```bash
# Check what's currently tracked
git ls-files data/cache/

# If anything shows up, untrack it:
git rm -r --cached data/cache/
git commit -m "Remove cache from version control"
```

---

## 2. Redundant Ingestion Scripts

### Deprecated Files (Safe to Archive/Delete)

#### `src/ingest/layer1_employment.py` (v1 - DEPRECATED)
- **Status**: Replaced by `layer1_economic_accessibility.py` (v2)
- **Why deprecated**: Uses simple job counts instead of wage-segmented accessibility
- **Action**:
  - Option A: Delete entirely (recommended)
  - Option B: Move to `src/ingest/legacy/layer1_employment_v1.py` for reference
- **Impact**: None - no other code depends on it

#### `src/ingest/layer2_mobility.py` (v1 - DEPRECATED)
- **Status**: Replaced by `layer2_accessibility.py` (v2)
- **Why deprecated**: Uses infrastructure counts instead of travel time accessibility
- **Action**:
  - Option A: Delete entirely (recommended)
  - Option B: Move to `src/ingest/legacy/layer2_mobility_v1.py` for reference
- **Impact**: None - no other code depends on it

### Recommendation
**Delete both files.** The v2 implementations are superior and the v1 logic is documented in:
- Git history (always recoverable)
- ANALYSIS_METHODS.md "V1 Fields (retained for backwards compatibility)" section

```bash
# Delete deprecated scripts
rm src/ingest/layer1_employment.py
rm src/ingest/layer2_mobility.py

# Commit
git add -u
git commit -m "Remove deprecated Layer 1 and Layer 2 v1 ingestion scripts"
```

---

## 3. Cache Directory Bloat

### Current Cache Structure (280 MB)

| Directory | Size | Purpose | Keep? |
|-----------|------|---------|-------|
| `data/cache/economic_v2/` | 13 MB | Layer 1 v2 LODES/ACS | ✅ Keep (active) |
| `data/cache/mobility_v2/` | 209 MB | Layer 2 v2 OSM/GTFS | ✅ Keep (active) |
| `data/cache/demographics/` | 38 MB | Layer 5 IRS/ACS | ✅ Keep (active) |
| `data/cache/schools/` | 16 MB | Layer 3 NCES | ✅ Keep (active) |
| `data/cache/housing/` | 1.8 MB | Layer 4 ACS/BPS | ✅ Keep (active) |
| `data/cache/mobility/` | 1.5 MB | **Layer 2 v1 (OLD)** | ❌ DELETE |

### Action: Delete Old Cache
```bash
# Remove deprecated Layer 2 v1 cache
rm -rf data/cache/mobility/

# Verify gitignore prevents future tracking
echo "data/cache/" >> .gitignore
```

### Cache Expiration Policy
Current cache expiration logic (defined in scripts):
- **OSM data**: 30 days
- **GTFS feeds**: 7 days
- **LODES/ACS**: No expiration (stable archives)

**Recommendation**: Add cache cleanup script:
```bash
# Future: src/utils/clear_old_cache.py
# Automatically remove caches older than expiration dates
```

---

## 4. Migration Files Review

### Active Migrations (Keep All)

| File | Purpose | Status |
|------|---------|--------|
| `006_layer2_accessibility_overhaul.sql` | Layer 2 v2 schema | ✅ Active |
| `007_layer1_economic_accessibility_overhaul.sql` | Layer 1 v2 schema | ✅ Active |
| `008_layer1_economic_opportunity_index.sql` | Composite index | ✅ Active |

All migrations are **active and necessary**. Do not delete.

### Missing Migration
There are migrations 006-008, but no migrations 001-005. These were likely:
- Initial schema setup
- Pre-existing before the accessibility overhaul

**Action**: None needed. The existing migrations are sufficient for the v2 system.

---

## 5. Database Schema Consolidation Opportunities

### Current Tables

#### Layer 1 (Economic Opportunity)
- `layer1_employment_gravity` (county-level, contains both v1 and v2 fields)
- `layer1_economic_opportunity_tract` (tract-level v2 only)

**Opportunity**: The `layer1_employment_gravity` table has **both v1 and v2 columns**. Consider:
- Keeping v1 fields for historical comparison
- OR marking v1 fields as deprecated in comments

**Recommendation**: Keep both for now. Add database comments:
```sql
COMMENT ON COLUMN layer1_employment_gravity.total_jobs IS
'V1 field: Local job count. Deprecated in favor of accessibility metrics.';
```

#### Layer 2 (Mobility)
- `layer2_mobility_optionality` (county-level, contains both v1 and v2 fields)
- `layer2_mobility_accessibility_tract` (tract-level v2 only)

**Same as Layer 1** - keep both, add deprecation comments.

### Action
Create a migration `009_add_deprecation_comments.sql` to document which fields are v1 vs v2.

---

## 6. Code Quality Issues

### Long Functions
Several functions in `layer1_economic_accessibility.py` and `layer2_accessibility.py` exceed 100 lines:
- `compute_accessibility_fallback()` - 200+ lines
- `aggregate_to_county()` - 100+ lines

**Recommendation**:
- Leave as-is for now (they work correctly)
- Future refactoring: extract helper functions for distance computations

### Duplicate Logic
Both Layer 1 and Layer 2 have:
- LODES download functions
- Tract centroid fetching
- Population-weighted aggregation

**Recommendation**: Create shared utilities:
```python
# src/utils/spatial_utils.py
def fetch_tract_centroids(year: int) -> gpd.GeoDataFrame:
    """Shared tract centroid fetching for all layers."""

# src/utils/lodes_utils.py
def download_lodes_wac(year: int, segments: bool = False) -> pd.DataFrame:
    """Shared LODES download logic."""
```

**Priority**: Low (current duplication is manageable)

---

## 7. Documentation Consolidation

### Current Docs
- `docs/ANALYSIS_METHODS.md` - **Complete, up-to-date**
- `docs/LAYER2_V2_QUICKSTART.md` - **Complete**
- `README.md` - May need updates for Layer 1/2 v2

**Missing**:
- `docs/LAYER1_V2_QUICKSTART.md` - Should mirror Layer 2's quickstart format

### Action
Create Layer 1 quickstart guide:
```bash
# Copy template from Layer 2
cp docs/LAYER2_V2_QUICKSTART.md docs/LAYER1_V2_QUICKSTART.md
# Edit for Layer 1 specifics
```

---

## 8. Testing & Validation Files

### Current Test Coverage
No unit tests found in `src/`.

**Recommendation**:
- Priority: Low (scripts work in production)
- Future: Add integration tests for data pipelines
```python
# tests/test_layer1_ingestion.py
def test_lodes_download():
    """Verify LODES download returns expected structure."""

def test_accessibility_computation():
    """Verify accessibility scores are in 0-1 range."""
```

---

## 9. Performance Optimization Opportunities

### Current Bottlenecks

1. **Layer 1 accessibility computation** (~5-10 min for 1,461 tracts)
   - Uses nested loops for pairwise distance calculation
   - Could be vectorized with scipy.spatial.distance_matrix

2. **Layer 2 fallback mode** (~15-25 min for 1,461 tracts)
   - Proximity model is slower than R5 routing
   - Acceptable tradeoff for no-Java requirement

**Recommendation**:
- Optimize Layer 1 distance computation using vectorized haversine
- Estimated speedup: 2-3x faster

**Priority**: Medium (script runs overnight, not critical)

---

## 10. Configuration Management

### Current Approach
- Hard-coded constants in each script (e.g., `DIST_30MIN = 20`, `LOCAL_STRENGTH_WEIGHT = 0.4`)
- Settings in `config/settings.py` for database/API

**Recommendation**: Centralize all layer-specific config:
```python
# config/layer_settings.py
LAYER1_CONFIG = {
    'dist_30min_km': 20,
    'dist_45min_km': 35,
    'local_strength_weight': 0.4,
    'regional_access_weight': 0.6,
}

LAYER2_CONFIG = {
    'transit_weight': 0.6,
    'walk_weight': 0.25,
    'bike_weight': 0.15,
}
```

**Priority**: Low (current approach works fine)

---

## Action Plan Summary

### Immediate (Do Now)

1. ✅ **Update `.gitignore`** to exclude `data/cache/`
   ```bash
   echo "data/cache/" >> .gitignore
   git rm -r --cached data/cache/ (if tracked)
   ```

2. ✅ **Delete deprecated v1 scripts**
   ```bash
   rm src/ingest/layer1_employment.py
   rm src/ingest/layer2_mobility.py
   ```

3. ✅ **Delete old cache**
   ```bash
   rm -rf data/cache/mobility/
   ```

### Short-term (This Week)

4. 📝 **Create Layer 1 quickstart guide** (`docs/LAYER1_V2_QUICKSTART.md`)

5. 📝 **Add deprecation comments** to database schema (migration 009)

### Long-term (Future Sprints)

6. 🔧 **Extract shared utilities** (spatial_utils, lodes_utils)

7. 🧪 **Add integration tests** for data pipelines

8. ⚡ **Optimize Layer 1 distance computation** (vectorize haversine)

---

## Cleanup Script

For convenience, here's a single script to perform all immediate actions:

```bash
#!/bin/bash
# cleanup.sh - Maryland Housing Atlas Cleanup

set -e  # Exit on error

echo "=== Maryland Housing Atlas - Cleanup ==="

# 1. Update .gitignore
echo ""
echo "[1/3] Updating .gitignore..."
if ! grep -q "data/cache/" .gitignore; then
    echo "" >> .gitignore
    echo "# Data cache (auto-downloaded, never commit)" >> .gitignore
    echo "data/cache/" >> .gitignore
    echo "data/cache/**/*" >> .gitignore
    echo "✓ Added data/cache/ to .gitignore"
else
    echo "✓ data/cache/ already in .gitignore"
fi

# 2. Delete deprecated scripts
echo ""
echo "[2/3] Removing deprecated v1 ingestion scripts..."
if [ -f "src/ingest/layer1_employment.py" ]; then
    rm src/ingest/layer1_employment.py
    echo "✓ Deleted src/ingest/layer1_employment.py"
else
    echo "  (already deleted)"
fi

if [ -f "src/ingest/layer2_mobility.py" ]; then
    rm src/ingest/layer2_mobility.py
    echo "✓ Deleted src/ingest/layer2_mobility.py"
else
    echo "  (already deleted)"
fi

# 3. Delete old cache
echo ""
echo "[3/3] Removing deprecated cache directories..."
if [ -d "data/cache/mobility" ]; then
    rm -rf data/cache/mobility/
    echo "✓ Deleted data/cache/mobility/ (1.5 MB freed)"
else
    echo "  (already deleted)"
fi

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Review changes: git status"
echo "  2. Commit: git add -A && git commit -m 'Cleanup: Remove deprecated v1 scripts and old cache'"
echo "  3. Create Layer 1 quickstart guide (see docs/LAYER2_V2_QUICKSTART.md)"
echo ""
```

Save as `cleanup.sh`, make executable, and run:
```bash
chmod +x cleanup.sh
./cleanup.sh
```

---

## Space Savings

| Action | Space Freed |
|--------|-------------|
| Delete `layer1_employment.py` | ~30 KB |
| Delete `layer2_mobility.py` | ~45 KB |
| Delete `data/cache/mobility/` | 1.5 MB |
| **Total** | **~1.6 MB** |

*Note: Cache directories will regenerate as needed - this is expected.*

---

## Questions & Considerations

### Should we keep v1 scripts for comparison?
**No.** The v2 implementations are objectively better:
- Layer 1 v2: Measures accessibility to high-wage jobs (policy-relevant)
- Layer 2 v2: Measures actual travel times (not just infrastructure counts)

Git history preserves v1 code if ever needed for reference.

### Will deleting v1 scripts break anything?
**No.** The timeseries pipeline now uses v2 metrics:
- `timeseries_features.py` uses `economic_opportunity_index` (Layer 1 v2)
- `timeseries_features.py` uses `mobility_optionality_index` (Layer 2 v2)

No other scripts import or reference the v1 files.

### What about database backward compatibility?
**Maintained.** The v1 fields remain in the database tables:
- `layer1_employment_gravity` keeps `total_jobs`, `sector_diversity_entropy`, etc.
- `layer2_mobility_optionality` keeps `highway_miles_total`, `transit_stations_count`, etc.

These can be used for historical analysis even though v2 is the primary metric.

---

## Conclusion

The codebase is in good shape after the Layer 1/2 v2 overhaul. The main cleanup tasks are:
1. **Exclude cache from git** (critical)
2. **Delete deprecated v1 scripts** (low risk, high clarity)
3. **Remove old cache** (minor space savings)

All other "bloat" is actually useful:
- Documentation is comprehensive
- Database schema preserves v1 for comparison
- Cache is large but necessary for performance

**Total cleanup time**: ~10 minutes
**Risk level**: Very low (scripts are deprecated, cache regenerates)
**Impact**: Cleaner codebase, no accidental cache commits
