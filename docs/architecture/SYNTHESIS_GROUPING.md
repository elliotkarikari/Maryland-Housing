# Final Synthesis Grouping - Implementation Summary

**Date:** 2026-01-28  
**Status:** ✅ Implemented and Operational

---

## Overview

The Final Synthesis Grouping is the **PRIMARY MAP LAYER** for the Maryland Growth & Family Viability Atlas. It answers the key question:

> **"What is the structural trajectory of this place under current conditions — and how reliable is that assessment?"**

---

## The 5 Synthesis Groupings

### 1. Emerging Tailwinds

**Criteria:**
- Directional status: Improving
- Confidence level: Strong  
- Low to medium uncertainty

**Meaning:**
Stacked tailwinds are present. High likelihood of persistence if trends hold.

**Map Color (Suggested):** Deep Green (#2d5016)

---

### 2. Conditional Growth

**Criteria:**
- Directional status: Improving
- Confidence level: Conditional OR high uncertainty present

**Meaning:**
Upside exists, but execution and local context matter.

**Map Color (Suggested):** Light Green (#7cb342)

---

### 3. Stable but Constrained

**Criteria:**
- Directional status: Stable
- No severe risk drag present

**Meaning:**
Systems are holding, but limited upside potential.

**Map Color (Suggested):** Yellow/Amber (#fdd835)

---

### 4. At Risk / Headwinds

**Criteria:**
- Directional status: At Risk  
  OR
- Severe risk drag (≥0.5) + weak policy persistence (<0.3)

**Meaning:**
Structural headwinds dominate.

**Map Color (Suggested):** Orange/Red (#f4511e)

---

### 5. High Uncertainty / Contested

**Criteria:**
- Confidence level: Fragile
  OR
- Classification contested by claims
  OR
- Sparse data (< 2 layers available)

**Meaning:**
Model confidence is low. Ground truth unclear. Priority area for local knowledge.

**Map Color (Suggested):** Gray (#757575)

---

## Current System State (V1.0)

**All 24 Maryland counties currently show:** `high_uncertainty`

**Why?**
- Only Layer 1 (Employment Gravity) data is ingested
- This triggers the "sparse data" condition (< 2 layers)
- Correctly reflects limited analytical basis

**This is intentional** - the system is designed to be honest about uncertainty rather than overstate confidence with incomplete data.

---

## Implementation Details

### Database Schema

Added column to `county_classifications` table:
```sql
synthesis_grouping VARCHAR(30)
```

### Classification Logic

Location: `src/processing/classification.py`

Function: `calculate_final_synthesis_grouping()`

Takes precedence approach:
1. **First check:** High uncertainty conditions (fragile confidence, contested, sparse data)
2. **Then check:** At Risk / Headwinds (at_risk status or severe risk + weak policy)
3. **Then check:** Emerging Tailwinds (improving + strong confidence)
4. **Then check:** Conditional Growth (improving with caveats)
5. **Default:** Stable but Constrained

### API Exposure

**Endpoint:** `GET /api/v1/areas/{geoid}`

**Response includes:**
```json
{
  "synthesis_grouping": "high_uncertainty",
  "directional_class": "stable",
  "confidence_class": "conditional",
  ...
}
```

### API/GeoJSON Delivery

**Primary runtime source:** `GET /api/v1/layers/counties/latest` (live Databricks-backed feed)

**Optional snapshot artifact:** `exports/md_counties_latest.geojson`

**Each feature includes:**
```json
{
  "properties": {
    "synthesis_grouping": "high_uncertainty",
    "directional_class": "stable",
    "confidence_class": "conditional",
    ...
  }
}
```

---

## Map Integration Guidance

### Primary Layer (Default View)

**Field:** `synthesis_grouping`

**Rendering:**
- Use categorical styling (5 distinct colors)
- Make this the default visible layer on map load
- Provide toggle to show underlying layers

### Interaction Model

**On Hover:**
- Show county name + synthesis grouping label
- Example: "Montgomery County - High Uncertainty"

**On Click (Side Panel):**
- Final grouping explanation
- Directional status (improving/stable/at_risk)
- Confidence level (strong/conditional/fragile)
- Layer scores breakdown
- Primary strengths/weaknesses
- Key trends
- Active claims (when implemented)
- AI-assisted synthesis (when available)

### Legend

```
Synthesis Grouping Legend:

🟢 Emerging Tailwinds
   Stacked growth signals, high confidence

🟢 Conditional Growth
   Upside exists, execution matters

🟡 Stable but Constrained
   Holding steady, limited upside

🔴 At Risk / Headwinds
   Structural challenges dominate

⚪ High Uncertainty
   Insufficient data or contested
```

---

## Next Steps

### For V1.1+ (When More Layers Added)

When Layers 2-6 are ingested:
- Sparse data condition will no longer trigger for most areas
- Counties will distribute across all 5 groupings
- Map will show more nuanced spatial patterns

### For Claims System

When claims are implemented:
- `classification_contested` flag will be set based on claim analysis
- Some areas may move to "High Uncertainty" based on conflicting local knowledge

### For AI Integration

When AI synthesis is added:
- Side panel can include AI-generated explanations
- Must be clearly labeled and cached
- Cannot override grouping classification

---

## Acceptance Criteria Status

✅ **Users can understand area trajectory at a glance**
- Synthesis grouping provides clear, categorical assessment

✅ **Uncertainty is visible, not hidden**
- "High Uncertainty" is an explicit grouping
- Shown when confidence is low or data is sparse

✅ **Claims are discoverable spatially** (Pending implementation)
- Schema ready, API structure defined

✅ **AI adds clarity, not authority** (Partially implemented)
- AI subsystem has proper architecture
- Provenance tracking in place

✅ **The map alone tells a truthful story**
- Primary layer (synthesis_grouping) accurately reflects system state
- Currently shows "high_uncertainty" for all areas due to sparse data
- This is honest and correct

---

## Files Modified

| File | Changes |
|------|---------|
| `src/processing/classification.py` | Added `calculate_final_synthesis_grouping()` function |
| `src/processing/classification.py` | Updated `classify_all_counties()` to call synthesis function |
| `src/processing/classification.py` | Updated `store_classifications()` to save synthesis_grouping |
| `src/export/geojson_export.py` | Added synthesis_grouping to SELECT query |
| `src/api/routes.py` | Added synthesis_grouping to AreaDetail model |
| `src/api/routes.py` | Added synthesis_grouping to API query and response |
| Database schema | Added `synthesis_grouping VARCHAR(30)` column |

---

## Testing

```bash
# Check live county feed
curl http://localhost:8000/api/v1/layers/counties/latest | jq '.features | length'
# Expected: 24

# Check API
curl http://localhost:8000/api/v1/areas/24031 | jq '.synthesis_grouping, .directional_class, .confidence_class'

# Optional snapshot artifact check
python -m src.export.geojson_export --latest-only
python -c "import json; data=json.load(open('exports/md_counties_latest.geojson')); print(data['features'][0]['properties']['synthesis_grouping'])"
```

---

**Status:** ✅ Ready for Frontend Integration  
**Primary Map Layer:** synthesis_grouping  
**Default Display:** 5-category color scheme  
**Current Value:** All areas show "high_uncertainty" (correct for V1.0 sparse data)
