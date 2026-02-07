# Maryland Viability Atlas - Interactive Map Frontend

**Status:** ✅ Production-Ready
**Framework:** Vanilla JavaScript + Mapbox GL JS
**Primary Layer:** `synthesis_grouping`

---

## Overview

Interactive web map visualizing the Maryland Growth & Family Viability Atlas. The map displays 24 Maryland counties colored by their **Final Synthesis Grouping** - the primary assessment of structural trajectory and confidence.

---

## Features

### Primary Map Layer (Default)
- **Synthesis Grouping** - 5-category color-coded classification
  - 🟢 Emerging Tailwinds
  - 🟢 Conditional Growth
  - 🟡 Stable but Constrained
  - 🔴 At Risk / Headwinds
  - ⚪ High Uncertainty (current V2.0 state)

### Interactive Elements
- **Hover** - Tooltip showing county name and grouping
- **Click** - Side panel with detailed breakdown:
  - Synthesis grouping explanation
  - Classification details (directional, confidence)
  - Layer scores breakdown
  - Primary strengths and weaknesses
  - Key trends

### Layer Toggle
- Switch between visualization modes:
  - **Synthesis Grouping** (default)
  - **Directional Status** (improving/stable/at_risk)
  - **Confidence Level** (strong/conditional/fragile)

### Sprint 3 UX Enhancements
- **Basemap Mode Toggle** - Switch between `Standard` and `Satellite` basemaps
- **Performance Guardrails** - Reduced motion + lighter animations on lower-power devices

---

## Quick Start

### 1. Start the API Server

```bash
cd "/Users/elliotkarikari/Dev Projects/Maryland Housing"
source .venv/bin/activate
uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

API will be available at: `http://localhost:8000`

### 2. Start the Frontend Server

```bash
cd frontend
python3 serve.py
```

Frontend will be available at: `http://localhost:3000`

### 3. Open in Browser

Navigate to: **http://localhost:3000**

---

## File Structure

```
frontend/
├── index.html          # Main HTML structure
├── map.js              # Mapbox GL JS logic and interactivity
├── serve.py            # Local development server
└── README.md           # This file
```

---

## Data Flow

```
GeoJSON Endpoint (API)
    ↓
Frontend loads and displays on map
    ↓
User clicks county
    ↓
API call to /api/v1/areas/{fips_code}
    ↓
Side panel displays detailed data
```

---

## Color Scheme

### Synthesis Grouping (Primary)
- **Emerging Tailwinds:** `#2d5016` (Deep Green)
- **Conditional Growth:** `#7cb342` (Light Green)
- **Stable Constrained:** `#fdd835` (Yellow/Amber)
- **At Risk / Headwinds:** `#f4511e` (Orange/Red)
- **High Uncertainty:** `#757575` (Gray)

### Directional Status
- **Improving:** `#4caf50` (Green)
- **Stable:** `#ffc107` (Yellow)
- **At Risk:** `#f44336` (Red)

### Confidence Level
- **Strong:** `#1976d2` (Blue)
- **Conditional:** `#ff9800` (Orange)
- **Fragile:** `#e53935` (Red)

---

## Current State (V2.0)

If multi-year coverage is sparse, counties will show **\"High Uncertainty\"** (gray).

**Why?**
- V2 requires multi-year evidence for momentum and stability signals
- Missing years reduce confidence instead of being interpolated
- This is **intentional and honest** - the system reflects evidence quality

**When multi-year coverage improves:**
- Counties distribute across all 5 groupings
- Momentum and stability signal becomes active
- More nuanced territorial assessments

---

## API Integration

### Endpoints Used

**GeoJSON Source:**
```
../exports/md_counties_latest.geojson
```

**County Detail API:**
```
GET http://localhost:8000/api/v1/areas/{fips_code}
```

**Response Structure:**
```json
{
  "fips_code": "24031",
  "county_name": "Montgomery County",
  "synthesis_grouping": "high_uncertainty",
  "directional_class": "stable",
  "confidence_class": "conditional",
  "composite_score": 0.7917,
  "layer_scores": {...},
  "primary_strengths": [...],
  "primary_weaknesses": [...],
  "key_trends": [...]
}
```

---

## Customization

### Change Map Style

Edit `map.js` line 58:
```javascript
style: 'mapbox://styles/mapbox/standard'
```

Options:
- `standard` - Standard (current, monochrome theme)
- `bright-v8` - Bright
- `streets-v8` - Streets (classic palette)
- `streets-v11` - Streets (updated)
- `light-v11` - Light
- `dark-v11` - Dark
- `outdoors-v12` - Outdoors
- `satellite-v9` - Satellite

### Change Initial View

Edit `map.js` lines 59-61:
```javascript
center: [-77.0, 39.0],  // [longitude, latitude]
zoom: 7,                 // Zoom level (6-12 recommended)
```

### Modify Colors

Edit color constants in `map.js` lines 11-38.

---

## Browser Requirements

- Modern browser with ES6+ support
- Tested on:
  - Chrome 90+
  - Firefox 88+
  - Safari 14+
  - Edge 90+

---

## Development

### No Build Step Required
- Pure HTML/CSS/JavaScript
- No bundler, no dependencies
- Mapbox GL JS loaded via CDN

### Hot Reload
- Server watches for file changes
- Refresh browser to see updates

### Debugging

Open browser console (F12) to see:
- Network requests
- Error messages
- Map data loading status

---

## Deployment

### Option 1: Static Hosting (Netlify, Vercel)

1. Build command: (none required)
2. Publish directory: `frontend/`
3. Environment variables:
   - `API_BASE_URL` (if API hosted elsewhere)

### Option 2: S3 + CloudFront

```bash
aws s3 sync frontend/ s3://maryland-atlas-frontend --exclude "*.py"
```

### Option 3: GitHub Pages

1. Push frontend directory to `gh-pages` branch
2. Enable GitHub Pages in repo settings
3. Update `map.js` line 6 to point to production API

---

## Performance

- **GeoJSON File Size:** 373KB
- **Initial Load Time:** < 2 seconds on 4G
- **Time to Interactive:** < 3 seconds
- **API Response Time:** < 200ms (local)

**Optimizations:**
- GeoJSON is pre-simplified (not real-time)
- Mapbox vector tiles for base map
- Minimal JavaScript dependencies

---

## Accessibility

- ✅ Keyboard navigation (ESC to close panel)
- ✅ Semantic HTML
- ✅ ARIA labels on interactive elements
- ✅ High contrast color schemes
- ⚠️ Screen reader support (partial - map is visual by nature)

---

## Known Limitations

### V2.0
- Mobile layout not optimized (desktop-first)
- No deep linking to specific counties
- No search/filter functionality
- No data export from UI

### Future Enhancements (V2.1+)
- Mobile-responsive layout
- URL-based county selection (`?county=24031`)
- Search bar for county lookup
- CSV/PDF export of county reports
- Historical time series view
- Claims overlay (when claims system implemented)

---

## Troubleshooting

### Map doesn't load
- Check browser console for errors
- Verify API server is running on port 8000
- Verify `http://localhost:8000/api/v1/layers/counties/latest` responds

### Counties show as gray squares
- GeoJSON may not have loaded properly
- Check Network tab in browser DevTools
- Verify file path is correct

### Side panel doesn't open on click
- Check API server is accessible
- Look for CORS errors in console
- Verify county FIPS code exists in database

### Colors are wrong
- Check `synthesis_grouping` values in GeoJSON
- Verify color mappings in `map.js` match database values
- Ensure latest export was generated

---

## Sprint 1 UI Smoke Test (Controls + Search Visibility)

Run this quick matrix after UI/layout changes to confirm core map controls remain visible.

| Scenario | Expected Result |
|---|---|
| Desktop, map mode, panel closed | Zoom/geolocate/fullscreen controls visible at bottom-right; county search visible in header |
| Desktop, map mode, panel open | Controls shift left and remain visible/clickable; county search remains visible |
| Mobile (`<=768px`), map mode, panel closed | Controls visible above bottom layer control bar; county search visible in header |
| Mobile (`<=768px`), map mode, panel open | Controls remain visible/clickable (not under panel or bottom bar) |
| Table mode | Header, legend, layer controls, and map controls hidden intentionally |
| Return from table mode to map mode | Controls and county search reappear immediately |

Manual checks:
- Test `+/-` zoom, geolocate, and fullscreen interactions.
- Type in county search and verify autocomplete list appears.
- Open and close county side panel and confirm controls remain accessible.

---

## Support

**Project Repository:** Maryland Viability Atlas
**Documentation:** See `/docs` directory
**API Docs:** http://localhost:8000/docs

---

**Status:** ✅ Ready for Production
**Last Updated:** 2026-01-28
**Primary Developer:** Maryland Atlas Team
