# Maryland Atlas Frontend

Interactive map client for the Maryland Growth & Family Viability Atlas.

## What Changed

- Single bivariate signal map (no layer switcher, no dark mode toggle)
- Always-visible interactive 3x3 legend (Trajectory x Signal Strength)
- Legend click filtering that dims non-matching counties
- Faster county story mode with optimistic render + cached API detail fetch
- Compare mode with side-by-side county cards and layer score diff table
- Floating `Ask Atlas` pill under map controls with sidebar chat mode
- Smaller Mapbox controls and compact attribution control
- Resizable desktop sidebar (width persisted locally)

## Run Locally

### 1) Start API

From repo root:

```bash
make serve
```

This now uses the local virtual environment Python (`.venv/bin/python -m uvicorn ...`).

### 2) Start Frontend

```bash
make frontend
```

Frontend URL: `http://localhost:3000`

The frontend server now exposes `runtime-config.js`, which injects:

- `MAPBOX_ACCESS_TOKEN` from repo-root `.env`
- optional `ATLAS_API_BASE_URL` from repo-root `.env`

So no Mapbox token should be hardcoded in `frontend/map.js`.

API URL expected by frontend:

- default: `http://<current-host>:8000/api/v1`
- override: set `window.ATLAS_API_BASE_URL` before loading `map.js`

### Live Data Source Model

- County layer source is **API-first only**: `GET /api/v1/layers/counties/latest`
- No static `frontend/md_counties_latest.geojson` fallback is required.
- Frontend retries county-feed requests with backoff and auto-recovers when API becomes reachable.
- County detail source is `GET /api/v1/areas/{geoid}` and now supports progressive Databricks-backed fallback when `final_synthesis_current` is not yet populated.

## UI Behavior

### Bivariate Coloring

Counties are colored from a fixed 3x3 matrix:

- X axis: trajectory (`at_risk`, `stable`, `improving`)
- Y axis: signal strength (`high`, `mid`, `low`)

Strength is derived from county `composite_score` terciles computed from loaded GeoJSON.

### Legend Interaction

- Click any legend cell to filter to that cell
- Click same cell again to clear
- Footer displays active filter + county count
- Hover legend cells to preview signal label text

### Story Mode

Selecting a county opens narrative sections:

- Trajectory Snapshot
- Pressure Points
- What to Watch Next

Story mode uses existing backend fields only:
`directional_class`, `composite_score`, `primary_strengths`, `primary_weaknesses`, `key_trends`, `data_year`.

### Compare Mode

Flow:

1. Select County A
2. Click `Compare`
3. Click County B

Panel shows county summaries and score diffs for:

- Employment Gravity
- Mobility Optionality
- School Trajectory
- Housing Elasticity
- Demographic Momentum
- Risk Drag

### Ask Atlas Chat

- Click `Ask Atlas...` pill below map controls
- Input expands and auto-focuses
- Submit message to switch sidebar into chat mode
- Multi-turn history + `previous_response_id` is maintained
- Close chat (or click outside sidebar) to return to prior story/compare state

## Backend Dependency

Chat relies on `POST /api/v1/chat`.

Request payload:

```json
{
  "message": "string",
  "context": {},
  "history": [{"role": "user", "content": "..."}],
  "previous_response_id": "resp_..."
}
```

Response payload:

```json
{
  "response": "string",
  "response_id": "resp_...",
  "model": "gpt-5.1-mini",
  "tokens": {"input": 0, "output": 0, "total": 0},
  "cost": 0.0
}
```

## Troubleshooting

### County detail fails to load

Verify API is running and reachable:

```bash
curl -I http://localhost:8000/health
curl -I http://localhost:8000/api/v1/layers/counties/latest
curl -I http://localhost:8000/api/v1/areas/24031
```

### Chat fails with OpenAI errors

- Ensure `OPENAI_API_KEY` is set
- Ensure `AI_ENABLED=true`
- Reinstall deps after update:

```bash
make install
```

### Port 8000 already in use

Find and stop stale process:

```bash
lsof -i :8000
kill <pid>
```
