# API Reference

Canonical API implementation lives in `src/api/`:

- App entrypoint: `src/api/main.py`
- Route modules: `src/api/routes.py`, `src/api/chat_routes.py`
- Startup command: `python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload`

## Core Endpoints

- `GET /` - API metadata and endpoint list
- `GET /health` - service health (database + export availability)
- `GET /api/v1/layers/counties/latest` - latest county GeoJSON
- `GET /api/v1/layers/counties/{version}` - versioned county GeoJSON
- `GET /api/v1/areas/{geoid}` - county-level synthesis details
- `GET /api/v1/areas/{geoid}/layers/{layer_key}` - layer breakdown for a county
- `GET /api/v1/metadata/sources` - data source registry metadata
- `GET /api/v1/metadata/capabilities` - runtime capability flags
- `GET /api/v1/metadata/refresh` - latest refresh records
- `POST /api/v1/chat` - Ask Atlas AI (gated by runtime capability)

## Local Run

```bash
make serve
```

Or:

```bash
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

## Notes

- The legacy root-level `api/` directory has been removed as part of file-structure cleanup.
- Use `docs/ARCHITECTURE.md` for system architecture and `QUICKSTART.md` for runbook commands.
