# Maryland Housing Atlas - Comprehensive Cleanup Plan

**Date**: January 30, 2026
**Author**: Senior Software Architect
**Status**: Proposed

---

## Executive Summary

The Maryland Growth & Family Viability Atlas has grown organically through rapid development. This cleanup plan consolidates scattered files, removes redundancy, and establishes a 12-Factor App-compliant structure for long-term maintainability.

**Current Issues Identified**:
- 6 shell scripts scattered at root level
- 2 duplicate API directories (`api/` and `src/api/`)
- 67+ cache files in uncommitted `cache/` directory
- 3 log files at root + `logs/` directory
- 2 requirements files with unclear purposes
- 21 markdown docs with overlapping content
- JVM crash logs (`hs_err_pid*.log`) at root

---

## 1. Proposed Directory Structure

```
maryland-housing/
├── .github/                          # NEW: GitHub workflows & templates
│   ├── workflows/
│   │   ├── ci.yml                    # Lint, test, type-check
│   │   └── deploy.yml                # Railway/production deploy
│   ├── ISSUE_TEMPLATE/
│   │   └── bug_report.md
│   └── PULL_REQUEST_TEMPLATE.md
│
├── config/                           # Application configuration
│   ├── __init__.py
│   ├── database.py
│   ├── settings.py
│   └── layer_settings.py             # NEW: Centralized layer constants
│
├── data/                             # Data directory (mostly gitignored)
│   ├── .gitkeep                      # Ensure directory exists
│   └── README.md                     # NEW: Data directory documentation
│
├── devtools/                         # Development utilities
│   └── agent_lightning/
│       ├── __init__.py
│       └── run_evals.py
│
├── docker/                           # Docker configurations
│   └── Dockerfile
│
├── docs/                             # Documentation (consolidated)
│   ├── api/
│   │   └── API_REFERENCE.md          # MERGE: api/README.md + API_IMPLEMENTATION.md
│   ├── architecture/
│   │   ├── ANALYSIS_METHODS.md       # Layer methodology
│   │   ├── DATA_SOURCES.md           # External data sources
│   │   └── SYNTHESIS_GROUPING.md     # Classification logic
│   ├── development/
│   │   ├── CONTRIBUTING.md           # NEW: Contribution guidelines
│   │   ├── DEPLOYMENT_GUIDE.md       # Deploy instructions
│   │   └── TESTING.md                # NEW: Test guidelines
│   ├── layers/
│   │   ├── LAYER1_ECONOMIC.md        # MERGE: LAYER1_V2_QUICKSTART
│   │   ├── LAYER2_MOBILITY.md        # MERGE: LAYER2_V2_QUICKSTART
│   │   ├── LAYER3_EDUCATION.md       # NEW: Layer 3 details
│   │   ├── LAYER4_HOUSING.md         # NEW: Layer 4 details
│   │   ├── LAYER5_DEMOGRAPHICS.md    # NEW: Layer 5 details
│   │   └── LAYER6_RISK.md            # NEW: Layer 6 details
│   └── CHANGELOG.md                  # NEW: Version history
│
├── exports/                          # GeoJSON output (versioned snapshots OK)
│   ├── .gitkeep
│   └── md_counties_latest.geojson    # GITIGNORED (generated)
│
├── frontend/                         # Static frontend
│   ├── index.html
│   ├── map.js
│   └── serve.py                      # Local dev server
│
├── logs/                             # All logs (gitignored)
│   └── .gitkeep
│
├── migrations/                       # SQL migrations
│   ├── 001_initial_schema.sql        # RENAME from existing
│   ├── ...
│   └── 012_layer6_risk_vulnerability_overhaul.sql
│
├── scripts/                          # CONSOLIDATED: All shell/CLI scripts
│   ├── cleanup.sh                    # MOVED from root
│   ├── init_db.py
│   ├── run_pipeline.sh               # NEW: Unified pipeline runner
│   ├── setup_database.sh             # MOVED from root
│   └── setup_env.sh                  # MOVED from root
│
├── src/                              # Application source
│   ├── __init__.py
│   ├── api/                          # FastAPI application
│   │   ├── __init__.py
│   │   ├── main.py                   # MERGE: api/main.py + src/api/main.py
│   │   └── routes.py
│   ├── export/
│   │   ├── __init__.py
│   │   └── geojson_export.py
│   ├── ingest/                       # Data ingestion layers
│   │   ├── __init__.py
│   │   ├── layer1_economic_accessibility.py
│   │   ├── layer2_accessibility.py
│   │   ├── layer3_education_accessibility.py
│   │   ├── layer3_schools.py
│   │   ├── layer4_housing.py
│   │   ├── layer4_housing_affordability.py
│   │   ├── layer5_demographic_equity.py
│   │   ├── layer5_demographics.py
│   │   ├── layer6_risk.py
│   │   ├── layer6_risk_vulnerability.py
│   │   └── policy_persistence.py
│   ├── processing/                   # Score computation
│   │   ├── __init__.py
│   │   ├── classification.py
│   │   ├── feature_registry.py
│   │   ├── multiyear_classification.py
│   │   ├── multiyear_scoring.py
│   │   ├── normalization.py
│   │   ├── scoring.py
│   │   └── timeseries_features.py
│   ├── run_multiyear_pipeline.py     # Pipeline orchestration
│   ├── run_pipeline.py
│   └── utils/
│       ├── __init__.py
│       ├── data_sources.py
│       ├── logging.py
│       └── spatial.py                # NEW: Shared spatial utilities
│
├── tests/                            # Test suite
│   ├── __init__.py
│   ├── conftest.py                   # NEW: pytest fixtures
│   ├── test_classification.py
│   ├── test_ingest/                  # NEW: Layer ingestion tests
│   │   └── test_layer1.py
│   └── test_api/                     # NEW: API endpoint tests
│       └── test_routes.py
│
├── .env.example                      # NEW: Environment template
├── .gitignore                        # REVISED (see section 4)
├── docker-compose.yml                # MERGE: existing compose files
├── LICENSE
├── Makefile                          # Build/run commands
├── Procfile                          # Railway deployment
├── pyproject.toml                    # NEW: Modern Python config
├── README.md                         # REVISED (see section 3)
├── QUICKSTART.md                     # REVISED (see section 3)
├── railway.json                      # Railway config
└── requirements.txt                  # CONSOLIDATED: Single requirements
```

---

## 2. Step-by-Step Actions

### Phase 1: Delete Obsolete Files (Low Risk)

| Action | File/Directory | Justification |
|--------|----------------|---------------|
| DELETE | `hs_err_pid50675.log` | JVM crash log, not project-related |
| DELETE | `hs_err_pid51139.log` | JVM crash log, not project-related |
| DELETE | `api.log` | Move to `logs/` via config |
| DELETE | `pipeline_output.log` | Empty file, unused |
| DELETE | `activate_correct_venv.sh` | Replaced by standard `.venv` activation |
| DELETE | `requirements_layer2_v2.txt` | Merge into main `requirements.txt` |
| DELETE | `cache/` directory | Add to gitignore, regenerates on demand |

```bash
# Execute Phase 1
rm -f hs_err_pid*.log
rm -f api.log pipeline_output.log
rm -f activate_correct_venv.sh
rm -f requirements_layer2_v2.txt
rm -rf cache/
```

### Phase 2: Consolidate API Directory (Medium Risk)

The project has two API directories:
- `api/` (root) - Contains `main.py`, `README.md`, `requirements.txt`, `start.sh`
- `src/api/` - Contains the actual routes and FastAPI app

**Action**: Merge into single `src/api/`

```bash
# 1. Check if api/main.py has unique code vs src/api/main.py
diff api/main.py src/api/main.py

# 2. If api/main.py has unique features, merge them into src/api/main.py
# 3. Move api/README.md to docs/api/API_REFERENCE.md
mkdir -p docs/api
mv api/README.md docs/api/API_REFERENCE.md

# 4. Delete redundant directory
rm -rf api/
```

### Phase 3: Move Scripts to `scripts/` (Low Risk)

```bash
# Move root-level scripts
mv setup_database.sh scripts/
mv setup_env.sh scripts/
mv cleanup.sh scripts/

# Update any references in Makefile or docs
```

### Phase 4: Consolidate Documentation (Medium Risk)

| Source File | Action | Target |
|-------------|--------|--------|
| `COMPLETE_SYSTEM_STATUS.md` | ARCHIVE | `docs/archive/` (historical snapshot) |
| `docs/PROJECT_STATUS.md` | DELETE | Redundant with COMPLETE_SYSTEM_STATUS |
| `docs/PROJECT_STATUS_SUMMARY.md` | DELETE | Redundant |
| `docs/BUGFIXES.md` | MERGE | `docs/CHANGELOG.md` |
| `docs/BUGS_FIXED_SUMMARY.md` | DELETE | Redundant with BUGFIXES |
| `docs/FINAL_IMPLEMENTATION_SUMMARY.md` | ARCHIVE | Historical |
| `docs/MULTIYEAR_IMPLEMENTATION.md` | ARCHIVE | Historical |
| `docs/LAYER1_V2_QUICKSTART.md` | RENAME | `docs/layers/LAYER1_ECONOMIC.md` |
| `docs/LAYER2_V2_QUICKSTART.md` | RENAME | `docs/layers/LAYER2_MOBILITY.md` |
| `docs/CLEANUP_REFACTORING_GUIDE.md` | ARCHIVE | Superseded by this plan |
| `docs/LIMITATIONS.md` | KEEP | Useful reference |
| `docs/METHODOLOGY.md` | MERGE | Into ANALYSIS_METHODS.md |
| `docs/SYNTHESIS_GROUPING_IMPLEMENTATION.md` | RENAME | `docs/architecture/SYNTHESIS_GROUPING.md` |
| `docs/API_IMPLEMENTATION.md` | MERGE | Into `docs/api/API_REFERENCE.md` |
| `docs/AGENT_LIGHTNING.md` | KEEP | Active devtool docs |

```bash
# Create new structure
mkdir -p docs/{api,architecture,development,layers,archive}

# Archive historical docs
mv COMPLETE_SYSTEM_STATUS.md docs/archive/
mv docs/PROJECT_STATUS.md docs/archive/
mv docs/PROJECT_STATUS_SUMMARY.md docs/archive/
mv docs/FINAL_IMPLEMENTATION_SUMMARY.md docs/archive/
mv docs/MULTIYEAR_IMPLEMENTATION.md docs/archive/
mv docs/CLEANUP_REFACTORING_GUIDE.md docs/archive/

# Reorganize
mv docs/LAYER1_V2_QUICKSTART.md docs/layers/LAYER1_ECONOMIC.md
mv docs/LAYER2_V2_QUICKSTART.md docs/layers/LAYER2_MOBILITY.md
mv docs/SYNTHESIS_GROUPING_IMPLEMENTATION.md docs/architecture/SYNTHESIS_GROUPING.md

# Delete truly redundant
rm docs/BUGS_FIXED_SUMMARY.md
```

### Phase 5: Create Missing Files

#### 5a. `.env.example`
```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/maryland_atlas
POSTGRES_USER=atlas_user
POSTGRES_PASSWORD=secure_password
POSTGRES_DB=maryland_atlas

# API Keys
MAPBOX_ACCESS_TOKEN=pk.your_token_here
CENSUS_API_KEY=your_census_key

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO
EXPORT_DIR=./exports
```

#### 5b. `docs/development/CONTRIBUTING.md`
```markdown
# Contributing to Maryland Housing Atlas

## Development Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and configure
3. Create virtual environment: `python -m venv .venv`
4. Activate: `source .venv/bin/activate`
5. Install dependencies: `pip install -r requirements.txt`
6. Initialize database: `make init-db`
7. Run API: `make run-api`

## Code Style

- Python: Follow PEP 8, use Black for formatting
- SQL: Use lowercase keywords, snake_case names
- JavaScript: Use Prettier, ES6+ syntax

## Pull Request Process

1. Create feature branch from `main`
2. Write tests for new functionality
3. Update documentation as needed
4. Run `make lint` and `make test`
5. Submit PR with clear description

## Layer Development

Each layer follows the pattern:
- `src/ingest/layerN_*.py` - Data ingestion
- `migrations/0XX_layerN_*.sql` - Schema
- `docs/layers/LAYERN_*.md` - Documentation
```

#### 5c. `pyproject.toml`
```toml
[project]
name = "maryland-housing-atlas"
version = "1.0.0"
description = "Spatial analysis of structural trajectories across Maryland counties"
requires-python = ">=3.12"

[tool.black]
line-length = 100
target-version = ['py310', 'py311']

[tool.isort]
profile = "black"
line_length = 100

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = "test_*.py"
addopts = "-v --tb=short"

[tool.mypy]
python_version = "3.12"
warn_return_any = true
warn_unused_ignores = true
```

---

## 3. README.md and QUICKSTART.md Updates

### README.md - Proposed Structure

```markdown
# Maryland Growth & Family Viability Atlas

Spatial analysis platform for evaluating growth trajectories and family viability
across Maryland's 24 counties using multi-year evidence synthesis.

## Quick Links

- [Quickstart Guide](QUICKSTART.md) - Get running in 5 minutes
- [API Documentation](docs/api/API_REFERENCE.md)
- [Layer Methodology](docs/architecture/ANALYSIS_METHODS.md)
- [Data Sources](docs/architecture/DATA_SOURCES.md)

## Features

- **6-Layer Analysis**: Employment, Mobility, Education, Housing, Demographics, Risk
- **Multi-Year Synthesis**: 3-year rolling averages with trend detection
- **Interactive Map**: Mapbox GL JS frontend with county drill-down
- **REST API**: FastAPI backend with Pydantic validation
- **GeoJSON Export**: Versioned spatial data outputs

## Architecture

```
[Census/LODES/OSM] → [Ingestion] → [PostgreSQL] → [Scoring] → [Classification]
                                                        ↓
[Frontend Map] ← [FastAPI] ← [GeoJSON Export] ← [Synthesis]
```

## Requirements

- Python 3.12+
- PostgreSQL 14+
- 4GB RAM minimum
- Mapbox API token (for frontend)

## Installation

See [QUICKSTART.md](QUICKSTART.md) for detailed setup instructions.

## Project Structure

```
├── src/              # Application source
│   ├── api/          # FastAPI endpoints
│   ├── ingest/       # Layer data ingestion
│   ├── processing/   # Scoring and classification
│   └── export/       # GeoJSON generation
├── frontend/         # Mapbox visualization
├── migrations/       # SQL schema migrations
├── config/           # Settings and database
├── docs/             # Documentation
└── tests/            # Test suite
```

## License

MIT License - see [LICENSE](LICENSE)
```

### QUICKSTART.md - Proposed Updates

Add these sections:

```markdown
## Prerequisites Checklist

- [ ] Python 3.12+ installed
- [ ] PostgreSQL 14+ running
- [ ] Git installed
- [ ] Census API key (free at census.gov)
- [ ] Mapbox token (free tier at mapbox.com)

## Troubleshooting

### Database Connection Errors
- Verify PostgreSQL is running: `pg_isready`
- Check `.env` DATABASE_URL format

### Missing Data
- Run full pipeline: `make pipeline`
- Check `logs/` for errors

### Frontend Not Loading
- Verify Mapbox token in `.env`
- Check browser console for errors
```

---

## 4. Revised .gitignore

```gitignore
# =============================================================================
# Maryland Housing Atlas - .gitignore
# =============================================================================

# -----------------------------------------------------------------------------
# Python
# -----------------------------------------------------------------------------
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
.venv/
venv/
env/
ENV/

# -----------------------------------------------------------------------------
# Environment & Secrets
# -----------------------------------------------------------------------------
.env
.env.local
.env.*.local
!.env.example

# -----------------------------------------------------------------------------
# IDE & Editor
# -----------------------------------------------------------------------------
.vscode/
.idea/
*.swp
*.swo
*~
.project
.pydevproject
.settings/

# -----------------------------------------------------------------------------
# Testing & Coverage
# -----------------------------------------------------------------------------
.coverage
.pytest_cache/
htmlcov/
.tox/
.hypothesis/
coverage.xml
*.cover
.coverage.*

# -----------------------------------------------------------------------------
# Logs (ALL logs belong in logs/ directory)
# -----------------------------------------------------------------------------
*.log
logs/*
!logs/.gitkeep

# JVM crash logs (from R5/Java routing)
hs_err_pid*.log

# -----------------------------------------------------------------------------
# Data & Cache (regenerated on demand)
# -----------------------------------------------------------------------------
cache/
data/cache/
data/raw/
data/temp/
scratchpad/

# -----------------------------------------------------------------------------
# Exports (keep versioned, ignore latest)
# -----------------------------------------------------------------------------
exports/md_counties_latest.geojson
exports/*.tmp

# -----------------------------------------------------------------------------
# Database
# -----------------------------------------------------------------------------
*.db
*.sqlite
*.sqlite3

# -----------------------------------------------------------------------------
# OS Generated
# -----------------------------------------------------------------------------
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# -----------------------------------------------------------------------------
# Jupyter
# -----------------------------------------------------------------------------
.ipynb_checkpoints/
*.ipynb

# -----------------------------------------------------------------------------
# Build & Deploy
# -----------------------------------------------------------------------------
.railway/
*.prof
*.lprof
.mypy_cache/
.dmypy.json
dmypy.json

# -----------------------------------------------------------------------------
# Temporary Files
# -----------------------------------------------------------------------------
*.bak
*.tmp
.tmp/
*.pdf
*.zip

# -----------------------------------------------------------------------------
# Documentation Build
# -----------------------------------------------------------------------------
docs/_build/
site/
```

---

## 5. New Files to Add

| File | Purpose |
|------|---------|
| `.env.example` | Environment variable template for new developers |
| `docs/development/CONTRIBUTING.md` | Contribution guidelines |
| `docs/CHANGELOG.md` | Version history and release notes |
| `pyproject.toml` | Modern Python tooling config (Black, isort, pytest) |
| `tests/conftest.py` | Shared pytest fixtures |
| `data/README.md` | Document data directory structure |
| `logs/.gitkeep` | Preserve empty logs directory |
| `exports/.gitkeep` | Preserve empty exports directory |
| `.github/workflows/ci.yml` | CI pipeline (lint, test) |

---

## 6. Risks and Follow-ups

### Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Breaking API imports | HIGH | Test all endpoints after api/ consolidation |
| Missing script references | MEDIUM | Search codebase for hardcoded paths |
| Documentation links breaking | LOW | Update all internal doc links |
| Cache regeneration time | LOW | Document expected first-run duration |

### Pre-Cleanup Checklist

```bash
# 1. Create backup branch
git checkout -b backup/pre-cleanup

# 2. Commit current state
git add -A && git commit -m "Backup before cleanup"

# 3. Push backup
git push origin backup/pre-cleanup

# 4. Return to main
git checkout main
```

### Post-Cleanup Validation

```bash
# 1. Verify API starts
make run-api
curl http://localhost:8000/api/v1/health

# 2. Verify pipeline runs
make pipeline

# 3. Verify frontend loads
python frontend/serve.py
# Open http://localhost:5000

# 4. Run tests
make test
```

### Follow-up Tasks

1. **Immediate**
   - [ ] Update Makefile targets to use new script paths
   - [ ] Update README with new structure
   - [ ] Merge api/ directories

2. **Short-term (This Week)**
   - [ ] Add CI workflow for linting/testing
   - [ ] Create layer documentation for layers 3-6
   - [ ] Write pytest fixtures in conftest.py

3. **Long-term**
   - [ ] Implement shared spatial utilities
   - [ ] Add comprehensive API tests
   - [ ] Set up automatic changelog generation

---

## Execution Script

Save as `scripts/execute_cleanup.sh`:

```bash
#!/bin/bash
set -e

echo "=== Maryland Housing Atlas - Full Cleanup ==="
echo ""

# Phase 1: Delete obsolete files
echo "[Phase 1] Removing obsolete files..."
rm -f hs_err_pid*.log
rm -f api.log pipeline_output.log
rm -f activate_correct_venv.sh
rm -f requirements_layer2_v2.txt
rm -rf cache/
echo "  Done."

# Phase 2: Move scripts
echo "[Phase 2] Consolidating scripts..."
[ -f setup_database.sh ] && mv setup_database.sh scripts/
[ -f setup_env.sh ] && mv setup_env.sh scripts/
[ -f cleanup.sh ] && mv cleanup.sh scripts/
echo "  Done."

# Phase 3: Create directory structure
echo "[Phase 3] Creating new directory structure..."
mkdir -p docs/{api,architecture,development,layers,archive}
mkdir -p tests/{test_ingest,test_api}
touch logs/.gitkeep
touch exports/.gitkeep
touch data/.gitkeep
echo "  Done."

# Phase 4: Reorganize docs
echo "[Phase 4] Reorganizing documentation..."
[ -f COMPLETE_SYSTEM_STATUS.md ] && mv COMPLETE_SYSTEM_STATUS.md docs/archive/
[ -f docs/PROJECT_STATUS.md ] && mv docs/PROJECT_STATUS.md docs/archive/
[ -f docs/PROJECT_STATUS_SUMMARY.md ] && mv docs/PROJECT_STATUS_SUMMARY.md docs/archive/
[ -f docs/FINAL_IMPLEMENTATION_SUMMARY.md ] && mv docs/FINAL_IMPLEMENTATION_SUMMARY.md docs/archive/
[ -f docs/MULTIYEAR_IMPLEMENTATION.md ] && mv docs/MULTIYEAR_IMPLEMENTATION.md docs/archive/
[ -f docs/CLEANUP_REFACTORING_GUIDE.md ] && mv docs/CLEANUP_REFACTORING_GUIDE.md docs/archive/
[ -f docs/LAYER1_V2_QUICKSTART.md ] && mv docs/LAYER1_V2_QUICKSTART.md docs/layers/LAYER1_ECONOMIC.md
[ -f docs/LAYER2_V2_QUICKSTART.md ] && mv docs/LAYER2_V2_QUICKSTART.md docs/layers/LAYER2_MOBILITY.md
[ -f docs/SYNTHESIS_GROUPING_IMPLEMENTATION.md ] && mv docs/SYNTHESIS_GROUPING_IMPLEMENTATION.md docs/architecture/SYNTHESIS_GROUPING.md
[ -f docs/BUGS_FIXED_SUMMARY.md ] && rm docs/BUGS_FIXED_SUMMARY.md
echo "  Done."

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Review changes: git status"
echo "  2. Manually merge api/ into src/api/"
echo "  3. Create .env.example from .env"
echo "  4. Update Makefile script paths"
echo "  5. Commit: git add -A && git commit -m 'Cleanup: Restructure project'"
echo ""
```

---

## Summary

| Metric | Before | After |
|--------|--------|-------|
| Root-level files | 18 | 11 |
| Shell scripts at root | 4 | 0 |
| API directories | 2 | 1 |
| Documentation files | 21 | 15 (organized) |
| Log files at root | 4 | 0 |

**Estimated Time**: 30-45 minutes
**Risk Level**: Low-Medium (with backup branch)
**Benefit**: Cleaner onboarding, reduced confusion, maintainable structure
