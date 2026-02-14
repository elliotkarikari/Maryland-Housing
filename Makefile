.PHONY: help install init-db db-setup db-migrate ingest-all process pipeline export serve frontend test lint clean agent-lightning claude-help claude-list claude-run claude-exec claude-new

# Prefer local venv if present.
ifeq (,$(wildcard .venv/bin/python))
PYTHON := python
else
PYTHON := .venv/bin/python
endif
PIP := $(PYTHON) -m pip

help:
	@echo "Maryland Viability Atlas - Available Commands"
	@echo ""
	@echo "  make install        - Install Python dependencies"
	@echo "  make init-db        - Initialize database with migrations"
	@echo "  make db-setup       - Initialize PostgreSQL/PostGIS database"
	@echo "  make db-migrate     - Run database migrations"
	@echo "  make ingest-all     - Run all data ingestion pipelines"
	@echo "  make ingest-layer1  - Ingest Economic Opportunity (v2) data"
	@echo "  make ingest-layer2  - Ingest Mobility Accessibility (v2) data"
	@echo "  make ingest-layer3  - Ingest School System data"
	@echo "  make ingest-layer4  - Ingest Housing Elasticity data"
	@echo "  make ingest-layer5  - Ingest Demographic Momentum data"
	@echo "  make ingest-layer6  - Ingest Risk Drag data"
	@echo "  make process        - Run multi-year scoring + classification"
	@echo "  make pipeline       - Run V2 pipeline + GeoJSON export"
	@echo "  make export         - Generate GeoJSON outputs (V2)"
	@echo "  make serve          - Start FastAPI development server"
	@echo "  make test           - Run test suite"
	@echo "  make clean          - Remove temporary files"
	@echo "  make agent-lightning - Start Agent Lightning pilot container"
	@echo ""
	@echo "Claude Prompt Management:"
	@echo "  make claude-list    - List all available prompts"
	@echo "  make claude-run PROMPT=name - Display a prompt"
	@echo "  make claude-run PROMPT=name EXEC=1 - Execute via API"
	@echo "  make claude-new NAME=name - Create new prompt from template"

install:
	$(PIP) install -r requirements.txt

init-db:
	@echo "Initializing database..."
	bash scripts/setup_database.sh
	$(PYTHON) scripts/init_db.py

db-setup:
	@echo "Setting up PostgreSQL with PostGIS..."
	psql $(DATABASE_URL) -c "CREATE EXTENSION IF NOT EXISTS postgis;"
	psql $(DATABASE_URL) -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"
	psql $(DATABASE_URL) -f data/schemas/schema.sql
	psql $(DATABASE_URL) -f data/schemas/schema_timeseries.sql

db-migrate:
	alembic upgrade head

ingest-all:
	@echo "Running all data ingestion pipelines..."
	$(PYTHON) -m src.ingest.layer1_economic_accessibility
	$(PYTHON) -m src.ingest.layer2_accessibility
	$(PYTHON) -m src.ingest.layer3_schools
	$(PYTHON) -m src.ingest.layer4_housing
	$(PYTHON) -m src.ingest.layer5_demographics
	$(PYTHON) -m src.ingest.layer6_risk
	$(PYTHON) -m src.ingest.policy_persistence

ingest-layer1:
	$(PYTHON) -m src.ingest.layer1_economic_accessibility

ingest-layer2:
	$(PYTHON) -m src.ingest.layer2_accessibility

ingest-layer3:
	$(PYTHON) -m src.ingest.layer3_schools

ingest-layer4:
	$(PYTHON) -m src.ingest.layer4_housing

ingest-layer5:
	$(PYTHON) -m src.ingest.layer5_demographics

ingest-layer6:
	$(PYTHON) -m src.ingest.layer6_risk

process:
	@echo "Running multi-year scoring and classification..."
	$(PYTHON) -m src.run_multiyear_pipeline

pipeline:
	@echo "Running multi-year pipeline and export..."
	$(PYTHON) src/run_pipeline.py

export:
	@echo "Generating GeoJSON outputs..."
	$(PYTHON) -m src.export.geojson_export

serve:
	$(PYTHON) -m uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

frontend:
	$(PYTHON) frontend/serve.py

lint:
	@echo "Running linters..."
	$(PYTHON) -m black --check src/ tests/ config/
	$(PYTHON) -m isort --check-only src/ tests/ config/
	$(PYTHON) -m mypy src/ --ignore-missing-imports

format:
	@echo "Formatting code..."
	$(PYTHON) -m black src/ tests/ config/
	$(PYTHON) -m isort src/ tests/ config/

test:
	$(PYTHON) -m pytest tests/ -v --cov=src

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +

agent-lightning:
	docker compose -f docker-compose.agent-lightning.yml up --build -d

# =============================================================================
# Claude Prompt Management
# =============================================================================
# Usage:
#   make claude-list              - List all available prompts
#   make claude-run PROMPT=name   - Display prompt (for copy-paste)
#   make claude-run PROMPT=name EXEC=1 - Execute via API
#   make claude-run PROMPT=name CONTEXT=1 - Include project context
#   make claude-new NAME=name     - Create new prompt from template
# =============================================================================

# Default values for optional parameters
PROMPT ?=
EXEC ?= 0
CONTEXT ?= 0
NAME ?=

claude-help:
	@echo "Claude Prompt Management"
	@echo ""
	@echo "Commands:"
	@echo "  make claude-list              - List all available prompts"
	@echo "  make claude-run PROMPT=name   - Display a prompt (for copy-paste)"
	@echo "  make claude-run PROMPT=name EXEC=1 - Execute prompt via API"
	@echo "  make claude-new NAME=name     - Create new prompt from template"
	@echo ""
	@echo "Options:"
	@echo "  PROMPT=name    - Prompt name (required for run)"
	@echo "  EXEC=1         - Execute via API instead of display"
	@echo "  CONTEXT=1      - Include project context in prompt"
	@echo "  NAME=name      - New prompt name (for claude-new)"
	@echo ""
	@echo "Examples:"
	@echo "  make claude-run PROMPT=cleanup"
	@echo "  make claude-run PROMPT=documentation EXEC=1"
	@echo "  make claude-run PROMPT=code-review CONTEXT=1"
	@echo "  make claude-new NAME=my-custom-prompt"

claude-list:
	@bash scripts/claude_list.sh

claude-run:
ifndef PROMPT
	@echo "Error: PROMPT is required"
	@echo "Usage: make claude-run PROMPT=<prompt_name>"
	@echo ""
	@bash scripts/claude_list.sh
	@exit 1
endif
ifeq ($(EXEC),1)
ifeq ($(CONTEXT),1)
	@bash scripts/claude_run.sh $(PROMPT) --exec --context
else
	@bash scripts/claude_run.sh $(PROMPT) --exec
endif
else
ifeq ($(CONTEXT),1)
	@bash scripts/claude_run.sh $(PROMPT) --context
else
	@bash scripts/claude_run.sh $(PROMPT)
endif
endif

claude-exec:
ifndef PROMPT
	@echo "Error: PROMPT is required"
	@echo "Usage: make claude-exec PROMPT=<prompt_name>"
	@exit 1
endif
ifeq ($(CONTEXT),1)
	@bash scripts/claude_run.sh $(PROMPT) --exec --context
else
	@bash scripts/claude_run.sh $(PROMPT) --exec
endif

claude-new:
ifndef NAME
	@echo "Error: NAME is required"
	@echo "Usage: make claude-new NAME=<new_prompt_name>"
	@exit 1
endif
	@if [ -f ".claude/prompts/$(NAME).md" ]; then \
		echo "Error: Prompt '$(NAME)' already exists"; \
		exit 1; \
	fi
	@cp .claude/templates/prompt_template.md .claude/prompts/$(NAME).md
	@echo "Created new prompt: .claude/prompts/$(NAME).md"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Edit the prompt: code .claude/prompts/$(NAME).md"
	@echo "  2. Test it: make claude-run PROMPT=$(NAME)"
	@echo "  3. Commit when ready: git add .claude/prompts/$(NAME).md"
