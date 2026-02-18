.PHONY: help install init-db db-setup db-migrate databricks-medallion ingest-all process pipeline export serve frontend test test-fast lint clean agent-lightning layer1-sensitivity claude-help claude-list claude-run claude-exec claude-new

# Prefer local venv if present.
ifeq (,$(wildcard .venv/bin/python))
PYTHON := python
else
PYTHON := .venv/bin/python
endif
PIP := $(PYTHON) -m pip
DATA_BACKEND ?= databricks

help:
	@echo "Maryland Viability Atlas - Available Commands"
	@echo ""
	@echo "  make install        - Install Python dependencies"
	@echo "  make init-db        - Initialize database with migrations"
	@echo "  make db-setup       - Initialize PostgreSQL/PostGIS database"
	@echo "  make db-migrate     - Run database migrations"
	@echo "  make databricks-medallion - Reorganize Databricks default schema into gold"
	@echo "  make ingest-all     - Run all data ingestion pipelines"
	@echo "  make ingest-layer1  - Ingest Economic Opportunity (v2) data"
	@echo "  make ingest-layer2  - Ingest Mobility Accessibility (v2) data"
	@echo "  make ingest-layer3  - Ingest Education Accessibility (v2) data"
	@echo "  make ingest-layer4  - Ingest Housing Affordability (v2) data"
	@echo "  make ingest-layer5  - Ingest Demographic Equity (v2) data"
	@echo "  make ingest-layer6  - Ingest Risk Vulnerability (v2) data"
	@echo "  make layer1-sensitivity - Run Layer 1 accessibility threshold sensitivity report"
	@echo "  make process        - Run multi-year scoring + classification"
	@echo "  make pipeline       - Run V2 pipeline + GeoJSON export"
	@echo "  make export         - Generate GeoJSON outputs (V2)"
		@echo "  make serve          - Start FastAPI development server"
		@echo "  make test           - Run test suite"
		@echo "  make test-fast      - Run fast-fail test pass for quick iteration"
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
	$(PYTHON) scripts/run_sql_migrations.py

databricks-medallion:
	@echo "Reorganizing Databricks objects to bronze/silver/gold schemas..."
	set -a; . ./.env; set +a; $(PYTHON) scripts/reorganize_databricks_medallion.py --apply --layout layered --skip-existing-target

ingest-all:
	@echo "Running all data ingestion pipelines..."
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer1_economic_accessibility
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer2_accessibility
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer3_education_accessibility
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer4_housing_affordability
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer5_demographic_equity
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer6_risk_vulnerability
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.policy_persistence

ingest-layer1:
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer1_economic_accessibility

ingest-layer2:
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer2_accessibility

ingest-layer3:
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer3_education_accessibility

ingest-layer4:
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer4_housing_affordability

ingest-layer5:
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer5_demographic_equity

ingest-layer6:
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.ingest.layer6_risk_vulnerability

layer1-sensitivity:
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) scripts/layer1_accessibility_sensitivity.py

process:
	@echo "Running multi-year scoring and classification..."
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.run_multiyear_pipeline

pipeline:
	@echo "Running multi-year pipeline and export..."
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) src/run_pipeline.py

export:
	@echo "Generating GeoJSON outputs..."
	DATA_BACKEND=$(DATA_BACKEND) $(PYTHON) -m src.export.geojson_export

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

test-fast:
	$(PYTHON) -m pytest tests/ -q --maxfail=1

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
