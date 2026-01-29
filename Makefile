.PHONY: help install db-setup db-migrate ingest-all process pipeline export serve test clean

help:
	@echo "Maryland Viability Atlas - Available Commands"
	@echo ""
	@echo "  make install        - Install Python dependencies"
	@echo "  make db-setup       - Initialize PostgreSQL/PostGIS database"
	@echo "  make db-migrate     - Run database migrations"
	@echo "  make ingest-all     - Run all data ingestion pipelines"
	@echo "  make ingest-layer1  - Ingest Employment Gravity data"
	@echo "  make ingest-layer2  - Ingest Mobility Optionality data"
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

install:
	pip install -r requirements.txt

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
	python -m src.ingest.layer1_employment
	python -m src.ingest.layer2_mobility
	python -m src.ingest.layer3_schools
	python -m src.ingest.layer4_housing
	python -m src.ingest.layer5_demographics
	python -m src.ingest.layer6_risk
	python -m src.ingest.policy_persistence

ingest-layer1:
	python -m src.ingest.layer1_employment

ingest-layer2:
	python -m src.ingest.layer2_mobility

ingest-layer3:
	python -m src.ingest.layer3_schools

ingest-layer4:
	python -m src.ingest.layer4_housing

ingest-layer5:
	python -m src.ingest.layer5_demographics

ingest-layer6:
	python -m src.ingest.layer6_risk

process:
	@echo "Running multi-year scoring and classification..."
	python -m src.run_multiyear_pipeline

pipeline:
	@echo "Running multi-year pipeline and export..."
	python src/run_pipeline.py

export:
	@echo "Generating GeoJSON outputs..."
	python -m src.export.geojson_export

serve:
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

test:
	pytest tests/ -v --cov=src

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
