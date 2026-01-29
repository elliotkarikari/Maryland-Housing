#!/bin/bash
# Start the Maryland Viability Atlas API

cd "$(dirname "$0")/.."

echo "Starting Maryland Viability Atlas API..."
echo "API will be available at: http://localhost:8000"
echo "API docs: http://localhost:8000/docs"
echo ""

python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
