#!/bin/bash
# Quick setup script to populate data for API demo

cd "$(dirname "$0")/.."

echo "=========================================="
echo "Maryland Viability Atlas - API Setup"
echo "=========================================="
echo ""

echo "This script will:"
echo "1. Run the multi-year pipeline to generate synthesis data"
echo "2. Export GeoJSON for map visualization"
echo "3. Start the API server"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

echo ""
echo "Step 1: Running multi-year pipeline..."
echo "----------------------------------------"
python src/run_multiyear_pipeline.py --year 2021

if [ $? -ne 0 ]; then
    echo "ERROR: Pipeline failed"
    exit 1
fi

echo ""
echo "Step 2: Exporting GeoJSON..."
echo "----------------------------------------"
python src/export/geojson.py

if [ $? -ne 0 ]; then
    echo "ERROR: GeoJSON export failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "To start the API:"
echo "  ./api/start.sh"
echo ""
echo "Or manually:"
echo "  python -m uvicorn api.main:app --reload"
echo ""
echo "API will be available at:"
echo "  http://localhost:8000"
echo "  http://localhost:8000/docs (interactive docs)"
echo ""
