#!/bin/bash
# Setup script for Maryland Viability Atlas
# Run with: source setup_env.sh

echo "Maryland Viability Atlas - Environment Setup"
echo "============================================="

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "Virtual environment is now active."
echo "To run the pipeline: python src/run_pipeline.py --help"
echo "To deactivate: deactivate"
