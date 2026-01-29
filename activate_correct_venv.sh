#!/bin/bash
# Quick script to activate the correct virtual environment

echo "Deactivating current environment..."
deactivate 2>/dev/null || true

echo "Activating .venv (with dot)..."
source .venv/bin/activate

echo ""
echo "✓ Correct virtual environment activated!"
echo ""
echo "Verify Python version:"
python --version

echo ""
echo "Now you can run:"
echo "  python scripts/init_db.py"
