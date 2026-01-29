#!/bin/bash
# cleanup.sh - Maryland Housing Atlas Cleanup

set -e  # Exit on error

echo "=== Maryland Housing Atlas - Cleanup ==="

# 1. Update .gitignore
echo ""
echo "[1/3] Updating .gitignore..."
if ! grep -q "data/cache/" .gitignore; then
    echo "" >> .gitignore
    echo "# Data cache (auto-downloaded, never commit)" >> .gitignore
    echo "data/cache/" >> .gitignore
    echo "data/cache/**/*" >> .gitignore
    echo "✓ Added data/cache/ to .gitignore"
else
    echo "✓ data/cache/ already in .gitignore"
fi

# 2. Delete deprecated scripts
echo ""
echo "[2/3] Removing deprecated v1 ingestion scripts..."
if [ -f "src/ingest/layer1_employment.py" ]; then
    rm src/ingest/layer1_employment.py
    echo "✓ Deleted src/ingest/layer1_employment.py"
else
    echo "  (already deleted)"
fi

if [ -f "src/ingest/layer2_mobility.py" ]; then
    rm src/ingest/layer2_mobility.py
    echo "✓ Deleted src/ingest/layer2_mobility.py"
else
    echo "  (already deleted)"
fi

# 3. Delete old cache
echo ""
echo "[3/3] Removing deprecated cache directories..."
if [ -d "data/cache/mobility" ]; then
    rm -rf data/cache/mobility/
    echo "✓ Deleted data/cache/mobility/ (1.5 MB freed)"
else
    echo "  (already deleted)"
fi

echo ""
echo "=== Cleanup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Review changes: git status"
echo "  2. Commit: git add -A && git commit -m 'Cleanup: Remove deprecated v1 scripts and old cache'"
echo "  3. Create Layer 1 quickstart guide (see docs/LAYER2_V2_QUICKSTART.md)"
echo ""
