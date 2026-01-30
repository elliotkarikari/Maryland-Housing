#!/bin/bash
# PostgreSQL Setup for Maryland Viability Atlas

echo "=========================================="
echo "Maryland Viability Atlas - Database Setup"
echo "=========================================="
echo ""

# Start PostgreSQL
echo "1. Starting PostgreSQL@17..."
brew services start postgresql@17

# Wait for PostgreSQL to start
echo "   Waiting for PostgreSQL to start..."
sleep 3

# Create database
echo ""
echo "2. Creating maryland_atlas database..."
createdb maryland_atlas 2>/dev/null && echo "   ✓ Database created" || echo "   ⚠ Database may already exist"

# Enable PostGIS
echo ""
echo "3. Enabling PostGIS extension..."
psql maryland_atlas -c "CREATE EXTENSION IF NOT EXISTS postgis;" 2>/dev/null && echo "   ✓ PostGIS enabled" || echo "   ⚠ PostGIS might need to be installed: brew install postgis"

# Test connection
echo ""
echo "4. Testing connection..."
psql maryland_atlas -c "SELECT version();" >/dev/null 2>&1 && echo "   ✓ Connection successful" || echo "   ✗ Connection failed"

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Your DATABASE_URL is: postgresql://localhost/maryland_atlas"
echo ""
echo "Next steps:"
echo "1. Activate virtual environment: source .venv/bin/activate"
echo "2. Initialize schema: python scripts/init_db.py"
echo "3. Run pipeline: python src/run_pipeline.py --help"
