"""
Initialize Databricks Delta tables for Maryland Viability Atlas.

Usage:
    python scripts/init_databricks.py [--load-geometries]

Requires DATA_BACKEND=databricks and valid Databricks credentials in .env.
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_settings
from config.databricks import get_databricks_db, test_databricks_connection

settings = get_settings()


def run_schema():
    """Execute the Databricks schema SQL to create all Delta tables."""
    schema_path = PROJECT_ROOT / "data" / "schemas" / "databricks_schema.sql"
    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}")
        sys.exit(1)

    schema_sql = schema_path.read_text()

    # Split into individual statements (Databricks SQL doesn't support multi-statement)
    statements = []
    current = []
    for line in schema_sql.split('\n'):
        stripped = line.strip()
        if stripped.startswith('--') or stripped == '':
            continue
        current.append(line)
        if stripped.endswith(';'):
            stmt = '\n'.join(current).strip().rstrip(';')
            if stmt:
                statements.append(stmt)
            current = []

    # Handle any remaining statement without trailing semicolon
    if current:
        stmt = '\n'.join(current).strip().rstrip(';')
        if stmt:
            statements.append(stmt)

    print(f"Found {len(statements)} SQL statements to execute")

    with get_databricks_db() as db:
        for i, stmt in enumerate(statements, 1):
            # Show first line of each statement for progress
            first_line = stmt.strip().split('\n')[0][:80]
            try:
                db.execute(stmt)
                print(f"  [{i}/{len(statements)}] OK: {first_line}")
            except Exception as e:
                print(f"  [{i}/{len(statements)}] WARN: {first_line}")
                print(f"    Error: {e}")

    print("\nSchema initialization complete.")


def load_geometries():
    """
    Load county geometries from Census TIGER/Line into md_counties.geometry_geojson.
    This populates the GeoJSON string column used for map export.
    """
    try:
        import pygris
        from pygris import counties as get_counties
        import json
    except ImportError:
        print("pygris is required for geometry loading. Install: pip install pygris")
        sys.exit(1)

    print("Fetching Maryland county boundaries from Census TIGER/Line...")
    md = get_counties(state="MD", year=2023, cb=True)
    md['GEOID'] = md['GEOID'].astype(str).str.zfill(5)

    if md.crs != 'EPSG:4326':
        md = md.to_crs('EPSG:4326')

    print(f"Loaded {len(md)} counties, updating geometry_geojson...")

    with get_databricks_db() as db:
        for _, row in md.iterrows():
            fips = row['GEOID']
            geojson_str = json.dumps(row['geometry'].__geo_interface__)
            db.execute(
                "UPDATE md_counties SET geometry_geojson = :geojson WHERE fips_code = :fips",
                {"geojson": geojson_str, "fips": fips}
            )
            print(f"  Updated {fips} ({row['NAME']})")

    print("Geometry loading complete.")


def main():
    parser = argparse.ArgumentParser(description='Initialize Databricks Delta tables')
    parser.add_argument('--load-geometries', action='store_true',
                        help='Also load county geometries from Census TIGER/Line')
    args = parser.parse_args()

    if settings.DATA_BACKEND != "databricks":
        print("ERROR: DATA_BACKEND must be set to 'databricks' in .env")
        print(f"Current value: {settings.DATA_BACKEND}")
        sys.exit(1)

    print("Testing Databricks connection...")
    if not test_databricks_connection():
        print("Connection failed. Check your .env credentials.")
        sys.exit(1)

    print(f"\nCatalog: {settings.DATABRICKS_CATALOG}")
    print(f"Schema:  {settings.DATABRICKS_SCHEMA}")
    print()

    run_schema()

    if args.load_geometries:
        print()
        load_geometries()

    print("\nDone. You can now run the ingestion pipeline with DATA_BACKEND=databricks.")


if __name__ == "__main__":
    main()
