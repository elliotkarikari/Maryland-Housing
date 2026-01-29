"""
Database Initialization Script

Sets up PostgreSQL database with PostGIS and loads Maryland county boundaries.

Usage:
    python scripts/init_db.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import init_db, get_db, test_connection
from config.settings import get_settings
from src.utils.logging import setup_logging, get_logger
from sqlalchemy import text

logger = setup_logging("init_db")
settings = get_settings()


def load_county_boundaries():
    """
    Fetch and load Maryland county boundaries into database.
    """
    logger.info("Loading Maryland county boundaries")

    try:
        import pygris
        from pygris import counties

        # Fetch Maryland counties
        md_counties = counties(state="MD", year=2023, cb=True)

        # Prepare data
        md_counties['GEOID'] = md_counties['GEOID'].astype(str).str.zfill(5)

        # Convert to WGS84
        if md_counties.crs != 'EPSG:4326':
            md_counties = md_counties.to_crs('EPSG:4326')

        # Insert into database
        with get_db() as db:
            for _, row in md_counties.iterrows():
                # Convert geometry to WKT
                geom_wkt = row['geometry'].wkt

                sql = text("""
                    UPDATE md_counties
                    SET geometry = ST_GeomFromText(:geom_wkt, 4326)
                    WHERE fips_code = :fips_code
                """)

                db.execute(sql, {
                    "geom_wkt": geom_wkt,
                    "fips_code": row['GEOID']
                })

            db.commit()

        logger.info(f"Loaded geometries for {len(md_counties)} counties")

    except Exception as e:
        logger.error(f"Failed to load county boundaries: {e}", exc_info=True)
        raise


def verify_setup():
    """Verify database is properly configured"""
    logger.info("Verifying database setup")

    with get_db() as db:
        # Check PostGIS
        result = db.execute(text("SELECT PostGIS_version()"))
        postgis_version = result.scalar()
        logger.info(f"PostGIS version: {postgis_version}")

        # Check tables
        result = db.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public'
                AND table_name LIKE 'layer%'
                OR table_name LIKE 'ai_%'
                OR table_name = 'md_counties'
        """))
        table_count = result.scalar()
        logger.info(f"Found {table_count} tables")

        # Check counties
        result = db.execute(text("SELECT COUNT(*) FROM md_counties"))
        county_count = result.scalar()
        logger.info(f"Found {county_count} Maryland counties")

        # Check counties with geometries
        result = db.execute(text("SELECT COUNT(*) FROM md_counties WHERE geometry IS NOT NULL"))
        geom_count = result.scalar()
        logger.info(f"Counties with geometries: {geom_count}")


def main():
    """Main initialization workflow"""
    logger.info("=" * 60)
    logger.info("Maryland Viability Atlas - Database Initialization")
    logger.info("=" * 60)

    # Check connection
    logger.info("\n1. Testing database connection...")
    if not test_connection():
        logger.error("Database connection failed. Check DATABASE_URL")
        sys.exit(1)
    logger.info("✓ Database connection successful")

    # Initialize schema
    logger.info("\n2. Initializing database schema...")
    try:
        init_db()
        logger.info("✓ Schema initialized")
    except Exception as e:
        logger.error(f"Schema initialization failed: {e}")
        sys.exit(1)

    # Load county boundaries
    logger.info("\n3. Loading Maryland county boundaries...")
    try:
        load_county_boundaries()
        logger.info("✓ County boundaries loaded")
    except Exception as e:
        logger.error(f"Failed to load boundaries: {e}")
        logger.warning("Continuing without geometries (can be loaded later)")

    # Verify
    logger.info("\n4. Verifying setup...")
    try:
        verify_setup()
        logger.info("✓ Verification complete")
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        sys.exit(1)

    logger.info("\n" + "=" * 60)
    logger.info("DATABASE INITIALIZATION COMPLETE")
    logger.info("=" * 60)
    logger.info("\nNext steps:")
    logger.info("1. Run data ingestion: python src/run_pipeline.py --level county")
    logger.info("2. Start API server: make serve")
    logger.info("3. View GeoJSON: exports/md_counties_latest.geojson")


if __name__ == "__main__":
    main()
