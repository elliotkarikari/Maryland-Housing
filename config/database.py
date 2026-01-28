"""
Maryland Viability Atlas - Database Connection Management
SQLAlchemy + PostGIS configuration
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from geoalchemy2 import Geometry
from contextlib import contextmanager
from typing import Generator
import logging

from config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

# SQLAlchemy engine
# For production on Railway, use NullPool to avoid connection exhaustion
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=NullPool if settings.ENVIRONMENT == "production" else None,
    echo=settings.DEBUG,
    connect_args={
        "options": "-c timezone=utc"
    }
)

# Enable PostGIS on connection
@event.listens_for(engine, "connect")
def receive_connect(dbapi_conn, connection_record):
    """Ensure PostGIS is available on connection"""
    with dbapi_conn.cursor() as cursor:
        cursor.execute("SELECT PostGIS_version();")
        version = cursor.fetchone()
        logger.debug(f"PostGIS version: {version[0] if version else 'Unknown'}")


# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base class for ORM models
Base = declarative_base()


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.

    Usage:
        with get_db() as db:
            db.execute(...)
            db.commit()
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        db.close()


def get_db_session() -> Session:
    """
    Dependency for FastAPI endpoints.

    Usage:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db_session)):
            ...
    """
    db = SessionLocal()
    try:
        return db
    finally:
        pass  # FastAPI will close it


def test_connection() -> bool:
    """
    Test database connectivity and PostGIS availability.

    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        with get_db() as db:
            # Test basic connectivity
            result = db.execute(text("SELECT 1"))
            assert result.scalar() == 1

            # Test PostGIS
            result = db.execute(text("SELECT PostGIS_version()"))
            version = result.scalar()
            logger.info(f"Database connection successful. PostGIS version: {version}")

            # Test Maryland counties table exists
            result = db.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'md_counties'"
            ))
            if result.scalar() == 1:
                result = db.execute(text("SELECT COUNT(*) FROM md_counties"))
                count = result.scalar()
                logger.info(f"Found {count} Maryland counties in database")

            return True

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def init_db():
    """
    Initialize database with schema.
    Should be run once during deployment.
    """
    logger.info("Initializing database schema...")

    try:
        # Read and execute schema.sql
        schema_path = "data/schemas/schema.sql"
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        with get_db() as db:
            # Execute schema (split by semicolons)
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            for statement in statements:
                try:
                    db.execute(text(statement))
                except Exception as e:
                    # Log but continue (some statements may fail if already exist)
                    logger.warning(f"Statement execution warning: {e}")

            db.commit()
            logger.info("Database schema initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


def log_refresh(
    layer_name: str,
    data_source: str,
    status: str,
    records_processed: int = 0,
    records_inserted: int = 0,
    records_updated: int = 0,
    error_message: str = None,
    metadata: dict = None
):
    """
    Log data refresh operation to data_refresh_log table.

    Args:
        layer_name: Name of analytical layer (e.g., 'layer1_employment')
        data_source: Source of data (e.g., 'LEHD/LODES')
        status: One of 'success', 'partial', 'failed'
        records_processed: Total records processed
        records_inserted: New records inserted
        records_updated: Existing records updated
        error_message: Error description if status != 'success'
        metadata: Additional JSON metadata
    """
    try:
        with get_db() as db:
            from datetime import datetime
            import json

            sql = text("""
                INSERT INTO data_refresh_log (
                    layer_name, data_source, refresh_date, status,
                    records_processed, records_inserted, records_updated,
                    error_message, metadata
                ) VALUES (
                    :layer_name, :data_source, :refresh_date, :status,
                    :records_processed, :records_inserted, :records_updated,
                    :error_message, :metadata
                )
            """)

            db.execute(sql, {
                "layer_name": layer_name,
                "data_source": data_source,
                "refresh_date": datetime.utcnow(),
                "status": status,
                "records_processed": records_processed,
                "records_inserted": records_inserted,
                "records_updated": records_updated,
                "error_message": error_message,
                "metadata": json.dumps(metadata) if metadata else None
            })

            logger.info(
                f"Logged refresh: {layer_name} ({data_source}) - "
                f"Status: {status}, Processed: {records_processed}"
            )

    except Exception as e:
        logger.error(f"Failed to log refresh: {e}")
        # Don't raise - logging failure shouldn't break ingestion


# Utility functions for common queries

def get_county_fips_list() -> list[str]:
    """Get list of all Maryland county FIPS codes from database."""
    with get_db() as db:
        result = db.execute(text("SELECT fips_code FROM md_counties ORDER BY fips_code"))
        return [row[0] for row in result]


def get_latest_data_year(layer_table: str) -> int:
    """Get the most recent data year for a given layer table."""
    with get_db() as db:
        result = db.execute(text(
            f"SELECT MAX(data_year) FROM {layer_table}"
        ))
        year = result.scalar()
        return year if year else 0


def bulk_insert(table_name: str, records: list[dict], conflict_cols: list[str] = None):
    """
    Bulk insert with optional conflict resolution.

    Args:
        table_name: Name of table to insert into
        records: List of dictionaries (column: value)
        conflict_cols: Columns to use for ON CONFLICT clause (upsert)
    """
    if not records:
        logger.warning(f"No records to insert into {table_name}")
        return

    with get_db() as db:
        from sqlalchemy import table, column, insert
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        # Dynamically create table object
        cols = [column(k) for k in records[0].keys()]
        tbl = table(table_name, *cols)

        if conflict_cols:
            # Upsert
            stmt = pg_insert(tbl).values(records)
            update_dict = {c.name: c for c in stmt.excluded if c.name not in conflict_cols}
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_dict
            )
        else:
            # Simple insert
            stmt = insert(tbl).values(records)

        db.execute(stmt)
        logger.info(f"Bulk inserted {len(records)} records into {table_name}")


if __name__ == "__main__":
    # Test connection when run directly
    logging.basicConfig(level=logging.INFO)
    if test_connection():
        print("✅ Database connection successful")
    else:
        print("❌ Database connection failed")
