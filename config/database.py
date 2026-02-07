"""
Maryland Viability Atlas - Database Connection Management
Supports PostgreSQL (default) and Azure Databricks backends.
Backend is selected via the DATA_BACKEND environment variable.
"""

from contextlib import contextmanager
from typing import Generator
import logging
from datetime import date, datetime

from config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

# ---------------------------------------------------------------------------
# Backend detection
# ---------------------------------------------------------------------------

_USE_DATABRICKS = settings.DATA_BACKEND == "databricks"

# ---------------------------------------------------------------------------
# PostgreSQL setup (only when using postgresql backend)
# ---------------------------------------------------------------------------

if not _USE_DATABRICKS:
    from sqlalchemy import create_engine, event, text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, Session
    from sqlalchemy.pool import NullPool

    try:
        from geoalchemy2 import Geometry
    except ImportError:
        Geometry = None  # Not required for non-spatial queries

    engine = create_engine(
        settings.DATABASE_URL,
        poolclass=NullPool if settings.ENVIRONMENT == "production" else None,
        echo=settings.DEBUG,
        connect_args={
            "options": "-c timezone=utc"
        }
    )

    @event.listens_for(engine, "connect")
    def receive_connect(dbapi_conn, connection_record):
        """Ensure PostGIS is available on connection"""
        with dbapi_conn.cursor() as cursor:
            cursor.execute("SELECT PostGIS_version();")
            version = cursor.fetchone()
            logger.debug(f"PostGIS version: {version[0] if version else 'Unknown'}")

    SessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine
    )

    Base = declarative_base()
else:
    # Stubs so imports don't break when using Databricks
    engine = None
    SessionLocal = None
    Base = None
    Session = None


# ---------------------------------------------------------------------------
# get_db() — unified context manager
# ---------------------------------------------------------------------------

@contextmanager
def get_db() -> Generator:
    """
    Context manager for database sessions.
    Returns a PostgreSQL SQLAlchemy session or a DatabricksSession
    depending on DATA_BACKEND setting.

    Usage:
        with get_db() as db:
            db.execute(...)
            db.commit()
    """
    if _USE_DATABRICKS:
        from config.databricks import get_databricks_db
        with get_databricks_db() as db:
            yield db
    else:
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


def get_db_session():
    """
    Dependency for FastAPI endpoints.

    Usage:
        @app.get("/endpoint")
        def endpoint(db = Depends(get_db_session)):
            ...
    """
    if _USE_DATABRICKS:
        from config.databricks import get_databricks_db_session
        return get_databricks_db_session()
    else:
        db = SessionLocal()
        try:
            return db
        finally:
            pass  # FastAPI will close it


def test_connection() -> bool:
    """
    Test database connectivity.
    Returns True if connection successful, False otherwise.
    """
    if _USE_DATABRICKS:
        from config.databricks import test_databricks_connection
        return test_databricks_connection()

    try:
        with get_db() as db:
            from sqlalchemy import text as sa_text
            result = db.execute(sa_text("SELECT 1"))
            assert result.scalar() == 1

            result = db.execute(sa_text("SELECT PostGIS_version()"))
            version = result.scalar()
            logger.info(f"Database connection successful. PostGIS version: {version}")

            result = db.execute(sa_text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'md_counties'"
            ))
            if result.scalar() == 1:
                result = db.execute(sa_text("SELECT COUNT(*) FROM md_counties"))
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
    if _USE_DATABRICKS:
        logger.info("For Databricks, run: python scripts/init_databricks.py")
        return

    logger.info("Initializing database schema...")

    try:
        import subprocess
        import os
        import platform
        schema_files = [
            "data/schemas/schema.sql",
            "data/schemas/schema_timeseries.sql"
        ]

        db_url = settings.DATABASE_URL

        psql_cmd = 'psql'
        if platform.system() == 'Darwin':
            homebrew_paths = [
                '/opt/homebrew/opt/postgresql@17/bin/psql',
                '/opt/homebrew/opt/postgresql@16/bin/psql',
                '/usr/local/opt/postgresql@17/bin/psql',
                '/usr/local/opt/postgresql@16/bin/psql'
            ]
            for path in homebrew_paths:
                if os.path.exists(path):
                    psql_cmd = path
                    break

        for schema_path in schema_files:
            if not os.path.exists(schema_path):
                logger.warning(f"Schema file not found, skipping: {schema_path}")
                continue

            logger.info(f"Executing schema from {schema_path} using {psql_cmd}")
            result = subprocess.run(
                [psql_cmd, db_url, '-f', schema_path],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                logger.info(f"Schema applied successfully: {schema_path}")
            else:
                logger.warning(f"Schema execution warnings for {schema_path}: {result.stderr}")
                logger.info("Schema initialization completed with warnings")

    except FileNotFoundError:
        logger.warning("psql not found, using SQLAlchemy (may have issues with functions)")

        schema_files = [
            "data/schemas/schema.sql",
            "data/schemas/schema_timeseries.sql"
        ]
        with get_db() as db:
            from sqlalchemy import text as sa_text
            for schema_path in schema_files:
                import os
                if not os.path.exists(schema_path):
                    logger.warning(f"Schema file not found, skipping: {schema_path}")
                    continue

                with open(schema_path, 'r') as f:
                    schema_sql = f.read()

                try:
                    db.execute(sa_text(schema_sql))
                    db.commit()
                    logger.info(f"Database schema initialized successfully: {schema_path}")
                except Exception as e:
                    logger.error(f"Failed to initialize schema {schema_path}: {e}")
                    logger.info("Trying statement-by-statement (may have issues)...")
                    statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
                    for statement in statements:
                        try:
                            db.execute(sa_text(statement))
                        except Exception as e2:
                            logger.warning(f"Statement warning: {e2}")
                    db.commit()

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
    Works with both PostgreSQL and Databricks backends.
    """
    if _USE_DATABRICKS:
        from config.databricks import databricks_log_refresh
        databricks_log_refresh(
            layer_name=layer_name,
            data_source=data_source,
            status=status,
            records_processed=records_processed,
            records_inserted=records_inserted,
            records_updated=records_updated,
            error_message=error_message,
            metadata=metadata,
        )
        return

    def _json_default(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, (set, tuple)):
            return list(value)
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        if hasattr(value, "tolist"):
            try:
                return value.tolist()
            except Exception:
                pass
        return str(value)

    try:
        with get_db() as db:
            import json
            from sqlalchemy import text as sa_text

            sql = sa_text("""
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
                "metadata": json.dumps(metadata, default=_json_default) if metadata else None
            })

            logger.info(
                f"Logged refresh: {layer_name} ({data_source}) - "
                f"Status: {status}, Processed: {records_processed}"
            )

    except Exception as e:
        logger.error(f"Failed to log refresh: {e}")


# ---------------------------------------------------------------------------
# Utility functions for common queries
# ---------------------------------------------------------------------------

def get_county_fips_list() -> list[str]:
    """Get list of all Maryland county FIPS codes from database."""
    with get_db() as db:
        if _USE_DATABRICKS:
            result = db.execute("SELECT fips_code FROM md_counties ORDER BY fips_code")
        else:
            from sqlalchemy import text as sa_text
            result = db.execute(sa_text("SELECT fips_code FROM md_counties ORDER BY fips_code"))
        return [row[0] for row in result]


def get_latest_data_year(layer_table: str) -> int:
    """Get the most recent data year for a given layer table."""
    with get_db() as db:
        if _USE_DATABRICKS:
            result = db.execute(f"SELECT MAX(data_year) FROM {layer_table}")
        else:
            from sqlalchemy import text as sa_text
            result = db.execute(sa_text(
                f"SELECT MAX(data_year) FROM {layer_table}"
            ))
        year = result.scalar()
        return year if year else 0


def bulk_insert(table_name: str, records: list[dict], conflict_cols: list[str] = None):
    """
    Bulk insert with optional conflict resolution.
    Uses ON CONFLICT for PostgreSQL, DELETE+INSERT for Databricks.
    """
    if not records:
        logger.warning(f"No records to insert into {table_name}")
        return

    if _USE_DATABRICKS:
        from config.databricks import databricks_bulk_insert
        databricks_bulk_insert(table_name, records, conflict_cols)
        return

    with get_db() as db:
        from sqlalchemy import table, column, insert
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        cols = [column(k) for k in records[0].keys()]
        tbl = table(table_name, *cols)

        if conflict_cols:
            stmt = pg_insert(tbl).values(records)
            update_dict = {c.name: c for c in stmt.excluded if c.name not in conflict_cols}
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_dict
            )
        else:
            stmt = insert(tbl).values(records)

        db.execute(stmt)
        logger.info(f"Bulk inserted {len(records)} records into {table_name}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if test_connection():
        print("Database connection successful")
    else:
        print("Database connection failed")
