"""
Maryland Viability Atlas - Database Connection Management
SQLAlchemy + PostGIS configuration
"""

import logging
import re
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional, Set
from urllib.parse import quote_plus

from geoalchemy2 import Geometry
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

BRONZE_TABLES: Set[str] = {
    "layer1_lodes_od_raw",
    "layer1_lodes2_od_raw",
    "layer2_acs_flows_raw",
    "layer2_gtfs_feeds_raw",
    "mobility_gtfs_feeds",
    "mobility_computation_cache",
    "education_msde_data",
    "education_schooldigger_data",
    "education_schooldigger_quarantine",
}

SILVER_TABLES: Set[str] = {
    "education_school_directory",
    "layer1_economic_opportunity_tract",
    "layer2_county_general_flows",
    "layer2_mobility_accessibility_tract",
    "layer3_education_accessibility_tract",
    "layer4_housing_affordability_tract",
    "layer5_demographic_equity_tract",
    "layer6_risk_vulnerability_tract",
}

GOLD_TABLES: Set[str] = {
    "md_counties",
    "data_refresh_log",
    "layer1_employment_gravity",
    "layer2_mobility_optionality",
    "layer3_school_trajectory",
    "layer4_housing_elasticity",
    "layer5_demographic_momentum",
    "layer6_risk_drag",
    "policy_persistence",
    "layer_scores",
    "county_classifications",
    "export_versions",
    "layer_timeseries_features",
    "layer_summary_scores",
    "final_synthesis_current",
    "normalized_features",
    "ai_document",
    "ai_extraction",
    "ai_evidence_link",
    "schema_migrations",
    "v_latest_synthesis",
    "v_timeseries_summary",
}


def _normalized_backend() -> str:
    return (settings.DATA_BACKEND or "databricks").strip().lower()


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier}`"


def _assert_safe_identifier(identifier: str) -> None:
    if not SAFE_IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")


def _normalize_table_identifier(table_name: str) -> str:
    return (table_name or "").strip()


def table_schema_for(table_name: str) -> str:
    """
    Resolve Databricks medallion schema for a logical table name.
    Unknown tables default to bronze.
    """
    name = _normalize_table_identifier(table_name)
    if "." in name:
        # Caller provided explicit schema/cross-catalog reference.
        return ""

    if name in GOLD_TABLES:
        return settings.DATABRICKS_GOLD_SCHEMA
    if name in BRONZE_TABLES or name.endswith("_raw") or name.startswith("raw_"):
        return settings.DATABRICKS_BRONZE_SCHEMA
    if name in SILVER_TABLES or name.endswith("_tract"):
        return settings.DATABRICKS_SILVER_SCHEMA
    # Default unknown/new tables to bronze so fresh data pulls land in raw.
    return settings.DATABRICKS_BRONZE_SCHEMA


def qualified_table_name(table_name: str) -> str:
    """
    Return a SQL-safe table reference for current backend.

    - Postgres: returns logical table name unchanged.
    - Databricks:
      - unqualified table names are routed to bronze/silver/gold schemas
      - explicit schema.table or catalog.schema.table are preserved
    """
    raw = _normalize_table_identifier(table_name)
    if not raw:
        raise ValueError("table_name must not be empty")

    if DATABASE_BACKEND != "databricks":
        return raw

    explicit_parts = raw.split(".")
    if len(explicit_parts) > 1:
        if len(explicit_parts) not in (2, 3):
            raise ValueError(f"Invalid table identifier: {raw}")
        for part in explicit_parts:
            _assert_safe_identifier(part)
        if len(explicit_parts) == 2:
            schema, table = explicit_parts
            catalog = settings.DATABRICKS_CATALOG
            _assert_safe_identifier(catalog)
            return ".".join(
                (_quote_identifier(catalog), _quote_identifier(schema), _quote_identifier(table))
            )
        catalog, schema, table = explicit_parts
        return ".".join(
            (_quote_identifier(catalog), _quote_identifier(schema), _quote_identifier(table))
        )

    _assert_safe_identifier(raw)
    schema = table_schema_for(raw)
    catalog = settings.DATABRICKS_CATALOG
    _assert_safe_identifier(schema)
    _assert_safe_identifier(catalog)
    return ".".join((_quote_identifier(catalog), _quote_identifier(schema), _quote_identifier(raw)))


def table_name(table_name: str) -> str:
    """Alias for `qualified_table_name` for concise call sites."""
    return qualified_table_name(table_name)


def _build_databricks_url() -> str:
    """
    Build a SQLAlchemy URL for Databricks SQL Warehouse.

    Priority:
    1) DATABRICKS_SQLALCHEMY_URL if explicitly provided
    2) Construct URL from hostname/http_path/token/catalog/schema
    """
    if settings.DATABRICKS_SQLALCHEMY_URL:
        return settings.DATABRICKS_SQLALCHEMY_URL

    required = {
        "DATABRICKS_SERVER_HOSTNAME": settings.DATABRICKS_SERVER_HOSTNAME,
        "DATABRICKS_HTTP_PATH": settings.DATABRICKS_HTTP_PATH,
        "DATABRICKS_ACCESS_TOKEN": settings.DATABRICKS_ACCESS_TOKEN,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            "DATA_BACKEND=databricks but required settings are missing: " + ", ".join(missing)
        )

    token = quote_plus(settings.DATABRICKS_ACCESS_TOKEN or "")
    http_path = quote_plus(settings.DATABRICKS_HTTP_PATH or "", safe="/")
    catalog = quote_plus(settings.DATABRICKS_CATALOG or "hive_metastore")
    schema = quote_plus(settings.DATABRICKS_SCHEMA or "default")
    hostname = settings.DATABRICKS_SERVER_HOSTNAME or ""

    return (
        f"databricks://token:{token}@{hostname}"
        f"?http_path={http_path}&catalog={catalog}&schema={schema}"
    )


def _resolve_database_url() -> str:
    backend = _normalized_backend()
    if backend == "databricks":
        return _build_databricks_url()
    return settings.DATABASE_URL


def _engine_connect_args(database_url: str) -> dict:
    # Postgres-specific connection option (timezone)
    if database_url.startswith("postgresql"):
        return {"options": "-c timezone=utc"}
    return {}


DATABASE_BACKEND = _normalized_backend()
RESOLVED_DATABASE_URL = _resolve_database_url()

# SQLAlchemy engine
# For production on Railway, use NullPool to avoid connection exhaustion
try:
    engine = create_engine(
        RESOLVED_DATABASE_URL,
        poolclass=NullPool if settings.ENVIRONMENT == "production" else None,
        echo=settings.DEBUG,
        connect_args=_engine_connect_args(RESOLVED_DATABASE_URL),
    )
except NoSuchModuleError as exc:
    if DATABASE_BACKEND == "databricks":
        raise RuntimeError(
            "Databricks SQLAlchemy dialect is not installed. "
            "Install `databricks-sql-connector[sqlalchemy]` (or `databricks-sqlalchemy`) "
            "to use DATA_BACKEND=databricks."
        ) from exc
    raise


# Enable PostGIS on connection
@event.listens_for(engine, "connect")
def receive_connect(dbapi_conn, connection_record):
    """Ensure PostGIS is available on connection"""
    if DATABASE_BACKEND != "postgres":
        return
    with dbapi_conn.cursor() as cursor:
        cursor.execute("SELECT PostGIS_version();")
        version = cursor.fetchone()
        logger.debug(f"PostGIS version: {version[0] if version else 'Unknown'}")


# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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

            if DATABASE_BACKEND == "postgres":
                # Test PostGIS
                result = db.execute(text("SELECT PostGIS_version()"))
                version = result.scalar()
                logger.info(f"Database connection successful. PostGIS version: {version}")

                # Test Maryland counties table exists
                result = db.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_name = 'md_counties'"
                    )
                )
                if result.scalar() == 1:
                    result = db.execute(text(f"SELECT COUNT(*) FROM {table_name('md_counties')}"))
                    count = result.scalar()
                    logger.info(f"Found {count} Maryland counties in database")
            else:
                logger.info("Databricks connectivity check passed")

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

    if DATABASE_BACKEND == "databricks":
        logger.info("Skipping local PostGIS schema init (DATA_BACKEND=databricks)")
        return

    try:
        # Use psql command directly to avoid SQL parsing issues
        import os
        import platform
        import subprocess

        schema_files = ["data/schemas/schema.sql", "data/schemas/schema_timeseries.sql"]

        # Get database URL from settings
        db_url = settings.DATABASE_URL

        # Find psql command (check common locations)
        psql_cmd = "psql"
        if platform.system() == "Darwin":  # macOS
            # Try Homebrew paths
            homebrew_paths = [
                "/opt/homebrew/opt/postgresql@17/bin/psql",
                "/opt/homebrew/opt/postgresql@16/bin/psql",
                "/usr/local/opt/postgresql@17/bin/psql",
                "/usr/local/opt/postgresql@16/bin/psql",
            ]
            for path in homebrew_paths:
                if os.path.exists(path):
                    psql_cmd = path
                    break

        # Execute schema using psql
        for schema_path in schema_files:
            if not os.path.exists(schema_path):
                logger.warning(f"Schema file not found, skipping: {schema_path}")
                continue

            logger.info(f"Executing schema from {schema_path} using {psql_cmd}")
            result = subprocess.run(
                [psql_cmd, db_url, "-f", schema_path], capture_output=True, text=True
            )

            if result.returncode == 0:
                logger.info(f"Schema applied successfully: {schema_path}")
            else:
                logger.warning(f"Schema execution warnings for {schema_path}: {result.stderr}")
                # Don't raise - tables may already exist
                logger.info("Schema initialization completed with warnings")

    except FileNotFoundError:
        # psql not in PATH - fall back to SQLAlchemy
        logger.warning("psql not found, using SQLAlchemy (may have issues with functions)")

        schema_files = ["data/schemas/schema.sql", "data/schemas/schema_timeseries.sql"]
        with get_db() as db:
            for schema_path in schema_files:
                if not os.path.exists(schema_path):
                    logger.warning(f"Schema file not found, skipping: {schema_path}")
                    continue

                with open(schema_path, "r") as f:
                    schema_sql = f.read()

                try:
                    # Try to execute as single statement
                    db.execute(text(schema_sql))
                    db.commit()
                    logger.info(f"Database schema initialized successfully: {schema_path}")
                except Exception as e:
                    logger.error(f"Failed to initialize schema {schema_path}: {e}")
                    logger.info("Trying statement-by-statement (may have issues)...")
                    # Fall back to splitting by semicolons
                    statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
                    for statement in statements:
                        try:
                            db.execute(text(statement))
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
    metadata: dict = None,
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
            import json
            from datetime import datetime

            sql = text(
                f"""
                INSERT INTO {table_name('data_refresh_log')} (
                    layer_name, data_source, refresh_date, status,
                    records_processed, records_inserted, records_updated,
                    error_message, metadata
                ) VALUES (
                    :layer_name, :data_source, :refresh_date, :status,
                    :records_processed, :records_inserted, :records_updated,
                    :error_message, :metadata
                )
            """
            )

            db.execute(
                sql,
                {
                    "layer_name": layer_name,
                    "data_source": data_source,
                    "refresh_date": datetime.utcnow(),
                    "status": status,
                    "records_processed": records_processed,
                    "records_inserted": records_inserted,
                    "records_updated": records_updated,
                    "error_message": error_message,
                    "metadata": json.dumps(metadata) if metadata else None,
                },
            )

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
        result = db.execute(
            text(f"SELECT fips_code FROM {table_name('md_counties')} ORDER BY fips_code")
        )
        return [row[0] for row in result]


def get_latest_data_year(layer_table: str) -> int:
    """Get the most recent data year for a given layer table."""
    with get_db() as db:
        result = db.execute(text(f"SELECT MAX(data_year) FROM {table_name(layer_table)}"))
        year = result.scalar()
        return int(year) if year is not None else 0


def bulk_insert(
    table_name: str,
    records: list[dict],
    conflict_cols: Optional[list[str]] = None,
):
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
        from sqlalchemy import column, insert, table

        # Dynamically create table object
        cols: list[Any] = [column(k) for k in records[0].keys()]
        tbl = table(table_name, *cols)
        stmt: Any

        if conflict_cols:
            if DATABASE_BACKEND != "postgres":
                raise NotImplementedError(
                    "bulk_insert with conflict_cols is Postgres-only. "
                    "Use explicit delete+insert for Databricks compatibility."
                )
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(tbl).values(records)
            update_dict = {c.name: c for c in stmt.excluded if c.name not in conflict_cols}
            stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_dict)
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
