"""
Maryland Viability Atlas - Databricks Connection Management
Drop-in replacement for config/database.py when DATA_BACKEND=databricks.

Uses databricks-sql-connector to query Delta tables in Unity Catalog.
Provides the same get_db() / log_refresh() / bulk_insert() interface
so downstream code requires zero changes.
"""

import json
import re
import logging
from contextlib import contextmanager
from datetime import date, datetime
from typing import Generator, Optional

from config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


# ---------------------------------------------------------------------------
# Result wrappers (mimic SQLAlchemy result interface)
# ---------------------------------------------------------------------------

class _Row:
    """Mimics a SQLAlchemy Row so callers can use row.column_name or row[0]."""
    def __init__(self, values, columns):
        self._values = list(values)
        self._columns = list(columns)
        for col, val in zip(self._columns, self._values):
            object.__setattr__(self, col, val)

    def __getitem__(self, idx):
        return self._values[idx]

    def __iter__(self):
        return iter(self._values)


class _ResultProxy:
    """Mimics a SQLAlchemy CursorResult."""
    def __init__(self, cursor):
        self._cursor = cursor
        if cursor.description:
            self._columns = [d[0] for d in cursor.description]
        else:
            self._columns = []

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        return _Row(row, self._columns)

    def fetchall(self):
        rows = self._cursor.fetchall()
        return [_Row(r, self._columns) for r in rows]

    def scalar(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        return row[0]

    def __iter__(self):
        for row in self._cursor.fetchall():
            yield _Row(row, self._columns)


# ---------------------------------------------------------------------------
# SQL translation helpers
# ---------------------------------------------------------------------------

def _extract_sql_string(query) -> str:
    """Extract raw SQL from a SQLAlchemy text() object or plain string."""
    if hasattr(query, 'text'):
        return query.text
    return str(query)


def _translate_params(sql: str, params: Optional[dict]) -> tuple:
    """
    Convert SQLAlchemy-style :named params to Databricks %s positional params.
    Returns (translated_sql, param_values_list).
    """
    if not params:
        return sql, []

    # Find all :param_name occurrences (not inside quotes)
    param_names_in_order = re.findall(r':(\w+)', sql)
    if not param_names_in_order:
        return sql, []

    values = []
    for name in param_names_in_order:
        if name in params:
            values.append(params[name])
        else:
            values.append(None)

    # Replace :param_name with %s
    translated = re.sub(r':(\w+)', '%s', sql)
    return translated, values


def _translate_pg_to_databricks(sql: str) -> str:
    """
    Lightweight translation of PostgreSQL-specific SQL to Databricks SQL.
    Handles the most common patterns found in this codebase.
    """
    # Remove PostgreSQL casts like ::float, ::integer, ::text, ::numeric
    sql = re.sub(r'::(\w+)', '', sql)

    # Replace JSONB with STRING (Databricks uses STRING for JSON)
    sql = sql.replace('JSONB', 'STRING')

    # Replace SERIAL PRIMARY KEY with BIGINT GENERATED ALWAYS AS IDENTITY
    sql = re.sub(
        r'(\w+)\s+SERIAL\s+PRIMARY\s+KEY',
        r'\1 BIGINT GENERATED ALWAYS AS IDENTITY',
        sql,
        flags=re.IGNORECASE
    )

    # Replace NUMERIC(x,y) with DECIMAL(x,y)
    sql = re.sub(r'NUMERIC\((\d+),(\d+)\)', r'DECIMAL(\1,\2)', sql)

    # Replace BOOLEAN with BOOLEAN (same, no-op)
    # Replace TEXT[] with STRING (Databricks doesn't have array type in DDL easily)
    sql = re.sub(r'TEXT\[\]', 'STRING', sql)

    # Remove REFERENCES clauses (no foreign keys in Delta)
    sql = re.sub(r'REFERENCES\s+\w+\(\w+\)(\s+ON\s+DELETE\s+\w+)?', '', sql)

    # Remove CREATE INDEX statements (Delta handles indexing differently)
    if sql.strip().upper().startswith('CREATE INDEX') or sql.strip().upper().startswith('CREATE UNIQUE INDEX'):
        return '-- ' + sql  # Comment out

    return sql


# ---------------------------------------------------------------------------
# DatabricksSession — drop-in for SQLAlchemy Session
# ---------------------------------------------------------------------------

class DatabricksSession:
    """
    Drop-in replacement for SQLAlchemy Session when using Databricks SQL.
    Supports the db.execute(text(...), params) pattern used throughout the codebase.
    """

    def __init__(self, connection):
        self._conn = connection
        self._cursor = None

    def execute(self, query, params=None):
        """Execute a SQL query, returning a result proxy."""
        sql = _extract_sql_string(query)
        sql = _translate_pg_to_databricks(sql)
        translated_sql, param_values = _translate_params(sql, params)

        # Skip commented-out statements
        if translated_sql.strip().startswith('--'):
            return _ResultProxy(type('FakeCursor', (), {
                'description': None,
                'fetchone': lambda: None,
                'fetchall': lambda: [],
            })())

        self._cursor = self._conn.cursor()
        try:
            if param_values:
                self._cursor.execute(translated_sql, param_values)
            else:
                self._cursor.execute(translated_sql)
            return _ResultProxy(self._cursor)
        except Exception as e:
            logger.error(f"Databricks SQL error: {e}\nSQL: {translated_sql}")
            raise

    def commit(self):
        """No-op — Databricks auto-commits."""
        pass

    def rollback(self):
        """No-op — Databricks doesn't support transactions in the same way."""
        pass

    def close(self):
        """Close the underlying connection."""
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
        try:
            self._conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def _get_connection():
    """Create a new Databricks SQL connection."""
    from databricks import sql as dbsql

    return dbsql.connect(
        server_hostname=settings.DATABRICKS_SERVER_HOSTNAME,
        http_path=settings.DATABRICKS_HTTP_PATH,
        access_token=settings.DATABRICKS_ACCESS_TOKEN,
        catalog=settings.DATABRICKS_CATALOG,
        schema=settings.DATABRICKS_SCHEMA,
    )


@contextmanager
def get_databricks_db() -> Generator[DatabricksSession, None, None]:
    """
    Context manager for Databricks sessions.
    Same interface as config.database.get_db().
    """
    conn = _get_connection()
    session = DatabricksSession(conn)
    try:
        yield session
    except Exception as e:
        logger.error(f"Databricks error: {e}")
        raise
    finally:
        session.close()


def get_databricks_db_session() -> DatabricksSession:
    """
    Dependency for FastAPI endpoints (matches get_db_session signature).
    Caller is responsible for closing.
    """
    conn = _get_connection()
    return DatabricksSession(conn)


# ---------------------------------------------------------------------------
# Utility functions (mirror config/database.py)
# ---------------------------------------------------------------------------

def test_databricks_connection() -> bool:
    """Test Databricks connectivity."""
    try:
        with get_databricks_db() as db:
            result = db.execute("SELECT 1 AS test")
            val = result.scalar()
            assert val == 1
            logger.info("Databricks connection successful")

            # Check if md_counties table exists
            result = db.execute(
                f"SHOW TABLES IN {settings.DATABRICKS_CATALOG}.{settings.DATABRICKS_SCHEMA} LIKE 'md_counties'"
            )
            rows = result.fetchall()
            if rows:
                result = db.execute("SELECT COUNT(*) FROM md_counties")
                count = result.scalar()
                logger.info(f"Found {count} Maryland counties in Databricks")

            return True
    except Exception as e:
        logger.error(f"Databricks connection failed: {e}")
        return False


def databricks_log_refresh(
    layer_name: str,
    data_source: str,
    status: str,
    records_processed: int = 0,
    records_inserted: int = 0,
    records_updated: int = 0,
    error_message: str = None,
    metadata: dict = None
):
    """Log data refresh operation to data_refresh_log Delta table."""
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
        with get_databricks_db() as db:
            metadata_str = json.dumps(metadata, default=_json_default) if metadata else None
            db.execute(
                """INSERT INTO data_refresh_log
                   (layer_name, data_source, refresh_date, status,
                    records_processed, records_inserted, records_updated,
                    error_message, metadata)
                   VALUES (:layer_name, :data_source, :refresh_date, :status,
                           :records_processed, :records_inserted, :records_updated,
                           :error_message, :metadata)""",
                {
                    "layer_name": layer_name,
                    "data_source": data_source,
                    "refresh_date": datetime.utcnow().isoformat(),
                    "status": status,
                    "records_processed": records_processed,
                    "records_inserted": records_inserted,
                    "records_updated": records_updated,
                    "error_message": error_message,
                    "metadata": metadata_str,
                }
            )
            logger.info(
                f"Logged refresh: {layer_name} ({data_source}) - "
                f"Status: {status}, Processed: {records_processed}"
            )
    except Exception as e:
        logger.error(f"Failed to log refresh: {e}")


def databricks_bulk_insert(
    table_name: str,
    records: list[dict],
    conflict_cols: list[str] = None
):
    """
    Bulk insert into a Delta table.
    If conflict_cols is provided, uses MERGE INTO for upsert semantics.
    """
    if not records:
        logger.warning(f"No records to insert into {table_name}")
        return

    columns = list(records[0].keys())

    with get_databricks_db() as db:
        if conflict_cols:
            # MERGE INTO for upsert
            # Create a temp view from VALUES, then merge
            cols_str = ", ".join(columns)
            placeholders = ", ".join([f":{c}" for c in columns])
            on_clause = " AND ".join([f"target.{c} = source.{c}" for c in conflict_cols])
            update_cols = [c for c in columns if c not in conflict_cols]
            update_str = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
            insert_cols = ", ".join(columns)
            insert_vals = ", ".join([f"source.{c}" for c in columns])

            for record in records:
                # Use INSERT with individual rows and handle conflicts via MERGE
                # For simplicity with Databricks, delete + insert pattern
                where_parts = " AND ".join([f"{c} = :{c}" for c in conflict_cols])
                db.execute(
                    f"DELETE FROM {table_name} WHERE {where_parts}",
                    {c: record[c] for c in conflict_cols}
                )
                values_str = ", ".join([f":{c}" for c in columns])
                db.execute(
                    f"INSERT INTO {table_name} ({cols_str}) VALUES ({values_str})",
                    record
                )
        else:
            # Simple insert
            cols_str = ", ".join(columns)
            for record in records:
                values_str = ", ".join([f":{c}" for c in columns])
                db.execute(
                    f"INSERT INTO {table_name} ({cols_str}) VALUES ({values_str})",
                    record
                )

        logger.info(f"Bulk inserted {len(records)} records into {table_name}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if test_databricks_connection():
        print("Databricks connection successful")
    else:
        print("Databricks connection failed")
