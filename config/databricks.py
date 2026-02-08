"""
Maryland Viability Atlas - Databricks Connection Management
Drop-in replacement for config/database.py when DATA_BACKEND=databricks.

Uses databricks-sql-connector to query Delta tables in Unity Catalog.
Provides the same get_db() / log_refresh() / bulk_insert() interface
so downstream code requires zero changes.
"""

import json
import re
import socket
import logging
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Generator, Optional
from urllib.parse import urlparse

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


class _EmptyCursor:
    """Cursor shim used when a translated statement is intentionally skipped."""
    description = None

    def fetchone(self):
        return None

    def fetchall(self):
        return []


# ---------------------------------------------------------------------------
# SQL translation helpers
# ---------------------------------------------------------------------------

def _extract_sql_string(query) -> str:
    """Extract raw SQL from a SQLAlchemy text() object or plain string."""
    if hasattr(query, 'text'):
        return query.text
    return str(query)


def _split_sql_csv(raw: str) -> list[str]:
    """
    Split a comma-separated SQL list while preserving commas inside
    parentheses and quoted strings.
    """
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_quote = False
    quote_char = ""

    for ch in raw:
        if ch in ("'", '"'):
            if in_quote and ch == quote_char:
                in_quote = False
                quote_char = ""
            elif not in_quote:
                in_quote = True
                quote_char = ch
            current.append(ch)
            continue

        if not in_quote:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue

        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)

    return parts


def _coerce_param_value(value: Any) -> Any:
    """Coerce Python values to Databricks-friendly SQL parameter values."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)
    return value


def _rewrite_insert_on_conflict(sql: str) -> Optional[list[str]]:
    """
    Rewrite common PostgreSQL ON CONFLICT inserts to Databricks-compatible SQL.

    Supported patterns:
      - INSERT ... ON CONFLICT (k1, k2) DO NOTHING
      - INSERT ... ON CONFLICT (k1, k2) DO UPDATE SET ...
      - INSERT ... ON CONFLICT DO NOTHING
    """
    pattern = re.compile(
        r"""
        ^\s*INSERT\s+INTO\s+(?P<table>[^\s(]+)\s*
        \((?P<columns>.*?)\)\s*
        VALUES\s*\((?P<values>.*?)\)\s*
        ON\s+CONFLICT
        (?:\s*\((?P<conflict_cols>.*?)\))?
        \s*DO\s+(?P<action>NOTHING|UPDATE\s+SET\s+.*)\s*$
        """,
        flags=re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )

    match = pattern.match(sql)
    if not match:
        return None

    table = match.group("table").strip()
    columns_raw = match.group("columns").strip()
    values_raw = match.group("values").strip()
    conflict_raw = (match.group("conflict_cols") or "").strip()
    action = match.group("action").strip().upper()

    insert_cols = [c.strip() for c in _split_sql_csv(columns_raw) if c.strip()]
    insert_vals = [v.strip() for v in _split_sql_csv(values_raw) if v.strip()]
    if len(insert_cols) != len(insert_vals):
        return None

    value_by_col = {col: val for col, val in zip(insert_cols, insert_vals)}
    conflict_cols = [c.strip() for c in _split_sql_csv(conflict_raw) if c.strip()] if conflict_raw else []

    where_terms = []
    for col in conflict_cols:
        conflict_expr = value_by_col.get(col)
        if conflict_expr is None:
            return None
        where_terms.append(f"{col} = {conflict_expr}")

    if action.startswith("NOTHING"):
        if not where_terms:
            return [f"INSERT INTO {table} ({columns_raw}) VALUES ({values_raw})"]
        return [
            f"INSERT INTO {table} ({columns_raw}) "
            f"SELECT {', '.join(insert_vals)} "
            f"WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE {' AND '.join(where_terms)})"
        ]

    if not where_terms:
        return None

    delete_sql = f"DELETE FROM {table} WHERE {' AND '.join(where_terms)}"
    insert_sql = f"INSERT INTO {table} ({columns_raw}) VALUES ({values_raw})"
    return [delete_sql, insert_sql]


def _translate_params(sql: str, params: Optional[dict]) -> tuple:
    """
    Convert SQLAlchemy-style :named params to Databricks qmark positional params.
    Returns (translated_sql, param_values_list).
    """
    if not params:
        return sql, []

    translated = sql
    values: list[Any] = []

    # Translate PostgreSQL array predicate: col = ANY(:values) -> col IN (?, ...)
    any_pattern = re.compile(
        r'(\b[\w."]+\b)\s*=\s*ANY\(\s*:(\w+)\s*\)',
        flags=re.IGNORECASE,
    )

    def _any_repl(match: re.Match) -> str:
        column_ref = match.group(1)
        param_name = match.group(2)
        raw_value = params.get(param_name)

        if raw_value is None:
            seq = []
        elif isinstance(raw_value, (list, tuple, set)):
            seq = list(raw_value)
        else:
            seq = [raw_value]

        if not seq:
            return "1 = 0"

        values.extend(_coerce_param_value(v) for v in seq)
        placeholders = ", ".join(["?"] * len(seq))
        return f"{column_ref} IN ({placeholders})"

    translated = any_pattern.sub(_any_repl, translated)

    # Find all remaining :param_name occurrences
    param_names_in_order = re.findall(r':(\w+)', translated)
    for name in param_names_in_order:
        values.append(_coerce_param_value(params.get(name)))

    translated = re.sub(r':(\w+)', '?', translated)
    return translated, values


def _translate_pg_to_databricks(sql: str) -> str:
    """
    Lightweight translation of PostgreSQL-specific SQL to Databricks SQL.
    Handles the most common patterns found in this codebase.
    """
    # Remove PostgreSQL casts like ::float, ::integer, ::text, ::numeric
    sql = re.sub(r'::(\w+)', '', sql)

    # Replace JSONB with STRING (Databricks stores JSON as STRING)
    sql = re.sub(r'\bJSONB\b', 'STRING', sql, flags=re.IGNORECASE)

    # Replace CAST(:x AS jsonb) with :x
    sql = re.sub(
        r'CAST\(\s*([^)]+?)\s+AS\s+jsonb\s*\)',
        r'\1',
        sql,
        flags=re.IGNORECASE,
    )
    # If JSONB was already rewritten to STRING, strip CAST(... AS STRING) too.
    sql = re.sub(
        r'CAST\(\s*([^)]+?)\s+AS\s+STRING\s*\)',
        r'\1',
        sql,
        flags=re.IGNORECASE,
    )

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
# Databricks connection helpers
# ---------------------------------------------------------------------------

def _normalize_databricks_hostname(hostname: str) -> str:
    """
    Normalize Databricks hostname from either raw host or full URL values.
    Examples:
      - adb-123.4.azuredatabricks.net
      - https://adb-123.4.azuredatabricks.net/
    """
    value = (hostname or "").strip().strip('"').strip("'")
    if not value:
        return ""

    if value.startswith(("http://", "https://")):
        parsed = urlparse(value)
        value = parsed.netloc or value
    elif "/" in value:
        value = value.split("/", 1)[0]

    return value.strip()


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

    def _execute_single(self, sql: str, params=None):
        """Execute a single translated SQL statement."""
        translated_sql, param_values = _translate_params(sql, params)

        # Skip commented-out statements
        if translated_sql.strip().startswith('--'):
            return _ResultProxy(_EmptyCursor())

        self._cursor = self._conn.cursor()
        if param_values:
            self._cursor.execute(translated_sql, param_values)
        else:
            self._cursor.execute(translated_sql)
        return _ResultProxy(self._cursor)

    def execute(self, query, params=None):
        """Execute a SQL query, returning a result proxy."""
        sql = _extract_sql_string(query)
        sql = _translate_pg_to_databricks(sql)
        rewritten_statements = _rewrite_insert_on_conflict(sql)

        try:
            if rewritten_statements:
                result = _ResultProxy(_EmptyCursor())
                for stmt in rewritten_statements:
                    result = self._execute_single(stmt, params)
                return result

            return self._execute_single(sql, params)
        except Exception as e:
            logger.error(f"Databricks SQL error: {e}\nSQL: {sql}")
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

    raw_hostname = settings.DATABRICKS_SERVER_HOSTNAME or ""
    server_hostname = _normalize_databricks_hostname(raw_hostname)
    if not server_hostname:
        raise ValueError("DATABRICKS_SERVER_HOSTNAME is required when DATA_BACKEND=databricks")

    # Fail fast on DNS problems instead of waiting through long connector retries.
    try:
        socket.getaddrinfo(server_hostname, 443)
    except socket.gaierror as exc:
        raise RuntimeError(
            "Could not resolve DATABRICKS_SERVER_HOSTNAME "
            f"'{server_hostname}'. Verify workspace URL/VPN/private DNS setup."
        ) from exc

    return dbsql.connect(
        server_hostname=server_hostname,
        http_path=settings.DATABRICKS_HTTP_PATH,
        access_token=settings.DATABRICKS_ACCESS_TOKEN,
        catalog=settings.DATABRICKS_CATALOG,
        schema=settings.DATABRICKS_SCHEMA,
        enable_telemetry=settings.DATABRICKS_ENABLE_TELEMETRY,
        _socket_timeout=settings.DATABRICKS_SOCKET_TIMEOUT_SECONDS,
        _retry_stop_after_attempts_count=settings.DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_COUNT,
        _retry_stop_after_attempts_duration=settings.DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION,
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

            # Best-effort table check; don't fail connectivity if catalog/schema is not yet initialized.
            try:
                result = db.execute(
                    f"SHOW TABLES IN {settings.DATABRICKS_CATALOG}.{settings.DATABRICKS_SCHEMA} LIKE 'md_counties'"
                )
                rows = result.fetchall()
                if rows:
                    result = db.execute("SELECT COUNT(*) FROM md_counties")
                    count = result.scalar()
                    logger.info(f"Found {count} Maryland counties in Databricks")
            except Exception as check_error:
                logger.info(
                    "Databricks connected, but catalog/schema check skipped: %s",
                    check_error,
                )

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
