"""
Shared ingest write-mode helpers.

Mode semantics:
- overwrite: existing year-level rows can be replaced.
- append: existing rows are preserved; only missing rows are inserted.
"""

from __future__ import annotations

from sqlalchemy import text

from config.settings import get_settings

settings = get_settings()


def get_ingest_write_mode() -> str:
    mode = (settings.INGEST_WRITE_MODE or "overwrite").strip().lower()
    if mode not in {"overwrite", "append"}:
        return "overwrite"
    return mode


def is_append_mode() -> bool:
    return get_ingest_write_mode() == "append"


def has_rows_for_year(db, table: str, data_year: int, year_col: str = "data_year") -> bool:
    result = db.execute(
        text(f"SELECT 1 FROM {table} WHERE {year_col} = :data_year LIMIT 1"),
        {"data_year": int(data_year)},
    )
    return result.fetchone() is not None
