"""Shared helpers for batched SQLAlchemy writes."""

from __future__ import annotations

from itertools import islice
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Sequence

import numpy as np
import pandas as pd


def _chunks(
    items: Sequence[Mapping[str, Any]], chunk_size: int
) -> Iterator[List[Mapping[str, Any]]]:
    it = iter(items)
    while True:
        chunk = list(islice(it, chunk_size))
        if not chunk:
            return
        yield chunk


def sanitize_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Convert NaN/NA values to None for DB compatibility."""
    sanitized: Dict[str, Any] = {}
    for key, value in record.items():
        if value is None:
            sanitized[key] = None
            continue
        if isinstance(value, (float, np.floating)) and np.isnan(value):
            sanitized[key] = None
            continue
        if pd.isna(value):
            sanitized[key] = None
            continue
        sanitized[key] = value
    return sanitized


def execute_batch(db, sql, rows: Iterable[Mapping[str, Any]], chunk_size: int = 1000) -> int:
    """Execute SQL with batched parameter sets.

    Returns the number of rows submitted.
    """
    row_list = [sanitize_record(row) for row in rows]
    if not row_list:
        return 0

    submitted = 0
    for chunk in _chunks(row_list, chunk_size):
        db.execute(sql, chunk)
        submitted += len(chunk)
    return submitted
