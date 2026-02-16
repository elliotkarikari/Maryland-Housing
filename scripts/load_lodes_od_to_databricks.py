#!/usr/bin/env python3
"""Load full LODES OD CSV into a Databricks raw table."""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.database import get_db
from config.settings import get_settings
from src.utils.db_bulk import execute_batch
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_table_name(table_name: str) -> str:
    if not TABLE_NAME_RE.match(table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    return table_name


def _to_int(value: str | None) -> int:
    if value is None or value == "":
        return 0
    return int(float(value))


def _ensure_raw_table(db, table_name: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        w_geocode STRING,
        h_geocode STRING,
        w_county STRING,
        h_county STRING,
        data_year INT,
        s000 INT,
        sa01 INT,
        sa02 INT,
        sa03 INT,
        se01 INT,
        se02 INT,
        se03 INT,
        si01 INT,
        si02 INT,
        si03 INT,
        source_file STRING
    )
    USING DELTA
    """
    db.execute(text(ddl))


def _insert_sql(table_name: str):
    return text(
        f"""
        INSERT INTO {table_name} (
            w_geocode, h_geocode, w_county, h_county, data_year,
            s000, sa01, sa02, sa03, se01, se02, se03, si01, si02, si03, source_file
        ) VALUES (
            :w_geocode, :h_geocode, :w_county, :h_county, :data_year,
            :s000, :sa01, :sa02, :sa03, :se01, :se02, :se03, :si01, :si02, :si03, :source_file
        )
        """
    )


def load_csv_to_table(
    csv_path: Path,
    table_name: str,
    chunk_size: int = 5000,
    truncate: bool = False,
    max_rows: int | None = None,
) -> int:
    source_file = str(csv_path.resolve())
    insert_sql = _insert_sql(table_name)
    buffer = []
    inserted = 0
    skipped = 0
    started = time.time()

    with get_db() as db:
        _ensure_raw_table(db, table_name)
        if truncate:
            logger.info(f"Truncating Databricks table {table_name}")
            db.execute(text(f"TRUNCATE TABLE {table_name}"))
        else:
            logger.info(f"Deleting prior rows for source_file={source_file}")
            db.execute(
                text(f"DELETE FROM {table_name} WHERE source_file = :source_file"),
                {"source_file": source_file},
            )
        db.commit()

        with csv_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if max_rows is not None and inserted >= max_rows:
                    break

                try:
                    w_geocode = str(row.get("w_geocode", "")).zfill(15)
                    h_geocode = str(row.get("h_geocode", "")).zfill(15)
                    if not w_geocode.strip() or not h_geocode.strip():
                        skipped += 1
                        continue

                    record = {
                        "w_geocode": w_geocode,
                        "h_geocode": h_geocode,
                        "w_county": w_geocode[:5],
                        "h_county": h_geocode[:5],
                        "data_year": _to_int(row.get("year")),
                        "s000": _to_int(row.get("S000")),
                        "sa01": _to_int(row.get("SA01")),
                        "sa02": _to_int(row.get("SA02")),
                        "sa03": _to_int(row.get("SA03")),
                        "se01": _to_int(row.get("SE01")),
                        "se02": _to_int(row.get("SE02")),
                        "se03": _to_int(row.get("SE03")),
                        "si01": _to_int(row.get("SI01")),
                        "si02": _to_int(row.get("SI02")),
                        "si03": _to_int(row.get("SI03")),
                        "source_file": source_file,
                    }
                    buffer.append(record)
                except Exception:
                    skipped += 1
                    continue

                if len(buffer) >= chunk_size:
                    execute_batch(db, insert_sql, buffer, chunk_size=chunk_size)
                    inserted += len(buffer)
                    db.commit()
                    buffer = []
                    elapsed = max(time.time() - started, 1e-6)
                    rate = inserted / elapsed
                    logger.info(f"Inserted {inserted:,} rows ({rate:,.0f} rows/sec)")

        if buffer:
            execute_batch(db, insert_sql, buffer, chunk_size=chunk_size)
            inserted += len(buffer)
            db.commit()

    elapsed = time.time() - started
    logger.info(
        f"OD CSV load complete: inserted={inserted:,}, skipped={skipped:,}, "
        f"elapsed={elapsed/60:.1f} min"
    )
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Load LODES OD CSV into Databricks raw table")
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=Path(settings.LODES_OD_DATA_PATH) if settings.LODES_OD_DATA_PATH else None,
        help="Path to LODES OD CSV file",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=settings.LODES_OD_TABLE,
        help="Target Databricks table name",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Batch size for insert operations",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target table before loading",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional cap for testing",
    )
    args = parser.parse_args()

    if args.csv_path is None:
        raise SystemExit("CSV path is required (--csv-path or LODES_OD_DATA_PATH)")
    if not args.csv_path.exists():
        raise SystemExit(f"CSV file not found: {args.csv_path}")
    if args.chunk_size <= 0:
        raise SystemExit("chunk-size must be > 0")

    table_name = _validate_table_name(args.table)
    load_csv_to_table(
        csv_path=args.csv_path,
        table_name=table_name,
        chunk_size=args.chunk_size,
        truncate=args.truncate,
        max_rows=args.max_rows,
    )


if __name__ == "__main__":
    main()
