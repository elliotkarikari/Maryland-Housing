#!/usr/bin/env python3
"""Apply SQL migrations from migrations/ in deterministic order.

This script is the repository-native migration runner used by `make db-migrate`.
It tracks applied files in a `schema_migrations` table.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from sqlalchemy import create_engine, text


MIGRATION_RE = re.compile(r"^(?P<prefix>\d+)_.*\.sql$")


@dataclass(frozen=True)
class MigrationFile:
    prefix: int
    filename: str
    path: Path


def _database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required to run migrations")
    return database_url


def _load_migrations(migrations_dir: Path) -> List[MigrationFile]:
    migrations: List[MigrationFile] = []
    prefixes: Dict[int, List[str]] = {}

    for path in sorted(migrations_dir.glob("*.sql")):
        match = MIGRATION_RE.match(path.name)
        if not match:
            continue

        prefix = int(match.group("prefix"))
        prefixes.setdefault(prefix, []).append(path.name)
        migrations.append(MigrationFile(prefix=prefix, filename=path.name, path=path))

    duplicate_prefixes = {k: v for k, v in prefixes.items() if len(v) > 1}
    if duplicate_prefixes:
        lines = []
        for prefix, files in sorted(duplicate_prefixes.items()):
            lines.append(f"{prefix:03d}: {', '.join(sorted(files))}")
        joined = "\n  ".join(lines)
        raise RuntimeError(
            "Duplicate migration prefixes found. Renumber migrations before applying:\n"
            f"  {joined}"
        )

    return sorted(migrations, key=lambda m: (m.prefix, m.filename))


def _ensure_tracking_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    filename TEXT PRIMARY KEY,
                    prefix INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )


def _fetch_applied(engine) -> Dict[str, str]:
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT filename, sha256 FROM schema_migrations")).fetchall()
    return {row.filename: row.sha256 for row in rows}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        digest.update(f.read())
    return digest.hexdigest()


def _apply_migration(engine, migration: MigrationFile, checksum: str) -> None:
    sql = migration.path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        raw = conn.connection
        cursor = raw.cursor()
        try:
            cursor.execute(sql)
            cursor.execute(
                """
                INSERT INTO schema_migrations (filename, prefix, sha256)
                VALUES (%s, %s, %s)
                """,
                (migration.filename, migration.prefix, checksum),
            )
        finally:
            cursor.close()


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    migrations_dir = repo_root / "migrations"

    if not migrations_dir.exists():
        print(f"No migrations directory found at {migrations_dir}", file=sys.stderr)
        return 1

    try:
        database_url = _database_url()
        migrations = _load_migrations(migrations_dir)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if not migrations:
        print("No SQL migrations found; nothing to apply")
        return 0

    engine = create_engine(database_url)

    try:
        _ensure_tracking_table(engine)
        applied = _fetch_applied(engine)

        pending: List[MigrationFile] = []
        for migration in migrations:
            checksum = _sha256_file(migration.path)
            if migration.filename in applied:
                if applied[migration.filename] != checksum:
                    print(
                        "ERROR: Applied migration checksum mismatch for "
                        f"{migration.filename}. Refusing to continue.",
                        file=sys.stderr,
                    )
                    return 1
                continue
            pending.append(migration)

        if not pending:
            print("All migrations already applied")
            return 0

        print(f"Applying {len(pending)} pending migration(s)...")
        for migration in pending:
            checksum = _sha256_file(migration.path)
            print(f"  -> {migration.filename}")
            _apply_migration(engine, migration, checksum)

        print("Migrations applied successfully")
        return 0

    except Exception as exc:
        print(f"ERROR: Failed to apply migrations: {exc}", file=sys.stderr)
        return 1

    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
