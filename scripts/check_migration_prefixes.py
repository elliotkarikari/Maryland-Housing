#!/usr/bin/env python3
"""Fail if SQL migrations have duplicate numeric prefixes."""

from __future__ import annotations

import re
import sys
from pathlib import Path

MIGRATION_RE = re.compile(r"^(?P<prefix>\d+)_.*\.sql$")


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    migrations_dir = repo_root / "migrations"

    if not migrations_dir.exists():
        print(f"No migrations directory found at {migrations_dir}")
        return 0

    prefix_to_files: dict[str, list[str]] = {}

    for path in sorted(migrations_dir.glob("*.sql")):
        match = MIGRATION_RE.match(path.name)
        if not match:
            continue
        prefix = match.group("prefix")
        prefix_to_files.setdefault(prefix, []).append(path.name)

    duplicates = {prefix: files for prefix, files in prefix_to_files.items() if len(files) > 1}

    if duplicates:
        print("Duplicate migration prefixes found:")
        for prefix in sorted(duplicates):
            files = ", ".join(sorted(duplicates[prefix]))
            print(f"  {prefix}: {files}")
        return 1

    print("Migration prefixes are unique")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
