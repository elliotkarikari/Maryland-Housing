#!/usr/bin/env python3
"""Fail on known hardcoded year patterns in pipeline/ingest entrypoints."""

from __future__ import annotations

import re
import sys
from pathlib import Path

CHECKS = [
    (r"default\s*=\s*2025", "Hardcoded default year 2025"),
    (r"min\(data_year\s*-\s*2,\s*2021\)", "Hardcoded LODES cap 2021"),
    (r"min\(year,\s*2024\)", "Hardcoded NCES cap 2024"),
    (r"min\(data_year,\s*2024\)", "Hardcoded data year cap 2024"),
    (r"min\(2025,\s*current_year\)", "Hardcoded year clamp 2025"),
    (r"--year\s+2025", "Hardcoded CLI year 2025"),
]

TARGETS = [
    "src/run_pipeline.py",
    "src/run_multiyear_pipeline.py",
    "src/ingest/layer2_accessibility.py",
    "src/ingest/layer3_education_accessibility.py",
    "src/ingest/layer5_demographic_equity.py",
    "src/ingest/layer6_risk_vulnerability.py",
    "railway.json",
]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    errors: list[str] = []

    for rel_path in TARGETS:
        path = root / rel_path
        if not path.exists():
            continue

        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()

        for idx, line in enumerate(lines, start=1):
            for pattern, reason in CHECKS:
                if re.search(pattern, line):
                    errors.append(f"{rel_path}:{idx}: {reason}: {line.strip()}")

    if errors:
        print("Hardcoded year policy violations found:")
        for item in errors:
            print(f"  {item}")
        return 1

    print("Year literal checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
