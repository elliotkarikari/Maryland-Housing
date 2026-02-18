#!/usr/bin/env python3
"""Lightweight perf-regression guard for key dataframe hot paths.

This script is intentionally self-contained for CI:
- It sets safe default env vars before importing app modules that call Settings().
- It uses synthetic data and ratio-based scaling checks (2x input size).
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

# Make script resilient in CI jobs that do not export app secrets.
os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("MAPBOX_ACCESS_TOKEN", "ci-placeholder-mapbox-token")
os.environ.setdefault("CENSUS_API_KEY", "ci-placeholder-census-key")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingest.layer5_demographic_equity import compute_segregation_indices
from src.ingest.layer5_demographics import apply_vacancy_predictions
from src.processing.feature_registry import FEATURES_BY_LAYER
from src.processing.scoring import calculate_layer_score

SEED = 42
REPEATS = 5
WARMUPS = 1
RATIO_LIMIT = 3.2


@dataclass
class BenchmarkResult:
    name: str
    small_seconds: float
    large_seconds: float
    scale_factor: float
    ratio: float
    ratio_limit: float
    ok: bool


def _time_call(fn: Callable[[], object], repeats: int = REPEATS, warmups: int = WARMUPS) -> float:
    for _ in range(warmups):
        fn()

    samples: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples)


def _build_scoring_df(rows: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + rows)
    features = FEATURES_BY_LAYER["employment_gravity"]
    data: dict[str, np.ndarray] = {}
    for feature in features:
        values = rng.random(rows)
        values[rng.random(rows) < 0.15] = np.nan
        data[f"{feature.name}_normalized"] = values
    return pd.DataFrame(data)


def _build_segregation_df(counties: int, tracts_per_county: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + counties + tracts_per_county)
    fips = np.repeat([f"{24000 + idx:05d}" for idx in range(1, counties + 1)], tracts_per_county)
    rows = len(fips)
    total_population = rng.integers(800, 6000, size=rows).astype(float)

    black_share = rng.uniform(0.05, 0.6, size=rows)
    hispanic_share = rng.uniform(0.03, 0.35, size=rows)
    minority_share = np.minimum(black_share + hispanic_share, 0.9)
    white_share = 1.0 - minority_share

    return pd.DataFrame(
        {
            "fips_code": fips,
            "pop_black_alone": (total_population * black_share).astype(int).astype(float),
            "pop_hispanic": (total_population * hispanic_share).astype(int).astype(float),
            "pop_white_alone": (total_population * white_share).astype(int).astype(float),
            "total_population": total_population,
        }
    )


def _build_vacancy_df(counties: int, years: list[int]) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + counties + len(years))
    records: list[dict[str, object]] = []
    for idx in range(counties):
        fips_code = f"{24000 + idx + 1:05d}"
        base = float(rng.uniform(0.03, 0.12))
        trend = float(rng.uniform(-0.01, 0.015))
        for year in years:
            vacancy_rate = base + trend * (year - years[0])
            vacancy_rate = max(0.0, min(1.0, vacancy_rate))
            # Leave last two years missing to exercise prediction path.
            if year >= years[-2]:
                vacancy = np.nan
            else:
                vacancy = vacancy_rate
            records.append(
                {
                    "fips_code": fips_code,
                    "data_year": int(year),
                    "vacancy_rate": vacancy,
                    "vacancy_source": pd.NA,
                }
            )
    return pd.DataFrame(records)


def _evaluate_scaling(
    name: str,
    small_fn: Callable[[], object],
    large_fn: Callable[[], object],
    ratio_limit: float = RATIO_LIMIT,
    scale_factor: float = 2.0,
) -> BenchmarkResult:
    small_seconds = _time_call(small_fn)
    large_seconds = _time_call(large_fn)
    ratio = large_seconds / max(small_seconds, 1e-9)
    return BenchmarkResult(
        name=name,
        small_seconds=small_seconds,
        large_seconds=large_seconds,
        scale_factor=scale_factor,
        ratio=ratio,
        ratio_limit=ratio_limit,
        ok=ratio <= ratio_limit,
    )


def run_benchmarks() -> list[BenchmarkResult]:
    scoring_small = _build_scoring_df(rows=40_000)
    scoring_large = _build_scoring_df(rows=80_000)

    segregation_small = _build_segregation_df(counties=300, tracts_per_county=24)
    segregation_large = _build_segregation_df(counties=600, tracts_per_county=24)

    years = list(range(2018, 2026))
    vacancy_small = _build_vacancy_df(counties=250, years=years)
    vacancy_large = _build_vacancy_df(counties=500, years=years)

    return [
        _evaluate_scaling(
            name="calculate_layer_score(employment_gravity)",
            small_fn=lambda: calculate_layer_score(scoring_small, "employment_gravity"),
            large_fn=lambda: calculate_layer_score(scoring_large, "employment_gravity"),
        ),
        _evaluate_scaling(
            name="compute_segregation_indices(layer5_demographic_equity)",
            small_fn=lambda: compute_segregation_indices(segregation_small),
            large_fn=lambda: compute_segregation_indices(segregation_large),
        ),
        _evaluate_scaling(
            name="apply_vacancy_predictions(layer5_demographics)",
            small_fn=lambda: apply_vacancy_predictions(vacancy_small),
            large_fn=lambda: apply_vacancy_predictions(vacancy_large),
        ),
    ]


def main() -> int:
    results = run_benchmarks()
    print(json.dumps([asdict(result) for result in results], indent=2))

    failed = [result for result in results if not result.ok]
    if failed:
        names = ", ".join(result.name for result in failed)
        print(f"Performance regression check failed: {names}")
        return 1

    print("Performance regression check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
