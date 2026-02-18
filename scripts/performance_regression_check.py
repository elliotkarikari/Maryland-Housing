#!/usr/bin/env python3
"""Lightweight performance regression checks for hot dataframe paths."""

from __future__ import annotations

import json
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingest.layer5_demographic_equity import compute_segregation_indices
from src.ingest.layer5_demographics import _apply_momentum_features
from src.processing.feature_registry import FEATURES_BY_LAYER
from src.processing.scoring import calculate_layer_score

SEED = 42
REPEATS = 5
WARMUPS = 1


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
        missing_mask = rng.random(rows) < 0.15
        values[missing_mask] = np.nan
        data[f"{feature.name}_normalized"] = values
    return pd.DataFrame(data)


def _build_momentum_df(counties: int, years: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + counties + years)
    fips_codes = [f"{24000 + idx:05d}" for idx in range(1, counties + 1)]
    year_values = np.arange(2016, 2016 + years)
    index = pd.MultiIndex.from_product([fips_codes, year_values], names=["fips_code", "data_year"])
    total_rows = len(index)

    return pd.DataFrame(
        {
            "fips_code": index.get_level_values("fips_code"),
            "data_year": index.get_level_values("data_year"),
            "pop_age_25_44": rng.integers(5_000, 100_000, size=total_rows).astype(float),
            "households_total": rng.integers(2_000, 50_000, size=total_rows).astype(float),
        }
    )


def _build_segregation_df(counties: int, tracts_per_county: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + counties + tracts_per_county)
    fips_codes = np.repeat(
        [f"{24000 + idx:05d}" for idx in range(1, counties + 1)], tracts_per_county
    )
    rows = len(fips_codes)

    total_population = rng.integers(800, 6_000, size=rows).astype(float)
    black_share = rng.uniform(0.05, 0.6, size=rows)
    hispanic_share = rng.uniform(0.03, 0.35, size=rows)
    combined_share = np.minimum(black_share + hispanic_share, 0.9)
    white_share = 1.0 - combined_share

    pop_black = (total_population * black_share).astype(int).astype(float)
    pop_hispanic = (total_population * hispanic_share).astype(int).astype(float)
    pop_white = (total_population * white_share).astype(int).astype(float)

    return pd.DataFrame(
        {
            "fips_code": fips_codes,
            "pop_black_alone": pop_black,
            "pop_hispanic": pop_hispanic,
            "pop_white_alone": pop_white,
            "total_population": total_population,
        }
    )


def _evaluate_scaling(
    name: str,
    small_fn: Callable[[], object],
    large_fn: Callable[[], object],
    ratio_limit: float,
    scale_factor: float = 2.0,
) -> BenchmarkResult:
    small_seconds = _time_call(small_fn)
    large_seconds = _time_call(large_fn)
    ratio = large_seconds / max(small_seconds, 1e-9)
    ok = ratio <= ratio_limit
    return BenchmarkResult(
        name=name,
        small_seconds=small_seconds,
        large_seconds=large_seconds,
        scale_factor=scale_factor,
        ratio=ratio,
        ratio_limit=ratio_limit,
        ok=ok,
    )


def run_benchmarks() -> list[BenchmarkResult]:
    scoring_small = _build_scoring_df(rows=40_000)
    scoring_large = _build_scoring_df(rows=80_000)

    momentum_small = _build_momentum_df(counties=600, years=8)  # 4,800 rows
    momentum_large = _build_momentum_df(counties=1_200, years=8)  # 9,600 rows

    segregation_small = _build_segregation_df(counties=300, tracts_per_county=24)  # 7,200 rows
    segregation_large = _build_segregation_df(counties=600, tracts_per_county=24)  # 14,400 rows

    return [
        _evaluate_scaling(
            name="calculate_layer_score(employment_gravity)",
            small_fn=lambda: calculate_layer_score(scoring_small, "employment_gravity"),
            large_fn=lambda: calculate_layer_score(scoring_large, "employment_gravity"),
            ratio_limit=3.2,
        ),
        _evaluate_scaling(
            name="_apply_momentum_features(layer5_demographics)",
            small_fn=lambda: _apply_momentum_features(momentum_small),
            large_fn=lambda: _apply_momentum_features(momentum_large),
            ratio_limit=3.2,
        ),
        _evaluate_scaling(
            name="compute_segregation_indices(layer5_demographic_equity)",
            small_fn=lambda: compute_segregation_indices(segregation_small),
            large_fn=lambda: compute_segregation_indices(segregation_large),
            ratio_limit=3.2,
        ),
    ]


def main() -> int:
    results = run_benchmarks()
    print(json.dumps([asdict(result) for result in results], indent=2))

    failed = [result for result in results if not result.ok]
    if failed:
        failed_names = ", ".join(result.name for result in failed)
        print(f"Performance regression check failed: {failed_names}")
        return 1

    print("Performance regression check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
