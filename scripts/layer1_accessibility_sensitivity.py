#!/usr/bin/env python3
"""
Layer 1 accessibility sensitivity analysis.

Runs threshold perturbations around the current Layer 1 accessibility settings and
publishes county-level classification stability for accessibility score bands:
- low:  < 0.30
- mid:  0.30 to < 0.60
- high: >= 0.60
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

# Ensure project root is on sys.path when run as a script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_settings
from src.ingest.layer1_economic_accessibility import (
    aggregate_lodes_to_tract,
    aggregate_to_county,
    compute_economic_accessibility,
    compute_sector_diversity,
    download_lodes_wac_segments,
    fetch_acs_demographics,
    fetch_tract_centroids,
    normalize_accessibility_scores,
)

settings = get_settings()


@dataclass
class ScenarioResult:
    name: str
    threshold_30_min: int
    threshold_45_min: int
    proxy_distance_30_km: float
    proxy_distance_45_km: float
    accessibility_method: str
    unchanged_counties: int
    unchanged_pct: float
    mean_abs_score_delta: float
    max_abs_score_delta: float


def _classify_accessibility(score: float) -> str:
    if pd.isna(score):
        return "missing"
    if score >= 0.60:
        return "high"
    if score < 0.30:
        return "low"
    return "mid"


def _compute_county_accessibility(
    tract_jobs: pd.DataFrame,
    tract_centroids: pd.DataFrame,
    acs_df: pd.DataFrame,
    mode: str,
    threshold_30_min: int,
    threshold_45_min: int,
    proxy_distance_30_km: float,
    proxy_distance_45_km: float,
) -> tuple[pd.DataFrame, str]:
    tract_df = compute_economic_accessibility(
        tract_jobs=tract_jobs,
        tract_centroids=tract_centroids,
        mode=mode,
        threshold_30_min=threshold_30_min,
        threshold_45_min=threshold_45_min,
        proxy_distance_30_km=proxy_distance_30_km,
        proxy_distance_45_km=proxy_distance_45_km,
    )

    if not acs_df.empty:
        tract_df = tract_df.merge(
            acs_df[["tract_geoid", "population", "working_age_pop", "labor_force_participation"]],
            on="tract_geoid",
            how="left",
        )
    tract_df["population"] = tract_df.get("population", 0).fillna(0).astype(int)
    tract_df["working_age_pop"] = tract_df.get("working_age_pop", 0).fillna(0).astype(int)
    tract_df["labor_force_participation"] = tract_df.get("labor_force_participation", 0).fillna(0)

    tract_df = normalize_accessibility_scores(tract_df)
    county_df = aggregate_to_county(tract_df)
    method = "unknown"
    if "accessibility_method" in tract_df.columns and not tract_df["accessibility_method"].empty:
        method = str(tract_df["accessibility_method"].mode().iloc[0])
    return county_df, method


def _evaluate(
    base: pd.DataFrame,
    scenario: pd.DataFrame,
) -> tuple[int, float, float, float]:
    merged = base.merge(
        scenario[["fips_code", "economic_accessibility_score"]],
        on="fips_code",
        how="inner",
        suffixes=("_base", "_scenario"),
    )
    merged["class_base"] = merged["economic_accessibility_score_base"].apply(_classify_accessibility)
    merged["class_scenario"] = merged["economic_accessibility_score_scenario"].apply(
        _classify_accessibility
    )
    merged["class_same"] = merged["class_base"] == merged["class_scenario"]
    merged["score_delta_abs"] = (
        merged["economic_accessibility_score_scenario"] - merged["economic_accessibility_score_base"]
    ).abs()
    unchanged_count = int(merged["class_same"].sum())
    total = len(merged)
    unchanged_pct = float((unchanged_count / total) * 100.0) if total else 0.0
    mean_abs_delta = float(merged["score_delta_abs"].mean()) if total else 0.0
    max_abs_delta = float(merged["score_delta_abs"].max()) if total else 0.0
    return unchanged_count, unchanged_pct, mean_abs_delta, max_abs_delta


def _build_report(
    output_path: Path,
    year: int,
    lodes_year: int,
    acs_year: int,
    mode: str,
    base_method: str,
    base_30: int,
    base_45: int,
    base_d30: float,
    base_d45: float,
    results: List[ScenarioResult],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    lines.append("# Layer 1 Accessibility Sensitivity Report")
    lines.append("")
    lines.append(f"- Generated: {date.today().isoformat()}")
    lines.append(f"- Data year: {year}")
    lines.append(f"- LODES year: {lodes_year}")
    lines.append(f"- ACS year: {acs_year}")
    lines.append(f"- Requested accessibility mode: `{mode}`")
    lines.append(f"- Effective base method: `{base_method}`")
    lines.append(
        f"- Base thresholds: {base_30}/{base_45} minutes, proxy {base_d30:.1f}/{base_d45:.1f} km"
    )
    lines.append(
        "- Commute-data calibration status: not performed (no observed statewide OD commute dataset configured in ingest)."
    )
    lines.append("")
    lines.append("## Classification Stability")
    lines.append("")
    lines.append(
        "| Scenario | Method | 30-min | 45-min | Proxy 30km | Proxy 45km | Unchanged counties | Stability % | Mean abs score delta | Max abs score delta |"
    )
    lines.append(
        "|----------|--------|--------|--------|------------|------------|--------------------|-------------|----------------------|--------------------|"
    )
    for r in results:
        lines.append(
            f"| {r.name} | `{r.accessibility_method}` | {r.threshold_30_min} | {r.threshold_45_min} | "
            f"{r.proxy_distance_30_km:.1f} | {r.proxy_distance_45_km:.1f} | {r.unchanged_counties} | "
            f"{r.unchanged_pct:.1f}% | {r.mean_abs_score_delta:.4f} | {r.max_abs_score_delta:.4f} |"
        )
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append(
        "- Higher stability % means county accessibility bands are less sensitive to threshold perturbation."
    )
    lines.append(
        "- This report evaluates Layer 1 accessibility score-band stability only; full six-layer directional classes may shift differently."
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Layer 1 accessibility sensitivity analysis")
    parser.add_argument("--year", type=int, default=None, help="As-of data year (default: latest)")
    parser.add_argument(
        "--mode",
        choices=["auto", "network", "proxy"],
        default=settings.LAYER1_ACCESSIBILITY_MODE,
        help="Accessibility impedance mode for baseline run",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output markdown path (default: docs/audits/layer1_accessibility_sensitivity_YYYY-MM-DD.md)",
    )
    args = parser.parse_args()

    latest_available_year = min(settings.LODES_LATEST_YEAR, settings.ACS_LATEST_YEAR) + 2
    year = args.year or latest_available_year
    if year > latest_available_year:
        year = latest_available_year
    lodes_year = min(year - 2, settings.LODES_LATEST_YEAR)
    acs_year = min(year - 2, settings.ACS_LATEST_YEAR)

    base_30 = int(settings.LAYER1_THRESHOLD_30_MINUTES)
    base_45 = int(settings.LAYER1_THRESHOLD_45_MINUTES)
    base_d30 = float(settings.LAYER1_PROXY_DISTANCE_30_KM)
    base_d45 = float(settings.LAYER1_PROXY_DISTANCE_45_KM)

    wac_df = download_lodes_wac_segments(lodes_year)
    tract_jobs = aggregate_lodes_to_tract(wac_df)
    tract_jobs = compute_sector_diversity(tract_jobs)
    tract_centroids = fetch_tract_centroids()
    acs_df = fetch_acs_demographics(acs_year)

    base_county, base_method = _compute_county_accessibility(
        tract_jobs=tract_jobs,
        tract_centroids=tract_centroids,
        acs_df=acs_df,
        mode=args.mode,
        threshold_30_min=base_30,
        threshold_45_min=base_45,
        proxy_distance_30_km=base_d30,
        proxy_distance_45_km=base_d45,
    )

    scenario_inputs: List[tuple[str, int, int, float, float]] = [("base", base_30, base_45, base_d30, base_d45)]
    if base_method.startswith("network_od"):
        scenario_inputs.extend(
            [
                ("minutes_minus_10", max(5, base_30 - 10), max(10, base_45 - 10), base_d30, base_d45),
                ("minutes_plus_10", base_30 + 10, base_45 + 10, base_d30, base_d45),
            ]
        )
    else:
        scenario_inputs.extend(
            [
                (
                    "km_minus_5",
                    base_30,
                    base_45,
                    max(1.0, base_d30 - 5.0),
                    max(max(1.0, base_d30 - 5.0), base_d45 - 5.0),
                ),
                ("km_plus_5", base_30, base_45, base_d30 + 5.0, base_d45 + 5.0),
            ]
        )

    results: List[ScenarioResult] = []
    for name, t30, t45, d30, d45 in scenario_inputs:
        county_df, scenario_method = _compute_county_accessibility(
            tract_jobs=tract_jobs,
            tract_centroids=tract_centroids,
            acs_df=acs_df,
            mode=args.mode,
            threshold_30_min=t30,
            threshold_45_min=t45,
            proxy_distance_30_km=d30,
            proxy_distance_45_km=d45,
        )
        unchanged_count, unchanged_pct, mean_abs_delta, max_abs_delta = _evaluate(base_county, county_df)
        results.append(
            ScenarioResult(
                name=name,
                threshold_30_min=t30,
                threshold_45_min=t45,
                proxy_distance_30_km=d30,
                proxy_distance_45_km=d45,
                accessibility_method=scenario_method,
                unchanged_counties=unchanged_count,
                unchanged_pct=unchanged_pct,
                mean_abs_score_delta=mean_abs_delta,
                max_abs_score_delta=max_abs_delta,
            )
        )

    output_path = (
        Path(args.output)
        if args.output
        else Path("docs/audits") / f"layer1_accessibility_sensitivity_{date.today().isoformat()}.md"
    )
    _build_report(
        output_path=output_path,
        year=year,
        lodes_year=lodes_year,
        acs_year=acs_year,
        mode=args.mode,
        base_method=base_method,
        base_30=base_30,
        base_45=base_45,
        base_d30=base_d30,
        base_d45=base_d45,
        results=results,
    )
    print(f"Wrote sensitivity report: {output_path}")


if __name__ == "__main__":
    main()
