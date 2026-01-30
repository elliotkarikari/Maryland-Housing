from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from config.database import get_db


@dataclass
class EvalResult:
    name: str
    passed: bool
    details: List[str]
    metrics: Dict[str, Any] = field(default_factory=dict)


LAYER_CONFIGS: Dict[str, str] = {
    "employment_gravity": "layer1_employment_gravity",
    "mobility_optionality": "layer2_mobility_optionality",
    "school_trajectory": "layer3_school_trajectory",
    "housing_elasticity": "layer4_housing_elasticity",
    "demographic_momentum": "layer5_demographic_momentum",
    "risk_drag": "layer6_risk_drag",
}


def _get_latest_as_of_year() -> Optional[int]:
    with get_db() as db:
        return db.execute(text("SELECT MAX(as_of_year) FROM layer_timeseries_features")).scalar()


def _get_county_count() -> int:
    with get_db() as db:
        return db.execute(text("SELECT COUNT(*) FROM md_counties")).scalar()


def run(min_coverage_ratio: float = 0.8) -> EvalResult:
    details: List[str] = []
    latest_year = _get_latest_as_of_year()
    if not latest_year:
        return EvalResult(
            name="timeseries_regression",
            passed=False,
            details=["No data found in layer_timeseries_features."]
        )

    county_count = _get_county_count()

    counts_by_layer: Dict[str, int] = {}
    coverage_by_layer: Dict[str, float] = {}

    with get_db() as db:
        for layer_name in LAYER_CONFIGS.keys():
            count = db.execute(
                text("""
                    SELECT COUNT(*)
                    FROM layer_timeseries_features
                    WHERE as_of_year = :year
                      AND layer_name = :layer
                """),
                {"year": latest_year, "layer": layer_name}
            ).scalar()
            counts_by_layer[layer_name] = int(count or 0)
            if count != county_count:
                details.append(
                    f"{layer_name}: expected {county_count} rows, found {count}."
                )

            coverage = db.execute(
                text("""
                    SELECT AVG(CASE WHEN coverage_years >= 3 THEN 1 ELSE 0 END)
                    FROM layer_timeseries_features
                    WHERE as_of_year = :year
                      AND layer_name = :layer
                """),
                {"year": latest_year, "layer": layer_name}
            ).scalar()
            coverage = float(coverage) if coverage is not None else 0.0
            coverage_by_layer[layer_name] = coverage
            if coverage < min_coverage_ratio:
                details.append(
                    f"{layer_name}: only {coverage:.0%} have >=3 years coverage."
                )

        out_of_range = db.execute(
            text("""
                SELECT COUNT(*)
                FROM layer_summary_scores
                WHERE as_of_year = :year
                  AND (layer_overall_score < 0 OR layer_overall_score > 1)
            """),
            {"year": latest_year}
        ).scalar()
        if out_of_range:
            details.append(
                f"{out_of_range} layer_summary_scores outside 0-1 range."
            )

        null_scores = db.execute(
            text("""
                SELECT COUNT(*)
                FROM layer_summary_scores
                WHERE as_of_year = :year
                  AND layer_overall_score IS NULL
            """),
            {"year": latest_year}
        ).scalar()
        if null_scores:
            details.append(
                f"{null_scores} layer_summary_scores missing layer_overall_score."
            )

    passed = len(details) == 0
    if passed:
        details.append(f"Timeseries regression OK for as_of_year {latest_year}.")

    return EvalResult(
        name="timeseries_regression",
        passed=passed,
        details=details,
        metrics={
            "latest_as_of_year": latest_year,
            "county_expected": county_count,
            "counts_by_layer": counts_by_layer,
            "coverage_ratio_by_layer": coverage_by_layer,
            "summary_out_of_range_rows": out_of_range,
            "summary_null_rows": null_scores,
        }
    )
