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


def _get_latest_year() -> Optional[int]:
    with get_db() as db:
        return db.execute(text("SELECT MAX(data_year) FROM layer6_risk_drag")).scalar()


def _get_county_count() -> int:
    with get_db() as db:
        return db.execute(text("SELECT COUNT(*) FROM md_counties")).scalar()


def run() -> EvalResult:
    details: List[str] = []
    latest_year = _get_latest_year()
    if not latest_year:
        return EvalResult(
            name="layer6_risk_quality",
            passed=False,
            details=["No data found in layer6_risk_drag."]
        )

    county_count = _get_county_count()

    with get_db() as db:
        count = db.execute(
            text("""
                SELECT COUNT(DISTINCT fips_code)
                FROM layer6_risk_drag
                WHERE data_year = :year
            """),
            {"year": latest_year}
        ).scalar()

        if count != county_count:
            details.append(
                f"Expected {county_count} counties for {latest_year}, found {count}."
            )

        required_cols = [
            "risk_drag_index",
            "modern_vulnerability_score",
            "static_risk_score",
            "climate_projection_score",
            "vulnerability_score",
            "resilience_deficit_score",
            "adaptive_capacity_index",
            "pollution_burden_score",
            "social_vulnerability_index",
        ]
        null_checks = " OR ".join([f"{col} IS NULL" for col in required_cols])
        null_count = db.execute(
            text(f"""
                SELECT COUNT(*)
                FROM layer6_risk_drag
                WHERE data_year = :year
                  AND ({null_checks})
            """),
            {"year": latest_year}
        ).scalar()

        if null_count:
            details.append(
                f"{null_count} county rows have nulls in required risk fields."
            )

        range_cols = [
            "risk_drag_index",
            "modern_vulnerability_score",
            "static_risk_score",
            "climate_projection_score",
            "vulnerability_score",
            "resilience_deficit_score",
            "adaptive_capacity_index",
            "pollution_burden_score",
            "social_vulnerability_index",
        ]
        range_checks = " OR ".join(
            [f"({col} < 0 OR {col} > 1)" for col in range_cols]
        )
        out_of_range = db.execute(
            text(f"""
                SELECT COUNT(*)
                FROM layer6_risk_drag
                WHERE data_year = :year
                  AND ({range_checks})
            """),
            {"year": latest_year}
        ).scalar()

        if out_of_range:
            details.append(
                f"{out_of_range} county rows have 0-1 score violations."
            )

    passed = len(details) == 0
    if passed:
        details.append(f"Layer6 risk_vulnerability data OK for {latest_year}.")

    return EvalResult(
        name="layer6_risk_quality",
        passed=passed,
        details=details,
        metrics={
            "latest_year": latest_year,
            "county_rows": count,
            "county_expected": county_count,
            "null_required_rows": null_count,
            "out_of_range_rows": out_of_range,
        }
    )
