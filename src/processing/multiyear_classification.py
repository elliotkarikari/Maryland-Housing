"""
Maryland Viability Atlas - Multi-Year Classification
Classifies counties using level + momentum + stability evidence

Implements:
- Directional Status: improving/stable/at_risk (considers momentum)
- Confidence Level: strong/conditional/fragile (considers data coverage)
- Final Synthesis Grouping: 5 categories with multi-year reasoning
"""

import json
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_db, log_refresh
from config.settings import get_settings
from src.utils.db_bulk import execute_batch
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Classification thresholds
THRESHOLD_IMPROVING_HIGH = 0.6
THRESHOLD_IMPROVING_LOW = 0.3
THRESHOLD_IMPROVING_MIN_LAYERS = 3

THRESHOLD_AT_RISK_LOW = 0.3
THRESHOLD_AT_RISK_COUNT = 2

THRESHOLD_RISK_DRAG_SEVERE = 0.5

# Multi-year thresholds
MOMENTUM_POSITIVE_THRESHOLD = 0.55  # Percentile threshold for positive momentum
COVERAGE_STRONG = 5  # Years needed for strong confidence
COVERAGE_CONDITIONAL = 3  # Years needed for conditional confidence


def load_layer_summary_scores(as_of_year: int = 2025) -> pd.DataFrame:
    """
    Load layer summary scores from database.

    Args:
        as_of_year: Year to load scores for

    Returns:
        DataFrame with layer scores pivoted wide (one row per geoid)
    """
    logger.info(f"Loading layer summary scores for {as_of_year}")

    with get_db() as db:
        query = text(
            """
            SELECT
                geoid,
                layer_name,
                layer_overall_score,
                layer_momentum_score,
                has_momentum,
                has_stability,
                coverage_years,
                missingness_penalty
            FROM layer_summary_scores
            WHERE as_of_year = :as_of_year
            ORDER BY geoid, layer_name
        """
        )

        query_result = db.execute(query, {"as_of_year": as_of_year})
        rows = query_result.fetchall()

    if not rows:
        logger.warning(f"No layer summary scores found for {as_of_year}")
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=[
            "geoid",
            "layer_name",
            "layer_overall_score",
            "layer_momentum_score",
            "has_momentum",
            "has_stability",
            "coverage_years",
            "missingness_penalty",
        ],
    )

    # Convert Decimal to float for all numeric columns
    numeric_cols = ["layer_overall_score", "layer_momentum_score", "missingness_penalty"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(f"Loaded {len(df)} layer score records")

    # Pivot to wide format (one row per geoid, columns for each layer)
    pivot_overall = df.pivot(index="geoid", columns="layer_name", values="layer_overall_score")
    pivot_overall.columns = [f"{col}_score" for col in pivot_overall.columns]

    pivot_momentum = df.pivot(index="geoid", columns="layer_name", values="layer_momentum_score")
    pivot_momentum.columns = [f"{col}_momentum" for col in pivot_momentum.columns]

    pivot_coverage = df.pivot(index="geoid", columns="layer_name", values="coverage_years")
    pivot_coverage.columns = [f"{col}_coverage" for col in pivot_coverage.columns]

    # Combine
    result = pivot_overall.join(pivot_momentum).join(pivot_coverage)
    result = result.reset_index()

    logger.info(f"Pivoted to {len(result)} geographies")

    return result


def classify_directional_status(row: pd.Series) -> str:
    """
    Classify directional status using multi-year evidence.

    Args:
        row: DataFrame row with layer scores

    Returns:
        'improving', 'stable', or 'at_risk'
    """
    # Extract layer overall scores
    layer_scores = {
        "employment": row.get("employment_gravity_score"),
        "mobility": row.get("mobility_optionality_score"),
        "schools": row.get("school_trajectory_score"),
        "housing": row.get("housing_elasticity_score"),
        "demographics": row.get("demographic_momentum_score"),
        "risk": row.get("risk_drag_score"),
    }

    # Extract momentum scores (if available)
    momentum_scores = {
        "employment": row.get("employment_gravity_momentum"),
        "mobility": row.get("mobility_optionality_momentum"),
        "schools": row.get("school_trajectory_momentum"),
        "housing": row.get("housing_elasticity_momentum"),
        "demographics": row.get("demographic_momentum_momentum"),
    }

    # Count valid scores
    valid_scores = {k: v for k, v in layer_scores.items() if pd.notna(v) and k != "risk"}
    valid_momentum = {k: v for k, v in momentum_scores.items() if pd.notna(v)}

    if len(valid_scores) < 3:
        # Insufficient data defaults to stable with high uncertainty
        return "stable"

    # IMPROVING: Multiple strong signals + positive momentum (if available)
    high_scores = [v for v in valid_scores.values() if v >= THRESHOLD_IMPROVING_HIGH]
    low_scores = [v for v in valid_scores.values() if v < THRESHOLD_IMPROVING_LOW]

    # Check momentum evidence
    positive_momentum = [v for v in valid_momentum.values() if v >= MOMENTUM_POSITIVE_THRESHOLD]
    has_positive_momentum_evidence = len(positive_momentum) >= 2

    if len(high_scores) >= THRESHOLD_IMPROVING_MIN_LAYERS and len(low_scores) <= 1:
        # Strong level scores
        if has_positive_momentum_evidence or len(valid_momentum) == 0:
            # Either has positive momentum OR no momentum data available (rely on level)
            return "improving"

    # AT RISK: Multiple weak signals OR severe risk drag
    risk_raw = layer_scores.get("risk")
    risk_score: Optional[float] = None
    if risk_raw is not None and pd.notna(risk_raw):
        risk_score = float(risk_raw)
    severe_risk = risk_score is not None and risk_score >= THRESHOLD_RISK_DRAG_SEVERE

    # Check for negative momentum
    negative_momentum = [
        v for v in valid_momentum.values() if v < (1 - MOMENTUM_POSITIVE_THRESHOLD)
    ]
    has_negative_momentum = len(negative_momentum) >= 2

    if len(low_scores) >= THRESHOLD_AT_RISK_COUNT or (severe_risk and len(low_scores) >= 1):
        return "at_risk"

    if has_negative_momentum:
        # Negative momentum trend despite OK levels
        return "at_risk"

    # STABLE: Everything else
    return "stable"


def classify_confidence_level(row: pd.Series) -> Tuple[str, List[str]]:
    """
    Classify confidence level based on data coverage and consistency.

    Args:
        row: DataFrame row with coverage info

    Returns:
        Tuple of (confidence_level, uncertainty_reasons)
    """
    # Check coverage across layers
    coverage_cols = [col for col in row.index if col.endswith("_coverage")]
    coverages = [row[col] for col in coverage_cols if pd.notna(row[col])]

    if not coverages:
        return "fragile", ["no_coverage_data"]

    avg_coverage = np.mean(coverages)
    min_coverage = np.min(coverages)

    uncertainty_reasons = []

    # STRONG: Mostly full coverage, few missing years
    if avg_coverage >= COVERAGE_STRONG and min_coverage >= COVERAGE_CONDITIONAL:
        return "strong", []

    # FRAGILE: Sparse data overall
    if avg_coverage < COVERAGE_CONDITIONAL:
        uncertainty_reasons.append("sparse_coverage")
        return "fragile", uncertainty_reasons

    # CONDITIONAL: Partial coverage
    if min_coverage < COVERAGE_CONDITIONAL:
        uncertainty_reasons.append("some_layers_sparse")

    return "conditional", uncertainty_reasons


def compute_composite_score(row: pd.Series) -> float:
    """
    Compute composite score across layers with risk drag penalty.

    Args:
        row: DataFrame row with layer scores

    Returns:
        Composite score (0-1)
    """
    # Get layer scores (excluding risk)
    layer_scores = []
    for layer in [
        "employment_gravity",
        "mobility_optionality",
        "school_trajectory",
        "housing_elasticity",
        "demographic_momentum",
    ]:
        score = row.get(f"{layer}_score")
        if pd.notna(score):
            layer_scores.append(score)

    if not layer_scores:
        return np.nan

    # Mean of available layers
    composite_raw = np.mean(layer_scores)

    # Apply risk drag as multiplicative penalty
    risk_score = row.get("risk_drag_score")
    if pd.notna(risk_score):
        # Risk drag reduces ceiling with a floor to prevent over-penalization
        risk_score = min(max(float(risk_score), 0.0), 1.0)
        risk_multiplier = max(1.0 - risk_score, settings.RISK_DRAG_PENALTY_FLOOR)
        composite_adjusted = composite_raw * risk_multiplier
    else:
        composite_adjusted = composite_raw

    return composite_adjusted


def determine_final_grouping(
    directional: str, confidence: str, uncertainty_reasons: List[str], composite_score: float
) -> str:
    """
    Determine final synthesis grouping.

    Args:
        directional: Directional status
        confidence: Confidence level
        uncertainty_reasons: List of uncertainty reasons
        composite_score: Overall composite score

    Returns:
        Final grouping name
    """
    # HIGH UNCERTAINTY takes precedence
    if confidence == "fragile" or len(uncertainty_reasons) >= 2:
        return "high_uncertainty"

    # AT RISK / HEADWINDS
    if directional == "at_risk":
        return "at_risk_headwinds"

    # IMPROVING path
    if directional == "improving":
        if confidence == "strong":
            return "emerging_tailwinds"
        else:
            return "conditional_growth"

    # STABLE path
    # Distinguish "stable constrained" (consistent but low upside) from volatile
    if directional == "stable":
        # If composite score is low-mid and confidence conditional
        # → stable but constrained
        return "stable_constrained"

    # Default fallback
    return "high_uncertainty"


def classify_all_counties(as_of_year: int = 2025) -> pd.DataFrame:
    """
    Classify all counties using multi-year evidence.

    Args:
        as_of_year: Year to classify for

    Returns:
        DataFrame with classifications
    """
    logger.info("=" * 70)
    logger.info("MULTI-YEAR CLASSIFICATION")
    logger.info("=" * 70)
    logger.info(f"As of year: {as_of_year}")

    # Load layer scores
    df = load_layer_summary_scores(as_of_year)

    if df.empty:
        logger.error("No layer scores available")
        return df

    # Classify each county
    df["directional_status"] = df.apply(classify_directional_status, axis=1)
    df[["confidence_level", "uncertainty_reasons"]] = df.apply(
        lambda row: pd.Series(classify_confidence_level(row)), axis=1
    )
    df["composite_score"] = df.apply(compute_composite_score, axis=1)
    df["final_grouping"] = df.apply(
        lambda row: determine_final_grouping(
            row["directional_status"],
            row["confidence_level"],
            row["uncertainty_reasons"],
            row["composite_score"],
        ),
        axis=1,
    )

    # Add as_of_year
    df["current_as_of_year"] = as_of_year

    # Log distribution
    logger.info("\nClassification distribution:")
    logger.info(f"  Directional status:")
    for status in ["improving", "stable", "at_risk"]:
        count = (df["directional_status"] == status).sum()
        logger.info(f"    {status}: {count} counties")

    logger.info(f"\n  Final synthesis grouping:")
    for grouping in df["final_grouping"].unique():
        count = (df["final_grouping"] == grouping).sum()
        logger.info(f"    {grouping}: {count} counties")

    return df


def store_final_synthesis(df: pd.DataFrame):
    """
    Store final synthesis classifications to database.

    Args:
        df: DataFrame with classifications
    """
    logger.info(f"Storing {len(df)} final synthesis records")

    with get_db() as db:
        # Delete existing records
        db.execute(text("DELETE FROM final_synthesis_current"))

        # Insert new records
        insert_sql = text(
            """
            INSERT INTO final_synthesis_current (
                geoid, current_as_of_year,
                final_grouping, directional_status, confidence_level,
                uncertainty_level, uncertainty_reasons,
                composite_score, risk_drag_applied,
                employment_gravity_score, mobility_optionality_score,
                school_trajectory_score, housing_elasticity_score,
                demographic_momentum_score, risk_drag_score,
                classification_version
            ) VALUES (
                :geoid, :current_as_of_year,
                :final_grouping, :directional_status, :confidence_level,
                :uncertainty_level, CAST(:uncertainty_reasons AS jsonb),
                :composite_score, :risk_drag_applied,
                :employment_gravity_score, :mobility_optionality_score,
                :school_trajectory_score, :housing_elasticity_score,
                :demographic_momentum_score, :risk_drag_score,
                :classification_version
            )
        """
        )

        rows = []
        for _, row in df.iterrows():
            # Determine uncertainty level from reasons
            n_reasons = len(row["uncertainty_reasons"])
            if n_reasons == 0:
                uncertainty_level = "low"
            elif n_reasons == 1:
                uncertainty_level = "medium"
            else:
                uncertainty_level = "high"

            row_dict = {
                "geoid": row["geoid"],
                "current_as_of_year": int(row["current_as_of_year"]),
                "final_grouping": row["final_grouping"],
                "directional_status": row["directional_status"],
                "confidence_level": row["confidence_level"],
                "uncertainty_level": uncertainty_level,
                "uncertainty_reasons": json.dumps(row["uncertainty_reasons"]),
                "composite_score": (
                    float(row["composite_score"]) if pd.notna(row["composite_score"]) else None
                ),
                "risk_drag_applied": (
                    float(row.get("risk_drag_score", 0))
                    if pd.notna(row.get("risk_drag_score"))
                    else None
                ),
                "employment_gravity_score": (
                    float(row.get("employment_gravity_score"))
                    if pd.notna(row.get("employment_gravity_score"))
                    else None
                ),
                "mobility_optionality_score": (
                    float(row.get("mobility_optionality_score"))
                    if pd.notna(row.get("mobility_optionality_score"))
                    else None
                ),
                "school_trajectory_score": (
                    float(row.get("school_trajectory_score"))
                    if pd.notna(row.get("school_trajectory_score"))
                    else None
                ),
                "housing_elasticity_score": (
                    float(row.get("housing_elasticity_score"))
                    if pd.notna(row.get("housing_elasticity_score"))
                    else None
                ),
                "demographic_momentum_score": (
                    float(row.get("demographic_momentum_score"))
                    if pd.notna(row.get("demographic_momentum_score"))
                    else None
                ),
                "risk_drag_score": (
                    float(row.get("risk_drag_score"))
                    if pd.notna(row.get("risk_drag_score"))
                    else None
                ),
                "classification_version": "v2.0-multiyear",
            }

            rows.append(row_dict)

        execute_batch(db, insert_sql, rows, chunk_size=1000)

        db.commit()

    logger.info("✓ Final synthesis stored successfully")


def main():
    """Main execution for multi-year classification"""
    try:
        df = classify_all_counties(as_of_year=2025)

        if not df.empty:
            store_final_synthesis(df)

            # Log refresh
            log_refresh(
                layer_name="final_synthesis_current",
                data_source="layer_summary_scores",
                status="success",
                records_processed=len(df),
                records_inserted=len(df),
                metadata={"classification_version": "v2.0-multiyear"},
            )

            logger.info("=" * 70)
            logger.info(f"✓ Multi-year classification complete: {len(df)} counties")
            logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Multi-year classification failed: {e}", exc_info=True)
        log_refresh(
            layer_name="final_synthesis_current",
            data_source="layer_summary_scores",
            status="error",
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    main()
