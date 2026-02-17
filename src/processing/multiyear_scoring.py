"""
Maryland Viability Atlas - Multi-Year Scoring
Normalizes level, momentum, stability features and computes composite layer scores

Implements deterministic weighted composition:
- Level: Current position (0-1 percentile)
- Momentum: Trend direction (0-1 percentile, if available)
- Stability: Consistency (0-1 score, if available)

Composition: 0.5*level + 0.3*momentum + 0.2*stability
"""

from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_db, log_refresh, table_name
from config.settings import get_settings
from src.utils.db_bulk import execute_batch
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Scoring weights
WEIGHTS_FULL = {"level": 0.50, "momentum": 0.30, "stability": 0.20}

WEIGHTS_NO_MOMENTUM = {"level": 1.00, "momentum": 0.00, "stability": 0.00}

# Missingness threshold
COVERAGE_THRESHOLD_FULL = 5  # 5 years for no penalty
COVERAGE_THRESHOLD_PARTIAL = 3  # 3 years for reduced penalty


def percentile_normalize(series: pd.Series) -> pd.Series:
    """
    Normalize values to 0-1 using percentile rank.

    Args:
        series: Pandas Series with values to normalize

    Returns:
        Series with percentile ranks (0-1)
    """
    # Drop NaN values for ranking
    valid_mask = series.notna()
    if valid_mask.sum() == 0:
        return pd.Series([np.nan] * len(series), index=series.index)

    # Rank only valid values
    ranks = series[valid_mask].rank(method="average", pct=True)

    # Create result series with NaN for invalid
    result = pd.Series([np.nan] * len(series), index=series.index)
    result[valid_mask] = ranks

    return result


def invert_score(score: float) -> float:
    """
    Invert a 0-1 score (for metrics where lower is better).

    Args:
        score: Original score (0-1)

    Returns:
        Inverted score (1 - score)
    """
    if pd.isna(score):
        return np.nan
    return 1.0 - score


def calculate_missingness_penalty(coverage_years: int, window_size: int = 5) -> float:
    """
    Calculate penalty for missing data.

    Args:
        coverage_years: Number of years with actual data
        window_size: Expected window size

    Returns:
        Penalty value (0 = no penalty, 1 = severe penalty)
    """
    if coverage_years >= COVERAGE_THRESHOLD_FULL:
        return 0.0  # No penalty
    elif coverage_years >= COVERAGE_THRESHOLD_PARTIAL:
        # Linear penalty between 3-5 years
        return (
            0.2
            * (COVERAGE_THRESHOLD_FULL - coverage_years)
            / (COVERAGE_THRESHOLD_FULL - COVERAGE_THRESHOLD_PARTIAL)
        )
    else:
        # High penalty for <3 years
        return (
            0.5 + 0.3 * (COVERAGE_THRESHOLD_PARTIAL - coverage_years) / COVERAGE_THRESHOLD_PARTIAL
        )


def load_timeseries_features(as_of_year: int = 2025) -> pd.DataFrame:
    """
    Load timeseries features from database.

    Args:
        as_of_year: Year to load features for

    Returns:
        DataFrame with all timeseries features
    """
    logger.info(f"Loading timeseries features for {as_of_year}")

    with get_db() as db:
        query = text(
            f"""
            SELECT
                geoid,
                layer_name,
                as_of_year,
                level_latest,
                level_baseline,
                momentum_slope,
                momentum_delta,
                momentum_percent_change,
                stability_volatility,
                stability_cv,
                stability_consistency,
                stability_persistence,
                coverage_years,
                min_year,
                max_year
            FROM {table_name('layer_timeseries_features')}
            WHERE as_of_year = :as_of_year
            ORDER BY geoid, layer_name
        """
        )

        result = db.execute(query, {"as_of_year": as_of_year})
        rows = result.fetchall()

    if not rows:
        logger.warning(f"No timeseries features found for {as_of_year}")
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=[
            "geoid",
            "layer_name",
            "as_of_year",
            "level_latest",
            "level_baseline",
            "momentum_slope",
            "momentum_delta",
            "momentum_percent_change",
            "stability_volatility",
            "stability_cv",
            "stability_consistency",
            "stability_persistence",
            "coverage_years",
            "min_year",
            "max_year",
        ],
    )

    # Convert Decimal to float for numeric operations
    numeric_cols = [
        "level_latest",
        "level_baseline",
        "momentum_slope",
        "momentum_delta",
        "momentum_percent_change",
        "stability_volatility",
        "stability_cv",
        "stability_consistency",
        "stability_persistence",
        "coverage_years",
        "min_year",
        "max_year",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(f"Loaded {len(df)} timeseries feature records")
    return df


def normalize_layer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize level, momentum, stability per layer across all geographies.

    Args:
        df: DataFrame with timeseries features

    Returns:
        DataFrame with added normalized score columns
    """
    logger.info("Normalizing features per layer")

    df = df.copy()

    # Initialize score columns
    df["layer_level_score"] = np.nan
    df["layer_momentum_score"] = np.nan
    df["layer_stability_score"] = np.nan

    # Normalize per layer
    for layer in df["layer_name"].unique():
        layer_mask = df["layer_name"] == layer

        # LEVEL: Percentile of level_latest (higher is better)
        df.loc[layer_mask, "layer_level_score"] = percentile_normalize(
            df.loc[layer_mask, "level_latest"]
        )

        # MOMENTUM: Percentile of momentum_slope (higher is better)
        # Only normalize if momentum exists for this layer
        momentum_valid = df.loc[layer_mask, "momentum_slope"].notna()
        if momentum_valid.any():
            df.loc[layer_mask & momentum_valid, "layer_momentum_score"] = percentile_normalize(
                df.loc[layer_mask & momentum_valid, "momentum_slope"]
            )

        # STABILITY: Derived from consistency (higher consistency = better stability)
        # consistency ranges 0-1, so we can use it directly as stability score
        stability_valid = df.loc[layer_mask, "stability_consistency"].notna()
        if stability_valid.any():
            df.loc[layer_mask & stability_valid, "layer_stability_score"] = df.loc[
                layer_mask & stability_valid, "stability_consistency"
            ]

        logger.debug(
            f"  {layer}: level={layer_mask.sum()}, "
            f"momentum={momentum_valid.sum()}, "
            f"stability={stability_valid.sum()}"
        )

    return df


def compute_composite_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute composite layer scores using weighted combination.

    Args:
        df: DataFrame with normalized scores

    Returns:
        DataFrame with composite scores and metadata
    """
    logger.info("Computing composite layer scores")

    df = df.copy()

    # Determine which components are available
    df["has_momentum"] = df["layer_momentum_score"].notna()
    df["has_stability"] = df["layer_stability_score"].notna()

    # Calculate missingness penalty
    df["missingness_penalty"] = df["coverage_years"].apply(calculate_missingness_penalty)

    # Compute composite score
    def compute_weighted_score(row):
        level = row["layer_level_score"]
        momentum = row["layer_momentum_score"]
        stability = row["layer_stability_score"]

        # Handle missing components
        if pd.isna(level):
            return np.nan

        # Full composition if all components available
        if pd.notna(momentum) and pd.notna(stability):
            weights = WEIGHTS_FULL
            score = (
                weights["level"] * level
                + weights["momentum"] * momentum
                + weights["stability"] * stability
            )
            used_weights = weights
        # Level + momentum only
        elif pd.notna(momentum):
            # Reweight: 0.625 level + 0.375 momentum (same ratio as 0.5:0.3)
            score = 0.625 * level + 0.375 * momentum
            used_weights = {"level": 0.625, "momentum": 0.375, "stability": 0.0}
        # Level only
        else:
            score = level
            used_weights = WEIGHTS_NO_MOMENTUM

        # Apply missingness penalty by reducing score slightly
        # Penalty reduces effective score but doesn't eliminate it
        penalty = row["missingness_penalty"]
        if penalty > 0:
            score = score * (1 - 0.5 * penalty)  # Max 50% reduction for full penalty

        return score, used_weights

    # Apply to all rows
    results = df.apply(compute_weighted_score, axis=1, result_type="expand")
    df["layer_overall_score"] = results[0]
    df["weights_used"] = results[1]

    logger.info(f"Computed {df['layer_overall_score'].notna().sum()} composite scores")

    return df


def store_layer_summary_scores(df: pd.DataFrame):
    """
    Store layer summary scores to database.

    Args:
        df: DataFrame with computed scores
    """
    logger.info(f"Storing {len(df)} layer summary scores")

    with get_db() as db:
        # Delete existing scores for this as_of_year
        as_of_year = df["as_of_year"].iloc[0]
        delete_sql = text(
            f"""
            DELETE FROM {table_name('layer_summary_scores')}
            WHERE as_of_year = :as_of_year
        """
        )
        db.execute(delete_sql, {"as_of_year": int(as_of_year)})

        # Insert new scores
        insert_sql = text(
            f"""
            INSERT INTO {table_name('layer_summary_scores')} (
                geoid, layer_name, as_of_year,
                layer_level_score, layer_momentum_score, layer_stability_score,
                layer_overall_score,
                missingness_penalty,
                has_momentum, has_stability,
                coverage_years,
                weights, normalization_method
            ) VALUES (
                :geoid, :layer_name, :as_of_year,
                :layer_level_score, :layer_momentum_score, :layer_stability_score,
                :layer_overall_score,
                :missingness_penalty,
                :has_momentum, :has_stability,
                :coverage_years,
                CAST(:weights AS jsonb), :normalization_method
            )
        """
        )

        import json

        rows = []
        for _, row in df.iterrows():
            row_dict = {
                "geoid": row["geoid"],
                "layer_name": row["layer_name"],
                "as_of_year": int(row["as_of_year"]),
                "layer_level_score": (
                    float(row["layer_level_score"]) if pd.notna(row["layer_level_score"]) else None
                ),
                "layer_momentum_score": (
                    float(row["layer_momentum_score"])
                    if pd.notna(row["layer_momentum_score"])
                    else None
                ),
                "layer_stability_score": (
                    float(row["layer_stability_score"])
                    if pd.notna(row["layer_stability_score"])
                    else None
                ),
                "layer_overall_score": (
                    float(row["layer_overall_score"])
                    if pd.notna(row["layer_overall_score"])
                    else None
                ),
                "missingness_penalty": float(row["missingness_penalty"]),
                "has_momentum": bool(row["has_momentum"]),
                "has_stability": bool(row["has_stability"]),
                "coverage_years": int(row["coverage_years"]),
                "weights": json.dumps(row["weights_used"]),
                "normalization_method": "percentile_rank",
            }
            rows.append(row_dict)

        execute_batch(db, insert_sql, rows, chunk_size=1000)

        db.commit()

    logger.info("✓ Layer summary scores stored successfully")


def compute_all_layer_scores(as_of_year: int = 2025) -> pd.DataFrame:
    """
    Main function to compute all layer summary scores.

    Args:
        as_of_year: Year to compute scores for

    Returns:
        DataFrame with computed scores
    """
    logger.info("=" * 70)
    logger.info("MULTI-YEAR LAYER SCORING")
    logger.info("=" * 70)
    logger.info(f"As of year: {as_of_year}")

    # Load timeseries features
    df = load_timeseries_features(as_of_year)

    if df.empty:
        logger.error("No timeseries features available")
        return df

    # Normalize features
    df = normalize_layer_features(df)

    # Compute composite scores
    df = compute_composite_scores(df)

    # Store results
    store_layer_summary_scores(df)

    # Log summary statistics
    logger.info("\nScoring summary by layer:")
    summary = (
        df.groupby("layer_name")
        .agg(
            {
                "layer_overall_score": ["mean", "min", "max"],
                "has_momentum": "sum",
                "has_stability": "sum",
                "coverage_years": "mean",
            }
        )
        .round(3)
    )

    for layer in summary.index:
        stats = summary.loc[layer]
        logger.info(f"  {layer}:")
        logger.info(
            f"    Overall score: {stats[('layer_overall_score', 'mean')]:.3f} "
            f"(min={stats[('layer_overall_score', 'min')]:.3f}, "
            f"max={stats[('layer_overall_score', 'max')]:.3f})"
        )
        logger.info(f"    Has momentum: {int(stats[('has_momentum', 'sum')])} counties")
        logger.info(f"    Has stability: {int(stats[('has_stability', 'sum')])} counties")
        logger.info(f"    Avg coverage: {stats[('coverage_years', 'mean')]:.1f} years")

    # Log refresh
    log_refresh(
        layer_name="layer_summary_scores",
        data_source="timeseries_features",
        status="success",
        records_processed=len(df),
        records_inserted=len(df),
        metadata={"as_of_year": as_of_year},
    )

    logger.info("=" * 70)
    logger.info(f"✓ Multi-year scoring complete: {len(df)} scores computed")
    logger.info("=" * 70)

    return df


def main():
    """Main execution for multi-year scoring"""
    try:
        df = compute_all_layer_scores(as_of_year=2025)

        if not df.empty:
            logger.info("✓ Layer scoring completed successfully")

    except Exception as e:
        logger.error(f"Multi-year scoring failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer_summary_scores",
            data_source="timeseries_features",
            status="error",
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    main()
