"""
Maryland Viability Atlas - Layer Scoring
Aggregates normalized features into layer scores

Rules:
- Equal weighting within each layer (can override with feature.weight)
- Risk Drag is SUBTRACTIVE only (penalty)
- All scores 0-1
- Missing features reduce confidence but don't break scoring
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from sqlalchemy import text

from config.database import get_db, log_refresh
from config.settings import get_settings
from src.processing.feature_registry import (
    FEATURES_BY_LAYER,
    LAYER_DEFINITIONS,
    get_primary_features
)
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def calculate_layer_score(
    df: pd.DataFrame,
    layer_name: str,
    use_weights: bool = True
) -> Tuple[pd.Series, pd.Series]:
    """
    Calculate aggregated score for a layer from normalized features.

    Args:
        df: DataFrame with normalized features (*_normalized columns)
        layer_name: Name of layer
        use_weights: If True, use feature weights; otherwise equal weights

    Returns:
        Tuple of (scores, coverage) where coverage is 0-1 indicating data completeness
    """
    features = FEATURES_BY_LAYER.get(layer_name, [])

    if not features:
        logger.warning(f"No features defined for layer {layer_name}")
        return pd.Series(np.nan, index=df.index), pd.Series(0.0, index=df.index)

    # Collect normalized feature columns
    feature_cols = [f"{f.name}_normalized" for f in features]
    available_cols = [col for col in feature_cols if col in df.columns]

    if not available_cols:
        logger.warning(f"No normalized features found for layer {layer_name}")
        return pd.Series(np.nan, index=df.index), pd.Series(0.0, index=df.index)

    # Get weights
    if use_weights:
        weights = {
            f"{f.name}_normalized": f.weight
            for f in features
            if f"{f.name}_normalized" in available_cols
        }
    else:
        weights = {col: 1.0 for col in available_cols}

    # Calculate weighted average
    scores = []
    coverage = []

    for _, row in df.iterrows():
        values = []
        wts = []

        for col in available_cols:
            val = row[col]
            if pd.notna(val):
                values.append(val)
                wts.append(weights[col])

        if values:
            # Weighted average
            score = np.average(values, weights=wts)
            cov = len(values) / len(available_cols)  # Feature coverage
        else:
            score = np.nan
            cov = 0.0

        scores.append(score)
        coverage.append(cov)

    return pd.Series(scores, index=df.index), pd.Series(coverage, index=df.index)


def calculate_all_layer_scores(
    normalized_layers: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Calculate scores for all layers.

    Args:
        normalized_layers: Dict of layer_name -> normalized features DataFrame

    Returns:
        DataFrame with fips_code and all layer scores + coverage metrics
    """
    logger.info("Calculating layer scores")

    # Start with fips_code list from first available layer
    result_df = None

    for layer_name, norm_df in normalized_layers.items():
        if norm_df.empty:
            continue

        if result_df is None:
            result_df = norm_df[['fips_code', 'data_year']].copy()

        # Calculate layer score
        score, coverage = calculate_layer_score(norm_df, layer_name)

        result_df[f"{layer_name}_score"] = score
        result_df[f"{layer_name}_coverage"] = coverage

        logger.info(
            f"Layer {layer_name}: "
            f"mean_score={score.mean():.3f}, "
            f"mean_coverage={coverage.mean():.3f}"
        )

    if result_df is None:
        logger.error("No layer data available for scoring")
        return pd.DataFrame()

    return result_df


def calculate_composite_score(
    layer_scores_df: pd.DataFrame,
    include_risk_drag: bool = True
) -> pd.Series:
    """
    Calculate composite score across all layers.

    Risk Drag is SUBTRACTIVE, not additive.

    Args:
        layer_scores_df: DataFrame with layer scores
        include_risk_drag: If True, apply risk drag penalty

    Returns:
        Series of composite scores (0-1)
    """
    logger.info("Calculating composite scores")

    # Positive layers (additive)
    positive_layers = [
        "employment_gravity",
        "mobility_optionality",
        "school_trajectory",
        "housing_elasticity",
        "demographic_momentum"
    ]

    positive_cols = [f"{layer}_score" for layer in positive_layers]
    available_positive = [col for col in positive_cols if col in layer_scores_df.columns]

    if not available_positive:
        logger.error("No positive layer scores available")
        return pd.Series(np.nan, index=layer_scores_df.index)

    # Calculate mean of positive layers
    composite = layer_scores_df[available_positive].mean(axis=1)

    # Apply risk drag penalty
    if include_risk_drag and "risk_drag_score" in layer_scores_df.columns:
        risk_drag = layer_scores_df["risk_drag_score"]

        # Risk drag is a penalty: composite_adjusted = composite * (1 - risk_drag)
        # This means high risk drag (closer to 1) reduces the composite score
        composite_adjusted = composite * (1 - risk_drag)

        logger.info(
            f"Applied risk drag penalty: "
            f"mean_before={composite.mean():.3f}, "
            f"mean_after={composite_adjusted.mean():.3f}"
        )

        return composite_adjusted
    else:
        return composite


def store_layer_scores(
    layer_scores_df: pd.DataFrame,
    data_year: int
):
    """
    Store layer scores in database.

    Args:
        layer_scores_df: DataFrame with layer scores
        data_year: Data year
    """
    logger.info(f"Storing layer scores for {len(layer_scores_df)} counties")

    with get_db() as db:
        for _, row in layer_scores_df.iterrows():
            # Extract scores
            scores = {
                "fips_code": row['fips_code'],
                "data_year": data_year,
                "employment_gravity_score": row.get('employment_gravity_score'),
                "mobility_optionality_score": row.get('mobility_optionality_score'),
                "school_trajectory_score": row.get('school_trajectory_score'),
                "housing_elasticity_score": row.get('housing_elasticity_score'),
                "demographic_momentum_score": row.get('demographic_momentum_score'),
                "risk_drag_score": row.get('risk_drag_score'),
                "composite_raw": row.get('composite_raw'),
                "composite_normalized": row.get('composite_normalized')
            }

            # Convert NaN to None for SQL
            scores = {k: (None if pd.isna(v) else float(v)) for k, v in scores.items()}

            sql = text("""
                INSERT INTO layer_scores (
                    fips_code, data_year,
                    employment_gravity_score, mobility_optionality_score,
                    school_trajectory_score, housing_elasticity_score,
                    demographic_momentum_score, risk_drag_score,
                    composite_raw, composite_normalized
                ) VALUES (
                    :fips_code, :data_year,
                    :employment_gravity_score, :mobility_optionality_score,
                    :school_trajectory_score, :housing_elasticity_score,
                    :demographic_momentum_score, :risk_drag_score,
                    :composite_raw, :composite_normalized
                )
                ON CONFLICT (fips_code, data_year)
                DO UPDATE SET
                    employment_gravity_score = EXCLUDED.employment_gravity_score,
                    mobility_optionality_score = EXCLUDED.mobility_optionality_score,
                    school_trajectory_score = EXCLUDED.school_trajectory_score,
                    housing_elasticity_score = EXCLUDED.housing_elasticity_score,
                    demographic_momentum_score = EXCLUDED.demographic_momentum_score,
                    risk_drag_score = EXCLUDED.risk_drag_score,
                    composite_raw = EXCLUDED.composite_raw,
                    composite_normalized = EXCLUDED.composite_normalized,
                    updated_at = CURRENT_TIMESTAMP
            """)

            db.execute(sql, scores)

        db.commit()

    logger.info("Layer scores stored successfully")


def run_scoring(
    normalized_layers: Dict[str, pd.DataFrame],
    data_year: int
) -> pd.DataFrame:
    """
    Main entry point for scoring pipeline.

    Args:
        normalized_layers: Dict of layer_name -> normalized features
        data_year: Data year being processed

    Returns:
        DataFrame with all layer scores and composite
    """
    logger.info("Starting scoring pipeline")

    try:
        # Calculate layer scores
        layer_scores_df = calculate_all_layer_scores(normalized_layers)

        if layer_scores_df.empty:
            logger.error("No layer scores calculated")
            return pd.DataFrame()

        # Calculate composite (raw)
        layer_scores_df['composite_raw'] = calculate_composite_score(
            layer_scores_df,
            include_risk_drag=True
        )

        # Normalize composite within Maryland (percentile rank)
        layer_scores_df['composite_normalized'] = (
            layer_scores_df['composite_raw'].rank(pct=True, method='average')
        )

        # Store in database
        store_layer_scores(layer_scores_df, data_year)

        # Log success
        log_refresh(
            layer_name="scoring",
            data_source="Normalized features",
            status="success",
            records_processed=len(layer_scores_df),
            records_inserted=len(layer_scores_df),
            metadata={"data_year": data_year}
        )

        logger.info("Scoring pipeline completed successfully")

        return layer_scores_df

    except Exception as e:
        logger.error(f"Scoring pipeline failed: {e}", exc_info=True)

        log_refresh(
            layer_name="scoring",
            data_source="Normalized features",
            status="failed",
            error_message=str(e)
        )

        raise


if __name__ == "__main__":
    import sys
    from src.utils.logging import setup_logging
    from src.processing.normalization import normalize_all_layers

    setup_logging("scoring")

    year = int(sys.argv[1]) if len(sys.argv) > 1 else None

    # Run normalization first
    normalized = normalize_all_layers(data_year=year)

    # Determine year
    if year is None:
        for layer_df in normalized.values():
            if not layer_df.empty:
                year = layer_df['data_year'].iloc[0]
                break

    # Run scoring
    run_scoring(normalized, year)
