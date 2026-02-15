"""
Maryland Viability Atlas - Feature Normalization
Normalizes all features within Maryland for scoring

Methods:
- Percentile rank (robust to outliers)
- Robust z-score (median/IQR for heavy-tailed distributions)
- Min-max (use sparingly)

All normalization is within-Maryland only (no national comparison).
"""

from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_db, log_refresh
from config.settings import get_settings
from src.processing.feature_registry import (
    ALL_FEATURES,
    FEATURES_BY_LAYER,
    Directionality,
    FeatureDefinition,
    NormMethod,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def percentile_normalize(values: pd.Series, directionality: Directionality) -> pd.Series:
    """
    Normalize using percentile rank (0-1).

    Args:
        values: Raw feature values
        directionality: POSITIVE or NEGATIVE

    Returns:
        Normalized scores (0-1)
    """
    # Calculate percentile rank
    scores = values.rank(pct=True, method="average")

    # Invert if negative directionality
    if directionality == Directionality.NEGATIVE:
        scores = 1 - scores

    return scores


def robust_zscore_normalize(
    values: pd.Series, directionality: Directionality, clip_std: float = 3.0
) -> pd.Series:
    """
    Normalize using robust z-score: (x - median) / IQR

    Then clip to ±3 IQR and map to 0-1.

    Args:
        values: Raw feature values
        directionality: POSITIVE or NEGATIVE
        clip_std: Number of IQRs to clip at

    Returns:
        Normalized scores (0-1)
    """
    median = values.median()
    q25 = values.quantile(0.25)
    q75 = values.quantile(0.75)
    iqr = q75 - q25

    if iqr == 0:
        # No variation - return all 0.5
        logger.warning(f"Zero IQR encountered in robust z-score normalization")
        return pd.Series(0.5, index=values.index)

    # Calculate robust z-scores
    z_scores = (values - median) / iqr

    # Clip outliers
    z_scores = z_scores.clip(-clip_std, clip_std)

    # Map to 0-1 (centered at 0.5)
    scores = (z_scores + clip_std) / (2 * clip_std)

    # Invert if negative directionality
    if directionality == Directionality.NEGATIVE:
        scores = 1 - scores

    return scores


def minmax_normalize(values: pd.Series, directionality: Directionality) -> pd.Series:
    """
    Normalize using min-max: (x - min) / (max - min)

    Use sparingly - sensitive to outliers.

    Args:
        values: Raw feature values
        directionality: POSITIVE or NEGATIVE

    Returns:
        Normalized scores (0-1)
    """
    min_val = values.min()
    max_val = values.max()

    if max_val == min_val:
        logger.warning(f"No variation in feature for min-max normalization")
        return pd.Series(0.5, index=values.index)

    scores = (values - min_val) / (max_val - min_val)

    # Invert if negative directionality
    if directionality == Directionality.NEGATIVE:
        scores = 1 - scores

    return scores


def normalize_feature(df: pd.DataFrame, feature: FeatureDefinition) -> pd.Series:
    """
    Normalize a single feature according to its definition.

    Args:
        df: DataFrame with raw feature values
        feature: Feature definition from registry

    Returns:
        Series of normalized scores (0-1)
    """
    if feature.source_column not in df.columns:
        logger.warning(f"Feature {feature.name} column {feature.source_column} not found")
        return pd.Series(np.nan, index=df.index)

    values = df[feature.source_column]

    # Skip if all NaN
    if values.isna().all():
        logger.warning(f"Feature {feature.name} has all NaN values")
        return values

    # Remove NaN for normalization
    valid_mask = ~values.isna()
    valid_values = values[valid_mask]

    if len(valid_values) < 3:
        logger.warning(f"Feature {feature.name} has fewer than 3 valid values")
        return pd.Series(np.nan, index=df.index)

    # Apply normalization method
    if feature.norm_method == NormMethod.PERCENTILE:
        normalized = percentile_normalize(valid_values, feature.directionality)

    elif feature.norm_method == NormMethod.ROBUST_ZSCORE:
        normalized = robust_zscore_normalize(valid_values, feature.directionality)

    elif feature.norm_method == NormMethod.MINMAX:
        normalized = minmax_normalize(valid_values, feature.directionality)

    else:
        raise ValueError(f"Unknown normalization method: {feature.norm_method}")

    # Re-insert NaN values
    result = pd.Series(np.nan, index=df.index)
    result[valid_mask] = normalized

    logger.info(
        f"Normalized {feature.name}: "
        f"min={result.min():.3f}, max={result.max():.3f}, "
        f"mean={result.mean():.3f}, missing={result.isna().sum()}"
    )

    return result


def fetch_layer_data(layer_name: str, data_year: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch raw data for a layer from database.

    Args:
        layer_name: Name of layer (e.g., 'employment_gravity')
        data_year: Specific year to fetch (default: latest)

    Returns:
        DataFrame with layer data
    """
    features = FEATURES_BY_LAYER.get(layer_name, [])

    if not features:
        logger.warning(f"No features defined for layer {layer_name}")
        return pd.DataFrame()

    # Get source table (should be same for all features in a layer)
    source_table = features[0].source_table

    with get_db() as db:
        if data_year:
            query = text(
                f"""
                SELECT *
                FROM {source_table}
                WHERE data_year = :data_year
            """
            )
            df = pd.read_sql(query, db.connection(), params={"data_year": data_year})
        else:
            # Get latest year
            query = text(
                f"""
                SELECT *
                FROM {source_table}
                WHERE data_year = (SELECT MAX(data_year) FROM {source_table})
            """
            )
            df = pd.read_sql(query, db.connection())

    logger.info(f"Fetched {len(df)} records for layer {layer_name}")
    return df


def normalize_layer(layer_name: str, data_year: Optional[int] = None) -> pd.DataFrame:
    """
    Normalize all features in a layer.

    Args:
        layer_name: Name of layer
        data_year: Specific year (default: latest)

    Returns:
        DataFrame with fips_code and normalized feature scores
    """
    logger.info(f"Normalizing layer: {layer_name}")

    # Fetch raw data
    df = fetch_layer_data(layer_name, data_year)

    if df.empty:
        logger.warning(f"No data found for layer {layer_name}")
        return pd.DataFrame()

    # Get features for this layer
    features = FEATURES_BY_LAYER[layer_name]

    # Normalize each feature
    normalized_df = df[["fips_code", "data_year"]].copy()

    for feature in features:
        normalized_df[f"{feature.name}_normalized"] = normalize_feature(df, feature)

    return normalized_df


def normalize_all_layers(
    data_year: Optional[int] = None, skip_ai_features: bool = False
) -> Dict[str, pd.DataFrame]:
    """
    Normalize all layers and return results.

    Args:
        data_year: Specific year (default: latest)
        skip_ai_features: If True, skip AI-dependent features

    Returns:
        Dict mapping layer_name -> normalized DataFrame
    """
    logger.info(f"Normalizing all layers (data_year={data_year}, skip_ai={skip_ai_features})")

    normalized_layers = {}

    for layer_name in FEATURES_BY_LAYER.keys():
        try:
            normalized_df = normalize_layer(layer_name, data_year)

            if skip_ai_features:
                # Remove AI-dependent feature columns
                ai_features = [f for f in FEATURES_BY_LAYER[layer_name] if f.requires_ai]
                ai_cols = [f"{f.name}_normalized" for f in ai_features]
                normalized_df = normalized_df.drop(columns=ai_cols, errors="ignore")

            normalized_layers[layer_name] = normalized_df

        except Exception as e:
            logger.error(f"Failed to normalize layer {layer_name}: {e}", exc_info=True)
            normalized_layers[layer_name] = pd.DataFrame()

    return normalized_layers


def store_normalized_features(normalized_layers: Dict[str, pd.DataFrame], data_year: int):
    """
    Store normalized features in database.

    Creates a denormalized table with all normalized features for easy access.

    Args:
        normalized_layers: Dict of layer_name -> normalized DataFrame
        data_year: Data year being processed
    """
    logger.info(f"Storing normalized features for year {data_year}")

    # Merge all layer normalized features
    merged = None

    for layer_name, df in normalized_layers.items():
        if df.empty:
            continue

        if merged is None:
            merged = df.copy()
        else:
            merged = merged.merge(
                df.drop(columns=["data_year"], errors="ignore"), on="fips_code", how="outer"
            )

    if merged is None or merged.empty:
        logger.error("No normalized features to store")
        return

    # Ensure data_year column
    merged["data_year"] = data_year

    # Store in database
    with get_db() as db:
        # Create normalized_features table if it doesn't exist
        create_table_sql = text(
            """
            CREATE TABLE IF NOT EXISTS normalized_features (
                id SERIAL PRIMARY KEY,
                fips_code VARCHAR(5) NOT NULL,
                data_year INTEGER NOT NULL,
                feature_name VARCHAR(100) NOT NULL,
                normalized_value NUMERIC(10,6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(fips_code, data_year, feature_name)
            )
        """
        )
        db.execute(create_table_sql)
        db.commit()

        # Insert normalized features (long format for flexibility)
        insert_count = 0

        for _, row in merged.iterrows():
            fips_code = row["fips_code"]

            for col in merged.columns:
                if col.endswith("_normalized"):
                    feature_name = col.replace("_normalized", "")
                    normalized_value = row[col]

                    if pd.notna(normalized_value):
                        insert_sql = text(
                            """
                            INSERT INTO normalized_features (
                                fips_code, data_year, feature_name, normalized_value
                            ) VALUES (
                                :fips_code, :data_year, :feature_name, :normalized_value
                            )
                            ON CONFLICT (fips_code, data_year, feature_name)
                            DO UPDATE SET
                                normalized_value = EXCLUDED.normalized_value,
                                created_at = CURRENT_TIMESTAMP
                        """
                        )

                        db.execute(
                            insert_sql,
                            {
                                "fips_code": str(fips_code),
                                "data_year": int(data_year),
                                "feature_name": str(feature_name),
                                "normalized_value": float(normalized_value),
                            },
                        )

                        insert_count += 1

        db.commit()

    logger.info(f"Stored {insert_count} normalized feature values")


def run_normalization(data_year: Optional[int] = None, skip_ai_features: bool = False):
    """
    Main entry point for normalization pipeline.

    Args:
        data_year: Specific year to process (default: latest)
        skip_ai_features: If True, skip AI-dependent features
    """
    logger.info("Starting normalization pipeline")

    try:
        # Normalize all layers
        normalized_layers = normalize_all_layers(
            data_year=data_year, skip_ai_features=skip_ai_features
        )

        # Determine actual data year
        if data_year is None:
            # Get from one of the normalized layers
            for layer_df in normalized_layers.values():
                if not layer_df.empty and "data_year" in layer_df.columns:
                    data_year = layer_df["data_year"].iloc[0]
                    break

        if data_year is None:
            logger.error("Could not determine data year")
            return

        # Store normalized features
        store_normalized_features(normalized_layers, data_year)

        # Log success
        total_features = sum(
            len([c for c in df.columns if c.endswith("_normalized")])
            for df in normalized_layers.values()
        )

        log_refresh(
            layer_name="normalization",
            data_source="All layers",
            status="success",
            records_processed=total_features,
            records_inserted=total_features,
            metadata={
                "data_year": data_year,
                "skip_ai_features": skip_ai_features,
                "layers_processed": len(normalized_layers),
            },
        )

        logger.info("Normalization pipeline completed successfully")

    except Exception as e:
        logger.error(f"Normalization pipeline failed: {e}", exc_info=True)

        log_refresh(
            layer_name="normalization",
            data_source="All layers",
            status="failed",
            error_message=str(e),
        )

        raise


if __name__ == "__main__":
    import sys

    from src.utils.logging import setup_logging

    setup_logging("normalization")

    year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    skip_ai = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False

    run_normalization(data_year=year, skip_ai_features=skip_ai)
