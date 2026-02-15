"""
Maryland Viability Atlas - Timeseries Feature Computation
Computes multi-year momentum and stability features from raw layer data

Implements:
- Level: Latest available value
- Momentum: Robust trend/slope across years
- Stability: Volatility and consistency metrics

NO INTERPOLATION. Missing years reduce coverage, not filled in.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text
from scipy import stats

from config.settings import get_settings
from config.database import get_db, log_refresh
from src.utils.logging import get_logger
from src.utils.db_bulk import execute_batch

logger = get_logger(__name__)
settings = get_settings()

# Configuration
DEFAULT_WINDOW_SIZE = 5  # Default N_YEARS for rolling window
MIN_YEARS_FOR_MOMENTUM = 3  # Minimum years needed to calculate meaningful trend
MIN_YEARS_FOR_STABILITY = 3  # Minimum years for volatility metrics


def compute_robust_slope(years: np.ndarray, values: np.ndarray) -> Tuple[float, float]:
    """
    Compute robust linear slope using Theil-Sen estimator.

    Args:
        years: Array of year values
        values: Array of metric values

    Returns:
        Tuple of (slope, median_absolute_deviation)
    """
    if len(years) < 2:
        return np.nan, np.nan

    try:
        # Theil-Sen slope estimator (robust to outliers)
        slope, intercept, lo_slope, up_slope = stats.theilslopes(values, years)

        # Calculate MAD as fit quality indicator
        predicted = slope * years + intercept
        residuals = values - predicted
        mad = np.median(np.abs(residuals - np.median(residuals)))

        return float(slope), float(mad)
    except Exception as e:
        logger.warning(f"Robust slope calculation failed: {e}")
        # Fallback to simple OLS
        try:
            slope, intercept, r_value, p_value, std_err = stats.linregress(years, values)
            return float(slope), float(std_err)
        except:
            return np.nan, np.nan


def compute_stability_metrics(values: np.ndarray) -> Dict[str, float]:
    """
    Compute stability indicators: volatility, CV, consistency.

    Args:
        values: Array of metric values across years

    Returns:
        Dict with volatility, cv, consistency, persistence metrics
    """
    if len(values) < 2:
        return {
            'volatility': np.nan,
            'cv': np.nan,
            'consistency': np.nan,
            'persistence': 0
        }

    # Volatility: Interquartile range (robust to outliers)
    q75, q25 = np.percentile(values, [75, 25])
    iqr = q75 - q25
    volatility = iqr

    # Coefficient of variation
    mean_val = np.mean(values)
    std_val = np.std(values, ddof=1) if len(values) > 1 else 0
    cv = (std_val / mean_val) if mean_val != 0 else np.nan

    # Consistency: fraction of year-over-year changes that are positive
    if len(values) >= 2:
        diffs = np.diff(values)
        positive_changes = np.sum(diffs > 0)
        consistency = positive_changes / len(diffs) if len(diffs) > 0 else np.nan
    else:
        consistency = np.nan

    # Persistence: count of consecutive positive changes
    if len(values) >= 2:
        diffs = np.diff(values)
        persistence = 0
        current_streak = 0
        for d in diffs:
            if d > 0:
                current_streak += 1
                persistence = max(persistence, current_streak)
            else:
                current_streak = 0
    else:
        persistence = 0

    return {
        'volatility': volatility,
        'cv': cv,
        'consistency': consistency,
        'persistence': persistence
    }


def extract_timeseries_data(
    layer_table: str,
    metric_column: str,
    geoid: str,
    window_size: int = DEFAULT_WINDOW_SIZE,
    as_of_year: int = 2025
) -> pd.DataFrame:
    """
    Extract timeseries data for a specific metric from a layer table.

    Args:
        layer_table: Name of the layer table (e.g., 'layer1_employment_gravity')
        metric_column: Column name for the metric
        geoid: Geography ID (FIPS code)
        window_size: Number of years to look back
        as_of_year: Reference year (latest year to consider)

    Returns:
        DataFrame with columns: year, value
    """
    with get_db() as db:
        # Calculate year range
        current_year = as_of_year
        min_year = current_year - window_size + 1

        query = text(f"""
            SELECT data_year as year, {metric_column} as value
            FROM {layer_table}
            WHERE fips_code = :geoid
              AND data_year >= :min_year
              AND {metric_column} IS NOT NULL
            ORDER BY data_year
        """)

        result = db.execute(query, {"geoid": geoid, "min_year": min_year})
        rows = result.fetchall()

        if not rows:
            return pd.DataFrame(columns=['year', 'value'])

        df = pd.DataFrame(rows, columns=['year', 'value'])

        # Convert Decimal to float for numeric operations
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        return df


def compute_layer_timeseries_features(
    geoid: str,
    layer_name: str,
    layer_table: str,
    metric_column: str,
    window_size: int = DEFAULT_WINDOW_SIZE,
    as_of_year: int = 2025
) -> Optional[Dict]:
    """
    Compute timeseries features for a single layer and geography.

    Args:
        geoid: Geography ID (FIPS code)
        layer_name: Layer name (e.g., 'employment_gravity')
        layer_table: Database table name
        metric_column: Primary metric column to analyze
        window_size: Years to look back
        as_of_year: Reference year for "current" calculation

    Returns:
        Dict with all timeseries features, or None if insufficient data
    """
    # Extract timeseries data
    ts_data = extract_timeseries_data(layer_table, metric_column, geoid, window_size, as_of_year)

    if ts_data.empty:
        logger.debug(f"No timeseries data for {geoid} {layer_name}")
        return None

    years = ts_data['year'].values
    values = ts_data['value'].values

    coverage_years = len(years)
    min_year = int(years.min())
    max_year = int(years.max())

    # Identify missing years in window
    expected_years = set(range(as_of_year - window_size + 1, as_of_year + 1))
    actual_years = set(years)
    missing_years = sorted(expected_years - actual_years)

    # LEVEL features
    level_latest = float(values[-1])  # Most recent value
    level_baseline = float(values[0])  # Earliest value in window

    # MOMENTUM features
    if coverage_years >= MIN_YEARS_FOR_MOMENTUM:
        momentum_slope, fit_quality = compute_robust_slope(years, values)
        momentum_delta = level_latest - level_baseline
        momentum_percent_change = ((level_latest - level_baseline) / level_baseline * 100
                                   if level_baseline != 0 else np.nan)
        computation_method = 'theil_sen'
    else:
        # Insufficient data for meaningful momentum
        momentum_slope = np.nan
        momentum_delta = level_latest - level_baseline if coverage_years >= 2 else np.nan
        momentum_percent_change = np.nan
        fit_quality = np.nan
        computation_method = 'insufficient_data'

    # STABILITY features
    if coverage_years >= MIN_YEARS_FOR_STABILITY:
        stability = compute_stability_metrics(values)
    else:
        stability = {
            'volatility': np.nan,
            'cv': np.nan,
            'consistency': np.nan,
            'persistence': 0
        }

    return {
        'geoid': geoid,
        'layer_name': layer_name,
        'as_of_year': as_of_year,

        # Level
        'level_latest': level_latest,
        'level_baseline': level_baseline,

        # Momentum
        'momentum_slope': momentum_slope,
        'momentum_delta': momentum_delta,
        'momentum_percent_change': momentum_percent_change,
        'momentum_fit_quality': fit_quality,

        # Stability
        'stability_volatility': stability['volatility'],
        'stability_cv': stability['cv'],
        'stability_consistency': stability['consistency'],
        'stability_persistence': stability['persistence'],

        # Coverage
        'coverage_years': coverage_years,
        'min_year': min_year,
        'max_year': max_year,
        'data_gaps': missing_years,

        # Metadata
        'window_size': window_size,
        'computation_method': computation_method
    }


def store_timeseries_features(features: List[Dict]):
    """
    Store computed timeseries features to database.

    Args:
        features: List of feature dicts
    """
    if not features:
        logger.warning("No timeseries features to store")
        return

    logger.info(f"Storing {len(features)} timeseries feature records")

    with get_db() as db:
        # Delete existing features for this as_of_year
        as_of_year = features[0]['as_of_year']
        delete_sql = text("""
            DELETE FROM layer_timeseries_features
            WHERE as_of_year = :as_of_year
        """)
        db.execute(delete_sql, {"as_of_year": as_of_year})

        # Insert new features
        insert_sql = text("""
            INSERT INTO layer_timeseries_features (
                geoid, layer_name, as_of_year,
                level_latest, level_baseline,
                momentum_slope, momentum_delta, momentum_percent_change, momentum_fit_quality,
                stability_volatility, stability_cv, stability_consistency, stability_persistence,
                coverage_years, min_year, max_year, data_gaps,
                window_size, computation_method
            ) VALUES (
                :geoid, :layer_name, :as_of_year,
                :level_latest, :level_baseline,
                :momentum_slope, :momentum_delta, :momentum_percent_change, :momentum_fit_quality,
                :stability_volatility, :stability_cv, :stability_consistency, :stability_persistence,
                :coverage_years, :min_year, :max_year, CAST(:data_gaps AS jsonb),
                :window_size, :computation_method
            )
        """)

        import json

        rows = []
        for feature_dict in features:
            row = dict(feature_dict)
            row['data_gaps'] = json.dumps(row.get('data_gaps', []))
            rows.append(row)

        execute_batch(db, insert_sql, rows, chunk_size=1000)

        db.commit()

    logger.info("✓ Timeseries features stored successfully")


def compute_all_timeseries_features(
    window_size: int = DEFAULT_WINDOW_SIZE,
    as_of_year: int = 2025
) -> int:
    """
    Compute timeseries features for all layers and geographies.

    Args:
        window_size: Years to look back
        as_of_year: Reference year

    Returns:
        Count of features computed
    """
    logger.info("=" * 70)
    logger.info("TIMESERIES FEATURE COMPUTATION")
    logger.info("=" * 70)
    logger.info(f"Window size: {window_size} years")
    logger.info(f"As of year: {as_of_year}")

    # Define layer configurations
    # Each layer specifies: table name, primary metric column
    def _metric_name(base_metric: str) -> str:
        return f"{base_metric}_effective" if settings.USE_EFFECTIVE_VALUES else base_metric

    layer_configs = {
        'employment_gravity': {
            'table': 'layer1_employment_gravity',
            'metric': _metric_name('economic_opportunity_index')
        },
        'mobility_optionality': {
            'table': 'layer2_mobility_optionality',
            'metric': _metric_name('mobility_optionality_index')
        },
        'school_trajectory': {
            'table': 'layer3_school_trajectory',
            'metric': _metric_name('education_opportunity_index')  # v1-v2 composite: 0.4×supply + 0.6×accessibility
        },
        'housing_elasticity': {
            'table': 'layer4_housing_elasticity',
            'metric': _metric_name('housing_opportunity_index')  # v1-v2 composite: 0.4×elasticity + 0.6×affordability
        },
        'demographic_momentum': {
            'table': 'layer5_demographic_momentum',
            'metric': _metric_name('demographic_opportunity_index')  # v1-v3 composite: 0.3×static + 0.4×equity + 0.3×migration
        },
        'risk_drag': {
            'table': 'layer6_risk_drag',
            'metric': _metric_name('risk_drag_index')  # v1-v2 composite: 0.4×static + 0.6×(climate+vulnerability-adaptive)
        }
    }

    # Get all geoids
    with get_db() as db:
        result = db.execute(text("SELECT fips_code FROM md_counties ORDER BY fips_code"))
        geoids = [row[0] for row in result.fetchall()]

    logger.info(f"Processing {len(geoids)} geographies across {len(layer_configs)} layers")

    all_features = []

    for geoid in geoids:
        for layer_name, config in layer_configs.items():
            try:
                features = compute_layer_timeseries_features(
                    geoid=geoid,
                    layer_name=layer_name,
                    layer_table=config['table'],
                    metric_column=config['metric'],
                    window_size=window_size,
                    as_of_year=as_of_year
                )

                if features:
                    all_features.append(features)

            except Exception as e:
                logger.warning(f"Error computing {layer_name} for {geoid}: {e}")
                continue

    # Store all features
    if all_features:
        store_timeseries_features(all_features)

    logger.info(f"✓ Computed {len(all_features)} timeseries feature records")

    # Log summary statistics
    df = pd.DataFrame(all_features)
    if not df.empty:
        logger.info("\nCoverage summary:")
        coverage_summary = df.groupby('layer_name')['coverage_years'].agg(['mean', 'min', 'max']).round(1)
        for layer, stats in coverage_summary.iterrows():
            logger.info(f"  {layer}: avg={stats['mean']:.1f} years (min={stats['min']:.0f}, max={stats['max']:.0f})")

    # Log refresh
    log_refresh(
        layer_name="timeseries_features",
        data_source="all_layers",
        status="success",
        records_processed=len(all_features),
        records_inserted=len(all_features),
        metadata={
            "window_size": window_size,
            "as_of_year": as_of_year,
            "layers_processed": len(layer_configs)
        }
    )

    return len(all_features)


def main():
    """Main execution for timeseries feature computation"""
    try:
        count = compute_all_timeseries_features(
            window_size=DEFAULT_WINDOW_SIZE,
            as_of_year=2025
        )

        logger.info("=" * 70)
        logger.info(f"✓ Timeseries feature computation complete: {count} records")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Timeseries feature computation failed: {e}", exc_info=True)
        log_refresh(
            layer_name="timeseries_features",
            data_source="all_layers",
            status="error",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
