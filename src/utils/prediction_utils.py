"""
Prediction utilities for aligning layer metrics to a target year.

Rules:
- No copy-forward. Only model-based predictions with explicit flags.
- Minimum 3 observed years to predict.
- Max extrapolation of 2 years beyond observed range.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_db, table_name as db_table_name
from config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

try:
    from scipy.stats import theilslopes
except Exception:  # pragma: no cover - optional dependency
    theilslopes = None


def _fit_trend(
    years: np.ndarray, values: np.ndarray, method: str = "theil_sen"
) -> Tuple[float, float]:
    if method == "theil_sen" and theilslopes is not None:
        slope, intercept, *_ = theilslopes(values, years)
        return float(slope), float(intercept)

    # Fallback to simple linear regression
    slope, intercept = np.polyfit(years, values, 1)
    return float(slope), float(intercept)


def _predict_series(
    years: np.ndarray,
    values: np.ndarray,
    target_year: int,
    min_years: int = 3,
    max_extrap: int = 2,
    method: str = "theil_sen",
    clip: Optional[Tuple[float, float]] = None,
) -> List[Tuple[int, float, int]]:
    if len(years) < min_years:
        return []

    last_year = int(np.max(years))
    if last_year >= target_year:
        return []

    end_year = min(target_year, last_year + max_extrap)
    if end_year <= last_year:
        return []

    slope, intercept = _fit_trend(years, values, method=method)
    predictions = []

    for year in range(last_year + 1, end_year + 1):
        pred = slope * year + intercept
        if clip is not None:
            pred = max(clip[0], min(clip[1], pred))
        predictions.append((year, float(pred), year - last_year))

    return predictions


def apply_predictions_to_table(
    table: str,
    metric_col: str,
    target_year: Optional[int] = None,
    fips_col: str = "fips_code",
    year_col: str = "data_year",
    min_years: Optional[int] = None,
    max_extrap: Optional[int] = None,
    method: str = "theil_sen",
    clip: Optional[Tuple[float, float]] = None,
    source_label: str = "predicted",
    use_effective: Optional[bool] = None,
) -> int:
    """
    Predict missing years for a metric and upsert into the table as *_pred columns.
    Returns the number of predicted rows inserted/updated.
    """
    target_year = target_year or settings.PREDICT_TO_YEAR
    min_years = min_years if min_years is not None else settings.PREDICTION_MIN_YEARS
    max_extrap = max_extrap if max_extrap is not None else settings.PREDICTION_MAX_EXTRAP_YEARS
    use_effective = use_effective if use_effective is not None else settings.USE_EFFECTIVE_VALUES
    table_ref = db_table_name(table)

    pred_col = f"{metric_col}_pred"
    pred_flag_col = f"{metric_col}_predicted"
    pred_method_col = f"{metric_col}_pred_method"
    pred_years_col = f"{metric_col}_pred_years"
    source_col = f"{metric_col}_source"
    effective_col = f"{metric_col}_effective"
    use_databricks_backend = (settings.DATA_BACKEND or "").strip().lower() == "databricks"

    with get_db() as db:
        result = db.execute(
            text(
                f"""
            SELECT {fips_col} AS fips_code, {year_col} AS data_year, {metric_col} AS value
            FROM {table_ref}
            WHERE {metric_col} IS NOT NULL
            ORDER BY {fips_col}, {year_col}
        """
            )
        )
        rows = result.fetchall()

        if not rows:
            logger.warning(f"No data found for {table}.{metric_col}; skipping prediction")
            return 0

        df = pd.DataFrame(rows, columns=["fips_code", "data_year", "value"])
        df["data_year"] = pd.to_numeric(df["data_year"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        inserted = 0

        for fips_code, sub in df.groupby("fips_code"):
            years = sub["data_year"].values
            values = sub["value"].values
            predictions = _predict_series(
                years=years,
                values=values,
                target_year=target_year,
                min_years=min_years,
                max_extrap=max_extrap,
                method=method,
                clip=clip,
            )

            for pred_year, pred_value, pred_years in predictions:
                params = {
                    "fips_code": fips_code,
                    "data_year": int(pred_year),
                    "pred_value": float(pred_value),
                    "pred_method": method,
                    "pred_years": int(pred_years),
                    "source_label": source_label,
                    "effective_value": float(pred_value) if use_effective else None,
                }

                if use_databricks_backend:
                    db.execute(
                        text(
                            f"""
                        INSERT INTO {table_ref} ({fips_col}, {year_col})
                        SELECT :fips_code, :data_year
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {table_ref}
                            WHERE {fips_col} = :fips_code AND {year_col} = :data_year
                        )
                        """
                        ),
                        params,
                    )
                    db.execute(
                        text(
                            f"""
                        UPDATE {table_ref}
                        SET
                            {pred_col} = :pred_value,
                            {pred_flag_col} = TRUE,
                            {pred_method_col} = :pred_method,
                            {pred_years_col} = :pred_years,
                            {source_col} = :source_label
                            {',' + f'{effective_col} = COALESCE({metric_col}, :pred_value)' if use_effective else ''}
                        WHERE {fips_col} = :fips_code AND {year_col} = :data_year
                        """
                        ),
                        params,
                    )
                else:
                    db.execute(
                        text(
                            f"""
                        INSERT INTO {table_ref} (
                            {fips_col}, {year_col},
                            {pred_col}, {pred_flag_col},
                            {pred_method_col}, {pred_years_col},
                            {source_col}{',' + effective_col if use_effective else ''}
                        ) VALUES (
                            :fips_code, :data_year,
                            :pred_value, TRUE,
                            :pred_method, :pred_years,
                            :source_label{',' + ':effective_value' if use_effective else ''}
                        )
                        ON CONFLICT ({fips_col}, {year_col})
                        DO UPDATE SET
                            {pred_col} = EXCLUDED.{pred_col},
                            {pred_flag_col} = EXCLUDED.{pred_flag_col},
                            {pred_method_col} = EXCLUDED.{pred_method_col},
                            {pred_years_col} = EXCLUDED.{pred_years_col},
                            {source_col} = EXCLUDED.{source_col}
                            {',' + f'{effective_col} = COALESCE({table_ref}.{metric_col}, EXCLUDED.{pred_col})' if use_effective else ''}
                    """
                        ),
                        params,
                    )
                inserted += 1

        if use_effective:
            db.execute(
                text(
                    f"""
                UPDATE {table_ref}
                SET {effective_col} = COALESCE({metric_col}, {pred_col})
                WHERE {year_col} <= :target_year
            """
                ),
                {"target_year": int(target_year)},
            )

        db.commit()

    logger.info(f"Applied predictions for {table_ref}.{metric_col}: {inserted} rows")
    return inserted
