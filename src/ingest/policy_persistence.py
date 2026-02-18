"""
Maryland Viability Atlas - Policy Persistence Layer
Combines deterministic metrics with AI-extracted evidence

Data sources:
1. USASpending.gov (deterministic) - Federal spending consistency
2. AI-extracted CIP data (optional) - Capital follow-through rate

This layer modifies CONFIDENCE, not scores.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_db, log_refresh, table_name
from config.settings import MD_COUNTY_FIPS, get_settings
from src.utils.data_sources import fetch_usaspending_county
from src.utils.db_bulk import execute_batch
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def calculate_federal_spending_consistency(spending_by_year: pd.DataFrame) -> float:
    """
    Calculate federal spending consistency (1 - coefficient of variation).

    Lower CV = more consistent = higher score.

    Args:
        spending_by_year: DataFrame with columns [year, amount]

    Returns:
        Consistency score (0-1)
    """
    if len(spending_by_year) < 2:
        return 0.5  # Neutral if insufficient data

    amounts = spending_by_year["amount"]

    mean_amount = amounts.mean()
    std_amount = amounts.std()

    if mean_amount == 0:
        return 0.0

    cv = std_amount / mean_amount

    # Convert CV to consistency score (inverse)
    # CV of 0 = perfect consistency = 1.0
    # CV of 1 = high volatility = 0.0
    # CV > 1 = extreme volatility = 0.0
    consistency = max(0, 1 - cv)

    return consistency


def fetch_usaspending_consistency(start_year: int = 2020, end_year: int = 2025) -> pd.DataFrame:
    """
    Fetch federal spending data and calculate year-over-year consistency.

    Args:
        start_year: Start year
        end_year: End year

    Returns:
        DataFrame with columns [fips_code, federal_awards_yoy_consistency]
    """
    logger.info(f"Fetching USASpending data for {start_year}-{end_year}")

    yearly_data = []

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        try:
            df_year = fetch_usaspending_county(start_date=start_date, end_date=end_date)

            if not df_year.empty:
                # Extract FIPS from shape_code
                if "shape_code" in df_year.columns:
                    df_year["fips_code"] = df_year["shape_code"].astype(str).str.zfill(5)
                    df_year = df_year[df_year["fips_code"].str.startswith("24")]

                    df_year["year"] = year
                    df_year = df_year.rename(columns={"aggregated_amount": "amount"})

                    yearly_data.append(df_year[["fips_code", "year", "amount"]])

        except Exception as e:
            logger.warning(f"Failed to fetch data for year {year}: {e}")
            continue

    if not yearly_data:
        logger.error("No USASpending data fetched")
        return pd.DataFrame()

    # Combine all years
    df_all = pd.concat(yearly_data, ignore_index=True)

    stats_df = (
        df_all.groupby("fips_code")["amount"]
        .agg(obs_count="count", mean_amount="mean", std_amount="std")
        .reset_index()
    )
    stats_df["std_amount"] = stats_df["std_amount"].fillna(0.0)
    stats_df["cv"] = np.where(
        stats_df["mean_amount"] != 0,
        stats_df["std_amount"] / stats_df["mean_amount"],
        np.nan,
    )
    stats_df["federal_awards_yoy_consistency"] = np.where(
        stats_df["obs_count"] < 2,
        0.5,
        np.where(
            stats_df["mean_amount"] == 0,
            0.0,
            np.maximum(0.0, 1.0 - stats_df["cv"]),
        ),
    )
    consistency_df = stats_df[["fips_code", "federal_awards_yoy_consistency"]]

    logger.info(f"Calculated consistency for {len(consistency_df)} counties")

    return consistency_df


def fetch_ai_cip_follow_through() -> pd.DataFrame:
    """
    Fetch AI-extracted CIP follow-through rates from database.

    Returns:
        DataFrame with columns [fips_code, cip_follow_through_rate]
    """
    logger.info("Fetching AI-extracted CIP follow-through rates")

    with get_db() as db:
        # Query ai_evidence_link for CIP follow-through claims
        query = text(
            f"""
            SELECT
                ael.geoid as fips_code,
                AVG(ael.claim_value) as cip_follow_through_rate,
                COUNT(*) as evidence_count
            FROM {table_name('ai_evidence_link')} ael
            JOIN {table_name('ai_extraction')} ae ON ael.extraction_id = ae.id
            WHERE ael.claim_type = 'cip_follow_through_rate'
                AND ae.validation_status = 'valid'
                AND ael.claim_value IS NOT NULL
            GROUP BY ael.geoid
        """
        )

        df = pd.read_sql(query, db.connection())

    if df.empty:
        logger.warning("No AI-extracted CIP data found")
        return pd.DataFrame()

    logger.info(f"Found CIP follow-through data for {len(df)} counties")

    return df


def calculate_confidence_score(
    federal_consistency: Optional[float], cip_follow_through: Optional[float], has_cip_data: bool
) -> Dict[str, float]:
    """
    Calculate composite confidence score from available evidence.

    Args:
        federal_consistency: Federal spending consistency (0-1)
        cip_follow_through: CIP follow-through rate (0-1) if available
        has_cip_data: Whether CIP data is available

    Returns:
        Dict with confidence_score and component weights
    """
    # Weight federal consistency higher if no CIP data
    if has_cip_data and cip_follow_through is not None and pd.notna(cip_follow_through):
        # Both sources available
        weights = {"federal": 0.4, "cip": 0.6}  # CIP is more direct evidence

        federal_score = (
            float(federal_consistency)
            if federal_consistency is not None and pd.notna(federal_consistency)
            else 0.5
        )
        cip_score = float(cip_follow_through)

        confidence = weights["federal"] * federal_score + weights["cip"] * cip_score

    elif federal_consistency is not None and pd.notna(federal_consistency):
        # Only federal data available
        confidence = float(federal_consistency) * 0.8  # Cap at 0.8 when missing CIP

    else:
        # No data available
        confidence = 0.5  # Neutral default

    return {
        "confidence_score": confidence,
        "federal_weight": 0.4 if has_cip_data else 1.0,
        "cip_weight": 0.6 if has_cip_data else 0.0,
    }


def classify_confidence(confidence_score: float) -> str:
    """
    Classify confidence level based on score.

    Args:
        confidence_score: Score (0-1)

    Returns:
        Classification: 'strong', 'conditional', or 'fragile'
    """
    if confidence_score >= settings.CONFIDENCE_STRONG_MIN:
        return "strong"
    elif confidence_score >= settings.CONFIDENCE_CONDITIONAL_MIN:
        return "conditional"
    else:
        return "fragile"


def merge_and_store_policy_persistence(
    federal_df: pd.DataFrame, cip_df: pd.DataFrame, data_year: int
):
    """
    Merge federal and CIP data, calculate confidence scores, and store.

    Args:
        federal_df: Federal spending consistency data
        cip_df: AI-extracted CIP data
        data_year: Data year for record
    """
    logger.info("Merging policy persistence data sources")

    # Start with all Maryland counties
    all_counties = pd.DataFrame({"fips_code": list(MD_COUNTY_FIPS.keys())})

    # Merge federal data
    if not federal_df.empty:
        all_counties = all_counties.merge(federal_df, on="fips_code", how="left")
    else:
        all_counties["federal_awards_yoy_consistency"] = np.nan

    # Merge CIP data
    if not cip_df.empty:
        all_counties = all_counties.merge(
            cip_df[["fips_code", "cip_follow_through_rate"]], on="fips_code", how="left"
        )
        all_counties["has_cip_data"] = all_counties["cip_follow_through_rate"].notna()
    else:
        all_counties["cip_follow_through_rate"] = np.nan
        all_counties["has_cip_data"] = False

    confidence_results = _build_policy_persistence_rows(all_counties, data_year)

    results_df = pd.DataFrame(confidence_results)

    # Store in database
    use_databricks_backend = (settings.DATA_BACKEND or "").strip().lower() == "databricks"
    with get_db() as db:
        insert_sql = text(
            f"""
            INSERT INTO {table_name('policy_persistence')} (
                fips_code, data_year,
                federal_awards_yoy_consistency,
                cip_follow_through_rate,
                confidence_score,
                confidence_class
            ) VALUES (
                :fips_code, :data_year,
                :federal_awards_yoy_consistency,
                :cip_follow_through_rate,
                :confidence_score,
                :confidence_class
            )
        """
        )
        upsert_sql = text(
            f"""
            INSERT INTO {table_name('policy_persistence')} (
                fips_code, data_year,
                federal_awards_yoy_consistency,
                cip_follow_through_rate,
                confidence_score,
                confidence_class
            ) VALUES (
                :fips_code, :data_year,
                :federal_awards_yoy_consistency,
                :cip_follow_through_rate,
                :confidence_score,
                :confidence_class
            )
            ON CONFLICT (fips_code, data_year)
            DO UPDATE SET
                federal_awards_yoy_consistency = EXCLUDED.federal_awards_yoy_consistency,
                cip_follow_through_rate = EXCLUDED.cip_follow_through_rate,
                confidence_score = EXCLUDED.confidence_score,
                confidence_class = EXCLUDED.confidence_class,
                updated_at = CURRENT_TIMESTAMP
        """
        )

        if use_databricks_backend:
            db.execute(
                text(
                    f"""
                    DELETE FROM {table_name('policy_persistence')}
                    WHERE data_year = :data_year
                    """
                ),
                {"data_year": data_year},
            )

        execute_batch(
            db,
            insert_sql if use_databricks_backend else upsert_sql,
            confidence_results,
            chunk_size=1000,
        )

        db.commit()

    logger.info(f"Stored policy persistence data for {len(results_df)} counties")

    # Log distribution
    logger.info(
        f"Confidence distribution: "
        f"strong={len(results_df[results_df['confidence_class']=='strong'])}, "
        f"conditional={len(results_df[results_df['confidence_class']=='conditional'])}, "
        f"fragile={len(results_df[results_df['confidence_class']=='fragile'])}"
    )


def _build_policy_persistence_rows(
    all_counties: pd.DataFrame, data_year: int
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in all_counties.to_dict(orient="records"):
        result = calculate_confidence_score(
            federal_consistency=row.get("federal_awards_yoy_consistency"),
            cip_follow_through=row.get("cip_follow_through_rate"),
            has_cip_data=bool(row.get("has_cip_data")),
        )
        confidence_score = result["confidence_score"]
        rows.append(
            {
                "fips_code": row.get("fips_code"),
                "federal_awards_yoy_consistency": _none_if_na(
                    row.get("federal_awards_yoy_consistency")
                ),
                "cip_follow_through_rate": _none_if_na(row.get("cip_follow_through_rate")),
                "confidence_score": _none_if_na(confidence_score),
                "confidence_class": classify_confidence(confidence_score),
                "data_year": int(data_year),
            }
        )
    return rows


def _none_if_na(value: Any) -> Any:
    return None if pd.isna(value) else value


def run_policy_persistence_ingestion(data_year: int = 2025, include_ai: bool = True):
    """
    Main entry point for policy persistence layer ingestion.

    Args:
        data_year: Year to assign to records
        include_ai: If True, include AI-extracted CIP data
    """
    logger.info(f"Starting policy persistence ingestion (include_ai={include_ai})")

    try:
        # Fetch federal spending consistency (deterministic)
        federal_df = fetch_usaspending_consistency(start_year=data_year - 5, end_year=data_year)

        # Fetch AI-extracted CIP data (optional)
        if include_ai and settings.AI_ENABLED:
            cip_df = fetch_ai_cip_follow_through()
        else:
            logger.info("Skipping AI-extracted CIP data (AI disabled or not requested)")
            cip_df = pd.DataFrame()

        # Merge and store
        merge_and_store_policy_persistence(federal_df, cip_df, data_year)

        # Log success
        cip_count = len(cip_df) if not cip_df.empty else 0

        log_refresh(
            layer_name="policy_persistence",
            data_source=f"USASpending + AI CIP (n={cip_count})",
            status="success",
            records_processed=len(MD_COUNTY_FIPS),
            records_inserted=len(MD_COUNTY_FIPS),
            metadata={"data_year": data_year, "include_ai": include_ai, "cip_counties": cip_count},
        )

        logger.info("Policy persistence ingestion completed successfully")

    except Exception as e:
        logger.error(f"Policy persistence ingestion failed: {e}", exc_info=True)

        log_refresh(
            layer_name="policy_persistence",
            data_source="USASpending + AI CIP",
            status="failed",
            error_message=str(e),
        )

        raise


if __name__ == "__main__":
    import sys

    from src.utils.logging import setup_logging

    setup_logging("policy_persistence")

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2025
    include_ai = sys.argv[2].lower() != "false" if len(sys.argv) > 2 else True

    run_policy_persistence_ingestion(data_year=year, include_ai=include_ai)
