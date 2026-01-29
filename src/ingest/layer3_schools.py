"""
Maryland Viability Atlas - Layer 3: School System Trajectory
Ingests school enrollment trends and capacity indicators

Data Sources:
- Maryland MSDE enrollment (simplified for V1)
- NCES Common Core of Data
- County characteristics

Signals Produced:
- Enrollment trends (3-year change)
- Capacity indicators
- Enrollment momentum score
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List
from sqlalchemy import text

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def calculate_school_indicators(data_year: int = 2024) -> pd.DataFrame:
    """
    Calculate school trajectory indicators for Maryland counties.

    For V1, this uses simplified estimates based on county demographics:
    - Growing counties: Montgomery, Howard, Frederick, Anne Arundel
    - Stable counties: Most others
    - Declining counties: Rural Western/Eastern Shore

    Args:
        data_year: Year for data reference

    Returns:
        DataFrame with school indicators by county
    """
    logger.error("Layer 3 school ingestion is disabled until real MSDE/NCES data is wired.")
    return pd.DataFrame()


def store_school_data(df: pd.DataFrame):
    """
    Store school trajectory data in database.

    Args:
        df: DataFrame with school indicators
    """
    logger.info(f"Storing {len(df)} school records")

    with get_db() as db:
        # Delete existing data for this year
        data_year = df['data_year'].iloc[0]
        delete_sql = text("""
            DELETE FROM layer3_school_trajectory
            WHERE data_year = :data_year
        """)
        db.execute(delete_sql, {"data_year": int(data_year)})

        # Insert new records
        insert_sql = text("""
            INSERT INTO layer3_school_trajectory (
                fips_code, data_year,
                total_enrollment, enrollment_3yr_change_pct, enrollment_trend,
                schools_total, schools_elementary, schools_middle, schools_high,
                capital_investment_total, capital_per_student, expansion_projects_count,
                enrollment_momentum_score, capacity_strain_indicator
            ) VALUES (
                :fips_code, :data_year,
                :total_enrollment, :enrollment_3yr_change_pct, :enrollment_trend,
                :schools_total, :schools_elementary, :schools_middle, :schools_high,
                :capital_investment_total, :capital_per_student, :expansion_projects_count,
                :enrollment_momentum_score, :capacity_strain_indicator
            )
        """)

        for _, row in df.iterrows():
            # Convert to dict and handle NaN/None
            row_dict = row.to_dict()
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ School data stored successfully")


def main():
    """Main execution for Layer 3 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 3: SCHOOL TRAJECTORY INGESTION")
        logger.info("=" * 60)

        data_year = 2024

        df = calculate_school_indicators(data_year)

        if df.empty:
            logger.error("No school data to store (real data not wired)")
            log_refresh(
                layer_name="layer3_school_trajectory",
                data_source="MSDE+NCES",
                status="failed",
                error_message="Ingestion disabled until real data sources are implemented",
                metadata={"data_year": data_year}
            )
            return

        # Store in database
        store_school_data(df)

        # Log refresh
        log_refresh(
            layer_name="layer3_school_trajectory",
            data_source="MSDE+NCES",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={"data_year": data_year}
        )

        logger.info("✓ Layer 3 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 3 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer3_school_trajectory",
            data_source="MSDE+NCES",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
