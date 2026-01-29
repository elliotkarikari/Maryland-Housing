"""
Maryland Viability Atlas - Layer 2: Mobility Optionality
Ingests transportation modal redundancy and accessibility data

Data Sources:
- OpenStreetMap (highway network)
- GTFS feeds (transit service)
- Maryland counties shapefile

Signals Produced:
- Highway network coverage
- Rail service availability
- Modal redundancy (highway + rail)
- Transit accessibility
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


def calculate_mobility_indicators(data_year: int = 2025) -> pd.DataFrame:
    """
    Calculate mobility optionality indicators for Maryland counties.

    Args:
        data_year: Year for data reference

    Returns:
        DataFrame with mobility indicators by county
    """
    logger.error("Layer 2 mobility ingestion is disabled until real data sources are wired.")
    return pd.DataFrame()


def store_mobility_data(df: pd.DataFrame):
    """
    Store mobility optionality data in database.

    Args:
        df: DataFrame with mobility indicators
    """
    logger.info(f"Storing {len(df)} mobility records")

    with get_db() as db:
        # Delete existing data for this year
        data_year = df['data_year'].iloc[0]
        delete_sql = text("""
            DELETE FROM layer2_mobility_optionality
            WHERE data_year = :data_year
        """)
        db.execute(delete_sql, {"data_year": int(data_year)})

        # Insert new records
        insert_sql = text("""
            INSERT INTO layer2_mobility_optionality (
                fips_code, data_year,
                highway_miles_total, interstate_exits, major_highway_redundancy,
                has_rail_service, has_frequent_bus, transit_stations_count,
                mode_count, mobility_optionality_index, aadt_major_corridors
            ) VALUES (
                :fips_code, :data_year,
                :highway_miles_total, :interstate_exits, :major_highway_redundancy,
                :has_rail_service, :has_frequent_bus, :transit_stations_count,
                :mode_count, :mobility_optionality_index, :aadt_major_corridors
            )
        """)

        for _, row in df.iterrows():
            # Convert to dict and handle NaN/None
            row_dict = row.to_dict()
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ Mobility data stored successfully")


def main():
    """Main execution for Layer 2 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 2: MOBILITY OPTIONALITY INGESTION")
        logger.info("=" * 60)

        data_year = 2025

        df = calculate_mobility_indicators(data_year)

        if df.empty:
            logger.error("No mobility data to store (real data not wired)")
            log_refresh(
                layer_name="layer2_mobility_optionality",
                data_source="OSM+GTFS",
                status="failed",
                error_message="Ingestion disabled until real data sources are implemented",
                metadata={"data_year": data_year}
            )
            return

        # Store in database
        store_mobility_data(df)

        # Log refresh
        log_refresh(
            layer_name="layer2_mobility_optionality",
            data_source="OSM+GTFS",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={"data_year": data_year}
        )

        logger.info("✓ Layer 2 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 2 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer2_mobility_optionality",
            data_source="OSM+GTFS",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
