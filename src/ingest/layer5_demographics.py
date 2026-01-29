"""
Maryland Viability Atlas - Layer 5: Demographic Momentum
Ingests migration patterns and household formation signals

Signals Produced:
- Working-age population trends
- Migration flows
- Household formation rates
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import text

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def calculate_demographic_indicators(data_year: int = 2023) -> pd.DataFrame:
    """Calculate demographic momentum indicators."""
    logger.error("Layer 5 demographic ingestion is disabled until real ACS/IRS data is wired.")
    return pd.DataFrame()


def store_demographic_data(df: pd.DataFrame):
    """Store demographic momentum data in database."""
    logger.info(f"Storing {len(df)} demographic records")

    with get_db() as db:
        data_year = df['data_year'].iloc[0]
        db.execute(text("DELETE FROM layer5_demographic_momentum WHERE data_year = :data_year"),
                   {"data_year": int(data_year)})

        insert_sql = text("""
            INSERT INTO layer5_demographic_momentum (
                fips_code, data_year,
                pop_total, pop_age_25_44, pop_age_25_44_pct,
                households_total, households_family, households_family_with_children,
                inflow_households, outflow_households, net_migration_households,
                inflow_exemptions, outflow_exemptions, net_migration_persons,
                total_addresses, vacant_addresses, vacancy_rate,
                family_household_inflow_rate, working_age_momentum,
                household_formation_change, demographic_momentum_score
            ) VALUES (
                :fips_code, :data_year,
                :pop_total, :pop_age_25_44, :pop_age_25_44_pct,
                :households_total, :households_family, :households_family_with_children,
                :inflow_households, :outflow_households, :net_migration_households,
                :inflow_exemptions, :outflow_exemptions, :net_migration_persons,
                :total_addresses, :vacant_addresses, :vacancy_rate,
                :family_household_inflow_rate, :working_age_momentum,
                :household_formation_change, :demographic_momentum_score
            )
        """)

        for _, row in df.iterrows():
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ Demographic data stored successfully")


def main():
    """Main execution for Layer 5 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 5: DEMOGRAPHIC MOMENTUM INGESTION")
        logger.info("=" * 60)

        data_year = 2023
        df = calculate_demographic_indicators(data_year)

        if df.empty:
            logger.error("No demographic data to store (real data not wired)")
            log_refresh(
                layer_name="layer5_demographic_momentum",
                data_source="ACS+IRS",
                status="failed",
                error_message="Ingestion disabled until real data sources are implemented",
                metadata={"data_year": data_year}
            )
            return

        store_demographic_data(df)

        log_refresh(
            layer_name="layer5_demographic_momentum",
            data_source="ACS+IRS",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={"data_year": data_year}
        )

        logger.info("✓ Layer 5 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 5 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer5_demographic_momentum",
            data_source="ACS+IRS",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
