"""
Maryland Viability Atlas - Layer 6: Risk Drag
Ingests environmental and infrastructure risk indicators

Signals Produced:
- Flood risk exposure
- Climate vulnerabilities
- Infrastructure deficiencies
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import text

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def calculate_risk_indicators(data_year: int = 2025) -> pd.DataFrame:
    """Calculate risk drag indicators."""
    logger.error("Layer 6 risk ingestion is disabled until real FEMA/EPA/MDOT data is wired.")
    return pd.DataFrame()


def store_risk_data(df: pd.DataFrame):
    """Store risk drag data in database."""
    logger.info(f"Storing {len(df)} risk records")

    with get_db() as db:
        data_year = df['data_year'].iloc[0]
        db.execute(text("DELETE FROM layer6_risk_drag WHERE data_year = :data_year"),
                   {"data_year": int(data_year)})

        insert_sql = text("""
            INSERT INTO layer6_risk_drag (
                fips_code, data_year,
                sfha_area_sq_mi, sfha_pct_of_county,
                sea_level_rise_exposure, extreme_heat_days_annual,
                pm25_avg, ozone_avg,
                proximity_hazwaste_score, traffic_proximity_score,
                bridges_total, bridges_structurally_deficient, bridges_deficient_pct,
                risk_drag_index
            ) VALUES (
                :fips_code, :data_year,
                :sfha_area_sq_mi, :sfha_pct_of_county,
                :sea_level_rise_exposure, :extreme_heat_days_annual,
                :pm25_avg, :ozone_avg,
                :proximity_hazwaste_score, :traffic_proximity_score,
                :bridges_total, :bridges_structurally_deficient, :bridges_deficient_pct,
                :risk_drag_index
            )
        """)

        for _, row in df.iterrows():
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ Risk data stored successfully")


def main():
    """Main execution for Layer 6 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 6: RISK DRAG INGESTION")
        logger.info("=" * 60)

        data_year = 2025
        df = calculate_risk_indicators(data_year)

        if df.empty:
            logger.error("No risk data to store (real data not wired)")
            log_refresh(
                layer_name="layer6_risk_drag",
                data_source="FEMA+EPA+MDOT",
                status="failed",
                error_message="Ingestion disabled until real data sources are implemented",
                metadata={"data_year": data_year}
            )
            return

        store_risk_data(df)

        log_refresh(
            layer_name="layer6_risk_drag",
            data_source="FEMA+EPA+MDOT",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={"data_year": data_year}
        )

        logger.info("✓ Layer 6 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 6 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer6_risk_drag",
            data_source="FEMA+EPA+MDOT",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
