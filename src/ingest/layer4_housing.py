"""
Maryland Viability Atlas - Layer 4: Housing Elasticity
Ingests housing supply responsiveness and growth capacity

Data Sources:
- ACS 5-Year Estimates (housing metrics)
- HUD Building Permits (future implementation)

Signals Produced:
- Median home values (multi-year)
- Median household income (multi-year)
- Price-to-income ratios
- Housing supply indicators
"""

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
import argparse

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.data_sources import fetch_census_data
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# ACS variables for housing
ACS_HOUSING_VARIABLES = {
    'B25077_001E': 'median_home_value',        # Median value (owner-occupied)
    'B19013_001E': 'median_household_income',  # Median household income
    'B25001_001E': 'total_housing_units',      # Total housing units
    'B25002_002E': 'occupied_units',           # Occupied housing units
    'B25002_003E': 'vacant_units',             # Vacant housing units
    'B25024_002E': 'units_single_family',      # 1-unit detached
    'B25024_003E': 'units_single_attached',    # 1-unit attached
    'B25024_004E': 'units_2_units',            # 2 units
    'B25024_005E': 'units_3_4_units',          # 3-4 units
    'B25024_006E': 'units_5_9_units',          # 5-9 units
    'B25024_007E': 'units_10_19_units',        # 10-19 units
    'B25024_008E': 'units_20_49_units',        # 20-49 units
    'B25024_009E': 'units_50plus_units',       # 50+ units
}


def fetch_acs_housing_data(data_year: int = 2021) -> pd.DataFrame:
    """
    Fetch ACS 5-year housing estimates for Maryland counties.

    Args:
        data_year: Year of ACS release (e.g., 2021 for 2017-2021 estimates)

    Returns:
        DataFrame with housing metrics for all MD counties
    """
    logger.info(f"Fetching ACS housing data for {data_year}")

    try:
        df = fetch_census_data(
            dataset='acs/acs5',
            variables=list(ACS_HOUSING_VARIABLES.keys()),
            geography='county:*',
            state='24',
            year=data_year
        )

        if df.empty:
            logger.warning(f"No ACS data for {data_year}")
            return pd.DataFrame()

        # Extract FIPS code
        df['fips_code'] = '24' + df['county'].str.zfill(3)

        # Rename columns
        for acs_var, col_name in ACS_HOUSING_VARIABLES.items():
            if acs_var in df.columns:
                df[col_name] = pd.to_numeric(df[acs_var], errors='coerce')

        # Keep only Maryland counties we track
        df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]

        # Select final columns
        cols_to_keep = ['fips_code'] + list(ACS_HOUSING_VARIABLES.values())
        df = df[[c for c in cols_to_keep if c in df.columns]]

        logger.info(f"Fetched ACS housing data for {len(df)} counties")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch ACS housing data for {data_year}: {e}")
        raise


def calculate_housing_indicators(df: pd.DataFrame, data_year: int) -> pd.DataFrame:
    """
    Calculate derived housing indicators from ACS data.

    Args:
        df: DataFrame with ACS housing data
        data_year: Data year

    Returns:
        DataFrame with calculated indicators
    """
    logger.info("Calculating housing indicators")

    df = df.copy()
    df['data_year'] = data_year

    # Price-to-income ratio
    df['price_to_income_ratio'] = df['median_home_value'] / df['median_household_income']

    # Vacancy rate
    df['vacancy_rate'] = df['vacant_units'] / df['total_housing_units']

    # Occupancy rate
    df['occupancy_rate'] = df['occupied_units'] / df['total_housing_units']

    # Multifamily share
    multifamily_cols = ['units_2_units', 'units_3_4_units', 'units_5_9_units',
                        'units_10_19_units', 'units_20_49_units', 'units_50plus_units']
    df['multifamily_units'] = df[[c for c in multifamily_cols if c in df.columns]].sum(axis=1)
    df['single_family_units'] = df['units_single_family'] + df['units_single_attached']
    df['multifamily_share'] = df['multifamily_units'] / df['total_housing_units']

    # Housing elasticity index (simplified: lower price-to-income + higher vacancy = more elastic)
    # Normalize components
    price_ratio_norm = 1 - (df['price_to_income_ratio'].rank(pct=True))
    vacancy_norm = df['vacancy_rate'].rank(pct=True)

    df['housing_elasticity_index'] = (0.6 * price_ratio_norm + 0.4 * vacancy_norm)

    # Supply responsiveness score (vacancy as proxy for supply-demand balance)
    df['supply_responsiveness_score'] = df['vacancy_rate'].clip(0, 0.15) / 0.15

    logger.info(f"Calculated indicators for {len(df)} counties")
    return df


def store_housing_data(df: pd.DataFrame):
    """
    Store housing elasticity data in database with ON CONFLICT handling.

    Args:
        df: DataFrame with housing indicators
    """
    logger.info(f"Storing {len(df)} housing records")

    with get_db() as db:
        insert_sql = text("""
            INSERT INTO layer4_housing_elasticity (
                fips_code, data_year,
                median_home_value, median_household_income,
                price_to_income_ratio,
                total_housing_units, occupied_units, vacant_units,
                vacancy_rate, occupancy_rate,
                single_family_units, multifamily_units, multifamily_share,
                supply_responsiveness_score, housing_elasticity_index
            ) VALUES (
                :fips_code, :data_year,
                :median_home_value, :median_household_income,
                :price_to_income_ratio,
                :total_housing_units, :occupied_units, :vacant_units,
                :vacancy_rate, :occupancy_rate,
                :single_family_units, :multifamily_units, :multifamily_share,
                :supply_responsiveness_score, :housing_elasticity_index
            )
            ON CONFLICT (fips_code, data_year)
            DO UPDATE SET
                median_home_value = EXCLUDED.median_home_value,
                median_household_income = EXCLUDED.median_household_income,
                price_to_income_ratio = EXCLUDED.price_to_income_ratio,
                total_housing_units = EXCLUDED.total_housing_units,
                occupied_units = EXCLUDED.occupied_units,
                vacant_units = EXCLUDED.vacant_units,
                vacancy_rate = EXCLUDED.vacancy_rate,
                occupancy_rate = EXCLUDED.occupancy_rate,
                single_family_units = EXCLUDED.single_family_units,
                multifamily_units = EXCLUDED.multifamily_units,
                multifamily_share = EXCLUDED.multifamily_share,
                supply_responsiveness_score = EXCLUDED.supply_responsiveness_score,
                housing_elasticity_index = EXCLUDED.housing_elasticity_index,
                updated_at = CURRENT_TIMESTAMP
        """)

        for _, row in df.iterrows():
            row_dict = {
                'fips_code': row['fips_code'],
                'data_year': int(row['data_year']),
                'median_home_value': float(row['median_home_value']) if pd.notna(row['median_home_value']) else None,
                'median_household_income': float(row['median_household_income']) if pd.notna(row['median_household_income']) else None,
                'price_to_income_ratio': float(row['price_to_income_ratio']) if pd.notna(row['price_to_income_ratio']) else None,
                'total_housing_units': int(row['total_housing_units']) if pd.notna(row['total_housing_units']) else None,
                'occupied_units': int(row['occupied_units']) if pd.notna(row['occupied_units']) else None,
                'vacant_units': int(row['vacant_units']) if pd.notna(row['vacant_units']) else None,
                'vacancy_rate': float(row['vacancy_rate']) if pd.notna(row['vacancy_rate']) else None,
                'occupancy_rate': float(row['occupancy_rate']) if pd.notna(row['occupancy_rate']) else None,
                'single_family_units': int(row['single_family_units']) if pd.notna(row['single_family_units']) else None,
                'multifamily_units': int(row['multifamily_units']) if pd.notna(row['multifamily_units']) else None,
                'multifamily_share': float(row['multifamily_share']) if pd.notna(row['multifamily_share']) else None,
                'supply_responsiveness_score': float(row['supply_responsiveness_score']) if pd.notna(row['supply_responsiveness_score']) else None,
                'housing_elasticity_index': float(row['housing_elasticity_index']) if pd.notna(row['housing_elasticity_index']) else None,
            }

            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ Housing data stored successfully")


def run_layer4_ingestion(data_year: int = None, multi_year: bool = True):
    """
    Main entry point for Layer 4 ingestion.

    Args:
        data_year: Latest year to process (default: 2021, latest ACS 5-year)
        multi_year: If True, fetch 3 years of ACS data; if False, single year only
    """
    latest_year = data_year or settings.ACS_LATEST_YEAR

    if multi_year:
        # Fetch 3 years of ACS 5-year estimates (2019, 2020, 2021)
        years_to_fetch = list(range(latest_year - 2, latest_year + 1))
        logger.info(f"Starting Layer 4 (Housing Elasticity) MULTI-YEAR ingestion for years {years_to_fetch[0]}-{years_to_fetch[-1]}")
    else:
        years_to_fetch = [latest_year]
        logger.info(f"Starting Layer 4 (Housing Elasticity) single-year ingestion for {latest_year}")

    total_records = 0
    failed_years = []

    for year in years_to_fetch:
        logger.info("=" * 70)
        logger.info(f"Processing year {year}")
        logger.info("=" * 70)

        try:
            # Fetch ACS housing data
            logger.info(f"  Fetching ACS housing data for {year}...")
            acs_df = fetch_acs_housing_data(data_year=year)

            if acs_df.empty:
                logger.warning(f"  No ACS housing data available for {year}, skipping")
                failed_years.append(year)
                continue

            # Calculate indicators
            logger.info(f"  Calculating housing indicators for {year}...")
            df = calculate_housing_indicators(acs_df, year)

            # Store in database
            logger.info(f"  Storing {year} data...")
            store_housing_data(df)

            total_records += len(df)
            logger.info(f"✓ Year {year} complete: {len(df)} records stored")

        except Exception as e:
            logger.error(f"✗ Year {year} ingestion failed: {e}", exc_info=True)
            failed_years.append(year)
            continue

    # Log final summary
    logger.info("=" * 70)
    if multi_year:
        logger.info(f"MULTI-YEAR INGESTION SUMMARY")
        logger.info(f"  Years requested: {years_to_fetch[0]}-{years_to_fetch[-1]} ({len(years_to_fetch)} years)")
        logger.info(f"  Years successful: {len(years_to_fetch) - len(failed_years)}")
        logger.info(f"  Years failed: {len(failed_years)} {failed_years if failed_years else ''}")
        logger.info(f"  Total records stored: {total_records}")
    else:
        logger.info(f"Single-year ingestion {'succeeded' if not failed_years else 'failed'}")

    # Log refresh
    log_refresh(
        layer_name="layer4_housing_elasticity",
        data_source="ACS 5-Year Estimates",
        status="success" if not failed_years or len(failed_years) < len(years_to_fetch) else "partial",
        records_processed=total_records,
        records_inserted=total_records,
        metadata={
            "multi_year": multi_year,
            "years_requested": years_to_fetch,
            "years_successful": len(years_to_fetch) - len(failed_years),
            "years_failed": failed_years
        }
    )

    if failed_years and len(failed_years) == len(years_to_fetch):
        raise Exception(f"All years failed: {failed_years}")

    logger.info("Layer 4 ingestion completed")
    logger.info("=" * 70)


def main():
    """Main execution for Layer 4 ingestion"""
    parser = argparse.ArgumentParser(description='Ingest Layer 4 Housing data')
    parser.add_argument('--year', type=int, help='Latest year to fetch (default: 2021)')
    parser.add_argument('--single-year', action='store_true', help='Fetch only single year (default: multi-year)')

    args = parser.parse_args()

    try:
        logger.info("=" * 60)
        logger.info("LAYER 4: HOUSING ELASTICITY INGESTION")
        logger.info("=" * 60)

        run_layer4_ingestion(data_year=args.year, multi_year=not args.single_year)

        logger.info("✓ Layer 4 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 4 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer4_housing_elasticity",
            data_source="ACS 5-Year Estimates",
            status="error",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
