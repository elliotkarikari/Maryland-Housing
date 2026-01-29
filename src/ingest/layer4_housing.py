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

import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
from sqlalchemy import text
import argparse

# Ensure project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.data_sources import fetch_census_data, download_file
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
    'B11001_001E': 'households_total',         # Total households
}

CACHE_DIR = Path("data/cache/housing")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BPS_COUNTY_BASE_URL = "https://www2.census.gov/econ/bps/County"


def _download_bps_file(year: int) -> Path:
    """Download Census BPS county annual file and return local path."""
    filename = f"co{year}a.txt"
    target = CACHE_DIR / filename
    if not target.exists():
        url = f"{BPS_COUNTY_BASE_URL}/{filename}"
        logger.info(f"Downloading BPS county file for {year}")
        ok = download_file(url, str(target))
        if not ok:
            raise RuntimeError(f"Failed to download BPS county file for {year}")
    return target


def fetch_bps_permits(data_year: int) -> pd.DataFrame:
    """Fetch annual county permit counts from BPS (Census)."""
    path = _download_bps_file(data_year)
    logger.info(f"Reading BPS county permits for {data_year}")

    columns = [
        "survey_date", "fips_state", "fips_county", "region", "division", "county_name",
        "bldg_101", "units_101", "val_101",
        "bldg_103", "units_103", "val_103",
        "bldg_104", "units_104", "val_104",
        "bldg_105", "units_105", "val_105",
        "bldg_101_rep", "units_101_rep", "val_101_rep",
        "bldg_103_rep", "units_103_rep", "val_103_rep",
        "bldg_104_rep", "units_104_rep", "val_104_rep",
        "bldg_105_rep", "units_105_rep", "val_105_rep",
    ]

    df = pd.read_csv(path, header=None, names=columns, dtype=str, low_memory=False)
    df['fips_state'] = df['fips_state'].str.zfill(2)
    df['fips_county'] = df['fips_county'].str.zfill(3)
    df['fips_code'] = df['fips_state'] + df['fips_county']
    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())].copy()

    for col in ['units_101', 'units_103', 'units_104', 'units_105']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['permits_single_family'] = df['units_101']
    df['permits_multifamily'] = df[['units_103', 'units_104', 'units_105']].sum(axis=1, min_count=1)
    df['permits_total'] = df[['units_101', 'units_103', 'units_104', 'units_105']].sum(axis=1, min_count=1)

    return df[['fips_code', 'permits_total', 'permits_single_family', 'permits_multifamily']]


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

    # Permit intensity (per 1000 households)
    if 'permits_total' in df.columns and 'households_total' in df.columns:
        df['permits_per_1000_households'] = (df['permits_total'] / df['households_total']) * 1000
    else:
        df['permits_per_1000_households'] = pd.NA

    # Housing elasticity index (simplified: lower price-to-income + higher vacancy = more elastic)
    # Normalize components
    price_ratio_norm = 1 - (df['price_to_income_ratio'].rank(pct=True))
    vacancy_norm = df['vacancy_rate'].rank(pct=True)

    df['housing_elasticity_index'] = (0.6 * price_ratio_norm + 0.4 * vacancy_norm)

    # Supply responsiveness score (vacancy + permits)
    permits_norm = df['permits_per_1000_households'].rank(pct=True) if df['permits_per_1000_households'].notna().any() else None
    if permits_norm is not None:
        df['supply_responsiveness_score'] = (0.5 * vacancy_norm + 0.5 * permits_norm)
    else:
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
                permits_total, permits_single_family, permits_multifamily,
                permits_per_1000_households, permits_3yr_trend,
                median_home_value, median_household_income,
                price_to_income_ratio, price_to_income_5yr_change,
                has_open_zoning_gis, zoning_capacity_indicator,
                supply_responsiveness_score, housing_elasticity_index
            ) VALUES (
                :fips_code, :data_year,
                :permits_total, :permits_single_family, :permits_multifamily,
                :permits_per_1000_households, :permits_3yr_trend,
                :median_home_value, :median_household_income,
                :price_to_income_ratio, :price_to_income_5yr_change,
                :has_open_zoning_gis, :zoning_capacity_indicator,
                :supply_responsiveness_score, :housing_elasticity_index
            )
            ON CONFLICT (fips_code, data_year)
            DO UPDATE SET
                permits_total = EXCLUDED.permits_total,
                permits_single_family = EXCLUDED.permits_single_family,
                permits_multifamily = EXCLUDED.permits_multifamily,
                permits_per_1000_households = EXCLUDED.permits_per_1000_households,
                permits_3yr_trend = EXCLUDED.permits_3yr_trend,
                median_home_value = EXCLUDED.median_home_value,
                median_household_income = EXCLUDED.median_household_income,
                price_to_income_ratio = EXCLUDED.price_to_income_ratio,
                price_to_income_5yr_change = EXCLUDED.price_to_income_5yr_change,
                has_open_zoning_gis = EXCLUDED.has_open_zoning_gis,
                zoning_capacity_indicator = EXCLUDED.zoning_capacity_indicator,
                supply_responsiveness_score = EXCLUDED.supply_responsiveness_score,
                housing_elasticity_index = EXCLUDED.housing_elasticity_index,
                updated_at = CURRENT_TIMESTAMP
        """)

        for _, row in df.iterrows():
            row_dict = {
                'fips_code': row['fips_code'],
                'data_year': int(row['data_year']),
                'permits_total': int(row['permits_total']) if pd.notna(row.get('permits_total')) else None,
                'permits_single_family': int(row['permits_single_family']) if pd.notna(row.get('permits_single_family')) else None,
                'permits_multifamily': int(row['permits_multifamily']) if pd.notna(row.get('permits_multifamily')) else None,
                'permits_per_1000_households': float(row['permits_per_1000_households']) if pd.notna(row.get('permits_per_1000_households')) else None,
                'permits_3yr_trend': None if pd.isna(row.get('permits_3yr_trend')) else row.get('permits_3yr_trend'),
                'median_home_value': float(row['median_home_value']) if pd.notna(row['median_home_value']) else None,
                'median_household_income': float(row['median_household_income']) if pd.notna(row['median_household_income']) else None,
                'price_to_income_ratio': float(row['price_to_income_ratio']) if pd.notna(row['price_to_income_ratio']) else None,
                'price_to_income_5yr_change': float(row['price_to_income_5yr_change']) if pd.notna(row.get('price_to_income_5yr_change')) else None,
                'has_open_zoning_gis': None,
                'zoning_capacity_indicator': None,
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
    current_year = datetime.utcnow().year
    latest_year = data_year or min(settings.ACS_LATEST_YEAR, current_year)

    if multi_year:
        # Fetch last 5 years (max lookback per project rules)
        years_to_fetch = list(range(latest_year - 4, latest_year + 1))
        logger.info(f"Starting Layer 4 (Housing Elasticity) MULTI-YEAR ingestion for years {years_to_fetch[0]}-{years_to_fetch[-1]}")
    else:
        years_to_fetch = [latest_year]
        logger.info(f"Starting Layer 4 (Housing Elasticity) single-year ingestion for {latest_year}")

    total_records = 0
    failed_years = []
    all_years = []

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

            # Fetch BPS permits (may not be available for all years)
            permits_df = pd.DataFrame()
            try:
                logger.info(f"  Fetching BPS permit data for {year}...")
                permits_df = fetch_bps_permits(data_year=year)
            except Exception as e:
                logger.warning(f"  BPS permits unavailable for {year}: {e}")

            # Calculate indicators
            logger.info(f"  Calculating housing indicators for {year}...")
            if not permits_df.empty:
                merged = acs_df.merge(permits_df, on='fips_code', how='left')
            else:
                merged = acs_df.copy()
                merged['permits_total'] = pd.NA
                merged['permits_single_family'] = pd.NA
                merged['permits_multifamily'] = pd.NA

            df = calculate_housing_indicators(merged, year)
            all_years.append(df)
            logger.info(f"✓ Year {year} prepared: {len(df)} records")

        except Exception as e:
            logger.error(f"✗ Year {year} ingestion failed: {e}", exc_info=True)
            failed_years.append(year)
            continue

    if not all_years:
        raise Exception("No housing data produced from ACS/BPS sources")

    combined = pd.concat(all_years, ignore_index=True)

    # Compute 5-year price-to-income change when baseline exists
    combined['price_to_income_5yr_change'] = pd.NA
    for fips in combined['fips_code'].unique():
        sub = combined[combined['fips_code'] == fips].copy()
        year_to_ratio = {row['data_year']: row['price_to_income_ratio'] for _, row in sub.iterrows()}
        for idx, row in sub.iterrows():
            baseline_year = row['data_year'] - 5
            if baseline_year in year_to_ratio:
                baseline = year_to_ratio[baseline_year]
                if pd.notna(baseline):
                    combined.loc[idx, 'price_to_income_5yr_change'] = round(row['price_to_income_ratio'] - baseline, 4)

    # Compute 3-year permits trend (per 1000 households) when baseline exists
    combined['permits_3yr_trend'] = pd.NA
    for fips in combined['fips_code'].unique():
        sub = combined[combined['fips_code'] == fips].copy()
        year_to_permits = {row['data_year']: row['permits_per_1000_households'] for _, row in sub.iterrows()}
        for idx, row in sub.iterrows():
            baseline_year = row['data_year'] - 2
            if baseline_year in year_to_permits:
                baseline = year_to_permits[baseline_year]
                current = row['permits_per_1000_households']
                if pd.notna(baseline) and pd.notna(current) and baseline != 0:
                    pct_change = (current - baseline) / baseline * 100
                    if pct_change >= 5:
                        combined.loc[idx, 'permits_3yr_trend'] = 'increasing'
                    elif pct_change <= -5:
                        combined.loc[idx, 'permits_3yr_trend'] = 'decreasing'
                    else:
                        combined.loc[idx, 'permits_3yr_trend'] = 'stable'

    # Store in database
    logger.info("Storing combined housing data...")
    store_housing_data(combined)
    total_records = len(combined)
    logger.info(f"✓ Stored {total_records} housing records")

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
        data_source="ACS 5-Year Estimates + Census BPS",
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
