"""
Maryland Viability Atlas - Layer 1: Employment Gravity
Ingests employment structure, sector diversity, and federal spending data

Data Sources:
- US Census LEHD/LODES (employment by sector)
- BLS QCEW (wages and establishments)
- USASpending.gov (federal awards)

Signals Produced:
- Sector diversity entropy (Shannon entropy)
- Stable sector employment share
- Federal spending persistence
- Employment diversification score
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List
from sqlalchemy import text

from config.settings import get_settings, MD_COUNTY_FIPS, LODES_SECTOR_CODES, STABLE_SECTORS
from config.database import get_db, log_refresh
from src.utils.data_sources import fetch_lodes_wac, fetch_bls_qcew, fetch_usaspending_county
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def calculate_shannon_entropy(sector_jobs: List[int]) -> float:
    """
    Calculate Shannon entropy for employment diversity.

    Higher entropy = more diversified economy

    Args:
        sector_jobs: List of job counts by sector

    Returns:
        Shannon entropy value
    """
    total_jobs = sum(sector_jobs)

    if total_jobs == 0:
        return 0.0

    entropy = 0.0
    for jobs in sector_jobs:
        if jobs > 0:
            prob = jobs / total_jobs
            entropy -= prob * np.log2(prob)

    return entropy


def calculate_stable_sector_share(row: pd.Series) -> float:
    """
    Calculate share of employment in stable sectors (education, health, public admin).

    Args:
        row: DataFrame row with CNS sector columns

    Returns:
        Share of employment in stable sectors (0-1)
    """
    stable_jobs = sum([
        row.get('jobs_education_health', 0),
        row.get('jobs_public_admin', 0)
    ])

    total_jobs = row.get('total_jobs', 0)

    if total_jobs == 0:
        return 0.0

    return stable_jobs / total_jobs


def ingest_lodes_data(data_year: int = None) -> pd.DataFrame:
    """
    Ingest LEHD/LODES Workplace Area Characteristics data.

    Args:
        data_year: Year to fetch (default: latest from settings)

    Returns:
        DataFrame aggregated to county level
    """
    data_year = data_year or settings.LODES_LATEST_YEAR

    logger.info(f"Ingesting LODES data for Maryland, year {data_year}")

    # Fetch LODES WAC data
    df = fetch_lodes_wac(state="md", year=data_year, job_type="JT00")

    if df.empty:
        logger.error("No LODES data fetched")
        return pd.DataFrame()

    # Extract county FIPS from block-level geocode
    # w_geocode format: SSCCCTTTTTT (State, County, Tract, Block)
    df['fips_code'] = df['w_geocode'].str[:5]

    # Aggregate to county level
    county_agg = df.groupby('fips_code').agg({
        'C000': 'sum',  # Total jobs
        'CNS01': 'sum', 'CNS02': 'sum', 'CNS03': 'sum', 'CNS04': 'sum',
        'CNS05': 'sum', 'CNS06': 'sum', 'CNS07': 'sum', 'CNS08': 'sum',
        'CNS09': 'sum', 'CNS10': 'sum', 'CNS11': 'sum', 'CNS12': 'sum',
        'CNS13': 'sum', 'CNS14': 'sum', 'CNS15': 'sum', 'CNS16': 'sum',
        'CNS17': 'sum', 'CNS18': 'sum', 'CNS19': 'sum', 'CNS20': 'sum'
    }).reset_index()

    # Rename columns to match schema
    county_agg = county_agg.rename(columns={
        'C000': 'total_jobs',
        'CNS01': 'cns01', 'CNS02': 'cns02', 'CNS03': 'cns03',
        'CNS04': 'cns04', 'CNS05': 'cns05', 'CNS06': 'cns06',
        'CNS07': 'cns07', 'CNS08': 'cns08', 'CNS09': 'cns09',
        'CNS10': 'cns10', 'CNS11': 'cns11', 'CNS12': 'cns12',
        'CNS13': 'cns13', 'CNS14': 'cns14', 'CNS15': 'cns15',
        'CNS16': 'cns16', 'CNS17': 'cns17', 'CNS18': 'cns18',
        'CNS19': 'cns19', 'CNS20': 'cns20'
    })

    # Group sectors for schema
    county_agg['jobs_goods_producing'] = (
        county_agg['cns01'] + county_agg['cns02'] +
        county_agg['cns03'] + county_agg['cns04'] + county_agg['cns05']
    )
    county_agg['jobs_trade_transport'] = county_agg['cns07'] + county_agg['cns08']
    county_agg['jobs_information'] = county_agg['cns09']
    county_agg['jobs_financial'] = county_agg['cns10']
    county_agg['jobs_professional'] = county_agg['cns11'] + county_agg['cns12']
    county_agg['jobs_education_health'] = county_agg['cns15'] + county_agg['cns16']
    county_agg['jobs_leisure'] = county_agg['cns17'] + county_agg['cns18']
    county_agg['jobs_other_services'] = county_agg['cns19']
    county_agg['jobs_public_admin'] = county_agg['cns20']

    # Calculate sector diversity entropy
    sector_cols = [f'cns{i:02d}' for i in range(1, 21)]
    county_agg['sector_diversity_entropy'] = county_agg[sector_cols].apply(
        lambda row: calculate_shannon_entropy(row.tolist()), axis=1
    )

    # Calculate stable sector share
    county_agg['stable_sector_share'] = county_agg.apply(calculate_stable_sector_share, axis=1)

    logger.info(f"Processed LODES data for {len(county_agg)} counties")

    return county_agg


def ingest_bls_qcew_data(data_year: int = 2025, quarter: int = 3) -> pd.DataFrame:
    """
    Ingest BLS QCEW data for Maryland counties.

    Args:
        data_year: Year to fetch
        quarter: Quarter (1-4)

    Returns:
        DataFrame with county-level QCEW metrics
    """
    logger.info(f"Ingesting BLS QCEW data for {data_year} Q{quarter}")

    df = fetch_bls_qcew(year=data_year, quarter=quarter)

    if df.empty:
        logger.error("No BLS QCEW data fetched")
        return pd.DataFrame()

    # Filter for county-level, all ownership, total all industries
    df_county = df[
        (df['agglvl_code'] == '70') &  # County level
        (df['own_code'] == '0') &      # All ownership
        (df['industry_code'] == '10')   # Total, all industries
    ].copy()

    # Extract county FIPS
    df_county['fips_code'] = df_county['area_fips'].astype(str).str.zfill(5)

    # Filter to Maryland counties
    df_county = df_county[df_county['fips_code'].str.startswith('24')]

    # Aggregate metrics
    county_qcew = df_county.groupby('fips_code').agg({
        'annual_avg_wkly_wage': 'mean',
        'annual_avg_estabs': 'sum'
    }).reset_index()

    county_qcew = county_qcew.rename(columns={
        'annual_avg_wkly_wage': 'avg_weekly_wage',
        'annual_avg_estabs': 'qcew_total_establishments'
    })

    logger.info(f"Processed BLS QCEW data for {len(county_qcew)} counties")

    return county_qcew


def ingest_usaspending_data(
    start_year: int = 2020,
    end_year: int = 2025
) -> pd.DataFrame:
    """
    Ingest USASpending.gov federal awards data.

    Calculates:
    - Total awards in period
    - 5-year average
    - Volatility (coefficient of variation)

    Args:
        start_year: Start year for analysis
        end_year: End year for analysis

    Returns:
        DataFrame with county-level federal spending metrics
    """
    logger.info(f"Ingesting USASpending data for {start_year}-{end_year}")

    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"

    df = fetch_usaspending_county(start_date=start_date, end_date=end_date)

    if df.empty:
        logger.error("No USASpending data fetched")
        return pd.DataFrame()

    # Extract county FIPS from shape_code
    # USASpending returns county FIPS in 'shape_code' field
    if 'shape_code' in df.columns:
        df['fips_code'] = df['shape_code'].astype(str).str.zfill(5)
    else:
        logger.warning("No shape_code in USASpending response, using display_name parsing")
        return pd.DataFrame()

    # Filter to Maryland
    df = df[df['fips_code'].str.startswith('24')]

    # Aggregate total awards
    county_spending = df.groupby('fips_code').agg({
        'aggregated_amount': 'sum'
    }).reset_index()

    county_spending = county_spending.rename(columns={
        'aggregated_amount': 'federal_awards_total'
    })

    # Calculate 5-year average
    num_years = end_year - start_year + 1
    county_spending['federal_awards_5yr_avg'] = (
        county_spending['federal_awards_total'] / num_years
    )

    # For volatility, we need year-by-year data
    # Fetch each year separately and calculate CV
    yearly_awards = []

    for year in range(start_year, end_year + 1):
        year_start = f"{year}-01-01"
        year_end = f"{year}-12-31"

        df_year = fetch_usaspending_county(start_date=year_start, end_date=year_end)

        if not df_year.empty:
            if 'shape_code' in df_year.columns:
                df_year['fips_code'] = df_year['shape_code'].astype(str).str.zfill(5)
                df_year = df_year[df_year['fips_code'].str.startswith('24')]

                year_agg = df_year.groupby('fips_code')['aggregated_amount'].sum().reset_index()
                year_agg['year'] = year
                yearly_awards.append(year_agg)

    if yearly_awards:
        df_yearly = pd.concat(yearly_awards, ignore_index=True)

        # Calculate coefficient of variation by county
        volatility = df_yearly.groupby('fips_code')['aggregated_amount'].agg(
            mean='mean',
            std='std'
        ).reset_index()

        volatility['federal_awards_volatility'] = volatility['std'] / volatility['mean']
        volatility = volatility[['fips_code', 'federal_awards_volatility']]

        # Merge with main spending data
        county_spending = county_spending.merge(volatility, on='fips_code', how='left')
    else:
        county_spending['federal_awards_volatility'] = np.nan

    logger.info(f"Processed USASpending data for {len(county_spending)} counties")

    return county_spending


def calculate_employment_diversification_score(row: pd.Series) -> float:
    """
    Calculate composite employment diversification score.

    Combines:
    - Sector entropy (normalized)
    - Stable sector share

    Args:
        row: DataFrame row with employment metrics

    Returns:
        Diversification score (0-1, higher is better)
    """
    # Normalize entropy (max entropy for 20 sectors = log2(20) = 4.32)
    max_entropy = np.log2(20)
    entropy_score = row['sector_diversity_entropy'] / max_entropy

    # Stable sector share (already 0-1)
    stable_score = row['stable_sector_share']

    # Weighted combination (70% diversity, 30% stability)
    composite = 0.7 * entropy_score + 0.3 * stable_score

    return composite


def merge_and_store(
    lodes_df: pd.DataFrame,
    qcew_df: pd.DataFrame,
    spending_df: pd.DataFrame,
    data_year: int
):
    """
    Merge all employment data sources and store in database.

    Args:
        lodes_df: LODES employment data
        qcew_df: BLS QCEW data
        spending_df: USASpending data
        data_year: Data year for record
    """
    logger.info("Merging employment data sources")

    # Start with LODES (most comprehensive)
    merged = lodes_df.copy()

    # Merge QCEW
    if not qcew_df.empty:
        merged = merged.merge(qcew_df, on='fips_code', how='left')
    else:
        merged['avg_weekly_wage'] = np.nan
        merged['qcew_total_establishments'] = np.nan

    # Merge USASpending
    if not spending_df.empty:
        merged = merged.merge(spending_df, on='fips_code', how='left')
    else:
        merged['federal_awards_total'] = np.nan
        merged['federal_awards_5yr_avg'] = np.nan
        merged['federal_awards_volatility'] = np.nan

    # Calculate composite diversification score
    merged['employment_diversification_score'] = merged.apply(
        calculate_employment_diversification_score, axis=1
    )

    # Add data year
    merged['data_year'] = data_year

    # Select columns matching schema
    columns_to_store = [
        'fips_code', 'data_year', 'total_jobs', 'sector_diversity_entropy',
        'jobs_goods_producing', 'jobs_trade_transport', 'jobs_information',
        'jobs_financial', 'jobs_professional', 'jobs_education_health',
        'jobs_leisure', 'jobs_other_services', 'jobs_public_admin',
        'avg_weekly_wage', 'qcew_total_establishments',
        'federal_awards_total', 'federal_awards_5yr_avg', 'federal_awards_volatility',
        'stable_sector_share', 'employment_diversification_score'
    ]

    df_to_store = merged[columns_to_store]

    # Store in database
    with get_db() as db:
        logger.info(f"Storing {len(df_to_store)} employment records in database")

        for _, row in df_to_store.iterrows():
            sql = text("""
                INSERT INTO layer1_employment_gravity (
                    fips_code, data_year, total_jobs, sector_diversity_entropy,
                    jobs_goods_producing, jobs_trade_transport, jobs_information,
                    jobs_financial, jobs_professional, jobs_education_health,
                    jobs_leisure, jobs_other_services, jobs_public_admin,
                    avg_weekly_wage, qcew_total_establishments,
                    federal_awards_total, federal_awards_5yr_avg, federal_awards_volatility,
                    stable_sector_share, employment_diversification_score
                ) VALUES (
                    :fips_code, :data_year, :total_jobs, :sector_diversity_entropy,
                    :jobs_goods_producing, :jobs_trade_transport, :jobs_information,
                    :jobs_financial, :jobs_professional, :jobs_education_health,
                    :jobs_leisure, :jobs_other_services, :jobs_public_admin,
                    :avg_weekly_wage, :qcew_total_establishments,
                    :federal_awards_total, :federal_awards_5yr_avg, :federal_awards_volatility,
                    :stable_sector_share, :employment_diversification_score
                )
                ON CONFLICT (fips_code, data_year)
                DO UPDATE SET
                    total_jobs = EXCLUDED.total_jobs,
                    sector_diversity_entropy = EXCLUDED.sector_diversity_entropy,
                    jobs_goods_producing = EXCLUDED.jobs_goods_producing,
                    jobs_trade_transport = EXCLUDED.jobs_trade_transport,
                    jobs_information = EXCLUDED.jobs_information,
                    jobs_financial = EXCLUDED.jobs_financial,
                    jobs_professional = EXCLUDED.jobs_professional,
                    jobs_education_health = EXCLUDED.jobs_education_health,
                    jobs_leisure = EXCLUDED.jobs_leisure,
                    jobs_other_services = EXCLUDED.jobs_other_services,
                    jobs_public_admin = EXCLUDED.jobs_public_admin,
                    avg_weekly_wage = EXCLUDED.avg_weekly_wage,
                    qcew_total_establishments = EXCLUDED.qcew_total_establishments,
                    federal_awards_total = EXCLUDED.federal_awards_total,
                    federal_awards_5yr_avg = EXCLUDED.federal_awards_5yr_avg,
                    federal_awards_volatility = EXCLUDED.federal_awards_volatility,
                    stable_sector_share = EXCLUDED.stable_sector_share,
                    employment_diversification_score = EXCLUDED.employment_diversification_score,
                    updated_at = CURRENT_TIMESTAMP
            """)

            db.execute(sql, row.to_dict())

        db.commit()

    logger.info("Layer 1 (Employment Gravity) ingestion complete")


def run_layer1_ingestion(data_year: int = None):
    """
    Main entry point for Layer 1 ingestion.

    Args:
        data_year: Year to process (default: latest from settings)
    """
    data_year = data_year or settings.LODES_LATEST_YEAR

    logger.info(f"Starting Layer 1 (Employment Gravity) ingestion for year {data_year}")

    try:
        # Ingest LODES
        lodes_df = ingest_lodes_data(data_year=data_year)

        # Ingest BLS QCEW
        qcew_df = ingest_bls_qcew_data(data_year=2025, quarter=3)

        # Ingest USASpending (5-year window)
        spending_df = ingest_usaspending_data(
            start_year=data_year - 4,
            end_year=data_year
        )

        # Merge and store
        merge_and_store(lodes_df, qcew_df, spending_df, data_year)

        # Log success
        log_refresh(
            layer_name="layer1_employment_gravity",
            data_source="LEHD/LODES, BLS QCEW, USASpending",
            status="success",
            records_processed=len(lodes_df),
            records_inserted=len(lodes_df),
            metadata={"data_year": data_year}
        )

        logger.info("Layer 1 ingestion completed successfully")

    except Exception as e:
        logger.error(f"Layer 1 ingestion failed: {e}", exc_info=True)

        log_refresh(
            layer_name="layer1_employment_gravity",
            data_source="LEHD/LODES, BLS QCEW, USASpending",
            status="failed",
            error_message=str(e),
            metadata={"data_year": data_year}
        )

        raise


if __name__ == "__main__":
    import sys
    from src.utils.logging import setup_logging

    setup_logging("layer1_employment")

    year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_layer1_ingestion(data_year=year)
