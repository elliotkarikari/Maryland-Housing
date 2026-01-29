"""
Maryland Viability Atlas - Layer 5: Demographic Momentum
Ingests migration patterns and household formation signals

Signals Produced:
- Working-age population trends
- Migration flows
- Household formation rates
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
import argparse
from typing import Optional

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

CACHE_DIR = Path("data/cache/demographics")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

ACS_DEMOGRAPHIC_VARIABLES = {
    "B01001_001E": "pop_total",
    "B01001_010E": "pop_m_25_29",
    "B01001_011E": "pop_m_30_34",
    "B01001_012E": "pop_m_35_39",
    "B01001_013E": "pop_m_40_44",
    "B01001_034E": "pop_f_25_29",
    "B01001_035E": "pop_f_30_34",
    "B01001_036E": "pop_f_35_39",
    "B01001_037E": "pop_f_40_44",
    "B11001_001E": "households_total",
    "B11001_002E": "households_family",
    "B11005_003E": "households_family_with_children",
}

IRS_YEAR_RANGES = ["1718", "1819", "1920", "2021", "2122"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _find_col(columns: list[str], candidates: list[str]) -> Optional[str]:
    for cand in candidates:
        if cand in columns:
            return cand
    for cand in candidates:
        for col in columns:
            if cand in col:
                return col
    return None


def _download_irs_flow(year_range: str, flow: str) -> Path:
    filename = f"irs_{flow}_{year_range}.csv"
    target = CACHE_DIR / filename
    if not target.exists():
        url = f"https://www.irs.gov/pub/irs-soi/county{flow}{year_range}.csv"
        logger.info(f"Downloading IRS {flow} data for {year_range}")
        ok = download_file(url, str(target))
        if not ok:
            raise RuntimeError(f"Failed to download IRS {flow} file for {year_range}")
    return target


def _load_irs_flow(year_range: str, flow: str) -> pd.DataFrame:
    path = _download_irs_flow(year_range, flow)
    df = pd.read_csv(path, dtype=str)
    return _normalize_columns(df)


def _aggregate_irs_flow(df: pd.DataFrame, flow: str) -> pd.DataFrame:
    columns = list(df.columns)
    if flow == "inflow":
        state_col = _find_col(columns, ["y2_statefips", "y2_state_fips", "statefips_dest"])
        county_col = _find_col(columns, ["y2_countyfips", "y2_county_fips", "countyfips_dest"])
        other_col = _find_col(columns, ["y1_countyfips", "y1_county_fips"])
    else:
        state_col = _find_col(columns, ["y1_statefips", "y1_state_fips", "statefips_orig"])
        county_col = _find_col(columns, ["y1_countyfips", "y1_county_fips", "countyfips_orig"])
        other_col = _find_col(columns, ["y2_countyfips", "y2_county_fips"])

    n1_col = _find_col(columns, ["n1", "num_returns", "returns"])
    n2_col = _find_col(columns, ["n2", "num_exemptions", "exemptions"])

    if not state_col or not county_col or not n1_col or not n2_col:
        raise RuntimeError("IRS migration file missing required columns")

    df[state_col] = df[state_col].astype(str).str.zfill(2)
    df[county_col] = df[county_col].astype(str).str.zfill(3)

    # Filter to Maryland counties and exclude aggregate rows
    df = df[df[state_col] == "24"].copy()
    df = df[df[county_col] != "000"]
    if other_col:
        df = df[df[other_col].astype(str).str.zfill(3) != "000"]

    df['fips_code'] = df[state_col] + df[county_col]
    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]

    df[n1_col] = pd.to_numeric(df[n1_col], errors='coerce')
    df[n2_col] = pd.to_numeric(df[n2_col], errors='coerce')

    agg = df.groupby('fips_code', as_index=False)[[n1_col, n2_col]].sum()
    agg = agg.rename(columns={n1_col: f"{flow}_households", n2_col: f"{flow}_exemptions"})
    return agg


def fetch_irs_migration_by_year() -> dict[int, pd.DataFrame]:
    """Fetch IRS inflow/outflow data across available year ranges."""
    results: dict[int, pd.DataFrame] = {}
    for year_range in IRS_YEAR_RANGES:
        try:
            inflow = _aggregate_irs_flow(_load_irs_flow(year_range, "inflow"), "inflow")
            outflow = _aggregate_irs_flow(_load_irs_flow(year_range, "outflow"), "outflow")
            df = inflow.merge(outflow, on='fips_code', how='outer')

            data_year = 2000 + int(year_range[2:])
            df['data_year'] = data_year
            results[data_year] = df
            logger.info(f"Loaded IRS migration data for {year_range} ({data_year})")
        except Exception as e:
            logger.warning(f"Skipping IRS migration {year_range}: {e}")
            continue
    return results


def fetch_acs_demographic_data(data_year: int) -> pd.DataFrame:
    logger.info(f"Fetching ACS demographic data for {data_year}")
    df = fetch_census_data(
        dataset='acs/acs5',
        variables=list(ACS_DEMOGRAPHIC_VARIABLES.keys()),
        geography='county:*',
        state='24',
        year=data_year
    )

    if df.empty:
        return pd.DataFrame()

    df['fips_code'] = '24' + df['county'].str.zfill(3)
    for acs_var, col_name in ACS_DEMOGRAPHIC_VARIABLES.items():
        if acs_var in df.columns:
            df[col_name] = pd.to_numeric(df[acs_var], errors='coerce')

    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]
    cols_to_keep = ['fips_code'] + list(ACS_DEMOGRAPHIC_VARIABLES.values())
    return df[[c for c in cols_to_keep if c in df.columns]]


def calculate_demographic_indicators(data_year: int = 2023) -> pd.DataFrame:
    """Calculate demographic momentum indicators."""
    acs_df = fetch_acs_demographic_data(data_year)
    if acs_df.empty:
        return pd.DataFrame()

    acs_df['data_year'] = data_year

    acs_df['pop_age_25_44'] = (
        acs_df['pop_m_25_29'] + acs_df['pop_m_30_34'] + acs_df['pop_m_35_39'] + acs_df['pop_m_40_44'] +
        acs_df['pop_f_25_29'] + acs_df['pop_f_30_34'] + acs_df['pop_f_35_39'] + acs_df['pop_f_40_44']
    )
    acs_df['pop_age_25_44_pct'] = acs_df['pop_age_25_44'] / acs_df['pop_total']

    return acs_df


def store_demographic_data(df: pd.DataFrame):
    """Store demographic momentum data in database."""
    logger.info(f"Storing {len(df)} demographic records")

    with get_db() as db:
        years = df['data_year'].unique().tolist()
        db.execute(
            text("DELETE FROM layer5_demographic_momentum WHERE data_year = ANY(:years)"),
            {"years": years}
        )

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

        parser = argparse.ArgumentParser(description='Ingest Layer 5 Demographic data')
        parser.add_argument('--year', type=int, help='Latest ACS year to fetch (default: latest)')
        parser.add_argument('--single-year', action='store_true', help='Fetch only single year (default: multi-year)')
        args = parser.parse_args()

        current_year = datetime.utcnow().year
        latest_year = args.year or min(settings.ACS_LATEST_YEAR, current_year)
        if args.single_year:
            years_to_fetch = [latest_year]
        else:
            years_to_fetch = list(range(latest_year - 4, latest_year + 1))

        irs_by_year = fetch_irs_migration_by_year()
        all_years = []

        for year in years_to_fetch:
            acs_df = calculate_demographic_indicators(year)
            if acs_df.empty:
                logger.warning(f"No ACS demographic data for {year}")
                continue

            irs_df = irs_by_year.get(year)
            if irs_df is not None and not irs_df.empty:
                merged = acs_df.merge(irs_df, on=['fips_code', 'data_year'], how='left')
            else:
                merged = acs_df.copy()
                merged['inflow_households'] = pd.NA
                merged['outflow_households'] = pd.NA
                merged['inflow_exemptions'] = pd.NA
                merged['outflow_exemptions'] = pd.NA

            merged['net_migration_households'] = merged['inflow_households'] - merged['outflow_households']
            merged['net_migration_persons'] = merged['inflow_exemptions'] - merged['outflow_exemptions']

            merged['family_household_inflow_rate'] = pd.NA
            if 'households_family_with_children' in merged.columns:
                merged['family_household_inflow_rate'] = merged['inflow_households'] / merged['households_family_with_children']
                merged.loc[merged['households_family_with_children'] == 0, 'family_household_inflow_rate'] = pd.NA

            all_years.append(merged)

        if not all_years:
            logger.error("No demographic data to store (real data not available)")
            log_refresh(
                layer_name="layer5_demographic_momentum",
                data_source="ACS+IRS",
                status="failed",
                error_message="No ACS/IRS records produced",
                metadata={"years_requested": years_to_fetch}
            )
            return

        combined = pd.concat(all_years, ignore_index=True)

        # Working-age momentum (3-year change) and household formation change (YoY)
        combined['working_age_momentum'] = pd.NA
        combined['household_formation_change'] = pd.NA

        for fips in combined['fips_code'].unique():
            sub = combined[combined['fips_code'] == fips].copy()
            year_to_pop = {row['data_year']: row['pop_age_25_44'] for _, row in sub.iterrows()}
            year_to_households = {row['data_year']: row['households_total'] for _, row in sub.iterrows()}

            for idx, row in sub.iterrows():
                year = row['data_year']
                baseline_year = year - 2
                if baseline_year in year_to_pop:
                    baseline = year_to_pop[baseline_year]
                    current = row['pop_age_25_44']
                    if pd.notna(baseline) and baseline != 0 and pd.notna(current):
                        combined.loc[idx, 'working_age_momentum'] = (current - baseline) / baseline * 100

                prior_year = year - 1
                if prior_year in year_to_households:
                    baseline = year_to_households[prior_year]
                    current = row['households_total']
                    if pd.notna(baseline) and baseline != 0 and pd.notna(current):
                        combined.loc[idx, 'household_formation_change'] = (current - baseline) / baseline * 100

        # Composite demographic momentum score (percentile of available signals)
        combined['demographic_momentum_score'] = pd.NA
        for year in combined['data_year'].unique():
            year_mask = combined['data_year'] == year
            sub = combined.loc[year_mask].copy()

            components = []
            if sub['net_migration_households'].notna().sum() >= 3:
                components.append(sub['net_migration_households'].rank(pct=True))
            if sub['working_age_momentum'].notna().sum() >= 3:
                components.append(sub['working_age_momentum'].rank(pct=True))
            if sub['household_formation_change'].notna().sum() >= 3:
                components.append(sub['household_formation_change'].rank(pct=True))
            if sub['pop_age_25_44_pct'].notna().sum() >= 3:
                components.append(sub['pop_age_25_44_pct'].rank(pct=True))

            if components:
                combined.loc[year_mask, 'demographic_momentum_score'] = pd.concat(components, axis=1).mean(axis=1)

        # USPS vacancy fields are not programmatically accessible; keep nulls
        combined['total_addresses'] = pd.NA
        combined['vacant_addresses'] = pd.NA
        combined['vacancy_rate'] = pd.NA
        combined.replace([np.inf, -np.inf], pd.NA, inplace=True)

        store_demographic_data(combined)

        log_refresh(
            layer_name="layer5_demographic_momentum",
            data_source="ACS+IRS",
            status="success",
            records_processed=len(combined),
            records_inserted=len(combined),
            metadata={"years": sorted(combined['data_year'].unique().tolist())}
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
