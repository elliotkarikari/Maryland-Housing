"""
Maryland Viability Atlas - Layer 5 v2: Demographic Equity Analysis
Modern equity-based demographic metrics with migration flow analysis.

Combines:
- v1: Static demographic metrics (population structure, household composition)
- v2: Equity metrics (segregation indices, diversity, family viability)
- v3: Migration dynamics (IRS flows, growth rates, movement patterns)

Data Sources:
- ACS 5-year estimates: Demographics by tract (race, age, income, family)
- IRS SOI Migration: County-to-county flows (apportioned to tract)
- Census: Tract boundaries and population

Methodology:
- Segregation indices computed using tract-level racial composition
- Migration flows apportioned from county to tract via population weights
- Composite index combines static + equity + migration

Composite Formula:
    demographic_opportunity_index = 0.3 × static_score + 0.4 × equity_score + 0.3 × migration_score

Where:
    static_score = normalized(working_age_pct + family_household_pct + diversity)
    equity_score = normalized((1 - dissimilarity) + exposure + family_viability)
    migration_score = normalized(net_migration_rate + population_growth + (1 - outflow_rate))
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

import pandas as pd
import numpy as np
from sqlalchemy import text

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.data_sources import download_file
from src.utils.logging import get_logger
from src.utils.prediction_utils import apply_predictions_to_table
from src.utils.year_policy import acs_geography_year, layer5_default_data_year

logger = get_logger(__name__)
settings = get_settings()

# =============================================================================
# CONFIGURATION
# =============================================================================

# Cache directories
CACHE_DIR = Path("data/cache/demographics_v2")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
ACS_CACHE_DIR = CACHE_DIR / "acs"
ACS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
IRS_CACHE_DIR = CACHE_DIR / "irs"
IRS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Composite weights
STATIC_WEIGHT = 0.30       # v1 static demographics
EQUITY_WEIGHT = 0.40       # v2 equity metrics
MIGRATION_WEIGHT = 0.30    # v3 migration dynamics

# Within equity score
SEGREGATION_WEIGHT = 0.40   # 1 - dissimilarity
DIVERSITY_WEIGHT = 0.30     # Racial diversity index
FAMILY_VIABILITY_WEIGHT = 0.30  # Family conditions

# Within migration score
NET_MIGRATION_WEIGHT = 0.50
POPULATION_GROWTH_WEIGHT = 0.30
STABILITY_WEIGHT = 0.20     # 1 - outflow_rate

DEFAULT_WINDOW_YEARS = 5
ACS_GEOGRAPHY_MAX_YEAR = settings.ACS_GEOGRAPHY_MAX_YEAR

# IRS year ranges available
IRS_YEAR_RANGES = ["1718", "1819", "1920", "2021", "2122"]


# =============================================================================
# DATA ACQUISITION - ACS Demographics
# =============================================================================

def download_acs_demographic_data(year: int) -> pd.DataFrame:
    """
    Download ACS 5-year demographic data for Maryland census tracts.

    Includes: age, race, household composition, income, poverty.

    Args:
        year: ACS year

    Returns:
        DataFrame with demographic metrics by tract
    """
    geo_year = acs_geography_year(year)
    cache_path = ACS_CACHE_DIR / f"md_demographics_{geo_year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached ACS demographics: {cache_path}")
        df = pd.read_csv(cache_path, dtype={'tract_geoid': str, 'fips_code': str})
        df['source_url'] = f"https://api.census.gov/data/{geo_year}/acs/acs5"
        df['fetch_date'] = datetime.utcnow().date().isoformat()
        df['is_real'] = True
        return df

    logger.info(f"Downloading ACS demographic data for {geo_year}...")

    try:
        from census import Census

        c = Census(settings.CENSUS_API_KEY)

        # Comprehensive demographic variables
        variables = [
            'NAME',
            # Total population and age groups
            'B01001_001E',  # Total population
            'B01001_003E', 'B01001_004E', 'B01001_005E', 'B01001_006E',  # Male under 5, 5-9, 10-14, 15-17
            'B01001_007E', 'B01001_008E', 'B01001_009E', 'B01001_010E',  # Male 18-19, 20, 21, 22-24
            'B01001_011E', 'B01001_012E', 'B01001_013E', 'B01001_014E',  # Male 25-29, 30-34, 35-39, 40-44
            'B01001_015E', 'B01001_016E', 'B01001_017E', 'B01001_018E',  # Male 45-49, 50-54, 55-59, 60-61
            'B01001_019E', 'B01001_020E', 'B01001_021E', 'B01001_022E',  # Male 62-64, 65-66, 67-69, 70-74
            'B01001_023E', 'B01001_024E', 'B01001_025E',  # Male 75-79, 80-84, 85+
            'B01001_027E', 'B01001_028E', 'B01001_029E', 'B01001_030E',  # Female under 5, 5-9, 10-14, 15-17
            'B01001_031E', 'B01001_032E', 'B01001_033E', 'B01001_034E',  # Female 18-19, 20, 21, 22-24
            'B01001_035E', 'B01001_036E', 'B01001_037E', 'B01001_038E',  # Female 25-29, 30-34, 35-39, 40-44
            'B01001_039E', 'B01001_040E', 'B01001_041E', 'B01001_042E',  # Female 45-49, 50-54, 55-59, 60-61
            'B01001_043E', 'B01001_044E', 'B01001_045E', 'B01001_046E',  # Female 62-64, 65-66, 67-69, 70-74
            'B01001_047E', 'B01001_048E', 'B01001_049E',  # Female 75-79, 80-84, 85+
            # Race/ethnicity
            'B02001_002E',  # White alone
            'B02001_003E',  # Black alone
            'B02001_005E',  # Asian alone
            'B03003_003E',  # Hispanic
            # Households
            'B11001_001E',  # Total households
            'B11001_002E',  # Family households
            'B11003_010E',  # Single father with children
            'B11003_016E',  # Single mother with children
            'B11003_003E',  # Married couple with children
            # Income and poverty
            'B19113_001E',  # Median family income
            'B17001_002E',  # Population below poverty
            'B17006_002E',  # Children below poverty
        ]

        data = c.acs5.state_county_tract(
            fields=variables,
            state_fips='24',
            county_fips='*',
            tract='*',
            year=geo_year
        )

        df = pd.DataFrame(data)

        # Build tract GEOID
        df['tract_geoid'] = df['state'] + df['county'] + df['tract']
        df['fips_code'] = df['state'] + df['county']

        # Calculate derived metrics
        # Total population
        df['total_population'] = pd.to_numeric(df['B01001_001E'], errors='coerce').fillna(0).astype(int)

        # Age groups (sum male + female)
        # Under 18
        under_18_vars = ['B01001_003E', 'B01001_004E', 'B01001_005E', 'B01001_006E',
                        'B01001_027E', 'B01001_028E', 'B01001_029E', 'B01001_030E']
        df['pop_under_18'] = sum(pd.to_numeric(df[v], errors='coerce').fillna(0) for v in under_18_vars if v in df.columns).astype(int)

        # 18-24
        age_18_24_vars = ['B01001_007E', 'B01001_008E', 'B01001_009E', 'B01001_010E',
                         'B01001_031E', 'B01001_032E', 'B01001_033E', 'B01001_034E']
        df['pop_18_24'] = sum(pd.to_numeric(df[v], errors='coerce').fillna(0) for v in age_18_24_vars if v in df.columns).astype(int)

        # 25-44 (prime working/family-forming)
        age_25_44_vars = ['B01001_011E', 'B01001_012E', 'B01001_013E', 'B01001_014E',
                         'B01001_035E', 'B01001_036E', 'B01001_037E', 'B01001_038E']
        df['pop_25_44'] = sum(pd.to_numeric(df[v], errors='coerce').fillna(0) for v in age_25_44_vars if v in df.columns).astype(int)

        # 45-64
        age_45_64_vars = ['B01001_015E', 'B01001_016E', 'B01001_017E', 'B01001_018E', 'B01001_019E',
                         'B01001_039E', 'B01001_040E', 'B01001_041E', 'B01001_042E', 'B01001_043E']
        df['pop_45_64'] = sum(pd.to_numeric(df[v], errors='coerce').fillna(0) for v in age_45_64_vars if v in df.columns).astype(int)

        # 65+
        age_65_plus_vars = ['B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E', 'B01001_024E', 'B01001_025E',
                           'B01001_044E', 'B01001_045E', 'B01001_046E', 'B01001_047E', 'B01001_048E', 'B01001_049E']
        df['pop_65_plus'] = sum(pd.to_numeric(df[v], errors='coerce').fillna(0) for v in age_65_plus_vars if v in df.columns).astype(int)

        # Race/ethnicity
        df['pop_white_alone'] = pd.to_numeric(df['B02001_002E'], errors='coerce').fillna(0).astype(int)
        df['pop_black_alone'] = pd.to_numeric(df['B02001_003E'], errors='coerce').fillna(0).astype(int)
        df['pop_asian_alone'] = pd.to_numeric(df['B02001_005E'], errors='coerce').fillna(0).astype(int)
        df['pop_hispanic'] = pd.to_numeric(df['B03003_003E'], errors='coerce').fillna(0).astype(int)
        df['pop_other_race'] = df['total_population'] - df['pop_white_alone'] - df['pop_black_alone'] - df['pop_asian_alone'] - df['pop_hispanic']
        df['pop_other_race'] = df['pop_other_race'].clip(lower=0)

        # Households
        df['total_households'] = pd.to_numeric(df['B11001_001E'], errors='coerce').fillna(0).astype(int)
        df['family_households'] = pd.to_numeric(df['B11001_002E'], errors='coerce').fillna(0).astype(int)
        df['single_parent_households'] = (
            pd.to_numeric(df['B11003_010E'], errors='coerce').fillna(0) +
            pd.to_numeric(df['B11003_016E'], errors='coerce').fillna(0)
        ).astype(int)
        df['married_couple_households'] = pd.to_numeric(df['B11003_003E'], errors='coerce').fillna(0).astype(int)
        df['nonfamily_households'] = df['total_households'] - df['family_households']
        df['family_with_children'] = df['single_parent_households'] + df['married_couple_households']

        # Income and poverty
        df['median_family_income'] = pd.to_numeric(df['B19113_001E'], errors='coerce')
        df.loc[df['median_family_income'] < 0, 'median_family_income'] = np.nan

        pop_poverty = pd.to_numeric(df['B17001_002E'], errors='coerce').fillna(0)
        df['poverty_rate'] = np.where(df['total_population'] > 0, pop_poverty / df['total_population'], 0)

        child_poverty = pd.to_numeric(df['B17006_002E'], errors='coerce').fillna(0)
        df['child_poverty_rate'] = np.where(df['pop_under_18'] > 0, child_poverty / df['pop_under_18'], 0)

        # Keep relevant columns
        keep_cols = [
            'tract_geoid', 'fips_code', 'NAME', 'total_population',
            'pop_under_18', 'pop_18_24', 'pop_25_44', 'pop_45_64', 'pop_65_plus',
            'pop_white_alone', 'pop_black_alone', 'pop_asian_alone', 'pop_hispanic', 'pop_other_race',
            'total_households', 'family_households', 'family_with_children',
            'single_parent_households', 'married_couple_households', 'nonfamily_households',
            'median_family_income', 'poverty_rate', 'child_poverty_rate'
        ]
        df = df[[c for c in keep_cols if c in df.columns]].copy()

        df['source_url'] = f"https://api.census.gov/data/{geo_year}/acs/acs5"
        df['fetch_date'] = datetime.utcnow().date().isoformat()
        df['is_real'] = True

        # Cache
        df.to_csv(cache_path, index=False)

        logger.info(f"✓ Downloaded ACS demographics: {len(df)} tracts")
        return df

    except Exception as e:
        logger.error(f"Failed to download ACS demographics: {e}")
        raise


# =============================================================================
# DATA ACQUISITION - IRS Migration
# =============================================================================

def download_irs_migration_data(year: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Download IRS SOI county-to-county migration data.

    Args:
        year: Data year (e.g., 2022 for 2021-2022 flows)

    Returns:
        Tuple of (inflow_df, outflow_df) aggregated by Maryland county
    """
    # Map year to IRS year range format
    year_to_range = {
        2018: "1718",
        2019: "1819",
        2020: "1920",
        2021: "2021",
        2022: "2122",
        2023: "2122",  # Use latest available
        2024: "2122",
        2025: "2122",
    }

    year_range = year_to_range.get(year, "2122")

    inflow_path = IRS_CACHE_DIR / f"irs_inflow_{year_range}.csv"
    outflow_path = IRS_CACHE_DIR / f"irs_outflow_{year_range}.csv"

    # Download if needed
    for flow, path in [("inflow", inflow_path), ("outflow", outflow_path)]:
        if not path.exists():
            url = f"https://www.irs.gov/pub/irs-soi/county{flow}{year_range}.csv"
            logger.info(f"Downloading IRS {flow} data for {year_range}")
            ok = download_file(url, str(path))
            if not ok:
                logger.warning(f"Failed to download IRS {flow} for {year_range}")

    # Parse inflow
    inflow_df = pd.DataFrame()
    if inflow_path.exists():
        try:
            df = _read_irs_csv(inflow_path)
            df.columns = [c.strip().lower() for c in df.columns]
            inflow_df = _parse_irs_flow(df, "inflow")
        except Exception as e:
            logger.warning(f"Failed to parse IRS inflow: {e}")

    # Parse outflow
    outflow_df = pd.DataFrame()
    if outflow_path.exists():
        try:
            df = _read_irs_csv(outflow_path)
            df.columns = [c.strip().lower() for c in df.columns]
            outflow_df = _parse_irs_flow(df, "outflow")
        except Exception as e:
            logger.warning(f"Failed to parse IRS outflow: {e}")

    source_url = (
        f"https://www.irs.gov/pub/irs-soi/countyinflow{year_range}.csv; "
        f"https://www.irs.gov/pub/irs-soi/countyoutflow{year_range}.csv"
    )
    for frame in [inflow_df, outflow_df]:
        if frame is not None and not frame.empty:
            frame['source_url'] = source_url
            frame['fetch_date'] = datetime.utcnow().date().isoformat()
            frame['is_real'] = True

    return inflow_df, outflow_df


def _read_irs_csv(path: Path) -> pd.DataFrame:
    """
    Read IRS migration CSV with encoding fallbacks.

    IRS files are occasionally encoded with latin-1/cp1252 and may contain
    non-UTF8 bytes (e.g., 0xF1). Try common encodings and fall back safely.
    """
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError as e:
            last_err = e
            continue
    # Final fallback: replace undecodable bytes
    try:
        return pd.read_csv(path, dtype=str, encoding="latin-1")
    except Exception as e:
        if last_err:
            raise last_err
        raise e


def _parse_irs_flow(df: pd.DataFrame, flow_type: str) -> pd.DataFrame:
    """Parse IRS migration file and aggregate by Maryland county."""
    columns = list(df.columns)

    # Find column names (varies by year)
    def find_col(candidates):
        for cand in candidates:
            if cand in columns:
                return cand
            for col in columns:
                if cand in col:
                    return col
        return None

    if flow_type == "inflow":
        state_col = find_col(["y2_statefips", "y2_state_fips", "statefips_dest", "dest_state"])
        county_col = find_col(["y2_countyfips", "y2_county_fips", "countyfips_dest", "dest_county"])
        other_state_col = find_col(["y1_statefips", "y1_state_fips", "statefips_orig", "orig_state"])
        other_county_col = find_col(["y1_countyfips", "y1_county_fips", "countyfips_orig", "orig_county"])
    else:
        state_col = find_col(["y1_statefips", "y1_state_fips", "statefips_orig", "orig_state"])
        county_col = find_col(["y1_countyfips", "y1_county_fips", "countyfips_orig", "orig_county"])
        other_state_col = find_col(["y2_statefips", "y2_state_fips", "statefips_dest", "dest_state"])
        other_county_col = find_col(["y2_countyfips", "y2_county_fips", "countyfips_dest", "dest_county"])

    n1_col = find_col(["n1", "num_returns", "returns"])
    n2_col = find_col(["n2", "num_exemptions", "exemptions"])
    agi_col = find_col(["agi", "a00100", "adj_gross_income"])

    if not state_col or not county_col or not n1_col:
        logger.warning(f"IRS {flow_type} file missing required columns")
        return pd.DataFrame()

    # Normalize FIPS codes
    df[state_col] = df[state_col].astype(str).str.zfill(2)
    df[county_col] = df[county_col].astype(str).str.zfill(3)

    # Filter to Maryland
    df = df[df[state_col] == "24"].copy()
    df = df[df[county_col] != "000"]  # Exclude aggregate rows

    # Exclude same-county flows if other_county exists
    if other_county_col:
        df[other_county_col] = df[other_county_col].astype(str).str.zfill(3)
        df = df[df[other_county_col] != "000"]

    df['fips_code'] = df[state_col] + df[county_col]
    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]

    # Convert numerics
    df['returns'] = pd.to_numeric(df[n1_col], errors='coerce').fillna(0)
    df['exemptions'] = pd.to_numeric(df[n2_col], errors='coerce').fillna(0) if n2_col else 0
    df['agi'] = pd.to_numeric(df[agi_col], errors='coerce').fillna(0) * 1000 if agi_col else 0  # AGI in thousands

    # Aggregate by county
    agg = df.groupby('fips_code', as_index=False).agg({
        'returns': 'sum',
        'exemptions': 'sum',
        'agi': 'sum'
    }).rename(columns={
        'returns': f'{flow_type}_households',
        'exemptions': f'{flow_type}_exemptions',
        'agi': f'{flow_type}_agi'
    })

    return agg


# =============================================================================
# EQUITY METRICS COMPUTATION
# =============================================================================

def compute_racial_diversity_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute racial diversity using entropy-based index.

    Diversity index ranges from 0 (homogeneous) to 1 (maximally diverse).
    """
    logger.info("Computing racial diversity index...")

    df = df.copy()

    # Calculate proportions
    race_cols = ['pop_white_alone', 'pop_black_alone', 'pop_asian_alone', 'pop_hispanic', 'pop_other_race']

    for col in race_cols:
        if col not in df.columns:
            df[col] = 0

    total = df['total_population'].replace(0, 1)

    # Shannon entropy for diversity
    def shannon_entropy(row):
        proportions = []
        for col in race_cols:
            p = row[col] / row['total_population'] if row['total_population'] > 0 else 0
            if p > 0:
                proportions.append(p)
        if not proportions:
            return 0
        entropy = -sum(p * np.log(p) for p in proportions if p > 0)
        # Normalize to 0-1 (max entropy for 5 groups is ln(5))
        max_entropy = np.log(len(race_cols))
        return entropy / max_entropy if max_entropy > 0 else 0

    df['racial_diversity_index'] = df.apply(shannon_entropy, axis=1)

    logger.info(f"Avg diversity index: {df['racial_diversity_index'].mean():.3f}")
    return df


def compute_segregation_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute segregation indices at tract level.

    Uses dissimilarity index (D) as primary measure.
    D = 0.5 * sum(|ti/T - wi/W|) where t=minority, w=majority in tract i
    """
    logger.info("Computing segregation indices...")

    df = df.copy()

    # For each county, compute dissimilarity and exposure indices
    results = []

    for fips_code, county_group in df.groupby('fips_code'):
        # Total county populations
        T_minority = county_group['pop_black_alone'].sum() + county_group['pop_hispanic'].sum()
        T_majority = county_group['pop_white_alone'].sum()
        T_total = county_group['total_population'].sum()

        if T_minority == 0 or T_majority == 0 or T_total == 0:
            # Can't compute meaningful segregation
            for idx in county_group.index:
                results.append({
                    'idx': idx,
                    'dissimilarity_index': 0,
                    'exposure_index': 0.5,
                    'isolation_index': 0.5
                })
            continue

        # Compute tract-level contributions
        for idx, row in county_group.iterrows():
            t_i = row['pop_black_alone'] + row['pop_hispanic']  # Minority in tract
            w_i = row['pop_white_alone']  # Majority in tract
            n_i = row['total_population']  # Total in tract

            # Tract-level dissimilarity contribution
            d_contribution = abs(t_i / T_minority - w_i / T_majority) if T_minority > 0 and T_majority > 0 else 0

            # Exposure: probability minority encounters majority
            # P*xy = sum((ti/T) * (wi/ni))
            exposure_contribution = (t_i / T_minority) * (w_i / n_i) if T_minority > 0 and n_i > 0 else 0

            # Isolation: probability minority encounters minority
            isolation_contribution = (t_i / T_minority) * (t_i / n_i) if T_minority > 0 and n_i > 0 else 0

            results.append({
                'idx': idx,
                'd_contribution': d_contribution,
                'exposure_contribution': exposure_contribution,
                'isolation_contribution': isolation_contribution
            })

    # Aggregate contributions back to county level
    result_df = pd.DataFrame(results)

    # Join back contributions
    df = df.reset_index(drop=True)
    if 'd_contribution' in result_df.columns:
        df['d_contribution'] = result_df['d_contribution'].values

    # Now compute county-level indices and assign to tracts
    for fips_code, county_group in df.groupby('fips_code'):
        county_D = 0.5 * county_group['d_contribution'].sum() if 'd_contribution' in county_group.columns else 0
        county_D = min(county_D, 1.0)  # Cap at 1

        # Assign county-level index to all tracts (uniform within county)
        df.loc[df['fips_code'] == fips_code, 'dissimilarity_index'] = county_D

        # Exposure and isolation (county-level, assigned to tracts)
        T_minority = county_group['pop_black_alone'].sum() + county_group['pop_hispanic'].sum()
        T_majority = county_group['pop_white_alone'].sum()

        exposure = 0
        isolation = 0
        for _, row in county_group.iterrows():
            t_i = row['pop_black_alone'] + row['pop_hispanic']
            w_i = row['pop_white_alone']
            n_i = row['total_population']
            if T_minority > 0 and n_i > 0:
                exposure += (t_i / T_minority) * (w_i / n_i)
                isolation += (t_i / T_minority) * (t_i / n_i)

        df.loc[df['fips_code'] == fips_code, 'exposure_index'] = exposure
        df.loc[df['fips_code'] == fips_code, 'isolation_index'] = isolation

    # Clean up temporary column
    if 'd_contribution' in df.columns:
        df = df.drop(columns=['d_contribution'])

    logger.info(f"Avg dissimilarity: {df['dissimilarity_index'].mean():.3f}")
    logger.info(f"Avg exposure: {df['exposure_index'].mean():.3f}")

    return df


def compute_family_viability_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute family viability metrics.

    Family viability considers: single-parent rate, poverty, income.
    """
    logger.info("Computing family viability metrics...")

    df = df.copy()

    # Single parent percentage (of families with children)
    df['single_parent_pct'] = np.where(
        df['family_with_children'] > 0,
        df['single_parent_households'] / df['family_with_children'],
        0
    )

    # Working age percentage
    df['working_age_pct'] = np.where(
        df['total_population'] > 0,
        df['pop_25_44'] / df['total_population'],
        0
    )

    # Age dependency ratio
    dependent = df['pop_under_18'] + df['pop_65_plus']
    working = df['pop_25_44'] + df['pop_45_64']
    df['age_dependency_ratio'] = np.where(working > 0, dependent / working, 0)

    # Family household percentage
    df['family_household_pct'] = np.where(
        df['total_households'] > 0,
        df['family_households'] / df['total_households'],
        0
    )

    # Family viability score (composite)
    # Higher = better family conditions
    # Penalize: high single-parent rate, high poverty
    # Reward: higher income, lower child poverty

    # Normalize income to 0-1 (using percentile)
    income_pctl = df['median_family_income'].rank(pct=True).fillna(0.5)

    # Invert poverty rates (lower poverty = better)
    poverty_score = 1 - df['poverty_rate'].clip(0, 1)
    child_poverty_score = 1 - df['child_poverty_rate'].clip(0, 1)

    # Invert single parent rate
    single_parent_score = 1 - df['single_parent_pct'].clip(0, 1)

    # Composite family viability
    df['family_viability_score'] = (
        0.30 * income_pctl +
        0.25 * poverty_score +
        0.25 * child_poverty_score +
        0.20 * single_parent_score
    ).clip(0, 1)

    logger.info(f"Avg family viability: {df['family_viability_score'].mean():.3f}")

    return df


# =============================================================================
# MIGRATION FLOW ANALYSIS
# =============================================================================

def apportion_county_migration_to_tracts(
    tract_df: pd.DataFrame,
    inflow_df: pd.DataFrame,
    outflow_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Apportion county-level IRS migration to tracts via population weights.

    Args:
        tract_df: Tract demographics
        inflow_df: County inflow totals
        outflow_df: County outflow totals

    Returns:
        tract_df with migration metrics added
    """
    logger.info("Apportioning county migration to tracts...")

    df = tract_df.copy()

    # Merge county totals
    if not inflow_df.empty:
        county_inflow = inflow_df.set_index('fips_code')['inflow_households'].to_dict()
    else:
        county_inflow = {}

    if not outflow_df.empty:
        county_outflow = outflow_df.set_index('fips_code')['outflow_households'].to_dict()
    else:
        county_outflow = {}

    # Calculate county total populations for weighting
    county_pop = df.groupby('fips_code')['total_population'].sum()

    # Apportion to tracts by population share
    def apportion(row, county_totals, county_populations):
        fips = row['fips_code']
        county_total = county_totals.get(fips, 0)
        county_pop = county_populations.get(fips, 1)
        tract_share = row['total_population'] / county_pop if county_pop > 0 else 0
        return county_total * tract_share

    df['est_inflow'] = df.apply(lambda r: apportion(r, county_inflow, county_pop.to_dict()), axis=1)
    df['est_outflow'] = df.apply(lambda r: apportion(r, county_outflow, county_pop.to_dict()), axis=1)

    # Net migration
    df['est_net_migration'] = df['est_inflow'] - df['est_outflow']

    # Rates (per population)
    df['est_inflow_rate'] = np.where(
        df['total_population'] > 0,
        df['est_inflow'] / df['total_population'],
        0
    )
    df['est_outflow_rate'] = np.where(
        df['total_population'] > 0,
        df['est_outflow'] / df['total_population'],
        0
    )
    df['est_net_migration_rate'] = np.where(
        df['total_population'] > 0,
        df['est_net_migration'] / df['total_population'],
        0
    )

    logger.info(f"Avg net migration rate: {df['est_net_migration_rate'].mean():.4f}")

    return df


# =============================================================================
# SCORE NORMALIZATION
# =============================================================================

def normalize_demographic_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize demographic metrics to 0-1 scores.

    Args:
        df: Tract DataFrame with computed metrics

    Returns:
        DataFrame with normalized scores
    """
    logger.info("Normalizing demographic scores...")

    df = df.copy()

    # --- v1 Static Score ---
    # Based on population structure
    working_age_score = df['working_age_pct'].rank(pct=True)
    family_hh_score = df['family_household_pct'].rank(pct=True)
    diversity_score = df['racial_diversity_index'].rank(pct=True)

    df['static_demographic_score'] = (
        0.40 * working_age_score +
        0.30 * family_hh_score +
        0.30 * diversity_score
    ).clip(0, 1)

    # --- v2 Equity Score ---
    # Based on segregation and family viability
    integration_score = 1 - df['dissimilarity_index'].fillna(0)  # Higher = more integrated
    exposure_score = df['exposure_index'].fillna(0.5).rank(pct=True)
    viability_score = df['family_viability_score'].fillna(0.5)

    df['equity_score'] = (
        SEGREGATION_WEIGHT * integration_score +
        DIVERSITY_WEIGHT * exposure_score +
        FAMILY_VIABILITY_WEIGHT * viability_score
    ).clip(0, 1)

    # --- v3 Migration Score ---
    # Based on flows and growth
    net_migration_score = df['est_net_migration_rate'].rank(pct=True)
    stability_score = 1 - df['est_outflow_rate'].clip(0, 0.2).rank(pct=True)  # Lower outflow = more stable

    # Population growth (placeholder - would need multi-year)
    df['population_growth_rate'] = 0  # Will be computed in multi-year

    df['migration_dynamics_score'] = (
        NET_MIGRATION_WEIGHT * net_migration_score +
        STABILITY_WEIGHT * stability_score +
        POPULATION_GROWTH_WEIGHT * 0.5  # Neutral placeholder
    ).clip(0, 1)

    # --- Composite Score ---
    df['demographic_opportunity_score'] = (
        STATIC_WEIGHT * df['static_demographic_score'] +
        EQUITY_WEIGHT * df['equity_score'] +
        MIGRATION_WEIGHT * df['migration_dynamics_score']
    ).clip(0, 1)

    logger.info(f"Score statistics:")
    logger.info(f"  Static: {df['static_demographic_score'].mean():.3f}")
    logger.info(f"  Equity: {df['equity_score'].mean():.3f}")
    logger.info(f"  Migration: {df['migration_dynamics_score'].mean():.3f}")
    logger.info(f"  Composite: {df['demographic_opportunity_score'].mean():.3f}")

    return df


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_to_county(tract_df: pd.DataFrame, inflow_df: pd.DataFrame, outflow_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tract-level demographics to county using population weighting.

    Args:
        tract_df: Tract-level metrics
        inflow_df: County inflow data
        outflow_df: County outflow data

    Returns:
        County-level DataFrame
    """
    logger.info("Aggregating to county level...")

    def weighted_mean(group, col, weight_col='total_population'):
        weights = group[weight_col].fillna(1)
        values = group[col].fillna(0)
        if weights.sum() > 0:
            return (values * weights).sum() / weights.sum()
        return values.mean()

    county_metrics = []

    for fips_code, group in tract_df.groupby('fips_code'):
        metrics = {
            'fips_code': fips_code,

            # Population totals
            'pop_total': group['total_population'].sum(),
            'pop_age_25_44': group['pop_25_44'].sum(),

            # Race totals
            'pop_white_alone': group['pop_white_alone'].sum(),
            'pop_black_alone': group['pop_black_alone'].sum(),
            'pop_asian_alone': group['pop_asian_alone'].sum(),
            'pop_hispanic': group['pop_hispanic'].sum(),
            'pop_other_race': group['pop_other_race'].sum(),

            # Household totals
            'households_total': group['total_households'].sum(),
            'households_family': group['family_households'].sum(),
            'households_family_with_children': group['family_with_children'].sum(),

            # Weighted metrics
            'racial_diversity_index': weighted_mean(group, 'racial_diversity_index'),
            'dissimilarity_index': group['dissimilarity_index'].iloc[0] if 'dissimilarity_index' in group.columns else 0,
            'exposure_index': group['exposure_index'].iloc[0] if 'exposure_index' in group.columns else 0,
            'isolation_index': group['isolation_index'].iloc[0] if 'isolation_index' in group.columns else 0,
            'family_viability_score': weighted_mean(group, 'family_viability_score'),
            'single_parent_pct': weighted_mean(group, 'single_parent_pct'),
            'poverty_rate': weighted_mean(group, 'poverty_rate'),
            'child_poverty_rate': weighted_mean(group, 'child_poverty_rate'),
            'age_dependency_ratio': weighted_mean(group, 'age_dependency_ratio'),
            'family_household_pct': weighted_mean(group, 'family_household_pct'),

            # Scores
            'static_demographic_score': weighted_mean(group, 'static_demographic_score'),
            'equity_score': weighted_mean(group, 'equity_score'),
            'migration_dynamics_score': weighted_mean(group, 'migration_dynamics_score'),
            'demographic_opportunity_index': weighted_mean(group, 'demographic_opportunity_score'),
        }

        # Add actual county migration data
        if not inflow_df.empty and fips_code in inflow_df['fips_code'].values:
            row = inflow_df[inflow_df['fips_code'] == fips_code].iloc[0]
            metrics['inflow_households'] = int(row.get('inflow_households', 0))
        else:
            metrics['inflow_households'] = None

        if not outflow_df.empty and fips_code in outflow_df['fips_code'].values:
            row = outflow_df[outflow_df['fips_code'] == fips_code].iloc[0]
            metrics['outflow_households'] = int(row.get('outflow_households', 0))
        else:
            metrics['outflow_households'] = None

        if metrics['inflow_households'] is not None and metrics['outflow_households'] is not None:
            metrics['net_migration_households'] = metrics['inflow_households'] - metrics['outflow_households']
            pop = metrics['pop_total']
            metrics['net_migration_rate'] = metrics['net_migration_households'] / pop if pop > 0 else 0
            metrics['inflow_rate'] = metrics['inflow_households'] / pop if pop > 0 else 0
            metrics['outflow_rate'] = metrics['outflow_households'] / pop if pop > 0 else 0
        else:
            metrics['net_migration_households'] = None
            metrics['net_migration_rate'] = None
            metrics['inflow_rate'] = None
            metrics['outflow_rate'] = None

        county_metrics.append(metrics)

    county_df = pd.DataFrame(county_metrics)

    logger.info(f"✓ Aggregated {len(county_df)} counties")
    logger.info(f"County avg demographic opportunity index: {county_df['demographic_opportunity_index'].mean():.3f}")

    return county_df


# =============================================================================
# STORAGE
# =============================================================================

def store_tract_demographic_equity(df: pd.DataFrame, data_year: int, acs_year: int):
    """Store tract-level demographic equity data."""
    logger.info(f"Storing {len(df)} tract demographic equity records...")

    with get_db() as db:
        # Clear existing data
        db.execute(text("""
            DELETE FROM layer5_demographic_equity_tract
            WHERE data_year = :data_year
        """), {"data_year": data_year})

        for _, row in df.iterrows():
            db.execute(
                text("""
                    INSERT INTO layer5_demographic_equity_tract (
                        tract_geoid, fips_code, data_year,
                        total_population, pop_under_18, pop_18_24, pop_25_44, pop_45_64, pop_65_plus,
                        working_age_pct,
                        total_households, family_households, family_with_children,
                        single_parent_households, married_couple_households, nonfamily_households,
                        pop_white_alone, pop_black_alone, pop_asian_alone, pop_hispanic, pop_other_race,
                        racial_diversity_index, age_dependency_ratio, family_household_pct,
                        dissimilarity_index, exposure_index, isolation_index,
                        single_parent_pct, median_family_income, poverty_rate, child_poverty_rate,
                        family_viability_score,
                        est_net_migration_rate, est_inflow_rate, est_outflow_rate,
                        static_demographic_score, equity_score, migration_dynamics_score,
                        demographic_opportunity_score,
                        acs_year
                    ) VALUES (
                        :tract, :fips, :data_year,
                        :pop, :under_18, :age_18_24, :age_25_44, :age_45_64, :age_65_plus,
                        :working_age_pct,
                        :hh_total, :hh_family, :hh_children,
                        :hh_single_parent, :hh_married, :hh_nonfamily,
                        :white, :black, :asian, :hispanic, :other,
                        :diversity, :dependency, :family_pct,
                        :dissimilarity, :exposure, :isolation,
                        :single_parent_pct, :median_income, :poverty, :child_poverty,
                        :viability,
                        :net_migration, :inflow, :outflow,
                        :static_score, :equity_score, :migration_score,
                        :composite,
                        :acs_year
                    )
                """),
                {
                    'tract': row['tract_geoid'],
                    'fips': row['fips_code'],
                    'data_year': data_year,
                    'pop': int(row.get('total_population', 0)),
                    'under_18': int(row.get('pop_under_18', 0)),
                    'age_18_24': int(row.get('pop_18_24', 0)),
                    'age_25_44': int(row.get('pop_25_44', 0)),
                    'age_45_64': int(row.get('pop_45_64', 0)),
                    'age_65_plus': int(row.get('pop_65_plus', 0)),
                    'working_age_pct': float(row.get('working_age_pct', 0)),
                    'hh_total': int(row.get('total_households', 0)),
                    'hh_family': int(row.get('family_households', 0)),
                    'hh_children': int(row.get('family_with_children', 0)),
                    'hh_single_parent': int(row.get('single_parent_households', 0)),
                    'hh_married': int(row.get('married_couple_households', 0)),
                    'hh_nonfamily': int(row.get('nonfamily_households', 0)),
                    'white': int(row.get('pop_white_alone', 0)),
                    'black': int(row.get('pop_black_alone', 0)),
                    'asian': int(row.get('pop_asian_alone', 0)),
                    'hispanic': int(row.get('pop_hispanic', 0)),
                    'other': int(row.get('pop_other_race', 0)),
                    'diversity': float(row.get('racial_diversity_index', 0)),
                    'dependency': float(row.get('age_dependency_ratio', 0)),
                    'family_pct': float(row.get('family_household_pct', 0)),
                    'dissimilarity': float(row.get('dissimilarity_index', 0)),
                    'exposure': float(row.get('exposure_index', 0)),
                    'isolation': float(row.get('isolation_index', 0)),
                    'single_parent_pct': float(row.get('single_parent_pct', 0)),
                    'median_income': int(row['median_family_income']) if pd.notna(row.get('median_family_income')) else None,
                    'poverty': float(row.get('poverty_rate', 0)),
                    'child_poverty': float(row.get('child_poverty_rate', 0)),
                    'viability': float(row.get('family_viability_score', 0)),
                    'net_migration': float(row.get('est_net_migration_rate', 0)),
                    'inflow': float(row.get('est_inflow_rate', 0)),
                    'outflow': float(row.get('est_outflow_rate', 0)),
                    'static_score': float(row.get('static_demographic_score', 0)),
                    'equity_score': float(row.get('equity_score', 0)),
                    'migration_score': float(row.get('migration_dynamics_score', 0)),
                    'composite': float(row.get('demographic_opportunity_score', 0)),
                    'acs_year': acs_year
                }
            )

        db.commit()

    logger.info("✓ Tract demographic equity stored")


def store_county_demographic_equity(df: pd.DataFrame, data_year: int, acs_year: int):
    """Update county-level demographic equity data."""
    logger.info(f"Updating {len(df)} county demographic equity records...")

    with get_db() as db:
        for _, row in df.iterrows():
            # Check if record exists
            existing = db.execute(
                text("SELECT id FROM layer5_demographic_momentum WHERE fips_code = :fips AND data_year = :year"),
                {"fips": row['fips_code'], "year": data_year}
            ).fetchone()

            if existing:
                # Update existing
                db.execute(
                    text("""
                        UPDATE layer5_demographic_momentum SET
                            pop_white_alone = :white,
                            pop_black_alone = :black,
                            pop_asian_alone = :asian,
                            pop_hispanic = :hispanic,
                            pop_other_race = :other,
                            racial_diversity_index = :diversity,
                            age_dependency_ratio = :dependency,
                            family_household_pct = :family_pct,
                            static_demographic_score = :static_score,
                            dissimilarity_index = :dissimilarity,
                            exposure_index = :exposure,
                            isolation_index = :isolation,
                            single_parent_pct = :single_parent,
                            poverty_rate = :poverty,
                            child_poverty_rate = :child_poverty,
                            family_viability_score = :viability,
                            equity_score = :equity_score,
                            net_migration_rate = :net_rate,
                            inflow_rate = :inflow_rate,
                            outflow_rate = :outflow_rate,
                            migration_dynamics_score = :migration_score,
                            demographic_opportunity_index = :opportunity_index,
                            demographic_momentum_score = COALESCE(demographic_momentum_score, :static_score),
                            acs_year = :acs_year,
                            demographic_version = 'v2-equity',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE fips_code = :fips AND data_year = :data_year
                    """),
                    {
                        'fips': row['fips_code'],
                        'data_year': data_year,
                        'white': int(row.get('pop_white_alone', 0)),
                        'black': int(row.get('pop_black_alone', 0)),
                        'asian': int(row.get('pop_asian_alone', 0)),
                        'hispanic': int(row.get('pop_hispanic', 0)),
                        'other': int(row.get('pop_other_race', 0)),
                        'diversity': float(row.get('racial_diversity_index', 0)),
                        'dependency': float(row.get('age_dependency_ratio', 0)),
                        'family_pct': float(row.get('family_household_pct', 0)),
                        'static_score': float(row.get('static_demographic_score', 0)),
                        'dissimilarity': float(row.get('dissimilarity_index', 0)),
                        'exposure': float(row.get('exposure_index', 0)),
                        'isolation': float(row.get('isolation_index', 0)),
                        'single_parent': float(row.get('single_parent_pct', 0)),
                        'poverty': float(row.get('poverty_rate', 0)),
                        'child_poverty': float(row.get('child_poverty_rate', 0)),
                        'viability': float(row.get('family_viability_score', 0)),
                        'equity_score': float(row.get('equity_score', 0)),
                        'net_rate': float(row['net_migration_rate']) if pd.notna(row.get('net_migration_rate')) else None,
                        'inflow_rate': float(row['inflow_rate']) if pd.notna(row.get('inflow_rate')) else None,
                        'outflow_rate': float(row['outflow_rate']) if pd.notna(row.get('outflow_rate')) else None,
                        'migration_score': float(row.get('migration_dynamics_score', 0)),
                        'opportunity_index': float(row.get('demographic_opportunity_index', 0)),
                        'static_score': float(row.get('static_demographic_score', 0)),
                        'acs_year': acs_year
                    }
                )
            else:
                # Insert new
                db.execute(
                    text("""
                        INSERT INTO layer5_demographic_momentum (
                            fips_code, data_year,
                            pop_total, pop_age_25_44, pop_age_25_44_pct,
                            households_total, households_family, households_family_with_children,
                            inflow_households, outflow_households, net_migration_households,
                            pop_white_alone, pop_black_alone, pop_asian_alone, pop_hispanic, pop_other_race,
                            racial_diversity_index, age_dependency_ratio, family_household_pct,
                            static_demographic_score, dissimilarity_index, exposure_index, isolation_index,
                            single_parent_pct, poverty_rate, child_poverty_rate, family_viability_score,
                            equity_score, net_migration_rate, inflow_rate, outflow_rate,
                            migration_dynamics_score, demographic_opportunity_index,
                            demographic_momentum_score,
                            acs_year, demographic_version
                        ) VALUES (
                            :fips, :data_year,
                            :pop_total, :pop_25_44, :pop_25_44_pct,
                            :hh_total, :hh_family, :hh_children,
                            :inflow_hh, :outflow_hh, :net_hh,
                            :white, :black, :asian, :hispanic, :other,
                            :diversity, :dependency, :family_pct,
                            :static_score, :dissimilarity, :exposure, :isolation,
                            :single_parent, :poverty, :child_poverty, :viability,
                            :equity_score, :net_rate, :inflow_rate, :outflow_rate,
                            :migration_score, :opportunity_index,
                            :momentum_score,
                            :acs_year, 'v2-equity'
                        )
                    """),
                    {
                        'fips': row['fips_code'],
                        'data_year': data_year,
                        'pop_total': int(row.get('pop_total', 0)),
                        'pop_25_44': int(row.get('pop_age_25_44', 0)),
                        'pop_25_44_pct': float(row.get('pop_age_25_44', 0) / row.get('pop_total', 1)) if row.get('pop_total', 0) > 0 else 0,
                        'hh_total': int(row.get('households_total', 0)),
                        'hh_family': int(row.get('households_family', 0)),
                        'hh_children': int(row.get('households_family_with_children', 0)),
                        'inflow_hh': row.get('inflow_households'),
                        'outflow_hh': row.get('outflow_households'),
                        'net_hh': row.get('net_migration_households'),
                        'white': int(row.get('pop_white_alone', 0)),
                        'black': int(row.get('pop_black_alone', 0)),
                        'asian': int(row.get('pop_asian_alone', 0)),
                        'hispanic': int(row.get('pop_hispanic', 0)),
                        'other': int(row.get('pop_other_race', 0)),
                        'diversity': float(row.get('racial_diversity_index', 0)),
                        'dependency': float(row.get('age_dependency_ratio', 0)),
                        'family_pct': float(row.get('family_household_pct', 0)),
                        'static_score': float(row.get('static_demographic_score', 0)),
                        'dissimilarity': float(row.get('dissimilarity_index', 0)),
                        'exposure': float(row.get('exposure_index', 0)),
                        'isolation': float(row.get('isolation_index', 0)),
                        'single_parent': float(row.get('single_parent_pct', 0)),
                        'poverty': float(row.get('poverty_rate', 0)),
                        'child_poverty': float(row.get('child_poverty_rate', 0)),
                        'viability': float(row.get('family_viability_score', 0)),
                        'equity_score': float(row.get('equity_score', 0)),
                        'net_rate': float(row['net_migration_rate']) if pd.notna(row.get('net_migration_rate')) else None,
                        'inflow_rate': float(row['inflow_rate']) if pd.notna(row.get('inflow_rate')) else None,
                        'outflow_rate': float(row['outflow_rate']) if pd.notna(row.get('outflow_rate')) else None,
                        'migration_score': float(row.get('migration_dynamics_score', 0)),
                        'opportunity_index': float(row.get('demographic_opportunity_index', 0)),
                        'momentum_score': float(row.get('static_demographic_score', 0)),
                        'acs_year': acs_year
                    }
                )

        db.commit()

    logger.info("✓ County demographic equity stored")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def calculate_demographic_equity_indicators(
    data_year: int = None,
    acs_year: int = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate demographic equity indicators for all Maryland tracts.

    Args:
        data_year: Year to associate with this data
        acs_year: ACS data year

    Returns:
        Tuple of (tract_df, county_df)
    """
    data_year = data_year or layer5_default_data_year()
    acs_year = acs_year or acs_geography_year(data_year - 1)

    logger.info("=" * 60)
    logger.info("LAYER 5 v2: DEMOGRAPHIC EQUITY ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Data year: {data_year}")
    logger.info(f"ACS year: {acs_year}")

    # Step 1: Download ACS demographics
    logger.info("\n[1/7] Loading ACS demographics...")
    tract_df = download_acs_demographic_data(acs_year)

    # Step 2: Download IRS migration
    logger.info("\n[2/7] Loading IRS migration data...")
    inflow_df, outflow_df = download_irs_migration_data(data_year)

    # Step 3: Compute diversity index
    logger.info("\n[3/7] Computing diversity metrics...")
    tract_df = compute_racial_diversity_index(tract_df)

    # Step 4: Compute segregation indices
    logger.info("\n[4/7] Computing segregation indices...")
    tract_df = compute_segregation_indices(tract_df)

    # Step 5: Compute family viability
    logger.info("\n[5/7] Computing family viability metrics...")
    tract_df = compute_family_viability_metrics(tract_df)

    # Step 6: Apportion migration to tracts
    logger.info("\n[6/7] Apportioning migration to tracts...")
    tract_df = apportion_county_migration_to_tracts(tract_df, inflow_df, outflow_df)

    # Step 7: Normalize scores
    logger.info("\n[7/7] Normalizing scores...")
    tract_df = normalize_demographic_scores(tract_df)

    # Aggregate to county
    county_df = aggregate_to_county(tract_df, inflow_df, outflow_df)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Tracts processed: {len(tract_df)}")
    logger.info(f"Counties processed: {len(county_df)}")
    logger.info(f"Avg demographic opportunity score: {tract_df['demographic_opportunity_score'].mean():.3f}")

    return tract_df, county_df


def run_layer5_v2_ingestion(
    data_year: int = None,
    multi_year: bool = True,
    store_data: bool = True,
    window_years: int = DEFAULT_WINDOW_YEARS,
    predict_to_year: Optional[int] = None
):
    """
    Run complete Layer 5 v2 ingestion pipeline.

    Args:
        data_year: End year for this data
        multi_year: If True, run multi-year window
        store_data: Whether to store results in database
        window_years: Window size for multi-year ingestion
    """
    if data_year is None:
        data_year = layer5_default_data_year()

    try:
        if multi_year:
            start_year = data_year - window_years + 1
            years_to_fetch = list(range(start_year, data_year + 1))
            logger.info(f"Starting Layer 5 v2 MULTI-YEAR ingestion for years {years_to_fetch[0]}-{years_to_fetch[-1]}")
        else:
            years_to_fetch = [data_year]
            logger.info(f"Starting Layer 5 v2 single-year ingestion for {data_year}")

        total_records = 0
        failed_years = []

        for year in years_to_fetch:
            acs_year = acs_geography_year(year - 1)

            logger.info("=" * 70)
            logger.info(f"Processing year {year}")
            logger.info("=" * 70)

            try:
                tract_df, county_df = calculate_demographic_equity_indicators(
                    data_year=year,
                    acs_year=acs_year
                )

                if store_data and not tract_df.empty:
                    store_tract_demographic_equity(tract_df, year, acs_year)
                    store_county_demographic_equity(county_df, year, acs_year)

                    log_refresh(
                        layer_name="layer5_demographic_momentum",
                        data_source="ACS/IRS (v2 equity)",
                        status="success",
                        records_processed=len(tract_df),
                        records_inserted=len(tract_df) + len(county_df),
                        metadata={
                            "data_year": year,
                            "acs_year": acs_year,
                            "version": "v2-equity",
                            "tracts": len(tract_df),
                            "counties": len(county_df),
                            "avg_equity_score": float(tract_df['equity_score'].mean()),
                            "avg_opportunity_score": float(tract_df['demographic_opportunity_score'].mean())
                        }
                    )

                total_records += len(tract_df)
                logger.info(f"✓ Year {year} complete: {len(tract_df)} tract records")

            except Exception as e:
                logger.error(f"✗ Year {year} ingestion failed: {e}", exc_info=True)
                failed_years.append(year)
                continue

        logger.info("=" * 70)
        if multi_year:
            logger.info("MULTI-YEAR INGESTION SUMMARY")
            logger.info(f"  Years requested: {years_to_fetch[0]}-{years_to_fetch[-1]}")
            logger.info(f"  Years successful: {len(years_to_fetch) - len(failed_years)}")
            logger.info(f"  Years failed: {len(failed_years)} {failed_years if failed_years else ''}")
            logger.info(f"  Total tract records: {total_records}")

        if failed_years and len(failed_years) == len(years_to_fetch):
            raise Exception(f"All years failed: {failed_years}")

        if store_data:
            target_year = predict_to_year or settings.PREDICT_TO_YEAR
            apply_predictions_to_table(
                table="layer5_demographic_momentum",
                metric_col="demographic_opportunity_index",
                target_year=target_year,
                clip=(0.0, 1.0)
            )

        logger.info("✓ Layer 5 v2 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 5 v2 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer5_demographic_momentum",
            data_source="ACS/IRS (v2 equity)",
            status="failed",
            error_message=str(e)
        )
        raise


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Layer 5 v2: Demographic Equity Analysis'
    )
    parser.add_argument(
        '--year', type=int, default=None,
        help='End year for window (default: latest available)'
    )
    parser.add_argument(
        '--single-year', action='store_true',
        help='Fetch only single year (default: multi-year window)'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Calculate but do not store results'
    )
    parser.add_argument(
        '--predict-to-year', type=int, default=None,
        help='Predict missing years up to target year (default: settings.PREDICT_TO_YEAR)'
    )

    args = parser.parse_args()

    run_layer5_v2_ingestion(
        data_year=args.year,
        multi_year=not args.single_year,
        store_data=not args.dry_run,
        predict_to_year=args.predict_to_year
    )


if __name__ == "__main__":
    main()
