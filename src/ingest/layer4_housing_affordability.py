"""
Maryland Viability Atlas - Layer 4 v2: Housing Affordability Analysis

Computes housing affordability metrics using:
- ACS 5-year estimates (housing costs, income, tenure)
- HUD CHAS data (cost burden by AMI bracket)
- Census tract boundaries for spatial analysis

Key metrics:
- Cost burden: % households paying >30% income on housing
- Affordable stock: % units affordable to low-income households
- Housing quality: Age, crowding, completeness indicators
- H+T burden: Combined housing + transportation cost

Composite: housing_opportunity_index = 0.4×elasticity + 0.6×affordability
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Tuple, Optional, Dict, List
import warnings

import pandas as pd
import numpy as np
from sqlalchemy import text
import zipfile
import io
import requests

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.logging import get_logger
from src.utils.prediction_utils import apply_predictions_to_table
from src.utils.data_sources import download_file

logger = get_logger(__name__)
settings = get_settings()

# =============================================================================
# CONSTANTS
# =============================================================================

# Cache directories
CACHE_DIR = Path("data/cache/housing_v2")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

ACS_CACHE_DIR = CACHE_DIR / "acs"
ACS_CACHE_DIR.mkdir(exist_ok=True)

CHAS_CACHE_DIR = CACHE_DIR / "chas"
CHAS_CACHE_DIR.mkdir(exist_ok=True)

# HUD CHAS cost burden thresholds
COST_BURDEN_THRESHOLD = 0.30   # >30% of income on housing
SEVERE_BURDEN_THRESHOLD = 0.50  # >50% of income on housing

# AMI (Area Median Income) brackets from HUD
AMI_BRACKETS = {
    '0_30': 'Extremely Low Income',
    '30_50': 'Very Low Income',
    '50_80': 'Low Income',
    '80_100': 'Moderate Income',
    '100_plus': 'Above Moderate'
}

# Composite score weights
# v1 elasticity (supply responsiveness) + v2 affordability (cost burden)
ELASTICITY_WEIGHT = 0.4
AFFORDABILITY_WEIGHT = 0.6

# Within affordability score
BURDEN_WEIGHT = 0.50      # 1 - cost_burdened_pct
STOCK_WEIGHT = 0.30       # affordable units as % of housing stock
QUALITY_WEIGHT = 0.20     # housing quality indicators

# H+T (Housing + Transportation) estimates
# Based on CNT H+T Index methodology
AVG_ANNUAL_COMMUTE_COST_PER_MILE = 0.58  # IRS 2024 rate
DAYS_WORKED_PER_YEAR = 250
DEFAULT_WINDOW_YEARS = 5

# ACS sentinel values for missing data
ACS_MISSING_SENTINELS = {-999999999, -888888888, -666666666}

# External enrichment blend weight (HUD FMR + LIHTC)
EXTERNAL_AFFORDABILITY_BLEND_WEIGHT = 0.15


# =============================================================================
# DATA ACQUISITION
# =============================================================================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _find_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    for cand in candidates:
        if cand in columns:
            return cand
    for cand in candidates:
        for col in columns:
            if cand in col:
                return col
    return None


def _resolve_data_path(local_path: Optional[str], url: Optional[str], cache_dir: Path, filename: str) -> Optional[Path]:
    if local_path:
        path = Path(local_path)
        if path.exists():
            return path
        logger.warning(f"Provided data path not found: {local_path}")
    if url:
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = cache_dir / filename
        if not target.exists():
            ok = download_file(url, str(target))
            if not ok:
                logger.warning(f"Failed to download data from {url}")
                return None
        return target
    return None


def _map_county_name_to_fips(name_series: pd.Series) -> pd.Series:
    name_to_fips = {v.lower().replace(" county", "").strip(): k for k, v in MD_COUNTY_FIPS.items()}
    cleaned = name_series.astype(str).str.lower().str.replace("county", "", regex=False).str.strip()
    return cleaned.map(name_to_fips)


def _fetch_hud_fmr_api(data_year: int) -> pd.DataFrame:
    if not settings.HUD_FMR_DATA_URL:
        return pd.DataFrame()
    if not settings.HUD_USER_API_TOKEN:
        logger.warning("HUD API token missing; cannot fetch FMR API")
        return pd.DataFrame()

    base_url = settings.HUD_FMR_DATA_URL.rstrip("/")
    headers = {"Authorization": f"Bearer {settings.HUD_USER_API_TOKEN}"}

    for year in range(data_year, data_year - 5, -1):
        url = f"{base_url}/statedata/MD"
        try:
            resp = requests.get(url, headers=headers, params={"year": year}, timeout=30)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            if not isinstance(payload, dict):
                continue
            data_block = payload.get("data", {})
            counties = []
            if isinstance(data_block, dict):
                counties = data_block.get("counties", [])
            if not isinstance(counties, list) or not counties:
                continue

            df = pd.DataFrame(counties)
            if df.empty:
                continue
            df = _normalize_columns(df)
            df['hud_fmr_year'] = year
            # Normalize fips_code to county FIPS (first 5 digits)
            if 'fips_code' in df.columns:
                df['fips_code'] = df['fips_code'].astype(str).str.zfill(10).str[:5]
            return df
        except Exception:
            continue

    logger.warning("HUD FMR API returned no data for requested years")
    return pd.DataFrame()


def _read_census_crosswalk(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, sep='|', dtype=str, encoding='utf-8-sig', low_memory=False)
    except Exception as e:
        logger.warning(f"Failed to read Census crosswalk {path}: {e}")
        return pd.DataFrame()

    df = _normalize_columns(df)
    if 'geoid_zcta5_20' not in df.columns or 'geoid_county_20' not in df.columns:
        logger.warning("Census crosswalk missing expected GEOID columns")
        return pd.DataFrame()

    df = df[['geoid_zcta5_20', 'geoid_county_20', 'arealand_part']].copy()
    df = df.rename(columns={
        'geoid_zcta5_20': 'zip',
        'geoid_county_20': 'county_fips',
        'arealand_part': 'weight'
    })
    df['zip'] = df['zip'].astype(str).str.zfill(5)
    df['county_fips'] = df['county_fips'].astype(str).str.zfill(5)
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0)

    # Normalize weights per ZIP
    total = df.groupby('zip')['weight'].transform('sum')
    df['res_ratio'] = df['weight'] / total.replace({0: pd.NA})
    df['res_ratio'] = df['res_ratio'].fillna(0)
    return df[['zip', 'county_fips', 'res_ratio']]


def _load_zip_crosswalk(zip_codes: list[str]) -> pd.DataFrame:
    if settings.USPS_ZIP_COUNTY_CROSSWALK_PATH:
        try:
            path = Path(settings.USPS_ZIP_COUNTY_CROSSWALK_PATH)
            if path.exists():
                if path.suffix.lower() in {'.txt', '.dat'}:
                    return _read_census_crosswalk(path)
                return pd.read_csv(path, dtype=str, low_memory=False)
        except Exception as e:
            logger.warning(f"Failed to read ZIP crosswalk {settings.USPS_ZIP_COUNTY_CROSSWALK_PATH}: {e}")
            return pd.DataFrame()

    if settings.CENSUS_ZIP_COUNTY_CROSSWALK_URL:
        target = CACHE_DIR / "census_zcta_county20.txt"
        if not target.exists():
            ok = download_file(settings.CENSUS_ZIP_COUNTY_CROSSWALK_URL, str(target))
            if not ok:
                logger.warning("Failed to download Census ZIP→county crosswalk")
            else:
                return _read_census_crosswalk(target)
        else:
            return _read_census_crosswalk(target)

    crosswalk_api_url = settings.USPS_ZIP_COUNTY_CROSSWALK_URL or settings.HUD_USPS_API_URL
    if crosswalk_api_url and settings.HUD_USER_API_TOKEN:
        base_url = crosswalk_api_url.rstrip("/")
        headers = {"Authorization": f"Bearer {settings.HUD_USER_API_TOKEN}"}
        rows = []
        if len(zip_codes) > 50:
            logger.warning("ZIP crosswalk API requested for large ZIP set; provide a local crosswalk file instead")
            return pd.DataFrame()
        for zip_code in zip_codes:
            params = {"type": 3, "query": zip_code}
            try:
                resp = requests.get(f"{base_url}/crosswalk", headers=headers, params=params, timeout=30)
                if resp.status_code != 200:
                    continue
                payload = resp.json()
                data = None
                if isinstance(payload, dict):
                    data = payload.get("data") or payload.get("Data") or payload.get("results")
                if not isinstance(data, list):
                    continue
                for item in data:
                    if isinstance(item, dict):
                        rows.append(item)
            except Exception:
                continue
        if rows:
            return pd.DataFrame(rows)

    return pd.DataFrame()


def fetch_hud_fmr_by_county(data_year: int) -> pd.DataFrame:
    """
    Fetch HUD Fair Market Rents (FMR) by county.

    Supports either a local CSV (settings.HUD_FMR_DATA_PATH) or a URL
    (settings.HUD_FMR_DATA_URL).
    """
    if settings.HUD_FMR_DATA_URL and "hudapi" in settings.HUD_FMR_DATA_URL:
        api_df = _fetch_hud_fmr_api(data_year)
        if api_df.empty:
            return pd.DataFrame()
        df = api_df
    else:
        source_path = _resolve_data_path(
            settings.HUD_FMR_DATA_PATH,
            settings.HUD_FMR_DATA_URL,
            CACHE_DIR,
            f"hud_fmr_{data_year}.csv"
        )

        if source_path is None:
            logger.warning("HUD FMR data source not configured; skipping FMR enrichment")
            return pd.DataFrame()

        try:
            df = pd.read_csv(source_path, dtype=str, compression='infer', low_memory=False)
        except Exception as e:
            logger.warning(f"Failed to read HUD FMR data {source_path}: {e}")
            return pd.DataFrame()

        df = _normalize_columns(df)
    columns = list(df.columns)

    year_col = _find_col(columns, ["year", "fmr_year", "fy", "fiscal_year"])
    fmr_year = data_year
    if year_col:
        df[year_col] = pd.to_numeric(df[year_col], errors='coerce')
        if (df[year_col] == data_year).any():
            fmr_year = data_year
            df = df[df[year_col] == data_year]
        elif df[year_col].notna().any():
            fmr_year = int(df[year_col].max())
            df = df[df[year_col] == fmr_year]

    fips_col = _find_col(columns, ["fips", "fips_code", "county_fips", "geoid", "fips2020", "fips2010"])
    if fips_col:
        df['fips_code'] = df[fips_col].astype(str).str.zfill(5).str[:5]
    else:
        state_col = _find_col(columns, ["st2020", "state_fips", "state"])
        county_col = _find_col(columns, ["cnty2020", "county_fips", "county"])
        if state_col and county_col:
            df['fips_code'] = (
                df[state_col].astype(str).str.zfill(2) +
                df[county_col].astype(str).str.zfill(3)
            )
        else:
            name_col = _find_col(columns, ["countyname", "county_name", "county"])
            if name_col:
                df['fips_code'] = _map_county_name_to_fips(df[name_col])

    if 'fips_code' not in df.columns:
        logger.warning("HUD FMR data missing FIPS columns; skipping FMR enrichment")
        return pd.DataFrame()

    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]
    if df.empty:
        logger.warning("HUD FMR data contains no Maryland counties after filtering")
        return pd.DataFrame()

    fmr_col = _find_col(columns, ["fmr_2br", "fmr2", "fmr_2", "fmr_2br_rent", "fmr2br", "two-bedroom", "two_bedroom"])
    if not fmr_col:
        logger.warning("HUD FMR data missing 2-bedroom FMR column; skipping FMR enrichment")
        return pd.DataFrame()

    df[fmr_col] = pd.to_numeric(df[fmr_col], errors='coerce')
    df = df[['fips_code', fmr_col]].copy()
    df = df.rename(columns={fmr_col: 'fmr_2br'})
    df['hud_fmr_year'] = fmr_year
    return df


def fetch_lihtc_by_county(data_year: int) -> pd.DataFrame:
    """
    Fetch HUD LIHTC project data aggregated to county.

    Supports either a local CSV (settings.HUD_LIHTC_DATA_PATH) or a URL
    (settings.HUD_LIHTC_DATA_URL).
    """
    default_name = "hud_lihtc.zip" if (settings.HUD_LIHTC_DATA_URL or "").lower().endswith(".zip") else "hud_lihtc.csv"
    source_path = _resolve_data_path(
        settings.HUD_LIHTC_DATA_PATH,
        settings.HUD_LIHTC_DATA_URL,
        CACHE_DIR,
        default_name
    )

    if source_path is None:
        logger.warning("HUD LIHTC data source not configured; skipping LIHTC enrichment")
        return pd.DataFrame()

    try:
        if source_path.suffix.lower() == ".zip":
            with zipfile.ZipFile(source_path, "r") as zf:
                candidates = [n for n in zf.namelist() if n.lower().endswith((".csv", ".txt"))]
                if not candidates:
                    logger.warning("LIHTC ZIP contains no CSV/TXT files")
                    return pd.DataFrame()
                preferred = None
                for name in candidates:
                    lower_name = name.lower()
                    if "lihtc" in lower_name and "pub" in lower_name:
                        preferred = name
                        break
                if preferred is None:
                    preferred = candidates[0]
                with zf.open(preferred) as fh:
                    df = pd.read_csv(fh, dtype=str, low_memory=False)
        else:
            df = pd.read_csv(source_path, dtype=str, compression='infer', low_memory=False)
    except Exception as e:
        logger.warning(f"Failed to read HUD LIHTC data {source_path}: {e}")
        return pd.DataFrame()

    df = _normalize_columns(df)
    columns = list(df.columns)

    fips_col = _find_col(columns, ["fips", "fips_code", "county_fips", "geoid", "fips2020", "fips2010"])
    if fips_col:
        df['fips_code'] = df[fips_col].astype(str).str.zfill(5).str[:5]
    else:
        state_col = _find_col(columns, ["st2020", "state_fips", "state"])
        county_col = _find_col(columns, ["cnty2020", "county_fips", "county"])
        if state_col and county_col:
            df['fips_code'] = (
                df[state_col].astype(str).str.zfill(2) +
                df[county_col].astype(str).str.zfill(3)
            )
        else:
            name_col = _find_col(columns, ["countyname", "county_name", "county"])
            if name_col:
                df['fips_code'] = _map_county_name_to_fips(df[name_col])

    if 'fips_code' in df.columns:
        df['fips_code'] = df['fips_code'].where(df['fips_code'].str.match(r'^\\d{5}$'), pd.NA)

    units_col = _find_col(columns, ["lihtc_units", "low_income_units", "li_units", "n_units", "total_units", "units"])
    if not units_col:
        logger.warning("HUD LIHTC data missing unit counts; skipping LIHTC enrichment")
        return pd.DataFrame()

    # If FIPS are missing/masked, attempt ZIP -> county crosswalk
    if 'fips_code' not in df.columns or df['fips_code'].isin(MD_COUNTY_FIPS.keys()).sum() < 3:
        zip_col = _find_col(columns, ["proj_zip", "zip", "zipcode", "zip5"])
        if zip_col:
            crosswalk = _load_zip_crosswalk(df[zip_col].astype(str).str.zfill(5).unique().tolist())
            if crosswalk is not None and not crosswalk.empty:
                crosswalk = _normalize_columns(crosswalk)
                cw_cols = list(crosswalk.columns)
                cw_zip = _find_col(cw_cols, ["zip", "zipcode", "zip5"])
                cw_fips = _find_col(cw_cols, ["county_fips", "fips", "fips_code", "geoid"])
                ratio_col = _find_col(cw_cols, ["res_ratio", "tot_ratio", "ratio", "weight"])
                if cw_zip and cw_fips:
                    df[zip_col] = df[zip_col].astype(str).str.zfill(5)
                    crosswalk[cw_zip] = crosswalk[cw_zip].astype(str).str.zfill(5)
                    crosswalk[cw_fips] = crosswalk[cw_fips].astype(str).str.zfill(5)
                    df = df.merge(
                        crosswalk[[cw_zip, cw_fips] + ([ratio_col] if ratio_col else [])],
                        left_on=zip_col,
                        right_on=cw_zip,
                        how='left'
                    )
                    df['fips_code'] = df[cw_fips]
                    df['zip_weight'] = pd.to_numeric(df[ratio_col], errors='coerce') if ratio_col else 1.0
                    df['zip_weight'] = df['zip_weight'].fillna(1.0)
                else:
                    logger.warning("LIHTC crosswalk missing ZIP or FIPS columns")
        else:
            logger.warning("LIHTC data missing ZIP column and valid FIPS; skipping LIHTC enrichment")
            return pd.DataFrame()

    if 'fips_code' not in df.columns:
        logger.warning("HUD LIHTC data missing FIPS columns; skipping LIHTC enrichment")
        return pd.DataFrame()

    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]
    if df.empty:
        logger.warning("HUD LIHTC data contains no Maryland counties after filtering")
        return pd.DataFrame()

    year_col = _find_col(columns, ["placed_in_service_year", "pis_year", "year", "placed_in_service"])
    if year_col:
        df[year_col] = pd.to_numeric(df[year_col], errors='coerce')
        df = df[df[year_col].notna() & (df[year_col] <= data_year)]
        lihtc_year = int(df[year_col].max()) if df[year_col].notna().any() else data_year
    else:
        lihtc_year = data_year

    df[units_col] = pd.to_numeric(df[units_col], errors='coerce')
    if 'zip_weight' in df.columns:
        df[units_col] = df[units_col] * df['zip_weight']
    agg = df.groupby('fips_code', as_index=False)[units_col].sum(min_count=1)
    agg = agg.rename(columns={units_col: 'lihtc_units'})
    agg['lihtc_year'] = lihtc_year
    return agg


def enrich_county_with_external_sources(county_df: pd.DataFrame, data_year: int) -> pd.DataFrame:
    """
    Enrich county housing affordability data with HUD FMR and LIHTC.
    """
    df = county_df.copy()

    fmr_df = fetch_hud_fmr_by_county(data_year)
    if not fmr_df.empty:
        df = df.merge(fmr_df, on='fips_code', how='left')
        df['fmr_2br_to_income'] = np.where(
            df['median_household_income'].fillna(0) > 0,
            (df['fmr_2br'] * 12) / df['median_household_income'],
            pd.NA
        )

    lihtc_df = fetch_lihtc_by_county(data_year)
    if not lihtc_df.empty:
        df = df.merge(lihtc_df, on='fips_code', how='left')
        df['lihtc_units_per_1000_households'] = np.where(
            df['total_households'].fillna(0) > 0,
            (df['lihtc_units'] / df['total_households']) * 1000,
            pd.NA
        )

    extra_scores = []
    if 'fmr_2br_to_income' in df.columns and df['fmr_2br_to_income'].notna().sum() >= 3:
        fmr_score = 1 - df['fmr_2br_to_income'].rank(pct=True)
        df['fmr_affordability_score'] = fmr_score
        extra_scores.append(fmr_score)

    if 'lihtc_units_per_1000_households' in df.columns and df['lihtc_units_per_1000_households'].notna().sum() >= 3:
        lihtc_score = df['lihtc_units_per_1000_households'].rank(pct=True)
        df['lihtc_supply_score'] = lihtc_score
        extra_scores.append(lihtc_score)

    if extra_scores:
        blended_extra = pd.concat(extra_scores, axis=1).mean(axis=1)
        base_score = df['housing_affordability_score']
        df['housing_affordability_score'] = np.where(
            base_score.notna(),
            (1 - EXTERNAL_AFFORDABILITY_BLEND_WEIGHT) * base_score +
            EXTERNAL_AFFORDABILITY_BLEND_WEIGHT * blended_extra,
            blended_extra
        )
        df['housing_affordability_score'] = df['housing_affordability_score'].clip(0, 1)
        df['affordability_version'] = 'v2-affordability+hud'
    else:
        df['affordability_version'] = 'v2-affordability'

    return df

def download_acs_housing_data(year: int) -> pd.DataFrame:
    """
    Download ACS 5-year housing data for Maryland census tracts.

    Key variables:
    - Housing units, tenure, vacancy
    - Median rent, median home value
    - Gross rent as % of income
    - Housing costs for owners
    - Commute time

    Args:
        year: End year of ACS 5-year estimates

    Returns:
        DataFrame with housing metrics by tract
    """
    cache_path = ACS_CACHE_DIR / f"md_acs_housing_{year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached ACS housing data: {cache_path}")
        df = pd.read_csv(cache_path, dtype={'tract_geoid': str, 'fips_code': str})
        median_cols = [
            'median_gross_rent',
            'median_home_value',
            'median_household_income',
            'housing_age_median_year'
        ]
        for col in median_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].replace(list(ACS_MISSING_SENTINELS), np.nan)
                df.loc[df[col] < 0, col] = np.nan
        return df

    logger.info(f"Downloading ACS 5-year housing data for {year}...")

    try:
        from census import Census

        c = Census(settings.CENSUS_API_KEY)

        # ACS variables for housing
        # B25001: Housing units
        # B25002: Occupancy status
        # B25003: Tenure
        # B25064: Median gross rent
        # B25077: Median home value
        # B25071: Median gross rent as % of household income
        # B25070: Gross rent as % of income (distribution)
        # B25091: Mortgage status by selected monthly owner costs as % of income
        # B08303: Travel time to work
        # B19013: Median household income

        variables = [
            'NAME',
            # Housing units
            'B25001_001E',  # Total housing units
            # Occupancy
            'B25002_001E',  # Total
            'B25002_002E',  # Occupied
            'B25002_003E',  # Vacant
            # Tenure
            'B25003_001E',  # Total occupied
            'B25003_002E',  # Owner occupied
            'B25003_003E',  # Renter occupied
            # Values and rents
            'B25064_001E',  # Median gross rent
            'B25077_001E',  # Median home value
            'B19013_001E',  # Median household income
            # Rent burden distribution
            'B25070_001E',  # Gross rent as % income - Total
            'B25070_007E',  # 30.0 to 34.9 percent
            'B25070_008E',  # 35.0 to 39.9 percent
            'B25070_009E',  # 40.0 to 49.9 percent
            'B25070_010E',  # 50.0 percent or more
            # Owner cost burden (with mortgage)
            'B25091_001E',  # Total with mortgage
            'B25091_008E',  # 30.0 to 34.9 percent
            'B25091_009E',  # 35.0 to 39.9 percent
            'B25091_010E',  # 40.0 to 49.9 percent
            'B25091_011E',  # 50.0 percent or more
            # Housing age
            'B25035_001E',  # Median year built
            'B25034_001E',  # Year built total
            'B25034_010E',  # Built 1940 to 1949
            'B25034_011E',  # Built 1939 or earlier
            # Commute time
            'B08303_001E',  # Total workers
            'B08303_012E',  # 45 to 59 minutes
            'B08303_013E',  # 60 or more minutes
            # Crowding
            'B25014_001E',  # Occupants per room - Total
            'B25014_005E',  # 1.01 to 1.50
            'B25014_006E',  # 1.51 to 2.00
            'B25014_007E',  # 2.01 or more
            'B25014_011E',  # Renter: 1.01 to 1.50
            'B25014_012E',  # Renter: 1.51 to 2.00
            'B25014_013E',  # Renter: 2.01 or more
            # Kitchen/plumbing
            'B25052_001E',  # Kitchen facilities total
            'B25052_003E',  # Lacking complete kitchen
            'B25047_001E',  # Plumbing facilities total
            'B25047_003E',  # Lacking complete plumbing
        ]

        # Download tract-level data for Maryland
        data = c.acs5.state_county_tract(
            fields=variables,
            state_fips='24',
            county_fips='*',
            tract='*',
            year=year
        )

        df = pd.DataFrame(data)

        # Create tract GEOID
        df['tract_geoid'] = (
            df['state'].astype(str).str.zfill(2) +
            df['county'].astype(str).str.zfill(3) +
            df['tract'].astype(str).str.zfill(6)
        )
        df['fips_code'] = df['state'].astype(str).str.zfill(2) + df['county'].astype(str).str.zfill(3)

        # Filter to valid Maryland counties
        df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]

        # Rename columns for clarity
        df = df.rename(columns={
            'B25001_001E': 'total_housing_units',
            'B25002_002E': 'occupied_units',
            'B25002_003E': 'vacant_units',
            'B25003_002E': 'owner_occupied_units',
            'B25003_003E': 'renter_occupied_units',
            'B25064_001E': 'median_gross_rent',
            'B25077_001E': 'median_home_value',
            'B19013_001E': 'median_household_income',
            'B25035_001E': 'housing_age_median_year',
            'B25070_001E': 'renter_total',
            'B25070_007E': 'renter_burden_30_35',
            'B25070_008E': 'renter_burden_35_40',
            'B25070_009E': 'renter_burden_40_50',
            'B25070_010E': 'renter_burden_50_plus',
            'B25091_001E': 'owner_with_mortgage_total',
            'B25091_008E': 'owner_burden_30_35',
            'B25091_009E': 'owner_burden_35_40',
            'B25091_010E': 'owner_burden_40_50',
            'B25091_011E': 'owner_burden_50_plus',
            'B25034_010E': 'built_1940_1949',
            'B25034_011E': 'built_pre_1940',
            'B08303_001E': 'total_workers',
            'B08303_012E': 'commute_45_59_min',
            'B08303_013E': 'commute_60_plus_min',
            'B25014_001E': 'occupants_per_room_total',
            'B25014_005E': 'crowded_1_to_1_5',
            'B25014_006E': 'crowded_1_5_to_2',
            'B25014_007E': 'crowded_2_plus',
            'B25052_003E': 'lacking_kitchen',
            'B25047_003E': 'lacking_plumbing',
        })

        # Keep relevant columns
        keep_cols = ['tract_geoid', 'fips_code', 'NAME'] + [c for c in df.columns if c not in ['NAME', 'state', 'county', 'tract', 'tract_geoid', 'fips_code'] and not c.startswith('B')]
        df = df[[c for c in keep_cols if c in df.columns]].copy()

        # Convert numeric columns
        for col in df.columns:
            if col not in ['tract_geoid', 'fips_code', 'NAME']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Clean ACS sentinel values in key median fields
        median_cols = [
            'median_gross_rent',
            'median_home_value',
            'median_household_income',
            'housing_age_median_year'
        ]
        for col in median_cols:
            if col in df.columns:
                df[col] = df[col].replace(list(ACS_MISSING_SENTINELS), np.nan)
                df.loc[df[col] < 0, col] = np.nan

        # Cache
        df.to_csv(cache_path, index=False)

        logger.info(f"✓ Downloaded ACS housing data: {len(df)} tracts")
        return df

    except Exception as e:
        logger.error(f"Failed to download ACS housing data: {e}")
        raise


def download_chas_data(year: int) -> pd.DataFrame:
    """
    Download HUD CHAS (Comprehensive Housing Affordability Strategy) data.

    CHAS provides detailed cost burden and income bracket data that's not
    available in standard ACS tables.

    Args:
        year: CHAS data year (typically lags 2-3 years)

    Returns:
        DataFrame with CHAS metrics by tract
    """
    cache_path = CHAS_CACHE_DIR / f"md_chas_{year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached CHAS data: {cache_path}")
        return pd.read_csv(cache_path, dtype={'tract_geoid': str, 'fips_code': str})

    logger.info(f"Downloading HUD CHAS data for {year}...")

    # CHAS data URL pattern
    # Data is available at: https://www.huduser.gov/portal/datasets/cp.html
    # CSV files are organized by year and geography level

    # For now, we'll create synthetic CHAS-equivalent data from ACS
    # In production, this would download from HUD CHAS API or CSV

    logger.warning("Using ACS-derived cost burden estimates (full CHAS data requires HUD download)")

    # Return empty DataFrame - we'll compute burden from ACS data
    return pd.DataFrame()


def fetch_tract_geometries(year: int = 2020) -> pd.DataFrame:
    """
    Fetch Maryland census tract geometries and areas.

    Args:
        year: Census year for tract boundaries

    Returns:
        DataFrame with tract centroids and areas
    """
    cache_path = CACHE_DIR / f"md_tract_geometries_{year}.csv"

    if cache_path.exists():
        logger.info("Using cached tract geometries")
        return pd.read_csv(cache_path, dtype={'tract_geoid': str, 'fips_code': str})

    logger.info("Fetching tract geometries...")

    try:
        import pygris

        tracts = pygris.tracts(state="MD", year=year, cb=True)
        tracts = tracts.to_crs("EPSG:4326")

        # Compute areas
        tracts_proj = tracts.to_crs("EPSG:3857")
        tracts['land_area_sq_mi'] = tracts_proj.geometry.area / 2.59e6

        # Extract centroids
        tracts['centroid_lon'] = tracts_proj.geometry.centroid.to_crs("EPSG:4326").x
        tracts['centroid_lat'] = tracts_proj.geometry.centroid.to_crs("EPSG:4326").y

        # Create identifiers
        tracts['tract_geoid'] = tracts['GEOID'].astype(str).str.zfill(11)
        tracts['fips_code'] = (tracts['STATEFP'].astype(str) + tracts['COUNTYFP'].astype(str)).str.zfill(5)

        # Filter to valid counties
        tracts = tracts[tracts['fips_code'].isin(MD_COUNTY_FIPS.keys())]

        # Keep essential columns
        result = tracts[['tract_geoid', 'fips_code', 'land_area_sq_mi', 'centroid_lon', 'centroid_lat']].copy()

        # Cache
        result.to_csv(cache_path, index=False)

        logger.info(f"✓ Loaded {len(result)} tract geometries")
        return result

    except Exception as e:
        logger.error(f"Failed to fetch tract geometries: {e}")
        raise


def fetch_tract_population(year: int) -> pd.DataFrame:
    """
    Fetch tract population for weighting.

    Args:
        year: ACS year

    Returns:
        DataFrame with tract population
    """
    cache_path = CACHE_DIR / f"md_tract_population_{year}.csv"

    if cache_path.exists():
        logger.info("Using cached tract population")
        return pd.read_csv(cache_path, dtype={'tract_geoid': str})

    logger.info("Fetching tract population...")

    try:
        from census import Census

        c = Census(settings.CENSUS_API_KEY)

        data = c.acs5.state_county_tract(
            fields=['B01003_001E'],  # Total population
            state_fips='24',
            county_fips='*',
            tract='*',
            year=year
        )

        df = pd.DataFrame(data)
        df['tract_geoid'] = (
            df['state'].astype(str).str.zfill(2) +
            df['county'].astype(str).str.zfill(3) +
            df['tract'].astype(str).str.zfill(6)
        )
        df['population'] = pd.to_numeric(df['B01003_001E'], errors='coerce').fillna(0).astype(int)

        result = df[['tract_geoid', 'population']].copy()
        result.to_csv(cache_path, index=False)

        logger.info(f"✓ Loaded population for {len(result)} tracts")
        return result

    except Exception as e:
        logger.error(f"Failed to fetch tract population: {e}")
        raise


# =============================================================================
# COST BURDEN COMPUTATION
# =============================================================================

def compute_cost_burden_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute cost burden metrics from ACS data.

    Cost burden = households paying >30% of income on housing
    Severe burden = households paying >50% of income on housing

    Args:
        df: DataFrame with ACS housing data

    Returns:
        DataFrame with cost burden metrics added
    """
    logger.info("Computing cost burden metrics...")

    # Renter cost burden (from B25070)
    df['renter_cost_burdened'] = (
        df['renter_burden_30_35'].fillna(0) +
        df['renter_burden_35_40'].fillna(0) +
        df['renter_burden_40_50'].fillna(0) +
        df['renter_burden_50_plus'].fillna(0)
    )

    df['renter_severely_burdened'] = df['renter_burden_50_plus'].fillna(0)

    df['renter_cost_burdened_pct'] = np.where(
        df['renter_total'] > 0,
        df['renter_cost_burdened'] / df['renter_total'],
        0
    )

    # Owner cost burden (from B25091 - with mortgage)
    df['owner_cost_burdened'] = (
        df['owner_burden_30_35'].fillna(0) +
        df['owner_burden_35_40'].fillna(0) +
        df['owner_burden_40_50'].fillna(0) +
        df['owner_burden_50_plus'].fillna(0)
    )

    df['owner_severely_burdened'] = df['owner_burden_50_plus'].fillna(0)

    df['owner_cost_burdened_pct'] = np.where(
        df['owner_with_mortgage_total'] > 0,
        df['owner_cost_burdened'] / df['owner_with_mortgage_total'],
        0
    )

    # Combined cost burden (weighted by tenure)
    df['total_households'] = df['occupied_units'].fillna(0)

    df['cost_burdened_households'] = (
        df['renter_cost_burdened'] + df['owner_cost_burdened']
    )

    df['severely_cost_burdened_households'] = (
        df['renter_severely_burdened'] + df['owner_severely_burdened']
    )

    df['cost_burdened_pct'] = np.where(
        df['total_households'] > 0,
        df['cost_burdened_households'] / df['total_households'],
        0
    )

    df['severely_cost_burdened_pct'] = np.where(
        df['total_households'] > 0,
        df['severely_cost_burdened_households'] / df['total_households'],
        0
    )

    # Clamp to valid range
    df['cost_burdened_pct'] = df['cost_burdened_pct'].clip(0, 1)
    df['severely_cost_burdened_pct'] = df['severely_cost_burdened_pct'].clip(0, 1)
    df['renter_cost_burdened_pct'] = df['renter_cost_burdened_pct'].clip(0, 1)
    df['owner_cost_burdened_pct'] = df['owner_cost_burdened_pct'].clip(0, 1)

    logger.info("✓ Computed cost burden metrics")
    return df


def compute_housing_quality_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute housing quality indicators.

    Args:
        df: DataFrame with ACS housing data

    Returns:
        DataFrame with quality metrics added
    """
    logger.info("Computing housing quality metrics...")

    # Pre-1950 housing stock
    df['pre_1950_housing'] = (
        df['built_1940_1949'].fillna(0) + df['built_pre_1940'].fillna(0)
    )
    df['pre_1950_housing_pct'] = np.where(
        df['total_housing_units'] > 0,
        df['pre_1950_housing'] / df['total_housing_units'],
        0
    )

    # Crowding (>1 person per room)
    df['crowded_units'] = (
        df['crowded_1_to_1_5'].fillna(0) +
        df['crowded_1_5_to_2'].fillna(0) +
        df['crowded_2_plus'].fillna(0)
    )
    df['crowded_units_pct'] = np.where(
        df['occupants_per_room_total'] > 0,
        df['crowded_units'] / df['occupants_per_room_total'],
        0
    )

    # Lacking facilities
    df['lacking_complete_kitchen_pct'] = np.where(
        df['total_housing_units'] > 0,
        df['lacking_kitchen'].fillna(0) / df['total_housing_units'],
        0
    )
    df['lacking_complete_plumbing_pct'] = np.where(
        df['total_housing_units'] > 0,
        df['lacking_plumbing'].fillna(0) / df['total_housing_units'],
        0
    )

    # Vacancy rate
    df['vacancy_rate'] = np.where(
        df['total_housing_units'] > 0,
        df['vacant_units'].fillna(0) / df['total_housing_units'],
        0
    )

    logger.info("✓ Computed housing quality metrics")
    return df


def compute_ht_burden(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute estimated Housing + Transportation (H+T) burden.

    Based on CNT H+T Index methodology, estimates combined housing
    and commute costs as percentage of income.

    Args:
        df: DataFrame with housing data including commute time

    Returns:
        DataFrame with H+T metrics added
    """
    logger.info("Computing Housing + Transportation burden estimates...")

    # Estimate average commute time (weighted by long commutes)
    df['long_commuters'] = (
        df['commute_45_59_min'].fillna(0) + df['commute_60_plus_min'].fillna(0)
    )
    df['long_commute_pct'] = np.where(
        df['total_workers'] > 0,
        df['long_commuters'] / df['total_workers'],
        0
    )

    # Estimate average commute time in minutes
    # Average for 45-59 = 52, for 60+ = 75
    df['avg_commute_time_minutes'] = np.where(
        df['total_workers'] > 0,
        (df['commute_45_59_min'].fillna(0) * 52 +
         df['commute_60_plus_min'].fillna(0) * 75 +
         (df['total_workers'] - df['long_commuters']).clip(lower=0) * 25) / df['total_workers'],
        25  # Default 25 minutes
    )

    # Estimate monthly commute cost
    # Assume 25 mph average speed for commute
    avg_speed_mph = 25
    df['commute_distance_miles'] = df['avg_commute_time_minutes'] / 60 * avg_speed_mph
    df['estimated_commute_cost_monthly'] = (
        df['commute_distance_miles'] * 2 * DAYS_WORKED_PER_YEAR / 12 * AVG_ANNUAL_COMMUTE_COST_PER_MILE
    ).astype(int)

    # Combined H+T burden
    # Housing cost = median rent or imputed owner cost
    df['monthly_housing_cost'] = df['median_gross_rent'].fillna(
        df['median_home_value'] * 0.006  # Rough owner cost estimate
    )

    df['monthly_income'] = df['median_household_income'].fillna(0) / 12

    df['housing_plus_transport_pct'] = np.where(
        df['monthly_income'] > 0,
        (df['monthly_housing_cost'] + df['estimated_commute_cost_monthly']) / df['monthly_income'],
        0
    )

    # Clamp to reasonable range
    df['housing_plus_transport_pct'] = df['housing_plus_transport_pct'].clip(0, 1)

    # Rent-to-income ratio
    df['rent_to_income_ratio'] = np.where(
        (df['monthly_income'] > 0) & df['median_gross_rent'].notna(),
        df['median_gross_rent'] / df['monthly_income'],
        np.nan
    ).clip(0, 1)

    # Price-to-income ratio
    df['price_to_income_ratio'] = np.where(
        (df['median_household_income'] > 0) & df['median_home_value'].notna(),
        df['median_home_value'] / df['median_household_income'],
        np.nan
    )

    logger.info("✓ Computed H+T burden estimates")
    return df


# =============================================================================
# SCORE NORMALIZATION
# =============================================================================

def normalize_affordability_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize affordability metrics to 0-1 scores.

    Higher score = better affordability.

    Args:
        df: DataFrame with affordability metrics

    Returns:
        DataFrame with normalized scores added
    """
    logger.info("Normalizing affordability scores...")

    # Affordability burden score: 1 - cost_burdened_pct (higher = better)
    df['affordability_burden_score'] = 1 - df['cost_burdened_pct'].clip(0, 1)

    # Affordable stock score: percentile rank within Maryland
    # For now, use inverse of rent-to-income ratio as proxy
    # In full implementation, this would use CHAS affordable unit counts
    df['affordable_stock_score'] = 1 - df['rent_to_income_ratio'].rank(pct=True)

    # Housing quality score: composite of quality indicators
    # Lower old housing %, lower crowding %, complete facilities = higher score
    quality_penalty = (
        df['pre_1950_housing_pct'] * 0.3 +
        df['crowded_units_pct'] * 0.4 +
        df['lacking_complete_kitchen_pct'] * 0.15 +
        df['lacking_complete_plumbing_pct'] * 0.15
    )
    df['housing_quality_score'] = (1 - quality_penalty).clip(0, 1)

    # Composite housing affordability score
    df['housing_affordability_score'] = (
        BURDEN_WEIGHT * df['affordability_burden_score'] +
        STOCK_WEIGHT * df['affordable_stock_score'] +
        QUALITY_WEIGHT * df['housing_quality_score']
    ).clip(0, 1)

    # Housing density
    df['housing_density_per_sq_mi'] = np.where(
        df['land_area_sq_mi'] > 0,
        df['total_housing_units'] / df['land_area_sq_mi'],
        0
    )

    logger.info("✓ Normalized affordability scores")
    return df


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_to_county(tract_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tract-level metrics to county level.

    Uses population-weighted averaging for score metrics.

    Args:
        tract_df: DataFrame with tract-level metrics

    Returns:
        DataFrame with county-level metrics
    """
    logger.info("Aggregating to county level...")

    # Prepare population weights
    tract_df['population'] = tract_df['population'].fillna(0)
    tract_df['pop_weight'] = tract_df.groupby('fips_code')['population'].transform(
        lambda x: x / x.sum() if x.sum() > 0 else 1 / len(x)
    )

    # Weighted average columns
    score_cols = [
        'cost_burdened_pct', 'severely_cost_burdened_pct',
        'owner_cost_burdened_pct', 'renter_cost_burdened_pct',
        'vacancy_rate', 'rent_to_income_ratio', 'price_to_income_ratio',
        'pre_1950_housing_pct', 'crowded_units_pct',
        'housing_plus_transport_pct', 'avg_commute_time_minutes',
        'affordability_burden_score', 'affordable_stock_score',
        'housing_quality_score', 'housing_affordability_score'
    ]

    weighted_cols = {}
    for col in score_cols:
        if col in tract_df.columns:
            tract_df[f'{col}_weighted'] = tract_df[col] * tract_df['pop_weight']
            weighted_cols[f'{col}_weighted'] = (f'{col}_weighted', 'sum')

    # Aggregation spec
    agg_spec = {
        # Sum counts
        'total_housing_units': ('total_housing_units', 'sum'),
        'occupied_units': ('occupied_units', 'sum'),
        'vacant_units': ('vacant_units', 'sum'),
        'owner_occupied_units': ('owner_occupied_units', 'sum'),
        'renter_occupied_units': ('renter_occupied_units', 'sum'),
        'total_households': ('total_households', 'sum'),
        'cost_burdened_households': ('cost_burdened_households', 'sum'),
        'severely_cost_burdened_households': ('severely_cost_burdened_households', 'sum'),

        # Sum population and area
        'population': ('population', 'sum'),
        'land_area_sq_mi': ('land_area_sq_mi', 'sum'),

        # Count tracts
        'tract_count': ('tract_geoid', 'count'),

        # Median values (use weighted median approximation)
        'median_gross_rent': ('median_gross_rent', 'median'),
        'median_home_value': ('median_home_value', 'median'),
        'median_household_income': ('median_household_income', 'median'),
        'housing_age_median_year': ('housing_age_median_year', 'median'),
    }

    # Add weighted columns to spec
    for new_col, (src_col, func) in weighted_cols.items():
        if src_col in tract_df.columns:
            agg_spec[new_col] = (src_col, func)

    county_agg = tract_df.groupby('fips_code').agg(**agg_spec).reset_index()

    # Rename weighted columns back
    for col in score_cols:
        weighted_name = f'{col}_weighted'
        if weighted_name in county_agg.columns:
            county_agg[col] = county_agg[weighted_name]
            county_agg = county_agg.drop(columns=[weighted_name])

    # Compute county-level derived metrics
    county_agg['vacancy_rate'] = np.where(
        county_agg['total_housing_units'] > 0,
        county_agg['vacant_units'] / county_agg['total_housing_units'],
        0
    )

    county_agg['housing_density_per_sq_mi'] = np.where(
        county_agg['land_area_sq_mi'] > 0,
        county_agg['total_housing_units'] / county_agg['land_area_sq_mi'],
        0
    )

    logger.info(f"✓ Aggregated to {len(county_agg)} counties")
    return county_agg


# =============================================================================
# DATABASE STORAGE
# =============================================================================

def store_tract_housing_affordability(df: pd.DataFrame, data_year: int, acs_year: int):
    """
    Store tract-level housing affordability data.

    Args:
        df: DataFrame with tract metrics
        data_year: Year for this data
        acs_year: Year of ACS data used
    """
    logger.info(f"Storing {len(df)} tract housing affordability records...")

    with get_db() as db:
        def safe_float(value, min_value: float = None, max_value: float = None):
            if value is None or pd.isna(value):
                return None
            val = float(value)
            if min_value is not None and val < min_value:
                return None
            if max_value is not None and val > max_value:
                return None
            return val

        # Clear existing data for this year
        db.execute(text("""
            DELETE FROM layer4_housing_affordability_tract
            WHERE data_year = :data_year
        """), {"data_year": data_year})

        # Insert new records
        for _, row in df.iterrows():
            db.execute(text("""
                INSERT INTO layer4_housing_affordability_tract (
                    tract_geoid, fips_code, data_year,
                    total_housing_units, occupied_units, owner_occupied_units, renter_occupied_units,
                    vacant_units, vacancy_rate,
                    total_households, cost_burdened_households, severely_cost_burdened_households,
                    cost_burdened_pct, severely_cost_burdened_pct,
                    owner_cost_burdened_pct, renter_cost_burdened_pct,
                    median_gross_rent, median_home_value, median_household_income,
                    price_to_income_ratio, rent_to_income_ratio,
                    avg_commute_time_minutes, estimated_commute_cost_monthly, housing_plus_transport_pct,
                    housing_age_median_year, pre_1950_housing_pct, crowded_units_pct,
                    lacking_complete_plumbing_pct, lacking_complete_kitchen_pct,
                    affordability_burden_score, affordable_stock_score, housing_quality_score,
                    housing_affordability_score,
                    land_area_sq_mi, housing_density_per_sq_mi, tract_population,
                    acs_year
                ) VALUES (
                    :tract_geoid, :fips_code, :data_year,
                    :total_units, :occupied, :owner, :renter,
                    :vacant, :vacancy_rate,
                    :households, :burdened, :severely_burdened,
                    :burden_pct, :severe_pct,
                    :owner_burden, :renter_burden,
                    :rent, :home_value, :income,
                    :pti, :rti,
                    :commute_time, :commute_cost, :ht_pct,
                    :year_built, :pre_1950, :crowded,
                    :no_plumbing, :no_kitchen,
                    :burden_score, :stock_score, :quality_score,
                    :affordability_score,
                    :area, :density, :population,
                    :acs_year
                )
            """), {
                'tract_geoid': row['tract_geoid'],
                'fips_code': row['fips_code'],
                'data_year': data_year,
                'total_units': int(row.get('total_housing_units', 0)),
                'occupied': int(row.get('occupied_units', 0)),
                'owner': int(row.get('owner_occupied_units', 0)),
                'renter': int(row.get('renter_occupied_units', 0)),
                'vacant': int(row.get('vacant_units', 0)),
                'vacancy_rate': float(row.get('vacancy_rate', 0)),
                'households': int(row.get('total_households', 0)),
                'burdened': int(row.get('cost_burdened_households', 0)),
                'severely_burdened': int(row.get('severely_cost_burdened_households', 0)),
                'burden_pct': float(row.get('cost_burdened_pct', 0)),
                'severe_pct': float(row.get('severely_cost_burdened_pct', 0)),
                'owner_burden': float(row.get('owner_cost_burdened_pct', 0)),
                'renter_burden': float(row.get('renter_cost_burdened_pct', 0)),
                'rent': int(row.get('median_gross_rent')) if pd.notna(row.get('median_gross_rent')) and row.get('median_gross_rent') > 0 else None,
                'home_value': int(row.get('median_home_value')) if pd.notna(row.get('median_home_value')) and row.get('median_home_value') > 0 else None,
                'income': int(row.get('median_household_income')) if pd.notna(row.get('median_household_income')) and row.get('median_household_income') > 0 else None,
                'pti': safe_float(row.get('price_to_income_ratio'), min_value=0, max_value=9999.99),
                'rti': safe_float(row.get('rent_to_income_ratio'), min_value=0, max_value=1),
                'commute_time': float(row.get('avg_commute_time_minutes', 0)),
                'commute_cost': int(row.get('estimated_commute_cost_monthly', 0)),
                'ht_pct': safe_float(row.get('housing_plus_transport_pct'), min_value=0, max_value=1),
                'year_built': int(row.get('housing_age_median_year', 0)) if pd.notna(row.get('housing_age_median_year')) else None,
                'pre_1950': float(row.get('pre_1950_housing_pct', 0)),
                'crowded': float(row.get('crowded_units_pct', 0)),
                'no_plumbing': float(row.get('lacking_complete_plumbing_pct', 0)),
                'no_kitchen': float(row.get('lacking_complete_kitchen_pct', 0)),
                'burden_score': float(row.get('affordability_burden_score', 0)),
                'stock_score': float(row.get('affordable_stock_score', 0)),
                'quality_score': float(row.get('housing_quality_score', 0)),
                'affordability_score': float(row.get('housing_affordability_score', 0)),
                'area': float(row.get('land_area_sq_mi', 0)),
                'density': float(row.get('housing_density_per_sq_mi', 0)),
                'population': int(row.get('population', 0)),
                'acs_year': acs_year
            })

        db.commit()

    logger.info("✓ Tract housing affordability data stored")


def store_county_housing_affordability(df: pd.DataFrame, data_year: int, acs_year: int):
    """
    Update county-level housing affordability in existing table.

    Creates composite housing_opportunity_index combining v1 elasticity
    with v2 affordability.

    Args:
        df: DataFrame with county metrics
        data_year: Year for this data
        acs_year: Year of ACS data used
    """
    logger.info(f"Updating {len(df)} county housing affordability records...")

    with get_db() as db:
        def safe_float(value, min_value: float = None, max_value: float = None):
            if value is None or pd.isna(value):
                return None
            val = float(value)
            if min_value is not None and val < min_value:
                return None
            if max_value is not None and val > max_value:
                return None
            return val

        # Get existing v1 elasticity scores
        elasticity_scores = {}
        result = db.execute(text("""
            SELECT fips_code, housing_elasticity_index
            FROM layer4_housing_elasticity
            WHERE data_year = :data_year
        """), {"data_year": data_year})

        for row in result.fetchall():
            fips_code, v1_score = row
            if v1_score is not None:
                elasticity_scores[fips_code] = float(v1_score)

        # Update each county
        for _, row in df.iterrows():
            fips_code = row['fips_code']
            elasticity = elasticity_scores.get(fips_code)
            affordability = row.get('housing_affordability_score')

            # Compute composite index
            if elasticity is None and affordability is None:
                opportunity_index = None
            elif elasticity is None:
                opportunity_index = affordability
            elif affordability is None:
                opportunity_index = elasticity
            else:
                opportunity_index = (
                    ELASTICITY_WEIGHT * elasticity +
                    AFFORDABILITY_WEIGHT * affordability
                )

            db.execute(text("""
                UPDATE layer4_housing_elasticity
                SET
                    total_households = :households,
                    cost_burdened_households = :burdened,
                    severely_cost_burdened_households = :severely_burdened,
                    cost_burdened_pct = :burden_pct,
                    severely_cost_burdened_pct = :severe_pct,
                    owner_cost_burdened_pct = :owner_burden,
                    renter_cost_burdened_pct = :renter_burden,
                    rent_to_income_ratio = :rti,
                    avg_commute_time_minutes = :commute_time,
                    housing_plus_transport_pct = :ht_pct,
                    housing_age_median_year = :year_built,
                    pre_1950_housing_pct = :pre_1950,
                    crowded_units_pct = :crowded,
                    housing_quality_score = :quality_score,
                    affordability_burden_score = :burden_score,
                    affordable_stock_score = :stock_score,
                    housing_affordability_score = :affordability_score,
                    fmr_2br = :fmr_2br,
                    fmr_2br_to_income = :fmr_2br_to_income,
                    hud_fmr_year = :hud_fmr_year,
                    lihtc_units = :lihtc_units,
                    lihtc_units_per_1000_households = :lihtc_units_per_1000_households,
                    lihtc_year = :lihtc_year,
                    housing_opportunity_index = :opportunity_index,
                    acs_year = :acs_year,
                    affordability_version = :affordability_version,
                    updated_at = CURRENT_TIMESTAMP
                WHERE fips_code = :fips_code AND data_year = :data_year
            """), {
                'fips_code': fips_code,
                'data_year': data_year,
                'households': int(row.get('total_households', 0)),
                'burdened': int(row.get('cost_burdened_households', 0)),
                'severely_burdened': int(row.get('severely_cost_burdened_households', 0)),
                'burden_pct': float(row.get('cost_burdened_pct', 0)),
                'severe_pct': float(row.get('severely_cost_burdened_pct', 0)),
                'owner_burden': float(row.get('owner_cost_burdened_pct', 0)),
                'renter_burden': float(row.get('renter_cost_burdened_pct', 0)),
                'rti': safe_float(row.get('rent_to_income_ratio'), min_value=0, max_value=1),
                'commute_time': float(row.get('avg_commute_time_minutes', 0)),
                'ht_pct': safe_float(row.get('housing_plus_transport_pct'), min_value=0, max_value=1),
                'year_built': int(row.get('housing_age_median_year', 0)) if pd.notna(row.get('housing_age_median_year')) else None,
                'pre_1950': float(row.get('pre_1950_housing_pct', 0)),
                'crowded': float(row.get('crowded_units_pct', 0)),
                'quality_score': float(row.get('housing_quality_score', 0)),
                'burden_score': float(row.get('affordability_burden_score', 0)),
                'stock_score': float(row.get('affordable_stock_score', 0)),
                'affordability_score': float(row.get('housing_affordability_score', 0)),
                'fmr_2br': int(row.get('fmr_2br')) if pd.notna(row.get('fmr_2br')) else None,
                'fmr_2br_to_income': safe_float(row.get('fmr_2br_to_income'), min_value=0, max_value=10),
                'hud_fmr_year': int(row.get('hud_fmr_year')) if pd.notna(row.get('hud_fmr_year')) else None,
                'lihtc_units': int(row.get('lihtc_units')) if pd.notna(row.get('lihtc_units')) else None,
                'lihtc_units_per_1000_households': safe_float(row.get('lihtc_units_per_1000_households'), min_value=0, max_value=1000),
                'lihtc_year': int(row.get('lihtc_year')) if pd.notna(row.get('lihtc_year')) else None,
                'opportunity_index': opportunity_index,
                'acs_year': acs_year,
                'affordability_version': row.get('affordability_version', 'v2-affordability')
            })

        db.commit()

    logger.info("✓ County housing affordability data updated")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def calculate_housing_affordability_indicators(
    data_year: int = None,
    acs_year: int = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate housing affordability indicators for all Maryland tracts.

    Args:
        data_year: Year to associate with this data
        acs_year: ACS year to use

    Returns:
        Tuple of (tract_df, county_df) with housing affordability metrics
    """
    data_year = data_year or datetime.now().year
    acs_year = acs_year or min(data_year - 1, settings.ACS_LATEST_YEAR)  # ACS lags 1-2 years

    logger.info("=" * 60)
    logger.info("LAYER 4 v2: HOUSING AFFORDABILITY ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Data year: {data_year}")
    logger.info(f"ACS year: {acs_year}")

    # Step 1: Download ACS housing data
    logger.info("\n[1/6] Downloading ACS housing data...")
    housing_df = download_acs_housing_data(acs_year)

    # Step 2: Get tract geometries and population
    logger.info("\n[2/6] Loading tract geometries and population...")
    geometries = fetch_tract_geometries()
    population = fetch_tract_population(acs_year)

    # Merge
    tract_df = housing_df.merge(geometries, on=['tract_geoid', 'fips_code'], how='left')
    tract_df = tract_df.merge(population, on='tract_geoid', how='left')
    tract_df['population'] = tract_df['population'].fillna(0).astype(int)

    # Step 3: Compute cost burden
    logger.info("\n[3/6] Computing cost burden metrics...")
    tract_df = compute_cost_burden_metrics(tract_df)

    # Step 4: Compute housing quality
    logger.info("\n[4/6] Computing housing quality metrics...")
    tract_df = compute_housing_quality_metrics(tract_df)

    # Step 5: Compute H+T burden
    logger.info("\n[5/6] Computing H+T burden estimates...")
    tract_df = compute_ht_burden(tract_df)

    # Step 6: Normalize scores
    logger.info("\n[6/6] Normalizing affordability scores...")
    tract_df = normalize_affordability_scores(tract_df)

    # Aggregate to county
    county_df = aggregate_to_county(tract_df)

    # Enrich with HUD FMR + LIHTC if available
    county_df = enrich_county_with_external_sources(county_df, data_year)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("HOUSING AFFORDABILITY ANALYSIS COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Tracts analyzed: {len(tract_df)}")
    logger.info(f"Counties: {len(county_df)}")
    logger.info(f"Avg cost burden: {tract_df['cost_burdened_pct'].mean():.1%}")
    logger.info(f"Avg affordability score: {tract_df['housing_affordability_score'].mean():.3f}")

    return tract_df, county_df


def run_layer4_v2_ingestion(
    data_year: int = None,
    multi_year: bool = True,
    store_data: bool = True,
    window_years: int = DEFAULT_WINDOW_YEARS,
    predict_to_year: Optional[int] = None
):
    """
    Run complete Layer 4 v2 ingestion pipeline.

    Args:
        data_year: End year for this data (default: latest available county year)
        multi_year: If True, run a multi-year window ending at latest available year
        store_data: Whether to store results in database
        window_years: Window size for multi-year ingestion (default: 5)
    """
    current_year = datetime.now().year
    latest_county_year = None
    try:
        with get_db() as db:
            latest_county_year = db.execute(
                text("SELECT MAX(data_year) FROM layer4_housing_elasticity")
            ).scalar()
    except Exception as e:
        logger.warning(f"Failed to determine latest county year: {e}")

    if data_year is None:
        data_year = latest_county_year or min(settings.ACS_LATEST_YEAR, current_year)

    if latest_county_year and data_year > latest_county_year:
        logger.warning(
            f"Requested year {data_year} exceeds latest county year "
            f"{latest_county_year}. Using {latest_county_year}."
        )
        data_year = latest_county_year

    try:
        if multi_year:
            start_year = data_year - window_years + 1
            years_to_fetch = list(range(start_year, data_year + 1))
            logger.info(
                "Starting Layer 4 v2 MULTI-YEAR ingestion for years "
                f"{years_to_fetch[0]}-{years_to_fetch[-1]}"
            )
        else:
            years_to_fetch = [data_year]
            logger.info(f"Starting Layer 4 v2 single-year ingestion for {data_year}")

        total_records = 0
        failed_years = []

        for year in years_to_fetch:
            acs_year = min(year - 1, settings.ACS_LATEST_YEAR)

            logger.info("=" * 70)
            logger.info(f"Processing year {year}")
            logger.info("=" * 70)

            try:
                tract_df, county_df = calculate_housing_affordability_indicators(
                    data_year=year,
                    acs_year=acs_year
                )

                if store_data and not tract_df.empty:
                    store_tract_housing_affordability(tract_df, year, acs_year)
                    store_county_housing_affordability(county_df, year, acs_year)

                    log_refresh(
                        layer_name="layer4_housing_elasticity",
                        data_source="ACS (v2 affordability)",
                        status="success",
                        records_processed=len(tract_df),
                        records_inserted=len(tract_df) + len(county_df),
                        metadata={
                            "data_year": year,
                            "acs_year": acs_year,
                            "version": "v2-affordability",
                            "tracts": len(tract_df),
                            "counties": len(county_df),
                            "avg_cost_burden": float(tract_df['cost_burdened_pct'].mean()),
                            "avg_affordability_score": float(tract_df['housing_affordability_score'].mean())
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
            logger.info(
                f"  Years requested: {years_to_fetch[0]}-{years_to_fetch[-1]} "
                f"({len(years_to_fetch)} years)"
            )
            logger.info(f"  Years successful: {len(years_to_fetch) - len(failed_years)}")
            logger.info(f"  Years failed: {len(failed_years)} {failed_years if failed_years else ''}")
            logger.info(f"  Total tract records stored: {total_records}")
        else:
            logger.info(f"Single-year ingestion {'succeeded' if not failed_years else 'failed'}")

        if failed_years and len(failed_years) == len(years_to_fetch):
            raise Exception(f"All years failed: {failed_years}")

        if store_data:
            target_year = predict_to_year or settings.PREDICT_TO_YEAR
            apply_predictions_to_table(
                table="layer4_housing_elasticity",
                metric_col="housing_opportunity_index",
                target_year=target_year,
                clip=(0.0, 1.0)
            )

        logger.info("✓ Layer 4 v2 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 4 v2 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer4_housing_elasticity",
            data_source="ACS (v2 affordability)",
            status="failed",
            error_message=str(e)
        )
        raise


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Layer 4 v2: Housing Affordability Analysis'
    )
    parser.add_argument(
        '--year', type=int, default=None,
        help='End year for window (default: latest available county year)'
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

    run_layer4_v2_ingestion(
        data_year=args.year,
        multi_year=not args.single_year,
        store_data=not args.dry_run,
        predict_to_year=args.predict_to_year
    )


if __name__ == "__main__":
    main()
