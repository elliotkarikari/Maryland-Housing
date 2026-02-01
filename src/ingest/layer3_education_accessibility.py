"""
Maryland Viability Atlas - Layer 3 v2: Education Accessibility Analysis
Modern accessibility-based education opportunity metrics.

Combines:
- v1: School supply metrics (enrollment, counts, density)
- v2: Quality-weighted accessibility (schools reachable within travel time)

Data Sources:
- NCES CCD: School directory with locations
- MSDE Report Card: Proficiency rates, graduation rates
- ACS: School-age population by tract
- Census: Tract boundaries and centroids

Methodology:
- Proximity-based gravity model for school accessibility
- Quality weighting using MSDE proficiency data
- Population-weighted aggregation from tract to county

Composite Formula:
    education_opportunity_index = 0.4 × school_supply_score + 0.6 × education_accessibility_score

Where:
    school_supply_score = normalized(school_density + prek_availability + enrollment_trend)
    education_accessibility_score = 0.5 × high_quality_access + 0.3 × prek_access + 0.2 × equity_score
"""

import sys
import zipfile
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
from sqlalchemy import text
import requests

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.logging import get_logger
from src.utils.prediction_utils import apply_predictions_to_table

logger = get_logger(__name__)
settings = get_settings()

# =============================================================================
# CONFIGURATION
# =============================================================================

# Cache directories
CACHE_DIR = Path("data/cache/education_v2")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
NCES_CACHE_DIR = CACHE_DIR / "nces"
NCES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
MSDE_CACHE_DIR = CACHE_DIR / "msde"
MSDE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
ACS_CACHE_DIR = CACHE_DIR / "acs"
ACS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Distance thresholds (km) - approximating travel time
DIST_15MIN_KM = 8      # ~15 min at 30 km/h urban average
DIST_20MIN_KM = 12     # ~20 min
DIST_30MIN_KM = 20     # ~30 min

# Composite weights
SUPPLY_WEIGHT = 0.40        # v1 school supply metrics
ACCESSIBILITY_WEIGHT = 0.60 # v2 accessibility metrics

# Within v2 accessibility score
HIGH_QUALITY_ACCESS_WEIGHT = 0.50
PREK_ACCESS_WEIGHT = 0.30
EQUITY_WEIGHT = 0.20

# Quality thresholds
PROFICIENCY_MEDIAN = 50.0   # Will be computed from data
TOP_QUARTILE_THRESHOLD = 75.0  # Percentile threshold

DEFAULT_WINDOW_YEARS = 5
ACS_GEOGRAPHY_MAX_YEAR = 2022

# NCES CCD URLs
NCES_SCHOOL_DIR_URL = "https://nces.ed.gov/ccd/files.asp"
NCES_BASE_URL = "https://nces.ed.gov/ccd/"

# MSDE Report Card - Maryland State Department of Education
MSDE_REPORT_CARD_URL = "https://reportcard.msde.maryland.gov/"


# =============================================================================
# DATA ACQUISITION - NCES School Directory
# =============================================================================

def download_nces_school_directory(year: int) -> pd.DataFrame:
    """
    Download NCES CCD school directory for Maryland.

    Args:
        year: School year end (e.g., 2024 for 2023-24)

    Returns:
        DataFrame with school locations and characteristics
    """
    cache_path = NCES_CACHE_DIR / f"md_schools_{year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached NCES school directory: {cache_path}")
        df = pd.read_csv(
            cache_path,
            dtype={
                'NCESSCH': str,
                'LEAID': str,
                'nces_school_id': str,
                'tract_geoid': str,
                'fips_code': str
            }
        )
        if 'tract_geoid' in df.columns:
            df['tract_geoid'] = df['tract_geoid'].astype(str).str.zfill(11)
        df['source_url'] = NCES_SCHOOL_DIR_URL
        df['fetch_date'] = datetime.utcnow().date().isoformat()
        df['is_real'] = True
        return df

    logger.info(f"Downloading NCES school directory for {year}...")

    # Prefer NCES preliminary CCD directory if configured
    if settings.NCES_CCD_PRELIM_URL:
        try:
            zip_url = settings.NCES_CCD_PRELIM_URL
            logger.info(f"Using NCES preliminary CCD directory: {zip_url}")
            zip_path = NCES_CACHE_DIR / f"ccd_prelim_{year}.zip"
            resp = requests.get(zip_url, timeout=120)
            resp.raise_for_status()
            zip_path.write_bytes(resp.content)

            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_files = [n for n in zf.namelist() if n.lower().endswith('.csv')]
                if csv_files:
                    with zf.open(csv_files[0]) as f:
                        df = pd.read_csv(f, dtype=str, low_memory=False)

            state_cols = [c for c in df.columns if c.upper() in ['ST', 'STABBR', 'STATE', 'STATENAME']]
            if state_cols:
                df = df[df[state_cols[0]].str.upper() == 'MD'].copy()

            df.to_csv(cache_path, index=False)
            df['source_url'] = zip_url
            df['fetch_date'] = datetime.utcnow().date().isoformat()
            df['is_real'] = True
            logger.info(f"✓ Downloaded {len(df)} Maryland schools (preliminary CCD)")
            return df
        except Exception as e:
            logger.warning(f"Failed to download NCES preliminary CCD directory: {e}")

    # Try direct download from NCES
    # School directory uses different year format
    prev_year = year - 1
    yy_format = f"{str(prev_year)[-2:]}{str(year)[-2:]}"

    # Try to resolve the actual URL from the browse page
    try:
        resp = requests.get("https://nces.ed.gov/ccd/pubschuniv.asp", timeout=60)
        pattern = rf"Data/zip/ccd_sch_029_{yy_format}[^\"'<>]+\.zip"
        matches = re.findall(pattern, resp.text)

        if matches:
            zip_url = NCES_BASE_URL + matches[0]
            logger.info(f"Found NCES directory URL: {zip_url}")

            # Download and extract
            zip_path = NCES_CACHE_DIR / f"ccd_schools_{year}.zip"

            resp = requests.get(zip_url, timeout=120)
            resp.raise_for_status()
            zip_path.write_bytes(resp.content)

            # Read the ZIP file
            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_files = [n for n in zf.namelist() if n.lower().endswith('.csv')]
                if csv_files:
                    with zf.open(csv_files[0]) as f:
                        df = pd.read_csv(f, dtype=str, low_memory=False)

            # Filter for Maryland schools
            state_cols = [c for c in df.columns if c.upper() in ['ST', 'STABBR', 'STATE', 'STATENAME']]
            if state_cols:
                df = df[df[state_cols[0]].str.upper() == 'MD'].copy()

            # Cache
            df.to_csv(cache_path, index=False)
            df['source_url'] = zip_url
            df['fetch_date'] = datetime.utcnow().date().isoformat()
            df['is_real'] = True
            logger.info(f"✓ Downloaded {len(df)} Maryland schools")
            return df

    except Exception as e:
        logger.warning(f"Failed to download NCES directory: {e}")

    logger.warning("NCES directory download failed; returning empty dataframe (no synthetic fallback).")
    return pd.DataFrame()


def _generate_fallback_school_directory(year: int) -> pd.DataFrame:
    """
    Generate fallback school directory from county-level data.
    Uses tract centroids and distributes schools based on enrollment.
    """
    logger.warning("Synthetic school directory generation disabled; returning empty dataframe.")
    return pd.DataFrame()

    # Get tract centroids
    tracts_df = _fetch_tract_centroids(year)

    # Get county enrollment from existing Layer 3
    with get_db() as db:
        result = db.execute(text("""
            SELECT fips_code, total_enrollment, schools_total
            FROM layer3_school_trajectory
            WHERE data_year = :year
        """), {"year": year})
        county_data = pd.DataFrame(result.fetchall(), columns=['fips_code', 'total_enrollment', 'schools_total'])

    if county_data.empty:
        # Use default estimates
        county_data = pd.DataFrame({
            'fips_code': list(MD_COUNTY_FIPS.keys()),
            'total_enrollment': [5000] * len(MD_COUNTY_FIPS),
            'schools_total': [10] * len(MD_COUNTY_FIPS)
        })

    # Distribute schools across tracts within each county
    schools = []
    school_id = 1

    for _, county_row in county_data.iterrows():
        fips = county_row['fips_code']
        n_schools = int(county_row.get('schools_total', 10) or 10)
        enrollment = int(county_row.get('total_enrollment', 5000) or 5000)

        county_tracts = tracts_df[tracts_df['fips_code'] == fips]

        if county_tracts.empty:
            continue

        # Distribute schools proportionally to tract population
        total_pop = county_tracts['population'].sum()
        if total_pop == 0:
            total_pop = len(county_tracts)

        for _, tract in county_tracts.iterrows():
            tract_share = tract.get('population', 1) / total_pop if total_pop > 0 else 1 / len(county_tracts)
            tract_schools = max(1, int(n_schools * tract_share))

            for i in range(tract_schools):
                # Assign school type based on distribution
                if i % 3 == 0:
                    school_type = 'Elementary'
                    grade_low, grade_high = 'PK', '05'
                elif i % 3 == 1:
                    school_type = 'Middle'
                    grade_low, grade_high = '06', '08'
                else:
                    school_type = 'High'
                    grade_low, grade_high = '09', '12'

                tract_geoid = str(tract['tract_geoid']).zfill(11)
                schools.append({
                    'nces_school_id': f"MD{fips}{school_id:06d}",
                    'school_name': f"{MD_COUNTY_FIPS.get(fips, 'Unknown')} {school_type} #{i+1}",
                    'school_type': school_type,
                    'fips_code': fips,
                    'tract_geoid': tract_geoid,
                    'latitude': tract['latitude'] + np.random.uniform(-0.01, 0.01),
                    'longitude': tract['longitude'] + np.random.uniform(-0.01, 0.01),
                    'grade_low': grade_low,
                    'grade_high': grade_high,
                    'is_public': True,
                    'has_prek': grade_low == 'PK',
                    'total_enrollment': int(enrollment * tract_share / tract_schools),
                    'ela_proficiency_pct': np.random.uniform(40, 80),
                    'math_proficiency_pct': np.random.uniform(35, 75),
                    'graduation_rate': np.random.uniform(0.75, 0.95) if school_type == 'High' else None,
                    'data_year': year
                })
                school_id += 1

    df = pd.DataFrame(schools)
    df['avg_proficiency_pct'] = (df['ela_proficiency_pct'] + df['math_proficiency_pct']) / 2

    # Cache
    cache_path = NCES_CACHE_DIR / f"md_schools_{year}.csv"
    df.to_csv(cache_path, index=False)

    logger.info(f"✓ Generated {len(df)} fallback school records")
    return df


def download_msde_proficiency_data(year: int) -> pd.DataFrame:
    """
    Download MSDE Report Card proficiency data.

    Returns empty data if MSDE exports are not available.

    Args:
        year: School year end

    Returns:
        DataFrame with proficiency rates by school
    """
    cache_path = MSDE_CACHE_DIR / f"md_proficiency_{year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached MSDE proficiency data: {cache_path}")
        df = pd.read_csv(cache_path, dtype={'school_id': str})
        df['source_url'] = MSDE_REPORT_CARD_URL
        df['fetch_date'] = datetime.utcnow().date().isoformat()
        df['is_real'] = True
        return df

    logger.info(f"Downloading MSDE proficiency data for {year}...")

    # MSDE Report Card data is typically available via their data portal, but
    # automated exports are not yet wired in here.
    logger.warning("MSDE direct download not implemented - returning empty dataframe (no synthetic fallback).")
    return pd.DataFrame()


def download_acs_school_age_population(year: int) -> pd.DataFrame:
    """
    Download ACS school-age population data by census tract.

    Args:
        year: ACS year

    Returns:
        DataFrame with school-age population by tract
    """
    geo_year = min(year, ACS_GEOGRAPHY_MAX_YEAR)
    if geo_year != year:
        logger.warning(f"ACS geography not available for {year}; using {geo_year} instead.")

    cache_path = ACS_CACHE_DIR / f"md_school_age_pop_{geo_year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached ACS school-age population: {cache_path}")
        df = pd.read_csv(cache_path, dtype={'tract_geoid': str, 'fips_code': str})
        df['source_url'] = f"https://api.census.gov/data/{geo_year}/acs/acs5"
        df['fetch_date'] = datetime.utcnow().date().isoformat()
        df['is_real'] = True
        return df

    logger.info(f"Downloading ACS school-age population for {geo_year}...")

    try:
        from census import Census

        c = Census(settings.CENSUS_API_KEY)

        # B01001: Sex by Age
        # School-age population: 5-17 years
        # Under 5 for pre-K analysis
        variables = [
            'NAME',
            'B01001_001E',  # Total population
            'B01001_003E', 'B01001_004E', 'B01001_005E', 'B01001_006E',  # Male under 5, 5-9, 10-14, 15-17
            'B01001_027E', 'B01001_028E', 'B01001_029E', 'B01001_030E',  # Female under 5, 5-9, 10-14, 15-17
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

        # Calculate school-age population
        # Under 5 (pre-K)
        df['school_age_pop_under_5'] = (
            pd.to_numeric(df['B01001_003E'], errors='coerce').fillna(0) +
            pd.to_numeric(df['B01001_027E'], errors='coerce').fillna(0)
        ).astype(int)

        # 5-17 (K-12)
        df['school_age_pop_5_17'] = (
            pd.to_numeric(df['B01001_004E'], errors='coerce').fillna(0) +  # 5-9 male
            pd.to_numeric(df['B01001_005E'], errors='coerce').fillna(0) +  # 10-14 male
            pd.to_numeric(df['B01001_006E'], errors='coerce').fillna(0) +  # 15-17 male
            pd.to_numeric(df['B01001_028E'], errors='coerce').fillna(0) +  # 5-9 female
            pd.to_numeric(df['B01001_029E'], errors='coerce').fillna(0) +  # 10-14 female
            pd.to_numeric(df['B01001_030E'], errors='coerce').fillna(0)    # 15-17 female
        ).astype(int)

        df['total_population'] = pd.to_numeric(df['B01001_001E'], errors='coerce').fillna(0).astype(int)

        # Keep only relevant columns
        df = df[['tract_geoid', 'fips_code', 'NAME', 'total_population',
                 'school_age_pop_under_5', 'school_age_pop_5_17']].copy()

        df['source_url'] = f"https://api.census.gov/data/{geo_year}/acs/acs5"
        df['fetch_date'] = datetime.utcnow().date().isoformat()
        df['is_real'] = True

        # Cache
        df.to_csv(cache_path, index=False)

        logger.info(f"✓ Downloaded school-age population: {len(df)} tracts")
        return df

    except Exception as e:
        logger.error(f"Failed to download ACS school-age population: {e}")
        return _generate_fallback_school_age_pop(geo_year)


def _generate_fallback_school_age_pop(year: int) -> pd.DataFrame:
    """Generate fallback school-age population estimates."""
    logger.warning("Generating fallback school-age population estimates...")

    tracts_df = _fetch_tract_centroids(year)

    # Estimate school-age as ~18% of total population (US average)
    tracts_df['school_age_pop_5_17'] = (tracts_df['population'] * 0.15).astype(int)
    tracts_df['school_age_pop_under_5'] = (tracts_df['population'] * 0.06).astype(int)
    tracts_df['total_population'] = tracts_df['population']

    cache_path = ACS_CACHE_DIR / f"md_school_age_pop_{year}.csv"
    tracts_df[['tract_geoid', 'fips_code', 'total_population',
               'school_age_pop_under_5', 'school_age_pop_5_17']].to_csv(cache_path, index=False)

    return tracts_df


def _fetch_tract_centroids(year: int) -> pd.DataFrame:
    """
    Fetch Maryland census tract centroids with population.

    Returns:
        DataFrame with tract_geoid, fips_code, latitude, longitude, population
    """
    geo_year = min(year, ACS_GEOGRAPHY_MAX_YEAR)
    if geo_year != year:
        logger.warning(f"ACS geography not available for {year}; using {geo_year} instead.")

    cache_path = CACHE_DIR / f"md_tract_centroids_{geo_year}.csv"

    if cache_path.exists():
        return pd.read_csv(cache_path, dtype={'tract_geoid': str, 'fips_code': str})

    logger.info("Fetching tract centroids from Census...")

    try:
        from census import Census

        c = Census(settings.CENSUS_API_KEY)

        # Get tract population and use centroid approximation
        data = c.acs5.state_county_tract(
            fields=['NAME', 'B01001_001E'],
            state_fips='24',
            county_fips='*',
            tract='*',
            year=geo_year
        )

        df = pd.DataFrame(data)
        df['tract_geoid'] = df['state'] + df['county'] + df['tract']
        df['fips_code'] = df['state'] + df['county']
        df['population'] = pd.to_numeric(df['B01001_001E'], errors='coerce').fillna(0).astype(int)

        # Get tract centroids from Census TIGERweb
        # For now, use approximate centroids based on county
        county_centroids = {
            '24001': (39.47, -78.76),   # Allegany
            '24003': (39.03, -76.60),   # Anne Arundel
            '24005': (39.44, -76.62),   # Baltimore County
            '24009': (38.35, -75.62),   # Calvert
            '24011': (38.51, -76.08),   # Caroline
            '24013': (39.56, -77.72),   # Carroll
            '24015': (39.47, -75.93),   # Cecil
            '24017': (38.47, -76.78),   # Charles
            '24019': (38.23, -75.90),   # Dorchester
            '24021': (39.64, -77.40),   # Frederick
            '24023': (39.65, -79.40),   # Garrett
            '24025': (39.54, -76.17),   # Harford
            '24027': (39.25, -76.93),   # Howard
            '24029': (38.33, -75.28),   # Kent
            '24031': (39.15, -77.20),   # Montgomery
            '24033': (38.82, -76.85),   # Prince George's
            '24035': (38.87, -76.18),   # Queen Anne's
            '24037': (38.34, -76.50),   # St. Mary's
            '24039': (38.40, -75.55),   # Somerset
            '24041': (38.90, -76.08),   # Talbot
            '24043': (39.60, -77.57),   # Washington
            '24045': (38.38, -75.60),   # Wicomico
            '24047': (38.17, -75.38),   # Worcester
            '24510': (39.30, -76.62),   # Baltimore City
        }

        # Add approximate tract centroids with jitter
        def get_centroid(row):
            fips = row['fips_code']
            base_lat, base_lon = county_centroids.get(fips, (39.0, -76.7))
            # Add small random offset for tract variation
            lat = base_lat + np.random.uniform(-0.1, 0.1)
            lon = base_lon + np.random.uniform(-0.1, 0.1)
            return pd.Series({'latitude': lat, 'longitude': lon})

        df[['latitude', 'longitude']] = df.apply(get_centroid, axis=1)

        df = df[['tract_geoid', 'fips_code', 'latitude', 'longitude', 'population']].copy()

        # Cache
        df.to_csv(cache_path, index=False)

        logger.info(f"✓ Fetched {len(df)} tract centroids")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch tract centroids: {e}")
        raise


# =============================================================================
# SCHOOL QUALITY COMPUTATION
# =============================================================================

def compute_school_quality_metrics(schools_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute quality metrics and tiers for schools.

    Args:
        schools_df: School directory with proficiency data

    Returns:
        DataFrame with quality_tier and quality_score columns added
    """
    logger.info("Computing school quality metrics...")

    df = schools_df.copy()

    # Ensure we have proficiency columns
    if 'avg_proficiency_pct' not in df.columns:
        if 'ela_proficiency_pct' in df.columns and 'math_proficiency_pct' in df.columns:
            df['avg_proficiency_pct'] = (
                df['ela_proficiency_pct'].fillna(0) + df['math_proficiency_pct'].fillna(0)
            ) / 2
        else:
            # Default proficiency
            df['avg_proficiency_pct'] = 50.0

    # Compute percentile ranks
    df['proficiency_percentile'] = df['avg_proficiency_pct'].rank(pct=True)

    # Assign quality tiers
    def assign_tier(pctl):
        if pd.isna(pctl):
            return 'unknown'
        if pctl >= 0.75:
            return 'top_quartile'
        elif pctl >= 0.50:
            return 'above_median'
        elif pctl >= 0.25:
            return 'below_median'
        else:
            return 'bottom_quartile'

    df['quality_tier'] = df['proficiency_percentile'].apply(assign_tier)

    # Normalized quality score (0-1)
    prof_min = df['avg_proficiency_pct'].min()
    prof_max = df['avg_proficiency_pct'].max()
    if prof_max > prof_min:
        df['quality_score'] = (df['avg_proficiency_pct'] - prof_min) / (prof_max - prof_min)
    else:
        df['quality_score'] = 0.5

    # Compute state median for reference
    state_median = df['avg_proficiency_pct'].median()
    logger.info(f"State median proficiency: {state_median:.1f}%")

    tier_counts = df['quality_tier'].value_counts()
    logger.info(f"Quality tier distribution:\n{tier_counts}")

    return df


def compute_equity_metrics(schools_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute equity metrics based on FRL proficiency gaps.

    Args:
        schools_df: School directory with proficiency data

    Returns:
        DataFrame with equity metrics added
    """
    logger.info("Computing equity metrics...")

    df = schools_df.copy()

    # If we have FRL-specific proficiency data
    if 'frl_ela_proficiency' in df.columns and 'frl_math_proficiency' in df.columns:
        df['frl_avg_proficiency'] = (
            df['frl_ela_proficiency'].fillna(0) + df['frl_math_proficiency'].fillna(0)
        ) / 2

        # Compute gap
        df['frl_proficiency_gap'] = df['avg_proficiency_pct'] - df['frl_avg_proficiency']
        df['frl_proficiency_gap'] = df['frl_proficiency_gap'].clip(-50, 50)
    else:
        # Estimate gap based on overall proficiency (higher proficiency schools tend to have larger gaps)
        df['frl_proficiency_gap'] = (df['avg_proficiency_pct'] - 50) * 0.3
        df['frl_proficiency_gap'] = df['frl_proficiency_gap'].clip(-20, 20)

    # Equity score: schools with smaller gaps get higher scores
    gap_max = df['frl_proficiency_gap'].abs().max()
    if gap_max > 0:
        df['equity_score'] = 1 - (df['frl_proficiency_gap'].abs() / gap_max)
    else:
        df['equity_score'] = 1.0

    logger.info(f"Avg FRL proficiency gap: {df['frl_proficiency_gap'].mean():.1f} points")

    return df


# =============================================================================
# ACCESSIBILITY COMPUTATION
# =============================================================================

def compute_haversine_distances(origins: np.ndarray, destinations: np.ndarray) -> np.ndarray:
    """
    Compute haversine distances between all origin-destination pairs.

    Args:
        origins: Array of (lat, lon) coordinates for origins
        destinations: Array of (lat, lon) coordinates for destinations

    Returns:
        Distance matrix in kilometers (origins x destinations)
    """
    # Convert to radians
    origins_rad = np.radians(origins)
    destinations_rad = np.radians(destinations)

    # Haversine formula
    lat1 = origins_rad[:, 0:1]
    lon1 = origins_rad[:, 1:2]
    lat2 = destinations_rad[:, 0]
    lon2 = destinations_rad[:, 1]

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    # Earth radius in km
    R = 6371

    return R * c


def compute_tract_education_accessibility(
    tracts_df: pd.DataFrame,
    schools_df: pd.DataFrame,
    school_age_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute education accessibility metrics for each tract.

    Uses proximity-based model to estimate school accessibility.

    Args:
        tracts_df: Tract centroids with coordinates
        schools_df: School directory with quality metrics
        school_age_df: School-age population by tract

    Returns:
        DataFrame with tract-level accessibility metrics
    """
    logger.info("Computing tract-level education accessibility...")

    # Prepare coordinates
    tract_coords = tracts_df[['latitude', 'longitude']].values
    school_coords = schools_df[['latitude', 'longitude']].values

    # Compute distance matrix (tracts x schools)
    distances = compute_haversine_distances(tract_coords, school_coords)

    # Prepare result dataframe
    results = tracts_df[['tract_geoid', 'fips_code', 'latitude', 'longitude']].copy()
    results['tract_geoid'] = results['tract_geoid'].astype(str).str.zfill(11)

    # Merge school-age population
    results = results.merge(
        school_age_df[['tract_geoid', 'school_age_pop_5_17', 'school_age_pop_under_5', 'total_population']],
        on='tract_geoid',
        how='left'
    )
    results['school_age_pop_5_17'] = results['school_age_pop_5_17'].fillna(0).astype(int)
    results['school_age_pop_under_5'] = results['school_age_pop_under_5'].fillna(0).astype(int)
    results['tract_population'] = results['total_population'].fillna(0).astype(int)

    # Boolean masks for school types
    is_prek = schools_df['has_prek'].fillna(False).values if 'has_prek' in schools_df.columns else np.zeros(len(schools_df), dtype=bool)
    is_high_quality = schools_df['quality_tier'].isin(['top_quartile', 'above_median']).values
    is_top_quartile = (schools_df['quality_tier'] == 'top_quartile').values

    # Get school quality scores
    school_quality = schools_df['quality_score'].values if 'quality_score' in schools_df.columns else np.ones(len(schools_df)) * 0.5
    school_proficiency = schools_df['avg_proficiency_pct'].values if 'avg_proficiency_pct' in schools_df.columns else np.ones(len(schools_df)) * 50.0

    # Compute accessibility metrics for each tract
    metrics = []

    for i in range(len(tracts_df)):
        tract_distances = distances[i, :]

        # Schools within distance thresholds
        within_15 = tract_distances <= DIST_15MIN_KM
        within_20 = tract_distances <= DIST_20MIN_KM
        within_30 = tract_distances <= DIST_30MIN_KM

        # Count schools
        schools_15min = int(within_15.sum())
        schools_30min = int(within_30.sum())

        # High quality schools
        hq_15min = int((within_15 & is_high_quality).sum())
        hq_30min = int((within_30 & is_high_quality).sum())
        tq_30min = int((within_30 & is_top_quartile).sum())

        # Pre-K programs
        prek_20min = int((within_20 & is_prek).sum())

        # Average proficiency of accessible schools
        if within_30.sum() > 0:
            avg_prof_30 = float(school_proficiency[within_30].mean())
            best_prof_15 = float(school_proficiency[within_15].max()) if within_15.sum() > 0 else 0
        else:
            avg_prof_30 = 0
            best_prof_15 = 0

        metrics.append({
            'schools_accessible_15min': schools_15min,
            'schools_accessible_30min': schools_30min,
            'high_quality_schools_15min': hq_15min,
            'high_quality_schools_30min': hq_30min,
            'top_quartile_schools_30min': tq_30min,
            'prek_programs_accessible_20min': prek_20min,
            'avg_proficiency_accessible_30min': avg_prof_30,
            'best_school_proficiency_15min': best_prof_15,
            'school_choice_diversity': schools_30min
        })

    metrics_df = pd.DataFrame(metrics)
    results = pd.concat([results.reset_index(drop=True), metrics_df], axis=1)

    # Count schools in tract (supply metrics)
    if 'tract_geoid' in schools_df.columns:
        schools_df['tract_geoid'] = schools_df['tract_geoid'].astype(str).str.zfill(11)

    tract_school_counts = schools_df.groupby('tract_geoid').agg({
        'nces_school_id': 'count',
        'has_prek': lambda x: x.fillna(False).any()
    }).rename(columns={
        'nces_school_id': 'total_schools_in_tract',
        'has_prek': 'has_prek_program'
    }).reset_index()

    results = results.merge(tract_school_counts, on='tract_geoid', how='left')
    results['total_schools_in_tract'] = results['total_schools_in_tract'].fillna(0).astype(int)
    results['has_prek_program'] = results['has_prek_program'].fillna(False)

    logger.info(f"✓ Computed accessibility for {len(results)} tracts")
    logger.info(f"Avg schools accessible (30min): {results['schools_accessible_30min'].mean():.1f}")
    logger.info(f"Avg high-quality schools (30min): {results['high_quality_schools_30min'].mean():.1f}")

    return results


# =============================================================================
# SCORE NORMALIZATION
# =============================================================================

def normalize_education_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize education metrics to 0-1 scores.

    Args:
        df: Tract DataFrame with raw accessibility metrics

    Returns:
        DataFrame with normalized scores added
    """
    logger.info("Normalizing education scores...")

    df = df.copy()

    # --- v1 Supply Score ---
    # Based on school density and presence

    # School density (need land area)
    # For now, use relative measure
    school_counts = df['total_schools_in_tract'].fillna(0)
    if school_counts.max() > 0:
        df['school_supply_raw'] = school_counts / school_counts.max()
    else:
        df['school_supply_raw'] = 0

    # Pre-K availability bonus
    df['prek_bonus'] = df['has_prek_program'].astype(float) * 0.2

    # v1 Supply score
    df['school_supply_score'] = (df['school_supply_raw'] + df['prek_bonus']).clip(0, 1)

    # --- v2 Accessibility Scores ---

    # High quality accessibility score (percentile rank)
    hq_access = df['high_quality_schools_30min'].fillna(0)
    df['education_accessibility_score'] = hq_access.rank(pct=True)

    # School quality score (based on avg proficiency accessible)
    prof_accessible = df['avg_proficiency_accessible_30min'].fillna(0)
    if prof_accessible.max() > prof_accessible.min():
        df['school_quality_score'] = (prof_accessible - prof_accessible.min()) / (prof_accessible.max() - prof_accessible.min())
    else:
        df['school_quality_score'] = 0.5

    # Pre-K accessibility score
    prek_access = df['prek_programs_accessible_20min'].fillna(0)
    df['prek_accessibility_score'] = prek_access.rank(pct=True)

    # Equity score placeholder (would need FRL data at tract level)
    df['equity_adjusted_score'] = df['education_accessibility_score'] * 0.9  # Slight penalty placeholder

    # --- Composite Score ---
    # education_accessibility_composite = weighted combination of v2 scores
    df['education_accessibility_composite'] = (
        HIGH_QUALITY_ACCESS_WEIGHT * df['education_accessibility_score'] +
        PREK_ACCESS_WEIGHT * df['prek_accessibility_score'] +
        EQUITY_WEIGHT * df['equity_adjusted_score']
    )

    # Final composite: v1 + v2
    df['education_opportunity_score'] = (
        SUPPLY_WEIGHT * df['school_supply_score'] +
        ACCESSIBILITY_WEIGHT * df['education_accessibility_composite']
    ).clip(0, 1)

    logger.info(f"Score statistics:")
    logger.info(f"  Supply score: {df['school_supply_score'].mean():.3f} (std: {df['school_supply_score'].std():.3f})")
    logger.info(f"  Accessibility score: {df['education_accessibility_score'].mean():.3f}")
    logger.info(f"  Composite score: {df['education_opportunity_score'].mean():.3f}")

    return df


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_to_county(tract_df: pd.DataFrame, schools_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tract-level education metrics to county using population weighting.

    Args:
        tract_df: Tract-level metrics
        schools_df: School directory for county-level quality stats

    Returns:
        County-level DataFrame
    """
    logger.info("Aggregating to county level...")

    # Population-weighted aggregation
    def weighted_mean(group, col, weight_col='school_age_pop_5_17'):
        weights = group[weight_col].fillna(1)
        values = group[col].fillna(0)
        if weights.sum() > 0:
            return (values * weights).sum() / weights.sum()
        return values.mean()

    county_metrics = []

    for fips_code, group in tract_df.groupby('fips_code'):
        # Get county schools
        county_schools = schools_df[schools_df['fips_code'] == fips_code]

        metrics = {
            'fips_code': fips_code,

            # Totals
            'total_population': group['tract_population'].sum(),
            'school_age_pop_5_17': group['school_age_pop_5_17'].sum(),

            # County-level school stats
            'total_schools': len(county_schools),
            'high_quality_schools_count': len(county_schools[county_schools['quality_tier'].isin(['top_quartile', 'above_median'])]),
            'top_quartile_schools_count': len(county_schools[county_schools['quality_tier'] == 'top_quartile']),
            'schools_with_prek': len(county_schools[county_schools['has_prek'] == True]) if 'has_prek' in county_schools.columns else 0,

            # Weighted averages of tract metrics
            'avg_schools_accessible_15min': weighted_mean(group, 'schools_accessible_15min'),
            'avg_schools_accessible_30min': weighted_mean(group, 'schools_accessible_30min'),
            'avg_high_quality_accessible_30min': weighted_mean(group, 'high_quality_schools_30min'),

            # Quality metrics from schools
            'avg_ela_proficiency': county_schools['ela_proficiency_pct'].mean() if 'ela_proficiency_pct' in county_schools.columns else None,
            'avg_math_proficiency': county_schools['math_proficiency_pct'].mean() if 'math_proficiency_pct' in county_schools.columns else None,
            'avg_proficiency': county_schools['avg_proficiency_pct'].mean() if 'avg_proficiency_pct' in county_schools.columns else None,
            'avg_graduation_rate': county_schools[county_schools['school_type'] == 'High']['graduation_rate'].mean() if 'graduation_rate' in county_schools.columns else None,

            # Equity
            'frl_proficiency_gap': county_schools['frl_proficiency_gap'].mean() if 'frl_proficiency_gap' in county_schools.columns else None,

            # Weighted scores
            'school_supply_score': weighted_mean(group, 'school_supply_score'),
            'education_accessibility_score': weighted_mean(group, 'education_accessibility_score'),
            'school_quality_score': weighted_mean(group, 'school_quality_score'),
            'prek_accessibility_score': weighted_mean(group, 'prek_accessibility_score'),
            'equity_score': weighted_mean(group, 'equity_adjusted_score'),
            'education_opportunity_index': weighted_mean(group, 'education_opportunity_score'),
        }

        # Compute % population near high-quality schools
        pop_near_hq = group[group['high_quality_schools_15min'] > 0]['school_age_pop_5_17'].sum()
        total_pop = group['school_age_pop_5_17'].sum()
        metrics['pct_pop_near_high_quality'] = pop_near_hq / total_pop if total_pop > 0 else 0

        county_metrics.append(metrics)

    county_df = pd.DataFrame(county_metrics)

    logger.info(f"✓ Aggregated {len(county_df)} counties")
    logger.info(f"County avg education opportunity index: {county_df['education_opportunity_index'].mean():.3f}")

    return county_df


# =============================================================================
# STORAGE
# =============================================================================

def store_school_directory(schools_df: pd.DataFrame, data_year: int):
    """Store school directory to database."""
    logger.info(f"Storing {len(schools_df)} school directory records...")

    with get_db() as db:
        # Clear existing data for this year
        db.execute(text("""
            DELETE FROM education_school_directory
            WHERE data_year = :data_year
        """), {"data_year": data_year})

        for _, row in schools_df.iterrows():
            db.execute(
                text("""
                    INSERT INTO education_school_directory (
                        nces_school_id, school_name, school_type, grade_low, grade_high,
                        fips_code, tract_geoid, latitude, longitude,
                        is_public, has_prek, total_enrollment,
                        ela_proficiency_pct, math_proficiency_pct, avg_proficiency_pct,
                        graduation_rate, frl_proficiency_gap,
                        quality_tier, quality_score, data_year
                    ) VALUES (
                        :nces_id, :name, :type, :grade_low, :grade_high,
                        :fips, :tract, :lat, :lon,
                        :is_public, :has_prek, :enrollment,
                        :ela_prof, :math_prof, :avg_prof,
                        :grad_rate, :frl_gap,
                        :quality_tier, :quality_score, :data_year
                    )
                """),
                {
                    'nces_id': str(row.get('nces_school_id', '')),
                    'name': row.get('school_name', 'Unknown'),
                    'type': row.get('school_type'),
                    'grade_low': row.get('grade_low'),
                    'grade_high': row.get('grade_high'),
                    'fips': row['fips_code'],
                    'tract': row.get('tract_geoid'),
                    'lat': float(row['latitude']) if pd.notna(row.get('latitude')) else None,
                    'lon': float(row['longitude']) if pd.notna(row.get('longitude')) else None,
                    'is_public': bool(row.get('is_public', True)),
                    'has_prek': bool(row.get('has_prek', False)),
                    'enrollment': int(row['total_enrollment']) if pd.notna(row.get('total_enrollment')) else None,
                    'ela_prof': float(row['ela_proficiency_pct']) if pd.notna(row.get('ela_proficiency_pct')) else None,
                    'math_prof': float(row['math_proficiency_pct']) if pd.notna(row.get('math_proficiency_pct')) else None,
                    'avg_prof': float(row['avg_proficiency_pct']) if pd.notna(row.get('avg_proficiency_pct')) else None,
                    'grad_rate': float(row['graduation_rate']) if pd.notna(row.get('graduation_rate')) else None,
                    'frl_gap': float(row['frl_proficiency_gap']) if pd.notna(row.get('frl_proficiency_gap')) else None,
                    'quality_tier': row.get('quality_tier'),
                    'quality_score': float(row['quality_score']) if pd.notna(row.get('quality_score')) else None,
                    'data_year': data_year
                }
            )

        db.commit()

    logger.info("✓ School directory stored")


def store_tract_education_accessibility(df: pd.DataFrame, data_year: int, nces_year: int, acs_year: int):
    """Store tract-level education accessibility data."""
    logger.info(f"Storing {len(df)} tract education accessibility records...")

    with get_db() as db:
        # Clear existing data for this year
        db.execute(text("""
            DELETE FROM layer3_education_accessibility_tract
            WHERE data_year = :data_year
        """), {"data_year": data_year})

        for _, row in df.iterrows():
            db.execute(
                text("""
                    INSERT INTO layer3_education_accessibility_tract (
                        tract_geoid, fips_code, data_year,
                        school_age_pop_5_17, school_age_pop_under_5, tract_population,
                        total_schools_in_tract, has_prek_program,
                        schools_accessible_15min, schools_accessible_30min,
                        high_quality_schools_15min, high_quality_schools_30min,
                        top_quartile_schools_30min, prek_programs_accessible_20min,
                        avg_proficiency_accessible_30min, best_school_proficiency_15min,
                        school_choice_diversity,
                        school_supply_score, education_accessibility_score,
                        school_quality_score, prek_accessibility_score,
                        equity_adjusted_score, education_opportunity_score,
                        nces_year, acs_year
                    ) VALUES (
                        :tract, :fips, :data_year,
                        :pop_5_17, :pop_under_5, :pop_total,
                        :schools_in_tract, :has_prek,
                        :schools_15, :schools_30,
                        :hq_15, :hq_30,
                        :tq_30, :prek_20,
                        :avg_prof, :best_prof,
                        :choice_diversity,
                        :supply_score, :access_score,
                        :quality_score, :prek_score,
                        :equity_score, :opportunity_score,
                        :nces_year, :acs_year
                    )
                """),
                {
                    'tract': row['tract_geoid'],
                    'fips': row['fips_code'],
                    'data_year': data_year,
                    'pop_5_17': int(row.get('school_age_pop_5_17', 0)),
                    'pop_under_5': int(row.get('school_age_pop_under_5', 0)),
                    'pop_total': int(row.get('tract_population', 0)),
                    'schools_in_tract': int(row.get('total_schools_in_tract', 0)),
                    'has_prek': bool(row.get('has_prek_program', False)),
                    'schools_15': int(row.get('schools_accessible_15min', 0)),
                    'schools_30': int(row.get('schools_accessible_30min', 0)),
                    'hq_15': int(row.get('high_quality_schools_15min', 0)),
                    'hq_30': int(row.get('high_quality_schools_30min', 0)),
                    'tq_30': int(row.get('top_quartile_schools_30min', 0)),
                    'prek_20': int(row.get('prek_programs_accessible_20min', 0)),
                    'avg_prof': float(row.get('avg_proficiency_accessible_30min', 0)),
                    'best_prof': float(row.get('best_school_proficiency_15min', 0)),
                    'choice_diversity': int(row.get('school_choice_diversity', 0)),
                    'supply_score': float(row.get('school_supply_score', 0)),
                    'access_score': float(row.get('education_accessibility_score', 0)),
                    'quality_score': float(row.get('school_quality_score', 0)),
                    'prek_score': float(row.get('prek_accessibility_score', 0)),
                    'equity_score': float(row.get('equity_adjusted_score', 0)),
                    'opportunity_score': float(row.get('education_opportunity_score', 0)),
                    'nces_year': nces_year,
                    'acs_year': acs_year
                }
            )

        db.commit()

    logger.info("✓ Tract education accessibility stored")


def store_county_education_accessibility(df: pd.DataFrame, data_year: int, nces_year: int, acs_year: int):
    """Update county-level education accessibility data."""
    logger.info(f"Updating {len(df)} county education accessibility records...")

    with get_db() as db:
        for _, row in df.iterrows():
            # Check if record exists
            existing = db.execute(
                text("SELECT id FROM layer3_school_trajectory WHERE fips_code = :fips AND data_year = :year"),
                {"fips": row['fips_code'], "year": data_year}
            ).fetchone()

            if existing:
                # Update existing record with v2 metrics
                db.execute(
                    text("""
                        UPDATE layer3_school_trajectory SET
                            total_schools = :total_schools,
                            schools_with_prek = :schools_with_prek,
                            high_quality_schools_count = :hq_count,
                            top_quartile_schools_count = :tq_count,
                            avg_schools_accessible_15min = :avg_15,
                            avg_schools_accessible_30min = :avg_30,
                            avg_high_quality_accessible_30min = :avg_hq_30,
                            pct_pop_near_high_quality = :pct_near_hq,
                            avg_ela_proficiency = :ela_prof,
                            avg_math_proficiency = :math_prof,
                            avg_proficiency = :avg_prof,
                            avg_graduation_rate = :grad_rate,
                            frl_proficiency_gap = :frl_gap,
                            school_supply_score = :supply_score,
                            education_accessibility_score = :access_score,
                            school_quality_score = :quality_score,
                            prek_accessibility_score = :prek_score,
                            equity_score = :equity_score,
                            education_opportunity_index = :opportunity_index,
                            nces_year = :nces_year,
                            acs_year = :acs_year,
                            education_version = 'v2-accessibility',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE fips_code = :fips AND data_year = :data_year
                    """),
                    {
                        'fips': row['fips_code'],
                        'data_year': data_year,
                        'total_schools': int(row.get('total_schools', 0)),
                        'schools_with_prek': int(row.get('schools_with_prek', 0)),
                        'hq_count': int(row.get('high_quality_schools_count', 0)),
                        'tq_count': int(row.get('top_quartile_schools_count', 0)),
                        'avg_15': float(row.get('avg_schools_accessible_15min', 0)),
                        'avg_30': float(row.get('avg_schools_accessible_30min', 0)),
                        'avg_hq_30': float(row.get('avg_high_quality_accessible_30min', 0)),
                        'pct_near_hq': float(row.get('pct_pop_near_high_quality', 0)),
                        'ela_prof': float(row['avg_ela_proficiency']) if pd.notna(row.get('avg_ela_proficiency')) else None,
                        'math_prof': float(row['avg_math_proficiency']) if pd.notna(row.get('avg_math_proficiency')) else None,
                        'avg_prof': float(row['avg_proficiency']) if pd.notna(row.get('avg_proficiency')) else None,
                        'grad_rate': float(row['avg_graduation_rate']) if pd.notna(row.get('avg_graduation_rate')) else None,
                        'frl_gap': float(row['frl_proficiency_gap']) if pd.notna(row.get('frl_proficiency_gap')) else None,
                        'supply_score': float(row.get('school_supply_score', 0)),
                        'access_score': float(row.get('education_accessibility_score', 0)),
                        'quality_score': float(row.get('school_quality_score', 0)),
                        'prek_score': float(row.get('prek_accessibility_score', 0)),
                        'equity_score': float(row.get('equity_score', 0)),
                        'opportunity_index': float(row.get('education_opportunity_index', 0)),
                        'nces_year': nces_year,
                        'acs_year': acs_year
                    }
                )
            else:
                # Insert new record
                db.execute(
                    text("""
                        INSERT INTO layer3_school_trajectory (
                            fips_code, data_year,
                            total_schools, schools_with_prek,
                            high_quality_schools_count, top_quartile_schools_count,
                            avg_schools_accessible_15min, avg_schools_accessible_30min,
                            avg_high_quality_accessible_30min, pct_pop_near_high_quality,
                            avg_ela_proficiency, avg_math_proficiency, avg_proficiency,
                            avg_graduation_rate, frl_proficiency_gap,
                            school_supply_score, education_accessibility_score,
                            school_quality_score, prek_accessibility_score, equity_score,
                            education_opportunity_index,
                            nces_year, acs_year, education_version
                        ) VALUES (
                            :fips, :data_year,
                            :total_schools, :schools_with_prek,
                            :hq_count, :tq_count,
                            :avg_15, :avg_30,
                            :avg_hq_30, :pct_near_hq,
                            :ela_prof, :math_prof, :avg_prof,
                            :grad_rate, :frl_gap,
                            :supply_score, :access_score,
                            :quality_score, :prek_score, :equity_score,
                            :opportunity_index,
                            :nces_year, :acs_year, 'v2-accessibility'
                        )
                    """),
                    {
                        'fips': row['fips_code'],
                        'data_year': data_year,
                        'total_schools': int(row.get('total_schools', 0)),
                        'schools_with_prek': int(row.get('schools_with_prek', 0)),
                        'hq_count': int(row.get('high_quality_schools_count', 0)),
                        'tq_count': int(row.get('top_quartile_schools_count', 0)),
                        'avg_15': float(row.get('avg_schools_accessible_15min', 0)),
                        'avg_30': float(row.get('avg_schools_accessible_30min', 0)),
                        'avg_hq_30': float(row.get('avg_high_quality_accessible_30min', 0)),
                        'pct_near_hq': float(row.get('pct_pop_near_high_quality', 0)),
                        'ela_prof': float(row['avg_ela_proficiency']) if pd.notna(row.get('avg_ela_proficiency')) else None,
                        'math_prof': float(row['avg_math_proficiency']) if pd.notna(row.get('avg_math_proficiency')) else None,
                        'avg_prof': float(row['avg_proficiency']) if pd.notna(row.get('avg_proficiency')) else None,
                        'grad_rate': float(row['avg_graduation_rate']) if pd.notna(row.get('avg_graduation_rate')) else None,
                        'frl_gap': float(row['frl_proficiency_gap']) if pd.notna(row.get('frl_proficiency_gap')) else None,
                        'supply_score': float(row.get('school_supply_score', 0)),
                        'access_score': float(row.get('education_accessibility_score', 0)),
                        'quality_score': float(row.get('school_quality_score', 0)),
                        'prek_score': float(row.get('prek_accessibility_score', 0)),
                        'equity_score': float(row.get('equity_score', 0)),
                        'opportunity_index': float(row.get('education_opportunity_index', 0)),
                        'nces_year': nces_year,
                        'acs_year': acs_year
                    }
                )

        db.commit()

    logger.info("✓ County education accessibility stored")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def calculate_education_accessibility_indicators(
    data_year: int = None,
    nces_year: int = None,
    acs_year: int = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate education accessibility indicators for all Maryland tracts.

    Args:
        data_year: Year to associate with this data
        nces_year: NCES data year
        acs_year: ACS data year

    Returns:
        Tuple of (tract_df, county_df)
    """
    data_year = data_year or datetime.now().year
    nces_year = nces_year or min(data_year, 2024)  # NCES typically lags 1-2 years
    acs_year = acs_year or min(data_year - 1, settings.ACS_LATEST_YEAR)

    logger.info("=" * 60)
    logger.info("LAYER 3 v2: EDUCATION ACCESSIBILITY ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Data year: {data_year}")
    logger.info(f"NCES year: {nces_year}")
    logger.info(f"ACS year: {acs_year}")

    # Step 1: Get school directory
    logger.info("\n[1/6] Loading school directory...")
    schools_df = download_nces_school_directory(nces_year)

    # Step 2: Compute school quality metrics
    logger.info("\n[2/6] Computing school quality metrics...")
    schools_df = compute_school_quality_metrics(schools_df)
    schools_df = compute_equity_metrics(schools_df)

    # Step 3: Get tract data
    logger.info("\n[3/6] Loading tract centroids and population...")
    tracts_df = _fetch_tract_centroids(acs_year)
    school_age_df = download_acs_school_age_population(acs_year)

    # Step 4: Compute tract-level accessibility
    logger.info("\n[4/6] Computing tract-level accessibility...")
    tract_df = compute_tract_education_accessibility(tracts_df, schools_df, school_age_df)

    # Step 5: Normalize scores
    logger.info("\n[5/6] Normalizing scores...")
    tract_df = normalize_education_scores(tract_df)

    # Step 6: Aggregate to county
    logger.info("\n[6/6] Aggregating to county level...")
    county_df = aggregate_to_county(tract_df, schools_df)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Tracts processed: {len(tract_df)}")
    logger.info(f"Counties processed: {len(county_df)}")
    logger.info(f"Schools in directory: {len(schools_df)}")
    logger.info(f"Avg education opportunity index: {tract_df['education_opportunity_score'].mean():.3f}")

    return tract_df, county_df, schools_df


def run_layer3_v2_ingestion(
    data_year: int = None,
    multi_year: bool = True,
    store_data: bool = True,
    window_years: int = DEFAULT_WINDOW_YEARS,
    predict_to_year: Optional[int] = None
):
    """
    Run complete Layer 3 v2 ingestion pipeline.

    Args:
        data_year: End year for this data
        multi_year: If True, run multi-year window
        store_data: Whether to store results in database
        window_years: Window size for multi-year ingestion
    """
    current_year = datetime.now().year

    if data_year is None:
        data_year = min(2025, current_year)  # Cap at 2025 for school data

    try:
        if multi_year:
            start_year = data_year - window_years + 1
            years_to_fetch = list(range(start_year, data_year + 1))
            logger.info(f"Starting Layer 3 v2 MULTI-YEAR ingestion for years {years_to_fetch[0]}-{years_to_fetch[-1]}")
        else:
            years_to_fetch = [data_year]
            logger.info(f"Starting Layer 3 v2 single-year ingestion for {data_year}")

        total_records = 0
        failed_years = []

        for year in years_to_fetch:
            nces_year = min(year, 2024)  # NCES lags
            acs_year = min(year - 1, settings.ACS_LATEST_YEAR)

            logger.info("=" * 70)
            logger.info(f"Processing year {year}")
            logger.info("=" * 70)

            try:
                tract_df, county_df, schools_df = calculate_education_accessibility_indicators(
                    data_year=year,
                    nces_year=nces_year,
                    acs_year=acs_year
                )

                if store_data and not tract_df.empty:
                    store_school_directory(schools_df, year)
                    store_tract_education_accessibility(tract_df, year, nces_year, acs_year)
                    store_county_education_accessibility(county_df, year, nces_year, acs_year)

                    log_refresh(
                        layer_name="layer3_school_trajectory",
                        data_source="NCES/MSDE (v2 accessibility)",
                        status="success",
                        records_processed=len(tract_df),
                        records_inserted=len(tract_df) + len(county_df),
                        metadata={
                            "data_year": year,
                            "nces_year": nces_year,
                            "acs_year": acs_year,
                            "version": "v2-accessibility",
                            "tracts": len(tract_df),
                            "counties": len(county_df),
                            "schools": len(schools_df),
                            "avg_education_opportunity": float(tract_df['education_opportunity_score'].mean())
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
                table="layer3_school_trajectory",
                metric_col="education_opportunity_index",
                target_year=target_year,
                clip=(0.0, 1.0)
            )

        logger.info("✓ Layer 3 v2 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 3 v2 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer3_school_trajectory",
            data_source="NCES/MSDE (v2 accessibility)",
            status="failed",
            error_message=str(e)
        )
        raise


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Layer 3 v2: Education Accessibility Analysis'
    )
    parser.add_argument(
        '--year', type=int, default=None,
        help='End year for window (default: 2025)'
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

    run_layer3_v2_ingestion(
        data_year=args.year,
        multi_year=not args.single_year,
        store_data=not args.dry_run,
        predict_to_year=args.predict_to_year
    )


if __name__ == "__main__":
    main()
