"""
Maryland Viability Atlas - Data Source Utilities
Helper functions for accessing open data APIs with rate limiting
"""

import requests
import time
import pandas as pd
from typing import Optional, Dict, Any
from functools import wraps
from config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class RateLimiter:
    """Simple rate limiter for API requests"""

    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call = 0

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()
            return func(*args, **kwargs)
        return wrapper


# Rate limiters for different APIs
census_limiter = RateLimiter(settings.CENSUS_API_RATE_LIMIT)
bls_limiter = RateLimiter(settings.BLS_API_RATE_LIMIT)
usaspending_limiter = RateLimiter(settings.USASPENDING_RATE_LIMIT)


@census_limiter
def fetch_census_data(
    dataset: str,
    variables: list[str],
    geography: str,
    state: str = "24",
    year: int = None,
    **kwargs
) -> pd.DataFrame:
    """
    Fetch data from Census API with rate limiting.

    Args:
        dataset: Dataset name (e.g., 'acs/acs5')
        variables: List of variable codes (e.g., ['B01001_001E'])
        geography: Geography level (e.g., 'county:*')
        state: State FIPS code (default: '24' for Maryland)
        year: Data year (default: latest from settings)
        **kwargs: Additional parameters for API

    Returns:
        DataFrame with census data
    """
    year = year or settings.ACS_LATEST_YEAR

    url = f"{settings.CENSUS_API_BASE_URL}/{year}/{dataset}"

    params = {
        "get": ",".join(["NAME"] + variables),
        "for": geography,
        "in": f"state:{state}",
        "key": settings.CENSUS_API_KEY
    }
    params.update(kwargs)

    logger.info(f"Fetching Census data: {dataset} ({year}), variables: {len(variables)}")

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        if not data or len(data) < 2:
            logger.warning(f"No data returned from Census API: {url}")
            return pd.DataFrame()

        # First row is headers
        df = pd.DataFrame(data[1:], columns=data[0])

        logger.info(f"Fetched {len(df)} records from Census API")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Census API request failed: {e}")
        raise


@bls_limiter
def fetch_bls_qcew(
    year: int,
    quarter: int,
    area_codes: list[str] = None
) -> pd.DataFrame:
    """
    Fetch BLS QCEW data for Maryland counties.

    Args:
        year: Data year (e.g., 2025)
        quarter: Quarter (1-4)
        area_codes: List of area codes (Maryland county codes)
                   If None, fetches all Maryland counties

    Returns:
        DataFrame with QCEW data
    """
    if area_codes is None:
        # All Maryland counties
        from config.settings import MD_COUNTY_FIPS
        area_codes = [f"{fips[2:]}" for fips in MD_COUNTY_FIPS.keys() if fips != "24510"]

    all_data = []

    for area_code in area_codes:
        url = f"https://data.bls.gov/cew/data/api/{year}/{quarter}/area/24{area_code}.csv"

        logger.info(f"Fetching BLS QCEW: {year} Q{quarter}, area 24{area_code}")

        try:
            df = pd.read_csv(url)
            all_data.append(df)
            time.sleep(0.5)  # Be respectful even with rate limiter

        except Exception as e:
            logger.warning(f"Failed to fetch QCEW for area 24{area_code}: {e}")
            continue

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        logger.info(f"Fetched {len(combined)} QCEW records")
        return combined
    else:
        logger.warning("No QCEW data fetched")
        return pd.DataFrame()


@usaspending_limiter
def fetch_usaspending_county(
    start_date: str,
    end_date: str,
    state_code: str = "MD"
) -> pd.DataFrame:
    """
    Fetch USASpending.gov data for Maryland counties.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        state_code: State abbreviation (default: MD)

    Returns:
        DataFrame with federal spending by county
    """
    url = f"{settings.USASPENDING_API_URL}/search/spending_by_geography/"

    payload = {
        "scope": "place_of_performance",
        "geo_layer": "county",
        "filters": {
            "time_period": [{"start_date": start_date, "end_date": end_date}],
            "place_of_performance_locations": [{"country": "USA", "state": state_code}]
        }
    }

    logger.info(f"Fetching USASpending data: {start_date} to {end_date}")

    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()

        data = response.json()

        if "results" not in data:
            logger.warning("No results in USASpending API response")
            return pd.DataFrame()

        df = pd.DataFrame(data["results"])
        logger.info(f"Fetched {len(df)} USASpending records")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"USASpending API request failed: {e}")
        raise


def fetch_lodes_wac(state: str = "md", year: int = None, job_type: str = "JT00") -> pd.DataFrame:
    """
    Fetch LEHD/LODES Workplace Area Characteristics data.

    Args:
        state: State abbreviation (lowercase, default: 'md')
        year: Data year (default: latest from settings)
        job_type: Job type (JT00=All Jobs, JT01=Primary Jobs, etc.)

    Returns:
        DataFrame with LODES WAC data
    """
    year = year or settings.LODES_LATEST_YEAR

    # LODES WAC file structure: st_wac_S000_JT00_YYYY.csv.gz
    url = f"{settings.LEHD_BASE_URL}/{state}/wac/{state}_wac_S000_{job_type}_{year}.csv.gz"

    logger.info(f"Fetching LODES WAC data: {state.upper()} {year}")

    try:
        df = pd.read_csv(url, compression='gzip', dtype={'w_geocode': str})
        logger.info(f"Fetched {len(df)} LODES records")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch LODES data: {e}")
        raise


def fetch_fema_nfhl(state_code: str = "MD") -> Dict[str, Any]:
    """
    Fetch FEMA National Flood Hazard Layer data.

    Args:
        state_code: State abbreviation (default: MD)

    Returns:
        GeoJSON FeatureCollection of flood zones
    """
    # FEMA MapServer query endpoint
    url = f"{settings.FEMA_NFHL_URL}/28/query"  # Layer 28 is SFHA

    params = {
        "where": f"STATE_CODE='{state_code}'",
        "outFields": "*",
        "f": "geojson"
    }

    logger.info(f"Fetching FEMA NFHL data for {state_code}")

    try:
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()

        data = response.json()

        if "features" in data:
            logger.info(f"Fetched {len(data['features'])} FEMA flood zones")
        else:
            logger.warning("No features in FEMA NFHL response")

        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"FEMA NFHL request failed: {e}")
        raise


def fetch_irs_migration(year_range: str = "2122") -> pd.DataFrame:
    """
    Fetch IRS county-to-county migration data.

    Args:
        year_range: Year range code (e.g., '2122' for 2021-2022)

    Returns:
        DataFrame with migration flows
    """
    url = f"https://www.irs.gov/pub/irs-soi/countymigration{year_range}.csv"

    logger.info(f"Fetching IRS migration data: {year_range}")

    try:
        df = pd.read_csv(url, dtype={'y1_countyfips': str, 'y2_countyfips': str})
        logger.info(f"Fetched {len(df)} IRS migration records")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch IRS migration data: {e}")
        raise


def fetch_epa_ejscreen(year: int = 2023) -> pd.DataFrame:
    """
    Fetch EPA EJScreen data.

    Args:
        year: Data year (default: 2023)

    Returns:
        DataFrame with environmental justice indicators
    """
    # Download and extract the national CSV file
    url = f"{settings.EPA_EJSCREEN_URL}/{year}/EJSCREEN_{year}_StatePct_with_AS_CNMI_GU_VI.csv.zip"

    logger.info(f"Fetching EPA EJScreen data for {year}")

    try:
        import io
        import zipfile

        response = requests.get(url, timeout=120)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Extract the CSV file
            csv_name = [f for f in zf.namelist() if f.endswith('.csv')][0]
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, dtype={'ID': str})

        # Filter to Maryland (state FIPS 24)
        df_md = df[df['ID'].str.startswith('24')].copy()

        logger.info(f"Fetched {len(df_md)} Maryland EJScreen records")
        return df_md

    except Exception as e:
        logger.error(f"Failed to fetch EPA EJScreen data: {e}")
        raise


def download_file(url: str, save_path: str, timeout: int = 300) -> bool:
    """
    Download a file from URL with progress logging.

    Args:
        url: URL to download from
        save_path: Local path to save file
        timeout: Request timeout in seconds

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Downloading: {url}")

    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))

        with open(save_path, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
            else:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    downloaded += len(chunk)
                    f.write(chunk)
                    if downloaded % (1024 * 1024) == 0:  # Log every MB
                        logger.debug(f"Downloaded {downloaded / (1024*1024):.1f} MB")

        logger.info(f"Download complete: {save_path}")
        return True

    except Exception as e:
        logger.error(f"Download failed: {e}")
        return False
