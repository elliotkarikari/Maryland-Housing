"""
Maryland Viability Atlas - Data Source Utilities
Helper functions for accessing open data APIs with rate limiting
"""

import requests
import time
import random
import logging
import pandas as pd
from typing import Optional, Dict, Any, Tuple, List
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
    year: Optional[int] = None,
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
    area_codes: Optional[list[str]] = None
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


def fetch_lodes_wac(state: str = "md", year: Optional[int] = None, job_type: str = "JT00") -> pd.DataFrame:
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


class FEMAAPIError(RuntimeError):
    """Raised when FEMA NFHL API fails after retries."""


def fetch_fema_nfhl(
    state_fips: str = "MD",
    geometry: Optional[Tuple[float, float, float, float]] = None
):
    """
    Fetch FEMA National Flood Hazard Layer (NFHL) flood hazard polygons.

    Endpoint (2026): https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query
    Deprecated: https://hazards.fema.gov/gis/nfhl/... (no longer valid)

    Args:
        state_fips: State abbreviation (e.g., "MD") or FIPS (unused if geometry is provided).
        geometry: (minx, miny, maxx, maxy) envelope in WGS84 (EPSG:4326).

    Returns:
        GeoDataFrame with NFHL features (can be empty).

    Example:
        md_bbox = (-79.487651, 37.911717, -75.048939, 39.723043)
        gdf = fetch_fema_nfhl("MD", geometry=md_bbox)

    Note:
        If the API fails persistently, download the NFHL geodatabase from FEMA MSC
        and ingest locally: https://msc.fema.gov/portal/search
    """
    import geopandas as gpd

    local_logger = logging.getLogger(__name__)
    session = requests.Session()
    base_url = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"

    base_params = {
        "outFields": "OBJECTID,SFHA_TF,FLD_ZONE,ZONE_SUBTY",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "geojson",
        "resultRecordCount": 500,
        "where": "1=1"
    }

    def _request_with_retries(params: Dict[str, Any]) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(1, 11):
            try:
                local_logger.info(f"FEMA NFHL request attempt {attempt} offset={params.get('resultOffset')}")
                response = session.get(base_url, params=params, timeout=60)
                local_logger.info(f"FEMA NFHL response status: {response.status_code}")
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    raise RuntimeError(data["error"])
                return data
            except (requests.exceptions.HTTPError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ReadTimeout) as e:
                last_error = e
                code = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
                if code not in (500, 429, None) and attempt >= 2:
                    break
                sleep_for = min(120, (4 ** attempt)) + random.uniform(0, 10)
                local_logger.warning(f"FEMA NFHL error on attempt {attempt}: {e}. Retrying in {sleep_for:.1f}s")
                time.sleep(sleep_for)
            except Exception as e:
                last_error = e
                sleep_for = min(120, (4 ** attempt)) + random.uniform(0, 10)
                local_logger.warning(f"FEMA NFHL parse error on attempt {attempt}: {e}. Retrying in {sleep_for:.1f}s")
                time.sleep(sleep_for)

        raise FEMAAPIError(f"FEMA NFHL API failed after retries: {last_error}")

    def _fetch_bbox_features(bbox: Tuple[float, float, float, float]) -> List[Dict[str, Any]]:
        minx, miny, maxx, maxy = bbox
        params = dict(base_params)
        params.update({
            "geometry": f"{minx},{miny},{maxx},{maxy}",
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "inSR": 4326
        })

        all_features: List[Dict[str, Any]] = []
        offset = 0
        while True:
            params["resultOffset"] = offset
            data = _request_with_retries(params)
            features = data.get("features", [])
            if not features:
                break
            all_features.extend(features)
            local_logger.info(f"Fetched {len(features)} features (offset {offset})")
            if len(features) < base_params["resultRecordCount"]:
                break
            offset += base_params["resultRecordCount"]
        return all_features

    def _split_bbox(bbox: Tuple[float, float, float, float], grid_size: float = 0.5) -> List[Tuple[float, float, float, float]]:
        minx, miny, maxx, maxy = bbox
        tiles = []
        x = minx
        while x < maxx:
            y = miny
            x2 = min(x + grid_size, maxx)
            while y < maxy:
                y2 = min(y + grid_size, maxy)
                tiles.append((x, y, x2, y2))
                y = y2
            x = x2
        return tiles

    def _precheck_service() -> None:
        if geometry is None:
            return
        minx, miny, maxx, maxy = geometry
        cx = (minx + maxx) / 2
        cy = (miny + maxy) / 2
        test_bbox = (cx - 0.01, cy - 0.01, cx + 0.01, cy + 0.01)
        params = dict(base_params)
        params.update({
            "geometry": f"{test_bbox[0]},{test_bbox[1]},{test_bbox[2]},{test_bbox[3]}",
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "inSR": 4326,
            "resultRecordCount": 1,
            "resultOffset": 0
        })
        _request_with_retries(params)

    def _fetch_wfs(bbox: Tuple[float, float, float, float]) -> List[Dict[str, Any]]:
        minx, miny, maxx, maxy = bbox
        wfs_url = "https://hazards.fema.gov/arcgis/services/public/NFHL/MapServer/WFSServer"
        params = {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "1.1.0",
            "TYPENAME": "public_NFHL:S_Fld_Haz_Ar",
            "OUTPUTFORMAT": "application/json",
            "BBOX": f"{minx},{miny},{maxx},{maxy},urn:ogc:def:crs:EPSG::4326",
            "MAXFEATURES": 1000
        }
        try:
            resp = session.get(wfs_url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            return data.get("features", [])
        except Exception as e:
            local_logger.warning(f"WFS fallback failed: {e}")
            return []

    if geometry is None:
        local_logger.warning("No geometry provided; returning empty GeoDataFrame")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    # Pre-check service responsiveness
    try:
        _precheck_service()
    except FEMAAPIError as e:
        local_logger.error(f"FEMA NFHL service precheck failed: {e}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    # Tiling for large bboxes
    minx, miny, maxx, maxy = geometry
    width = maxx - minx
    height = maxy - miny
    tiles = [geometry]
    if width > 0.5 or height > 0.5:
        tiles = _split_bbox(geometry, grid_size=0.5)

    all_features: List[Dict[str, Any]] = []
    for bbox in tiles:
        try:
            all_features.extend(_fetch_bbox_features(bbox))
        except FEMAAPIError as e:
            local_logger.warning(f"REST tile failed {bbox}: {e}")
            all_features.extend(_fetch_wfs(bbox))

    if not all_features:
        local_logger.warning("No FEMA NFHL features returned")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    local_logger.info(f"Total FEMA NFHL features fetched: {len(all_features)}")
    return gpd.GeoDataFrame.from_features(all_features, crs="EPSG:4326")


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
        df_md = df[df['ID'].str.startswith('24')].copy().reset_index(drop=True)

        logger.info(f"Fetched {len(df_md)} Maryland EJScreen records")
        return df_md.to_frame() if isinstance(df_md, pd.Series) else df_md

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
