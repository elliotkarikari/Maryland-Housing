"""
Maryland Viability Atlas - Layer 2: Mobility Accessibility (v2)
Modern accessibility-based mobility analysis using R5/r5py routing engine.

This module computes travel time-based accessibility metrics that measure
actual ability to reach jobs and opportunities, replacing simple infrastructure
counts with policy-relevant reachability analysis.

Data Sources:
- GTFS: MTA Maryland, WMATA, local transit agencies
- OSM: Maryland statewide extract (.osm.pbf)
- LODES: Workplace Area Characteristics (jobs by tract)
- Census: Tract boundaries and population

Core Metrics:
- Jobs accessible by transit (30min, 45min)
- Jobs accessible by walking (30min)
- Jobs accessible by biking (30min)
- Jobs accessible by car (30min, baseline comparison)
- Transit competitiveness ratio (transit jobs / car jobs)

Author: Maryland Viability Atlas Team
Date: 2026-01-29
Version: 2.0
"""

import os
import sys
import hashlib
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
import warnings

import pandas as pd
import geopandas as gpd
import numpy as np
from sqlalchemy import text
import requests

# Note: gtfs_kit imports are deferred to avoid r5py import hook triggering
# when Java is not available. Import only in functions that need it.

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.data_sources import download_file
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# =============================================================================
# CONFIGURATION
# =============================================================================

CACHE_DIR = Path("data/cache/mobility_v2")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OSM_CACHE_DIR = CACHE_DIR / "osm"
OSM_CACHE_DIR.mkdir(exist_ok=True)

GTFS_CACHE_DIR = CACHE_DIR / "gtfs"
GTFS_CACHE_DIR.mkdir(exist_ok=True)

LODES_CACHE_DIR = CACHE_DIR / "lodes"
LODES_CACHE_DIR.mkdir(exist_ok=True)

R5_CACHE_DIR = CACHE_DIR / "r5_networks"
R5_CACHE_DIR.mkdir(exist_ok=True)

# Maryland bounding box (approximate)
MD_BBOX = {
    'west': -79.5,
    'south': 37.9,
    'east': -75.0,
    'north': 39.75
}

# GTFS feeds for Maryland region
GTFS_FEEDS = {
    # MTA Maryland feeds
    'mta_local_bus': {
        'url': 'https://feeds.mta.maryland.gov/gtfs/local-bus',
        'agency': 'MTA Maryland',
        'priority': 1
    },
    'mta_light_rail': {
        'url': 'https://feeds.mta.maryland.gov/gtfs/light-rail',
        'agency': 'MTA Maryland',
        'priority': 1
    },
    'mta_metro': {
        'url': 'https://feeds.mta.maryland.gov/gtfs/metro',
        'agency': 'MTA Maryland',
        'priority': 1
    },
    'mta_marc': {
        'url': 'https://feeds.mta.maryland.gov/gtfs/marc',
        'agency': 'MTA Maryland',
        'priority': 1
    },
    'mta_commuter_bus': {
        'url': 'https://feeds.mta.maryland.gov/gtfs/commuter-bus',
        'agency': 'MTA Maryland',
        'priority': 1
    },
    # WMATA (for DC-adjacent counties) - Note: requires API key or use Transitland
    # Disabled by default due to API key requirement
    # 'wmata': {
    #     'url': 'https://api.wmata.com/gtfs/bus-rail-gtfs.zip',
    #     'agency': 'WMATA',
    #     'priority': 2
    # },
    # Regional transit (optional)
    'ride_on': {
        'url': 'https://www.montgomerycountymd.gov/DOT-Transit/Resources/Files/gtfs.zip',
        'agency': 'Montgomery County Ride On',
        'priority': 3
    },
}

# Time thresholds for accessibility computation (minutes)
TIME_THRESHOLDS = {
    'transit_45': 45,
    'transit_30': 30,
    'walk_30': 30,
    'bike_30': 30,
    'car_30': 30
}

# Accessibility score weights (must sum to 1.0)
ACCESSIBILITY_WEIGHTS = {
    'transit_45': 0.60,  # Primary: transit within 45 min
    'walk_30': 0.25,     # Secondary: walkable jobs
    'bike_30': 0.15      # Tertiary: bikeable jobs
}

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class AccessibilityResult:
    """Results from accessibility computation for a single origin."""
    tract_geoid: str
    fips_code: str
    jobs_transit_45: int
    jobs_transit_30: int
    jobs_walk_30: int
    jobs_bike_30: int
    jobs_car_30: int
    tract_population: int = 0


@dataclass
class GTFSFeedInfo:
    """Metadata about a downloaded GTFS feed."""
    name: str
    path: Path
    agency: str
    feed_date: date
    file_hash: str


# =============================================================================
# DATA ACQUISITION
# =============================================================================

def download_maryland_osm() -> Path:
    """
    Download Maryland OSM extract (.osm.pbf) from Geofabrik.

    Returns:
        Path to downloaded .osm.pbf file
    """
    osm_path = OSM_CACHE_DIR / "maryland-latest.osm.pbf"

    # Check if recent (< 30 days old)
    if osm_path.exists():
        mtime = datetime.fromtimestamp(osm_path.stat().st_mtime)
        if datetime.now() - mtime < timedelta(days=30):
            logger.info(f"Using cached OSM extract: {osm_path}")
            return osm_path

    # Download from Geofabrik
    url = "https://download.geofabrik.de/north-america/us/maryland-latest.osm.pbf"
    logger.info(f"Downloading Maryland OSM extract from Geofabrik...")

    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        with open(osm_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"✓ Downloaded OSM extract: {osm_path.stat().st_size / 1e6:.1f} MB")
        return osm_path

    except Exception as e:
        logger.error(f"Failed to download OSM extract: {e}")
        raise


def download_gtfs_feeds(feed_names: Optional[List[str]] = None) -> List[GTFSFeedInfo]:
    """
    Download GTFS feeds for Maryland transit agencies.

    Args:
        feed_names: Specific feeds to download (default: all priority 1 feeds)

    Returns:
        List of GTFSFeedInfo objects
    """
    feeds_to_download = feed_names or [
        name for name, info in GTFS_FEEDS.items()
        if info.get('priority', 99) <= 2
    ]

    downloaded = []

    for feed_name in feeds_to_download:
        if feed_name not in GTFS_FEEDS:
            logger.warning(f"Unknown GTFS feed: {feed_name}")
            continue

        feed_info = GTFS_FEEDS[feed_name]
        feed_path = GTFS_CACHE_DIR / f"{feed_name}.zip"

        # Check if recent (< 7 days old)
        needs_download = True
        if feed_path.exists():
            mtime = datetime.fromtimestamp(feed_path.stat().st_mtime)
            if datetime.now() - mtime < timedelta(days=7):
                needs_download = False

        if needs_download:
            logger.info(f"Downloading GTFS feed: {feed_name}")
            try:
                response = requests.get(feed_info['url'], timeout=120)
                response.raise_for_status()

                with open(feed_path, 'wb') as f:
                    f.write(response.content)

                logger.info(f"✓ Downloaded {feed_name}")

            except Exception as e:
                logger.warning(f"Failed to download {feed_name}: {e}")
                continue

        # Compute file hash and extract feed date
        file_hash = hashlib.md5(open(feed_path, 'rb').read()).hexdigest()

        # Try to extract feed date from feed_info.txt
        feed_date = _extract_gtfs_date(feed_path)

        downloaded.append(GTFSFeedInfo(
            name=feed_name,
            path=feed_path,
            agency=feed_info['agency'],
            feed_date=feed_date,
            file_hash=file_hash
        ))

    return downloaded


def _extract_gtfs_date(gtfs_path: Path) -> date:
    """Extract feed start date from GTFS feed_info.txt or calendar.txt."""
    try:
        with zipfile.ZipFile(gtfs_path, 'r') as zf:
            # Try feed_info.txt first
            if 'feed_info.txt' in zf.namelist():
                with zf.open('feed_info.txt') as f:
                    df = pd.read_csv(f, dtype=str)
                    if 'feed_start_date' in df.columns:
                        date_str = df['feed_start_date'].iloc[0]
                        return datetime.strptime(date_str, '%Y%m%d').date()

            # Fallback to calendar.txt
            if 'calendar.txt' in zf.namelist():
                with zf.open('calendar.txt') as f:
                    df = pd.read_csv(f, dtype=str)
                    if 'start_date' in df.columns:
                        date_str = df['start_date'].min()
                        return datetime.strptime(date_str, '%Y%m%d').date()

    except Exception as e:
        logger.warning(f"Could not extract GTFS date: {e}")

    return date.today()


def download_lodes_wac(year: int = 2021) -> pd.DataFrame:
    """
    Download LODES Workplace Area Characteristics (WAC) for Maryland.

    This provides job counts by census tract which serve as
    destinations for accessibility analysis.

    Args:
        year: LODES year (latest available is typically 2 years behind)

    Returns:
        DataFrame with tract_geoid and total_jobs columns
    """
    # Try parquet first, fall back to CSV
    cache_path_parquet = LODES_CACHE_DIR / f"md_wac_{year}.parquet"
    cache_path_csv = LODES_CACHE_DIR / f"md_wac_{year}.csv"

    # Check for existing cache
    if cache_path_csv.exists():
        logger.info(f"Using cached LODES WAC: {cache_path_csv}")
        return pd.read_csv(cache_path_csv, dtype={'tract_geoid': str})

    if cache_path_parquet.exists():
        try:
            logger.info(f"Using cached LODES WAC: {cache_path_parquet}")
            return pd.read_parquet(cache_path_parquet)
        except ImportError:
            pass  # Fall through to download

    # LODES file URL
    url = f"https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_S000_JT00_{year}.csv.gz"

    logger.info(f"Downloading LODES WAC for {year}...")

    try:
        df = pd.read_csv(url, compression='gzip', dtype={'w_geocode': str})

        # Extract tract GEOID (first 11 digits of block GEOID)
        df['tract_geoid'] = df['w_geocode'].str[:11]

        # Aggregate jobs to tract level
        tract_jobs = df.groupby('tract_geoid').agg(
            total_jobs=('C000', 'sum'),  # Total jobs
            jobs_retail=('CNS07', 'sum'),  # Retail trade
            jobs_healthcare=('CNS12', 'sum'),  # Healthcare
            jobs_education=('CNS15', 'sum'),  # Education
            jobs_construction=('CNS04', 'sum'),  # Construction
        ).reset_index()

        # Filter to Maryland tracts (FIPS 24)
        tract_jobs = tract_jobs[tract_jobs['tract_geoid'].str.startswith('24')]

        # Cache result (try parquet, fall back to CSV)
        try:
            tract_jobs.to_parquet(cache_path_parquet, index=False)
        except ImportError:
            tract_jobs.to_csv(cache_path_csv, index=False)
            logger.info("Cached as CSV (parquet not available)")

        logger.info(f"✓ Loaded LODES WAC: {len(tract_jobs)} tracts, {tract_jobs['total_jobs'].sum():,} total jobs")
        return tract_jobs

    except Exception as e:
        logger.error(f"Failed to download LODES WAC: {e}")
        raise


def fetch_maryland_tracts(year: int = 2020) -> gpd.GeoDataFrame:
    """
    Fetch Maryland census tract boundaries with population.

    Args:
        year: Census year for tract boundaries

    Returns:
        GeoDataFrame with tract geometries and population
    """
    cache_path_parquet = CACHE_DIR / f"md_tracts_{year}.parquet"
    cache_path_geojson = CACHE_DIR / f"md_tracts_{year}.geojson"

    # Check for existing cache
    if cache_path_geojson.exists():
        logger.info(f"Using cached tract boundaries")
        return gpd.read_file(cache_path_geojson)

    if cache_path_parquet.exists():
        try:
            logger.info(f"Using cached tract boundaries")
            return gpd.read_parquet(cache_path_parquet)
        except (ImportError, Exception):
            pass  # Fall through to fetch

    try:
        import pygris

        # Get tract boundaries
        tracts = pygris.tracts(state="MD", year=year, cb=True)
        tracts = tracts.to_crs("EPSG:4326")

        # Rename columns
        tracts = tracts.rename(columns={
            'GEOID': 'tract_geoid',
            'STATEFP': 'state_fips',
            'COUNTYFP': 'county_fips'
        })

        # Create full county FIPS
        tracts['fips_code'] = tracts['state_fips'] + tracts['county_fips']

        # Add centroids for routing
        tracts['centroid_lon'] = tracts.geometry.centroid.x
        tracts['centroid_lat'] = tracts.geometry.centroid.y

        # Get tract population from ACS
        try:
            from census import Census
            c = Census(settings.CENSUS_API_KEY)

            pop_data = c.acs5.state_county_tract(
                ('B01003_001E',),  # Total population
                state_fips='24',
                county_fips='*',
                tract='*',
                year=year
            )

            pop_df = pd.DataFrame(pop_data)
            pop_df['tract_geoid'] = pop_df['state'] + pop_df['county'] + pop_df['tract']
            pop_df = pop_df.rename(columns={'B01003_001E': 'population'})

            tracts = tracts.merge(
                pop_df[['tract_geoid', 'population']],
                on='tract_geoid',
                how='left'
            )

        except Exception as e:
            logger.warning(f"Could not fetch population data: {e}")
            tracts['population'] = 0

        # Filter to valid Maryland counties
        tracts = tracts[tracts['fips_code'].isin(MD_COUNTY_FIPS.keys())]

        # Keep essential columns
        tracts = tracts[['tract_geoid', 'fips_code', 'centroid_lon', 'centroid_lat',
                         'population', 'geometry']]

        # Cache (try parquet, fall back to GeoJSON)
        try:
            tracts.to_parquet(cache_path_parquet)
        except (ImportError, Exception):
            tracts.to_file(cache_path_geojson, driver='GeoJSON')
            logger.info("Cached as GeoJSON (parquet not available)")

        logger.info(f"✓ Loaded {len(tracts)} census tracts")
        return tracts

    except Exception as e:
        logger.error(f"Failed to fetch tract boundaries: {e}")
        raise


# =============================================================================
# R5 ACCESSIBILITY COMPUTATION
# =============================================================================

def check_r5py_available() -> bool:
    """Check if r5py is installed and functional."""
    try:
        import r5py
        return True
    except (ImportError, Exception) as e:
        # r5py may fail to import if Java is not available
        if "java" in str(e).lower():
            logger.warning("r5py requires Java 11+. Install Java or use --no-r5 flag.")
        return False


def compute_accessibility_r5py(
    tracts: gpd.GeoDataFrame,
    jobs: pd.DataFrame,
    osm_path: Path,
    gtfs_feeds: List[GTFSFeedInfo],
    departure_time: datetime = None
) -> pd.DataFrame:
    """
    Compute accessibility using r5py routing engine.

    This is the core accessibility computation using Conveyal's R5 engine
    wrapped by r5py. It computes travel time matrices and accessibility
    metrics for all Maryland census tracts.

    Args:
        tracts: GeoDataFrame with tract centroids
        jobs: DataFrame with jobs by tract
        osm_path: Path to .osm.pbf file
        gtfs_feeds: List of GTFS feed info
        departure_time: Departure time for transit analysis

    Returns:
        DataFrame with accessibility metrics by tract
    """
    import r5py

    if departure_time is None:
        # Use Tuesday 8 AM as representative commute time
        departure_time = datetime(2026, 1, 28, 8, 0)  # Recent Tuesday

    logger.info("Building R5 transport network...")

    # Create transport network with all GTFS feeds
    gtfs_paths = [str(f.path) for f in gtfs_feeds]

    transport_network = r5py.TransportNetwork(
        osm_pbf=str(osm_path),
        gtfs=gtfs_paths
    )

    # Prepare origin points (tract centroids)
    origins = gpd.GeoDataFrame(
        tracts[['tract_geoid', 'fips_code']].copy(),
        geometry=gpd.points_from_xy(tracts['centroid_lon'], tracts['centroid_lat']),
        crs="EPSG:4326"
    )
    origins = origins.rename(columns={'tract_geoid': 'id'})

    # Prepare destination points (also tract centroids, weighted by jobs)
    destinations = origins.copy()

    # Merge job counts
    jobs_lookup = jobs.set_index('tract_geoid')['total_jobs'].to_dict()

    logger.info(f"Computing travel time matrices for {len(origins)} origins...")

    results = []

    # Transit accessibility (45 min and 30 min)
    logger.info("Computing transit accessibility...")

    travel_time_matrix_transit = r5py.TravelTimeMatrixComputer(
        transport_network,
        origins=origins,
        destinations=destinations,
        departure=departure_time,
        departure_time_window=timedelta(hours=1),
        transport_modes=[r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK],
        max_time=timedelta(minutes=60)
    ).compute_travel_times()

    # Walk-only accessibility (30 min)
    logger.info("Computing walk accessibility...")

    travel_time_matrix_walk = r5py.TravelTimeMatrixComputer(
        transport_network,
        origins=origins,
        destinations=destinations,
        departure=departure_time,
        transport_modes=[r5py.TransportMode.WALK],
        max_time=timedelta(minutes=45)
    ).compute_travel_times()

    # Bike accessibility (30 min)
    logger.info("Computing bike accessibility...")

    travel_time_matrix_bike = r5py.TravelTimeMatrixComputer(
        transport_network,
        origins=origins,
        destinations=destinations,
        departure=departure_time,
        transport_modes=[r5py.TransportMode.BICYCLE],
        max_time=timedelta(minutes=45)
    ).compute_travel_times()

    # Car accessibility (30 min, for comparison)
    logger.info("Computing car accessibility (baseline)...")

    travel_time_matrix_car = r5py.TravelTimeMatrixComputer(
        transport_network,
        origins=origins,
        destinations=destinations,
        departure=departure_time,
        transport_modes=[r5py.TransportMode.CAR],
        max_time=timedelta(minutes=45)
    ).compute_travel_times()

    # Aggregate to accessibility metrics
    logger.info("Aggregating accessibility metrics...")

    for tract_id in tracts['tract_geoid'].unique():
        # Get travel times from this origin
        transit_times = travel_time_matrix_transit[
            travel_time_matrix_transit['from_id'] == tract_id
        ][['to_id', 'travel_time']].set_index('to_id')['travel_time']

        walk_times = travel_time_matrix_walk[
            travel_time_matrix_walk['from_id'] == tract_id
        ][['to_id', 'travel_time']].set_index('to_id')['travel_time']

        bike_times = travel_time_matrix_bike[
            travel_time_matrix_bike['from_id'] == tract_id
        ][['to_id', 'travel_time']].set_index('to_id')['travel_time']

        car_times = travel_time_matrix_car[
            travel_time_matrix_car['from_id'] == tract_id
        ][['to_id', 'travel_time']].set_index('to_id')['travel_time']

        # Count jobs reachable within thresholds
        jobs_transit_45 = sum(
            jobs_lookup.get(dest, 0)
            for dest, time in transit_times.items()
            if pd.notna(time) and time <= 45
        )

        jobs_transit_30 = sum(
            jobs_lookup.get(dest, 0)
            for dest, time in transit_times.items()
            if pd.notna(time) and time <= 30
        )

        jobs_walk_30 = sum(
            jobs_lookup.get(dest, 0)
            for dest, time in walk_times.items()
            if pd.notna(time) and time <= 30
        )

        jobs_bike_30 = sum(
            jobs_lookup.get(dest, 0)
            for dest, time in bike_times.items()
            if pd.notna(time) and time <= 30
        )

        jobs_car_30 = sum(
            jobs_lookup.get(dest, 0)
            for dest, time in car_times.items()
            if pd.notna(time) and time <= 30
        )

        tract_row = tracts[tracts['tract_geoid'] == tract_id].iloc[0]

        results.append({
            'tract_geoid': tract_id,
            'fips_code': tract_row['fips_code'],
            'jobs_transit_45': jobs_transit_45,
            'jobs_transit_30': jobs_transit_30,
            'jobs_walk_30': jobs_walk_30,
            'jobs_bike_30': jobs_bike_30,
            'jobs_car_30': jobs_car_30,
            'tract_population': tract_row.get('population', 0)
        })

    return pd.DataFrame(results)


def compute_accessibility_fallback(
    tracts: gpd.GeoDataFrame,
    jobs: pd.DataFrame,
    gtfs_feeds: List[GTFSFeedInfo]
) -> pd.DataFrame:
    """
    Fallback accessibility computation when r5py is not available.

    Uses a simplified gravity-based model with straight-line distances
    and transit stop proximity as a proxy for transit access.

    Args:
        tracts: GeoDataFrame with tract centroids
        jobs: DataFrame with jobs by tract
        gtfs_feeds: List of GTFS feeds (for stop locations)

    Returns:
        DataFrame with estimated accessibility metrics
    """
    try:
        import gtfs_kit as gk
    except Exception as e:
        logger.error(f"Cannot import gtfs_kit: {e}")
        logger.error("This likely means r5py is installed but Java is not configured.")
        logger.error("Either: (1) Configure Java properly, or (2) pip uninstall r5py")
        raise

    logger.warning("Using fallback accessibility computation (r5py not available)")
    logger.info("This provides estimates based on proximity, not actual routing")

    # Load all GTFS stops
    all_stops = []
    for feed_info in gtfs_feeds:
        try:
            feed = gk.read_feed(str(feed_info.path), dist_units='mi')
            stops = feed.stops[['stop_id', 'stop_lat', 'stop_lon']].copy()
            stops['feed'] = feed_info.name
            all_stops.append(stops)
        except Exception as e:
            logger.warning(f"Could not read {feed_info.name}: {e}")

    if all_stops:
        stops_df = pd.concat(all_stops, ignore_index=True)
        stops_gdf = gpd.GeoDataFrame(
            stops_df,
            geometry=gpd.points_from_xy(stops_df['stop_lon'], stops_df['stop_lat']),
            crs="EPSG:4326"
        ).to_crs("EPSG:3857")
    else:
        stops_gdf = gpd.GeoDataFrame(columns=['geometry'], crs="EPSG:3857")

    # Prepare tracts
    tracts_proj = tracts.to_crs("EPSG:3857")
    tract_centroids = tracts_proj.copy()
    tract_centroids['geometry'] = tract_centroids.geometry.centroid

    # Jobs lookup
    jobs_lookup = jobs.set_index('tract_geoid')['total_jobs'].to_dict()
    total_regional_jobs = sum(jobs_lookup.values())

    results = []

    for idx, row in tract_centroids.iterrows():
        tract_id = row['tract_geoid']
        centroid = row['geometry']

        # Count nearby transit stops (within 800m ~ 0.5 mi)
        if not stops_gdf.empty:
            nearby_stops = stops_gdf[stops_gdf.distance(centroid) <= 800]
            stop_count = len(nearby_stops)
            has_transit = stop_count > 0
        else:
            stop_count = 0
            has_transit = False

        # Compute distance-decayed accessibility
        # Using simplified gravity model: jobs / distance^2
        jobs_transit_45 = 0
        jobs_transit_30 = 0
        jobs_walk_30 = 0
        jobs_bike_30 = 0
        jobs_car_30 = 0

        for other_idx, other_row in tract_centroids.iterrows():
            other_id = other_row['tract_geoid']
            other_centroid = other_row['geometry']

            jobs_at_dest = jobs_lookup.get(other_id, 0)
            if jobs_at_dest == 0:
                continue

            dist_m = centroid.distance(other_centroid)
            dist_km = dist_m / 1000

            # Approximate travel times based on mode speeds
            # Walking: 5 km/h → 30 min = 2.5 km
            # Biking: 15 km/h → 30 min = 7.5 km
            # Transit: ~20 km/h avg → 45 min = 15 km, 30 min = 10 km
            # Car: ~40 km/h avg → 30 min = 20 km

            if dist_km <= 2.5:
                jobs_walk_30 += jobs_at_dest

            if dist_km <= 7.5:
                jobs_bike_30 += jobs_at_dest

            if has_transit and dist_km <= 15:
                jobs_transit_45 += jobs_at_dest

            if has_transit and dist_km <= 10:
                jobs_transit_30 += jobs_at_dest

            if dist_km <= 20:
                jobs_car_30 += jobs_at_dest

        # Apply transit penalty if no stops nearby
        if not has_transit:
            jobs_transit_45 = int(jobs_transit_45 * 0.1)  # 90% reduction
            jobs_transit_30 = int(jobs_transit_30 * 0.1)

        results.append({
            'tract_geoid': tract_id,
            'fips_code': row['fips_code'],
            'jobs_transit_45': int(jobs_transit_45),
            'jobs_transit_30': int(jobs_transit_30),
            'jobs_walk_30': int(jobs_walk_30),
            'jobs_bike_30': int(jobs_bike_30),
            'jobs_car_30': int(jobs_car_30),
            'tract_population': row.get('population', 0),
            'transit_stops_nearby': stop_count
        })

    return pd.DataFrame(results)


# =============================================================================
# SCORING AND NORMALIZATION
# =============================================================================

def normalize_accessibility_scores(df: pd.DataFrame, jobs: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw accessibility counts to 0-1 scores.

    Uses percentile normalization within Maryland to create
    comparable scores across tracts and time periods.

    Args:
        df: DataFrame with raw job counts
        jobs: DataFrame with total jobs for regional context

    Returns:
        DataFrame with added score columns
    """
    total_regional_jobs = jobs['total_jobs'].sum()

    # Normalize each metric to 0-1 using percentile rank
    for col in ['jobs_transit_45', 'jobs_transit_30', 'jobs_walk_30',
                'jobs_bike_30', 'jobs_car_30']:
        if col in df.columns:
            score_col = col.replace('jobs_', '') + '_score'
            df[score_col] = df[col].rank(pct=True)

    # Compute percentage of regional jobs reachable
    df['pct_regional_jobs_transit'] = (df['jobs_transit_45'] / total_regional_jobs).clip(0, 1)

    # Transit competitiveness ratio
    df['transit_car_ratio'] = np.where(
        df['jobs_car_30'] > 0,
        df['jobs_transit_45'] / df['jobs_car_30'],
        0
    ).clip(0, 1)

    # Composite multimodal accessibility score
    # Weight: 60% transit, 25% walk, 15% bike
    df['multimodal_accessibility_score'] = (
        0.60 * df.get('transit_45_score', 0) +
        0.25 * df.get('walk_30_score', 0) +
        0.15 * df.get('bike_30_score', 0)
    ).clip(0, 1)

    return df


def compute_transit_quality_metrics(
    tracts: gpd.GeoDataFrame,
    gtfs_feeds: List[GTFSFeedInfo]
) -> pd.DataFrame:
    """
    Compute transit service quality metrics by tract.

    Args:
        tracts: GeoDataFrame with tract geometries
        gtfs_feeds: List of GTFS feed info

    Returns:
        DataFrame with transit quality metrics
    """
    import gtfs_kit as gk

    # Load all stops with frequencies
    all_stops = []

    for feed_info in gtfs_feeds:
        try:
            feed = gk.read_feed(str(feed_info.path), dist_units='mi')

            # Get stop locations
            stops = feed.stops[['stop_id', 'stop_lat', 'stop_lon']].copy()

            # Compute headways from stop_times
            if 'stop_times' in dir(feed) and feed.stop_times is not None:
                stop_times = feed.stop_times.copy()

                # Group by stop and count trips per day
                trips_per_stop = stop_times.groupby('stop_id').size().reset_index()
                trips_per_stop.columns = ['stop_id', 'daily_trips']

                stops = stops.merge(trips_per_stop, on='stop_id', how='left')
                stops['daily_trips'] = stops['daily_trips'].fillna(0)

                # Estimate headway (minutes between trips, assuming 16-hour service day)
                stops['avg_headway'] = np.where(
                    stops['daily_trips'] > 0,
                    (16 * 60) / stops['daily_trips'],
                    999  # No service
                )
            else:
                stops['daily_trips'] = 0
                stops['avg_headway'] = 999

            stops['feed'] = feed_info.name
            all_stops.append(stops)

        except Exception as e:
            logger.warning(f"Could not process {feed_info.name}: {e}")

    if not all_stops:
        return pd.DataFrame({
            'tract_geoid': tracts['tract_geoid'],
            'transit_stop_density': 0,
            'frequent_transit_area_pct': 0,
            'average_headway_minutes': 999
        })

    stops_df = pd.concat(all_stops, ignore_index=True)
    stops_gdf = gpd.GeoDataFrame(
        stops_df,
        geometry=gpd.points_from_xy(stops_df['stop_lon'], stops_df['stop_lat']),
        crs="EPSG:4326"
    )

    # Spatial join stops to tracts
    tracts_proj = tracts.to_crs("EPSG:3857")
    stops_proj = stops_gdf.to_crs("EPSG:3857")

    joined = gpd.sjoin(stops_proj, tracts_proj[['tract_geoid', 'geometry']],
                       how='left', predicate='within')

    # Aggregate by tract
    tract_metrics = joined.groupby('tract_geoid').agg(
        stop_count=('stop_id', 'count'),
        avg_headway=('avg_headway', 'mean'),
        frequent_stops=('avg_headway', lambda x: (x <= 15).sum())
    ).reset_index()

    # Compute area for density
    tracts_area = tracts_proj.copy()
    tracts_area['area_sq_mi'] = tracts_area.geometry.area / 2.59e6  # m² to sq mi

    tract_metrics = tract_metrics.merge(
        tracts_area[['tract_geoid', 'area_sq_mi']],
        on='tract_geoid',
        how='left'
    )

    # Calculate final metrics
    tract_metrics['transit_stop_density'] = np.where(
        tract_metrics['area_sq_mi'] > 0,
        tract_metrics['stop_count'] / tract_metrics['area_sq_mi'],
        0
    )

    tract_metrics['frequent_transit_area_pct'] = np.where(
        tract_metrics['stop_count'] > 0,
        tract_metrics['frequent_stops'] / tract_metrics['stop_count'],
        0
    )

    tract_metrics['average_headway_minutes'] = tract_metrics['avg_headway'].fillna(999)

    return tract_metrics[['tract_geoid', 'transit_stop_density',
                          'frequent_transit_area_pct', 'average_headway_minutes']]


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_to_county(
    tract_df: pd.DataFrame,
    tracts: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    Aggregate tract-level accessibility to county level.

    Uses population-weighted averaging for score metrics
    and sum for job count metrics.

    Args:
        tract_df: DataFrame with tract-level metrics
        tracts: GeoDataFrame with tract population

    Returns:
        DataFrame with county-level metrics
    """
    # Merge population
    df = tract_df.merge(
        tracts[['tract_geoid', 'population']],
        on='tract_geoid',
        how='left'
    )
    df['population'] = df['population'].fillna(0)

    # Compute weighted scores
    df['weighted_multimodal'] = df['multimodal_accessibility_score'] * df['population']
    df['weighted_transit'] = df.get('transit_45_score', 0) * df['population']
    df['weighted_walk'] = df.get('walk_30_score', 0) * df['population']
    df['weighted_bike'] = df.get('bike_30_score', 0) * df['population']

    # Aggregate
    county_agg = df.groupby('fips_code').agg(
        # Sum job counts
        jobs_accessible_transit_45min=('jobs_transit_45', 'sum'),
        jobs_accessible_transit_30min=('jobs_transit_30', 'sum'),
        jobs_accessible_walk_30min=('jobs_walk_30', 'sum'),
        jobs_accessible_bike_30min=('jobs_bike_30', 'sum'),
        jobs_accessible_car_30min=('jobs_car_30', 'sum'),

        # Population-weighted average scores
        weighted_multimodal_sum=('weighted_multimodal', 'sum'),
        weighted_transit_sum=('weighted_transit', 'sum'),
        weighted_walk_sum=('weighted_walk', 'sum'),
        weighted_bike_sum=('weighted_bike', 'sum'),
        total_population=('population', 'sum'),

        # Transit quality (simple average)
        transit_stop_density=('transit_stop_density', 'mean'),
        frequent_transit_area_pct=('frequent_transit_area_pct', 'mean'),
        average_headway_minutes=('average_headway_minutes', 'mean'),

        # Regional metrics (max as representative)
        pct_regional_jobs_by_transit=('pct_regional_jobs_transit', 'max'),
        transit_car_accessibility_ratio=('transit_car_ratio', 'max'),

        # Tract count
        tract_count=('tract_geoid', 'count')
    ).reset_index()

    # Compute population-weighted averages
    county_agg['multimodal_accessibility_score'] = np.where(
        county_agg['total_population'] > 0,
        county_agg['weighted_multimodal_sum'] / county_agg['total_population'],
        county_agg['multimodal_accessibility_score'] if 'multimodal_accessibility_score' in county_agg else 0
    )

    county_agg['transit_accessibility_score'] = np.where(
        county_agg['total_population'] > 0,
        county_agg['weighted_transit_sum'] / county_agg['total_population'],
        0
    )

    county_agg['walk_accessibility_score'] = np.where(
        county_agg['total_population'] > 0,
        county_agg['weighted_walk_sum'] / county_agg['total_population'],
        0
    )

    county_agg['bike_accessibility_score'] = np.where(
        county_agg['total_population'] > 0,
        county_agg['weighted_bike_sum'] / county_agg['total_population'],
        0
    )

    # Drop intermediate columns
    county_agg = county_agg.drop(columns=[
        'weighted_multimodal_sum', 'weighted_transit_sum',
        'weighted_walk_sum', 'weighted_bike_sum', 'total_population', 'tract_count'
    ])

    return county_agg


# =============================================================================
# DATABASE STORAGE
# =============================================================================

def store_tract_accessibility(df: pd.DataFrame, data_year: int,
                               gtfs_date: date, osm_date: date, lodes_year: int):
    """
    Store tract-level accessibility data in database.

    Args:
        df: DataFrame with tract accessibility metrics
        data_year: Year for this data
        gtfs_date: Date of GTFS feeds used
        osm_date: Date of OSM extract used
        lodes_year: Year of LODES data used
    """
    logger.info(f"Storing {len(df)} tract accessibility records")

    with get_db() as db:
        # Clear existing data for this year
        db.execute(text("""
            DELETE FROM layer2_mobility_accessibility_tract
            WHERE data_year = :data_year
        """), {"data_year": data_year})

        # Insert new records
        for _, row in df.iterrows():
            db.execute(text("""
                INSERT INTO layer2_mobility_accessibility_tract (
                    tract_geoid, fips_code, data_year,
                    jobs_accessible_transit_45min, jobs_accessible_transit_30min,
                    jobs_accessible_walk_30min, jobs_accessible_bike_30min,
                    jobs_accessible_car_30min,
                    transit_accessibility_score, walk_accessibility_score,
                    bike_accessibility_score, multimodal_accessibility_score,
                    pct_regional_jobs_by_transit, transit_car_accessibility_ratio,
                    transit_stop_density, frequent_transit_area_pct,
                    average_headway_minutes, tract_population,
                    gtfs_feed_date, osm_extract_date, lodes_year
                ) VALUES (
                    :tract_geoid, :fips_code, :data_year,
                    :jobs_transit_45, :jobs_transit_30,
                    :jobs_walk_30, :jobs_bike_30, :jobs_car_30,
                    :transit_45_score, :walk_30_score,
                    :bike_30_score, :multimodal_accessibility_score,
                    :pct_regional_jobs_transit, :transit_car_ratio,
                    :transit_stop_density, :frequent_transit_area_pct,
                    :average_headway_minutes, :tract_population,
                    :gtfs_date, :osm_date, :lodes_year
                )
            """), {
                'tract_geoid': row['tract_geoid'],
                'fips_code': row['fips_code'],
                'data_year': data_year,
                'jobs_transit_45': int(row.get('jobs_transit_45', 0)),
                'jobs_transit_30': int(row.get('jobs_transit_30', 0)),
                'jobs_walk_30': int(row.get('jobs_walk_30', 0)),
                'jobs_bike_30': int(row.get('jobs_bike_30', 0)),
                'jobs_car_30': int(row.get('jobs_car_30', 0)),
                'transit_45_score': float(row.get('transit_45_score', 0)),
                'walk_30_score': float(row.get('walk_30_score', 0)),
                'bike_30_score': float(row.get('bike_30_score', 0)),
                'multimodal_accessibility_score': float(row.get('multimodal_accessibility_score', 0)),
                'pct_regional_jobs_transit': float(row.get('pct_regional_jobs_transit', 0)),
                'transit_car_ratio': float(row.get('transit_car_ratio', 0)),
                'transit_stop_density': float(row.get('transit_stop_density', 0)),
                'frequent_transit_area_pct': float(row.get('frequent_transit_area_pct', 0)),
                'average_headway_minutes': float(row.get('average_headway_minutes', 999)),
                'tract_population': int(row.get('tract_population', 0)),
                'gtfs_date': gtfs_date,
                'osm_date': osm_date,
                'lodes_year': lodes_year
            })

        db.commit()

    logger.info("✓ Tract accessibility data stored")


def store_county_accessibility(df: pd.DataFrame, data_year: int,
                                gtfs_date: date, osm_date: date, lodes_year: int):
    """
    Store county-level accessibility data in the main Layer 2 table.

    Updates the existing layer2_mobility_optionality table with
    new accessibility-based metrics.

    Args:
        df: DataFrame with county accessibility metrics
        data_year: Year for this data
        gtfs_date: Date of GTFS feeds used
        osm_date: Date of OSM extract used
        lodes_year: Year of LODES data used
    """
    logger.info(f"Updating {len(df)} county accessibility records")

    with get_db() as db:
        for _, row in df.iterrows():
            # Update existing records or insert new
            db.execute(text("""
                INSERT INTO layer2_mobility_optionality (
                    fips_code, data_year,
                    jobs_accessible_transit_45min, jobs_accessible_transit_30min,
                    jobs_accessible_walk_30min, jobs_accessible_bike_30min,
                    jobs_accessible_car_30min,
                    transit_accessibility_score, walk_accessibility_score,
                    bike_accessibility_score, multimodal_accessibility_score,
                    pct_regional_jobs_by_transit, transit_car_accessibility_ratio,
                    transit_stop_density, frequent_transit_area_pct,
                    average_headway_minutes,
                    gtfs_feed_date, osm_extract_date, lodes_year,
                    accessibility_version, mobility_optionality_index
                ) VALUES (
                    :fips_code, :data_year,
                    :jobs_transit_45, :jobs_transit_30,
                    :jobs_walk_30, :jobs_bike_30, :jobs_car_30,
                    :transit_score, :walk_score, :bike_score, :multimodal_score,
                    :pct_regional, :transit_car_ratio,
                    :stop_density, :frequent_pct, :avg_headway,
                    :gtfs_date, :osm_date, :lodes_year,
                    'v2-accessibility', :multimodal_score
                )
                ON CONFLICT (fips_code, data_year)
                DO UPDATE SET
                    jobs_accessible_transit_45min = EXCLUDED.jobs_accessible_transit_45min,
                    jobs_accessible_transit_30min = EXCLUDED.jobs_accessible_transit_30min,
                    jobs_accessible_walk_30min = EXCLUDED.jobs_accessible_walk_30min,
                    jobs_accessible_bike_30min = EXCLUDED.jobs_accessible_bike_30min,
                    jobs_accessible_car_30min = EXCLUDED.jobs_accessible_car_30min,
                    transit_accessibility_score = EXCLUDED.transit_accessibility_score,
                    walk_accessibility_score = EXCLUDED.walk_accessibility_score,
                    bike_accessibility_score = EXCLUDED.bike_accessibility_score,
                    multimodal_accessibility_score = EXCLUDED.multimodal_accessibility_score,
                    pct_regional_jobs_by_transit = EXCLUDED.pct_regional_jobs_by_transit,
                    transit_car_accessibility_ratio = EXCLUDED.transit_car_accessibility_ratio,
                    transit_stop_density = EXCLUDED.transit_stop_density,
                    frequent_transit_area_pct = EXCLUDED.frequent_transit_area_pct,
                    average_headway_minutes = EXCLUDED.average_headway_minutes,
                    gtfs_feed_date = EXCLUDED.gtfs_feed_date,
                    osm_extract_date = EXCLUDED.osm_extract_date,
                    lodes_year = EXCLUDED.lodes_year,
                    accessibility_version = 'v2-accessibility',
                    mobility_optionality_index = EXCLUDED.mobility_optionality_index,
                    updated_at = CURRENT_TIMESTAMP
            """), {
                'fips_code': row['fips_code'],
                'data_year': data_year,
                'jobs_transit_45': int(row.get('jobs_accessible_transit_45min', 0)),
                'jobs_transit_30': int(row.get('jobs_accessible_transit_30min', 0)),
                'jobs_walk_30': int(row.get('jobs_accessible_walk_30min', 0)),
                'jobs_bike_30': int(row.get('jobs_accessible_bike_30min', 0)),
                'jobs_car_30': int(row.get('jobs_accessible_car_30min', 0)),
                'transit_score': float(row.get('transit_accessibility_score', 0)),
                'walk_score': float(row.get('walk_accessibility_score', 0)),
                'bike_score': float(row.get('bike_accessibility_score', 0)),
                'multimodal_score': float(row.get('multimodal_accessibility_score', 0)),
                'pct_regional': float(row.get('pct_regional_jobs_by_transit', 0)),
                'transit_car_ratio': float(row.get('transit_car_accessibility_ratio', 0)),
                'stop_density': float(row.get('transit_stop_density', 0)),
                'frequent_pct': float(row.get('frequent_transit_area_pct', 0)),
                'avg_headway': float(row.get('average_headway_minutes', 999)),
                'gtfs_date': gtfs_date,
                'osm_date': osm_date,
                'lodes_year': lodes_year
            })

        db.commit()

    logger.info("✓ County accessibility data stored")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def calculate_accessibility_indicators(
    data_year: int = None,
    use_r5: bool = True,
    lodes_year: int = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main function to calculate accessibility indicators.

    Args:
        data_year: Year to associate with this data (default: current year)
        use_r5: Whether to use r5py (True) or fallback (False)
        lodes_year: LODES year to use (default: data_year - 2)

    Returns:
        Tuple of (tract_df, county_df) with accessibility metrics
    """
    data_year = data_year or datetime.now().year
    lodes_year = lodes_year or min(data_year - 2, 2021)  # LODES typically 2 years behind

    logger.info("=" * 60)
    logger.info("LAYER 2 v2: ACCESSIBILITY-BASED MOBILITY ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Data year: {data_year}")
    logger.info(f"LODES year: {lodes_year}")

    # Step 1: Download data
    logger.info("\n[1/6] Downloading data sources...")

    osm_path = download_maryland_osm()
    osm_date = date.fromtimestamp(osm_path.stat().st_mtime)

    gtfs_feeds = download_gtfs_feeds()
    gtfs_date = max(f.feed_date for f in gtfs_feeds) if gtfs_feeds else date.today()

    logger.info(f"  OSM extract date: {osm_date}")
    logger.info(f"  GTFS feed date: {gtfs_date}")
    logger.info(f"  Feeds loaded: {[f.name for f in gtfs_feeds]}")

    # Step 2: Load LODES jobs data
    logger.info("\n[2/6] Loading LODES workplace data...")
    jobs = download_lodes_wac(year=lodes_year)

    # Step 3: Load tract boundaries
    logger.info("\n[3/6] Loading census tract boundaries...")
    tracts = fetch_maryland_tracts()

    # Step 4: Compute accessibility
    logger.info("\n[4/6] Computing accessibility metrics...")

    if use_r5 and check_r5py_available():
        logger.info("Using R5 routing engine (r5py)")
        tract_accessibility = compute_accessibility_r5py(
            tracts=tracts,
            jobs=jobs,
            osm_path=osm_path,
            gtfs_feeds=gtfs_feeds
        )
    else:
        logger.info("Using fallback proximity-based accessibility")
        tract_accessibility = compute_accessibility_fallback(
            tracts=tracts,
            jobs=jobs,
            gtfs_feeds=gtfs_feeds
        )

    # Step 5: Compute transit quality metrics
    logger.info("\n[5/6] Computing transit service quality...")
    transit_quality = compute_transit_quality_metrics(tracts, gtfs_feeds)

    # Merge transit quality into tract results
    tract_accessibility = tract_accessibility.merge(
        transit_quality, on='tract_geoid', how='left'
    )

    # Step 6: Normalize scores
    logger.info("\n[6/6] Normalizing and aggregating...")
    tract_accessibility = normalize_accessibility_scores(tract_accessibility, jobs)

    # Aggregate to county
    county_accessibility = aggregate_to_county(tract_accessibility, tracts)

    # Summary statistics
    logger.info("\n" + "=" * 60)
    logger.info("ACCESSIBILITY ANALYSIS COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Tracts analyzed: {len(tract_accessibility)}")
    logger.info(f"Counties: {len(county_accessibility)}")
    logger.info(f"Avg jobs accessible by transit (45min): {tract_accessibility['jobs_transit_45'].mean():,.0f}")
    logger.info(f"Avg multimodal score: {tract_accessibility['multimodal_accessibility_score'].mean():.3f}")

    return tract_accessibility, county_accessibility


def run_layer2_v2_ingestion(
    data_year: int = None,
    use_r5: bool = True,
    store_data: bool = True
):
    """
    Run complete Layer 2 v2 ingestion pipeline.

    Args:
        data_year: Year for this data
        use_r5: Whether to use r5py routing
        store_data: Whether to store results in database
    """
    data_year = data_year or datetime.now().year
    lodes_year = min(data_year - 2, 2021)

    try:
        # Calculate accessibility
        tract_df, county_df = calculate_accessibility_indicators(
            data_year=data_year,
            use_r5=use_r5,
            lodes_year=lodes_year
        )

        if store_data:
            # Get dates for provenance
            osm_path = OSM_CACHE_DIR / "maryland-latest.osm.pbf"
            osm_date = date.fromtimestamp(osm_path.stat().st_mtime) if osm_path.exists() else date.today()

            gtfs_feeds = download_gtfs_feeds()
            gtfs_date = max(f.feed_date for f in gtfs_feeds) if gtfs_feeds else date.today()

            # Store tract-level data
            store_tract_accessibility(
                tract_df, data_year, gtfs_date, osm_date, lodes_year
            )

            # Store county-level data
            store_county_accessibility(
                county_df, data_year, gtfs_date, osm_date, lodes_year
            )

            # Log refresh
            log_refresh(
                layer_name="layer2_mobility_optionality",
                data_source="OSM+GTFS+LODES (v2 accessibility)",
                status="success",
                records_processed=len(tract_df),
                records_inserted=len(tract_df) + len(county_df),
                metadata={
                    "data_year": data_year,
                    "lodes_year": lodes_year,
                    "gtfs_date": str(gtfs_date),
                    "osm_date": str(osm_date),
                    "version": "v2-accessibility",
                    "tracts": len(tract_df),
                    "counties": len(county_df),
                    "used_r5": use_r5 and check_r5py_available()
                }
            )

        logger.info("✓ Layer 2 v2 ingestion complete")
        return tract_df, county_df

    except Exception as e:
        logger.error(f"Layer 2 v2 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer2_mobility_optionality",
            data_source="OSM+GTFS+LODES (v2 accessibility)",
            status="failed",
            error_message=str(e)
        )
        raise


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Layer 2 v2: Accessibility-based Mobility Analysis'
    )
    parser.add_argument(
        '--year', type=int, default=datetime.now().year,
        help='Data year (default: current year)'
    )
    parser.add_argument(
        '--no-r5', action='store_true',
        help='Use fallback instead of r5py'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Calculate but do not store results'
    )

    args = parser.parse_args()

    run_layer2_v2_ingestion(
        data_year=args.year,
        use_r5=not args.no_r5,
        store_data=not args.dry_run
    )


if __name__ == "__main__":
    main()
