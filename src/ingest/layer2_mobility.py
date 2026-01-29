"""
Maryland Viability Atlas - Layer 2: Mobility Optionality
Ingests transportation modal redundancy and accessibility data

Data Sources:
- OpenStreetMap (highway + rail networks)
- GTFS feeds (MTA Maryland: local bus, light rail, metro, MARC, commuter bus)

Signals Produced:
- Highway network coverage (major highways)
- Interstate exit density (motorway_junction nodes)
- Rail service availability (GTFS + OSM rail)
- Transit stop density (GTFS stops)
- Mobility optionality composite
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import geopandas as gpd
import osmnx as ox
import gtfs_kit as gk
from shapely.geometry import Point
from sqlalchemy import text

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.data_sources import download_file
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

CACHE_DIR = Path("data/cache/mobility")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# GTFS feeds (static)
GTFS_FEEDS = [
    {"name": "mta_local_bus", "url": "https://feeds.mta.maryland.gov/gtfs/local-bus"},
    {"name": "mta_light_rail", "url": "https://feeds.mta.maryland.gov/gtfs/light-rail"},
    {"name": "mta_metro", "url": "https://feeds.mta.maryland.gov/gtfs/metro"},
    {"name": "mta_marc", "url": "https://feeds.mta.maryland.gov/gtfs/marc"},
    {"name": "mta_commuter_bus", "url": "https://feeds.mta.maryland.gov/gtfs/commuter-bus"},
]

BUS_ROUTE_TYPES = {3}  # Bus
RAIL_ROUTE_TYPES = {1, 2}  # Subway, Rail


def _is_stale(path: Path, max_age_days: int = 7) -> bool:
    if not path.exists():
        return True
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.utcnow() - mtime > timedelta(days=max_age_days)


def _fetch_md_counties() -> gpd.GeoDataFrame:
    """Fetch Maryland county boundaries (EPSG:4326)."""
    import pygris
    from pygris import counties

    md_counties = counties(state="MD", year=2023, cb=True)
    md_counties['GEOID'] = md_counties['GEOID'].astype(str).str.zfill(5)
    md_counties = md_counties.rename(columns={'GEOID': 'fips_code', 'NAME': 'county_name'})

    if md_counties.crs != 'EPSG:4326':
        md_counties = md_counties.to_crs('EPSG:4326')

    md_counties = md_counties[md_counties['fips_code'].isin(MD_COUNTY_FIPS.keys())]
    return md_counties[['fips_code', 'county_name', 'geometry']]


def _fetch_osm_highways() -> gpd.GeoDataFrame:
    """Fetch major highways from OSM for Maryland."""
    tags = {"highway": ["motorway", "trunk"]}
    gdf = ox.geometries_from_place("Maryland, USA", tags=tags)
    gdf = gdf.reset_index()
    gdf = gdf[gdf.geometry.type.isin(["LineString", "MultiLineString"])].copy()
    return gdf


def _fetch_osm_motorway_junctions() -> gpd.GeoDataFrame:
    tags = {"highway": "motorway_junction"}
    gdf = ox.geometries_from_place("Maryland, USA", tags=tags)
    gdf = gdf.reset_index()
    gdf = gdf[gdf.geometry.type.isin(["Point", "MultiPoint"])].copy()
    return gdf


def _fetch_osm_rail_lines() -> gpd.GeoDataFrame:
    tags = {"railway": "rail"}
    gdf = ox.geometries_from_place("Maryland, USA", tags=tags)
    gdf = gdf.reset_index()
    gdf = gdf[gdf.geometry.type.isin(["LineString", "MultiLineString"])].copy()
    return gdf


def _download_gtfs(feed_url: str, name: str) -> Path:
    """Download GTFS feed to cache and return local path."""
    target = CACHE_DIR / f"{name}.zip"
    if _is_stale(target, max_age_days=7):
        logger.info(f"Downloading GTFS feed: {name}")
        ok = download_file(feed_url, str(target))
        if not ok:
            raise RuntimeError(f"Failed to download GTFS feed: {name}")
    return target


def _load_gtfs_feeds() -> List[Tuple[str, gk.Feed]]:
    feeds = []
    for feed in GTFS_FEEDS:
        path = _download_gtfs(feed["url"], feed["name"])
        logger.info(f"Loading GTFS feed: {feed['name']}")
        gtfs = gk.read_feed(str(path), dist_units="mi")
        feeds.append((feed["name"], gtfs))
    return feeds


def _extract_stop_sets(feed: gk.Feed) -> Dict[str, set]:
    """Return stop_id sets for rail and bus based on route_type."""
    trips = feed.trips[['trip_id', 'route_id']].copy()
    routes = feed.routes[['route_id', 'route_type']].copy()
    trips = trips.merge(routes, on='route_id', how='left')

    bus_trip_ids = trips[trips['route_type'].isin(BUS_ROUTE_TYPES)]['trip_id']
    rail_trip_ids = trips[trips['route_type'].isin(RAIL_ROUTE_TYPES)]['trip_id']

    stop_times = feed.stop_times[['trip_id', 'stop_id']]

    bus_stop_ids = set(stop_times[stop_times['trip_id'].isin(bus_trip_ids)]['stop_id'].unique())
    rail_stop_ids = set(stop_times[stop_times['trip_id'].isin(rail_trip_ids)]['stop_id'].unique())

    return {
        "bus_stop_ids": bus_stop_ids,
        "rail_stop_ids": rail_stop_ids
    }


def _build_stops_gdf(feeds: List[Tuple[str, gk.Feed]]) -> gpd.GeoDataFrame:
    """Build unified stops GeoDataFrame with mode tags."""
    records = []
    for feed_name, feed in feeds:
        mode_sets = _extract_stop_sets(feed)
        stops = feed.stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].copy()
        stops = stops.dropna(subset=['stop_lat', 'stop_lon'])
        for _, row in stops.iterrows():
            stop_id = row['stop_id']
            records.append({
                'stop_uid': f"{feed_name}:{stop_id}",
                'stop_id': stop_id,
                'feed': feed_name,
                'stop_name': row.get('stop_name'),
                'stop_lat': row['stop_lat'],
                'stop_lon': row['stop_lon'],
                'is_bus': stop_id in mode_sets['bus_stop_ids'],
                'is_rail': stop_id in mode_sets['rail_stop_ids']
            })

    if not records:
        return gpd.GeoDataFrame(columns=['stop_uid', 'geometry'], geometry='geometry', crs='EPSG:4326')

    df = pd.DataFrame(records)
    geometry = [Point(xy) for xy in zip(df['stop_lon'], df['stop_lat'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    return gdf


def _compute_highway_metrics(counties: gpd.GeoDataFrame,
                             highways: gpd.GeoDataFrame,
                             junctions: gpd.GeoDataFrame) -> pd.DataFrame:
    """Compute highway metrics by county."""
    counties_proj = counties.to_crs('EPSG:3857')
    highways_proj = highways.to_crs('EPSG:3857')
    junctions_proj = junctions.to_crs('EPSG:3857')

    results = []

    for _, county in counties_proj.iterrows():
        fips = county['fips_code']
        geom = county['geometry']

        # Clip highways to county geometry
        clipped = gpd.clip(highways_proj, geom)
        highway_miles = clipped.length.sum() / 1609.34 if not clipped.empty else 0.0

        # Motorway junctions (interstate exits) count
        junctions_in = junctions_proj[junctions_proj.within(geom)]
        interstate_exits = len(junctions_in)

        # Unique route refs as redundancy proxy
        refs = set()
        if not clipped.empty and 'ref' in clipped.columns:
            for ref in clipped['ref'].dropna().astype(str).tolist():
                for part in ref.replace(';', ',').split(','):
                    p = part.strip()
                    if p:
                        refs.add(p)
        redundancy = len(refs) >= 2

        results.append({
            'fips_code': fips,
            'highway_miles_total': round(highway_miles, 2),
            'interstate_exits': int(interstate_exits),
            'major_highway_redundancy': bool(redundancy)
        })

    return pd.DataFrame(results)


def _compute_transit_metrics(counties: gpd.GeoDataFrame, stops_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Compute transit stop counts and rail/bus presence by county."""
    if stops_gdf.empty:
        return pd.DataFrame({
            'fips_code': list(counties['fips_code']),
            'transit_stations_count': 0,
            'bus_stops_count': 0,
            'rail_stops_count': 0,
            'has_rail_service': False,
            'has_frequent_bus': False
        })

    stops_proj = stops_gdf.to_crs('EPSG:3857')
    counties_proj = counties.to_crs('EPSG:3857')

    joined = gpd.sjoin(stops_proj, counties_proj[['fips_code', 'geometry']], how='left', predicate='within')

    agg = joined.groupby('fips_code').agg(
        transit_stations_count=('stop_uid', 'count'),
        bus_stops_count=('is_bus', 'sum'),
        rail_stops_count=('is_rail', 'sum')
    ).reset_index()

    # Ensure all counties present
    agg = counties[['fips_code']].merge(agg, on='fips_code', how='left').fillna(0)

    agg['has_rail_service'] = agg['rail_stops_count'] > 0

    # Frequent bus proxy: at least 10 bus stops in county
    agg['has_frequent_bus'] = agg['bus_stops_count'] >= 10

    return agg


def _compute_rail_line_presence(counties: gpd.GeoDataFrame, rail_lines: gpd.GeoDataFrame) -> pd.Series:
    """Return boolean series for rail line presence by county."""
    if rail_lines.empty:
        return pd.Series([False] * len(counties), index=counties['fips_code'])

    counties_proj = counties.to_crs('EPSG:3857')
    rail_proj = rail_lines.to_crs('EPSG:3857')

    has_rail = {}
    for _, county in counties_proj.iterrows():
        fips = county['fips_code']
        geom = county['geometry']
        intersects = rail_proj.intersects(geom).any()
        has_rail[fips] = bool(intersects)

    return pd.Series(has_rail)


def calculate_mobility_indicators(data_year: int = None) -> pd.DataFrame:
    """Calculate mobility optionality indicators from real data sources."""
    data_year = data_year or datetime.utcnow().year
    logger.info(f"Calculating mobility optionality for {data_year}")

    counties = _fetch_md_counties()
    counties = counties[counties['fips_code'].isin(MD_COUNTY_FIPS.keys())].copy()

    highways = _fetch_osm_highways()
    junctions = _fetch_osm_motorway_junctions()
    rail_lines = _fetch_osm_rail_lines()

    feeds = _load_gtfs_feeds()
    stops_gdf = _build_stops_gdf(feeds)

    highway_metrics = _compute_highway_metrics(counties, highways, junctions)
    transit_metrics = _compute_transit_metrics(counties, stops_gdf)

    # Combine rail presence from GTFS + OSM rail lines
    rail_line_presence = _compute_rail_line_presence(counties, rail_lines)

    df = counties[['fips_code']].merge(highway_metrics, on='fips_code', how='left')
    df = df.merge(transit_metrics, on='fips_code', how='left')

    df['has_rail_service'] = df['has_rail_service'] | df['fips_code'].map(rail_line_presence).fillna(False)

    # Mode count: highway + rail + bus
    df['mode_count'] = 0
    df.loc[df['highway_miles_total'] > 0, 'mode_count'] += 1
    df.loc[df['has_rail_service'], 'mode_count'] += 1
    df.loc[df['bus_stops_count'] > 0, 'mode_count'] += 1

    # Mobility optionality composite
    highway_rank = df['highway_miles_total'].rank(pct=True)
    transit_rank = df['transit_stations_count'].rank(pct=True)
    rail_bonus = df['has_rail_service'].astype(int) * 0.2

    df['mobility_optionality_index'] = (0.5 * highway_rank + 0.3 * transit_rank + rail_bonus).clip(0, 1)

    df['data_year'] = data_year

    # Add required columns not computed (set to None)
    df['highway_miles_total'] = df['highway_miles_total'].round(2)
    df['interstate_exits'] = df['interstate_exits'].fillna(0).astype(int)
    df['aadt_major_corridors'] = None
    df['transit_stations_count'] = df['transit_stations_count'].fillna(0).astype(int)

    logger.info(f"Calculated mobility indicators for {len(df)} counties")
    logger.info(f"Rail service: {df['has_rail_service'].sum()} counties")
    logger.info(f"Major highway redundancy: {df['major_highway_redundancy'].sum()} counties")

    return df


def store_mobility_data(df: pd.DataFrame):
    """Store mobility optionality data in database."""
    logger.info(f"Storing {len(df)} mobility records")

    with get_db() as db:
        data_year = df['data_year'].iloc[0]
        delete_sql = text("""
            DELETE FROM layer2_mobility_optionality
            WHERE data_year = :data_year
        """)
        db.execute(delete_sql, {"data_year": int(data_year)})

        insert_sql = text("""
            INSERT INTO layer2_mobility_optionality (
                fips_code, data_year,
                highway_miles_total, interstate_exits, major_highway_redundancy,
                has_rail_service, has_frequent_bus, transit_stations_count,
                mode_count, mobility_optionality_index, aadt_major_corridors
            ) VALUES (
                :fips_code, :data_year,
                :highway_miles_total, :interstate_exits, :major_highway_redundancy,
                :has_rail_service, :has_frequent_bus, :transit_stations_count,
                :mode_count, :mobility_optionality_index, :aadt_major_corridors
            )
        """)

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ Mobility data stored successfully")


def main():
    """Main execution for Layer 2 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 2: MOBILITY OPTIONALITY INGESTION")
        logger.info("=" * 60)

        data_year = datetime.utcnow().year

        df = calculate_mobility_indicators(data_year)

        if df.empty:
            logger.error("No mobility data to store")
            log_refresh(
                layer_name="layer2_mobility_optionality",
                data_source="OSM+GTFS",
                status="failed",
                error_message="No records produced",
                metadata={"data_year": data_year}
            )
            return

        store_mobility_data(df)

        log_refresh(
            layer_name="layer2_mobility_optionality",
            data_source="OSM+GTFS",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={"data_year": data_year}
        )

        logger.info("✓ Layer 2 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 2 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer2_mobility_optionality",
            data_source="OSM+GTFS",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
