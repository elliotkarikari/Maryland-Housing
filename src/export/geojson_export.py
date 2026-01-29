"""
Maryland Viability Atlas - GeoJSON Export
Generates map-ready GeoJSON outputs with county boundaries and classifications

Data sources:
- Geometries: US Census TIGER/Line (via pygris)
- Classifications: Database (county_classifications table)
- Layer scores: Database (layer_scores table)

Outputs:
- exports/md_counties_latest.geojson (always current)
- exports/md_counties_{YYYYMMDD}.geojson (versioned snapshots)
"""

import geopandas as gpd
import pandas as pd
import json
import os
from datetime import datetime
from typing import Optional
from sqlalchemy import text
import hashlib

from config.database import get_db, log_refresh
from config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def fetch_maryland_county_boundaries() -> gpd.GeoDataFrame:
    """
    Fetch Maryland county boundaries from Census TIGER/Line.

    Uses pygris library for programmatic access to official boundaries.

    Returns:
        GeoDataFrame with county geometries
    """
    logger.info("Fetching Maryland county boundaries from Census TIGER/Line")

    try:
        import pygris
        from pygris import counties

        # Fetch Maryland counties (FIPS 24)
        # pygris caches automatically in ~/.cache/pygris
        md_counties = counties(state="MD", year=2023, cb=True)  # cb=True for simplified boundaries

        # Ensure FIPS code is properly formatted
        md_counties['GEOID'] = md_counties['GEOID'].astype(str).str.zfill(5)

        # Rename for clarity
        md_counties = md_counties.rename(columns={
            'GEOID': 'fips_code',
            'NAME': 'county_name',
            'ALAND': 'land_area_m2',
            'AWATER': 'water_area_m2'
        })

        # Convert to WGS84 (EPSG:4326) for web mapping
        if md_counties.crs != 'EPSG:4326':
            md_counties = md_counties.to_crs('EPSG:4326')

        # Select relevant columns
        md_counties = md_counties[['fips_code', 'county_name', 'geometry']]

        logger.info(f"Fetched {len(md_counties)} Maryland county boundaries")

        return md_counties

    except Exception as e:
        logger.error(f"Failed to fetch county boundaries: {e}", exc_info=True)
        raise


def fetch_latest_classifications() -> pd.DataFrame:
    """
    Fetch latest county classifications and layer scores from database.

    Returns:
        DataFrame with all classification and score data
    """
    logger.info("Fetching latest classifications from database")

    with get_db() as db:
        query = text("""
            SELECT
                cc.fips_code,
                cc.data_year,
                cc.directional_class,
                cc.composite_score,
                cc.confidence_class,
                cc.synthesis_grouping,
                cc.primary_strengths,
                cc.primary_weaknesses,
                cc.key_trends,
                cc.classification_method,
                cc.version,
                ls.employment_gravity_score,
                ls.mobility_optionality_score,
                ls.school_trajectory_score,
                ls.housing_elasticity_score,
                ls.demographic_momentum_score,
                ls.risk_drag_score,
                ls.composite_raw,
                ls.composite_normalized
            FROM county_classifications cc
            LEFT JOIN layer_scores ls
                ON cc.fips_code = ls.fips_code
                AND cc.data_year = ls.data_year
            WHERE cc.data_year = (
                SELECT MAX(data_year) FROM county_classifications
            )
        """)

        df = pd.read_sql(query, db.connection())

    if df.empty:
        logger.warning("No classifications found in database")
        return pd.DataFrame()

    logger.info(f"Fetched classifications for {len(df)} counties")

    return df


def merge_geojson_data(
    boundaries_gdf: gpd.GeoDataFrame,
    classifications_df: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Merge county boundaries with classification data.

    Args:
        boundaries_gdf: GeoDataFrame with geometries
        classifications_df: DataFrame with classifications and scores

    Returns:
        GeoDataFrame ready for export
    """
    logger.info("Merging geometries with classification data")

    # Merge on FIPS code
    merged = boundaries_gdf.merge(
        classifications_df,
        on='fips_code',
        how='left'
    )

    # Add metadata
    merged['last_updated'] = datetime.utcnow().isoformat()

    logger.info(f"Merged data for {len(merged)} counties")

    return merged


def prepare_geojson_properties(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Prepare GeoJSON properties for optimal frontend consumption.

    Ensures all fields are JSON-serializable and properly typed.

    Args:
        gdf: GeoDataFrame with merged data

    Returns:
        GeoDataFrame with cleaned properties
    """
    logger.info("Preparing GeoJSON properties")

    # Convert arrays to lists (for JSON serialization)
    for col in ['primary_strengths', 'primary_weaknesses', 'key_trends']:
        if col in gdf.columns:
            gdf[col] = gdf[col].apply(
                lambda x: list(x) if isinstance(x, (list, tuple)) else []
            )

    # Round numeric columns
    numeric_cols = [
        'composite_score', 'composite_raw', 'composite_normalized',
        'employment_gravity_score', 'mobility_optionality_score',
        'school_trajectory_score', 'housing_elasticity_score',
        'demographic_momentum_score', 'risk_drag_score'
    ]

    # Round numeric columns (only non-null values)
    for col in numeric_cols:
        if col in gdf.columns:
            # Only round non-null values
            mask = pd.notna(gdf[col])
            if mask.any():
                gdf.loc[mask, col] = gdf.loc[mask, col].round(4)

    # Convert array columns to JSON strings for GeoJSON compatibility
    import json
    array_cols = ['primary_strengths', 'primary_weaknesses', 'key_trends']
    for col in array_cols:
        if col in gdf.columns:
            def safe_json_dumps(x):
                if x is None:
                    return None
                try:
                    # Check if it's a scalar NaN
                    if isinstance(x, float) and pd.isna(x):
                        return None
                    return json.dumps(x)
                except (TypeError, ValueError):
                    return None
            gdf[col] = gdf[col].apply(safe_json_dumps)

    # Fill NaN with None (for JSON null)
    gdf = gdf.where(pd.notna(gdf), None)

    return gdf


def export_geojson(
    gdf: gpd.GeoDataFrame,
    output_path: str,
    indent: Optional[int] = None
) -> str:
    """
    Export GeoDataFrame to GeoJSON file.

    Args:
        gdf: GeoDataFrame to export
        output_path: Output file path
        indent: JSON indentation (None for compact, 2 for readable)

    Returns:
        Path to exported file
    """
    logger.info(f"Exporting GeoJSON to {output_path}")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Export
    gdf.to_file(output_path, driver='GeoJSON', indent=indent)

    # Calculate file size
    file_size = os.path.getsize(output_path)
    logger.info(f"Exported {len(gdf)} features, file size: {file_size / 1024:.1f} KB")

    return output_path


def calculate_file_checksum(file_path: str) -> str:
    """
    Calculate SHA256 checksum of file.

    Args:
        file_path: Path to file

    Returns:
        Hex digest of checksum
    """
    sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)

    return sha256.hexdigest()


def log_export_version(
    version: str,
    geojson_path: str,
    record_count: int,
    data_year: int
):
    """
    Log export version to database for reproducibility.

    Args:
        version: Version string (e.g., '20260128')
        geojson_path: Path to exported file
        record_count: Number of features exported
        data_year: Data year included in export
    """
    logger.info(f"Logging export version {version}")

    checksum = calculate_file_checksum(geojson_path)

    with get_db() as db:
        sql = text("""
            INSERT INTO export_versions (
                version, export_date, data_year, geojson_path,
                record_count, checksum, metadata
            ) VALUES (
                :version, :export_date, :data_year, :geojson_path,
                :record_count, :checksum, :metadata
            )
            ON CONFLICT (version)
            DO UPDATE SET
                export_date = EXCLUDED.export_date,
                geojson_path = EXCLUDED.geojson_path,
                record_count = EXCLUDED.record_count,
                checksum = EXCLUDED.checksum,
                metadata = EXCLUDED.metadata
        """)

        db.execute(sql, {
            "version": version,
            "export_date": datetime.utcnow(),
            "data_year": data_year,
            "geojson_path": geojson_path,
            "record_count": record_count,
            "checksum": checksum,
            "metadata": json.dumps({
                "file_size_bytes": os.path.getsize(geojson_path),
                "export_tool": "geojson_export.py",
                "crs": "EPSG:4326"
            })
        })

        db.commit()


def run_geojson_export(
    level: str = "county",
    versioned: bool = True
) -> dict:
    """
    Main entry point for GeoJSON export pipeline.

    Args:
        level: Geography level ('county' for V1, 'tract' for future)
        versioned: If True, create dated snapshot in addition to 'latest'

    Returns:
        Dict with export metadata
    """
    logger.info(f"Starting GeoJSON export (level={level}, versioned={versioned})")

    try:
        if level != "county":
            raise ValueError("Only 'county' level supported in V1")

        # Fetch county boundaries
        boundaries_gdf = fetch_maryland_county_boundaries()

        # Fetch latest classifications
        classifications_df = fetch_latest_classifications()

        if classifications_df.empty:
            raise ValueError("No classification data available for export")

        # Merge data
        merged_gdf = merge_geojson_data(boundaries_gdf, classifications_df)

        # Prepare properties
        merged_gdf = prepare_geojson_properties(merged_gdf)

        # Determine data year
        data_year = int(merged_gdf['data_year'].iloc[0])

        # Export latest
        latest_path = os.path.join(settings.EXPORT_DIR, "md_counties_latest.geojson")
        export_geojson(merged_gdf, latest_path, indent=None)  # Compact for production

        # Export versioned snapshot
        versioned_path = None
        if versioned:
            version = datetime.utcnow().strftime("%Y%m%d")
            versioned_path = os.path.join(
                settings.EXPORT_DIR,
                f"md_counties_{version}.geojson"
            )
            export_geojson(merged_gdf, versioned_path, indent=2)  # Readable for archive

            # Log version
            log_export_version(version, versioned_path, len(merged_gdf), data_year)

        # Log success
        log_refresh(
            layer_name="geojson_export",
            data_source="county_classifications",
            status="success",
            records_processed=len(merged_gdf),
            records_inserted=len(merged_gdf),
            metadata={
                "data_year": data_year,
                "output_latest": latest_path,
                "output_versioned": versioned_path
            }
        )

        logger.info("GeoJSON export completed successfully")

        return {
            "level": level,
            "record_count": len(merged_gdf),
            "data_year": data_year,
            "latest_path": latest_path,
            "versioned_path": versioned_path
        }

    except Exception as e:
        logger.error(f"GeoJSON export failed: {e}", exc_info=True)

        log_refresh(
            layer_name="geojson_export",
            data_source="county_classifications",
            status="failed",
            error_message=str(e)
        )

        raise


if __name__ == "__main__":
    import sys
    import argparse
    from src.utils.logging import setup_logging

    setup_logging("geojson_export")

    parser = argparse.ArgumentParser(description="Export Maryland county data to GeoJSON")
    parser.add_argument(
        "--level",
        type=str,
        default="county",
        choices=["county"],
        help="Geography level"
    )
    parser.add_argument(
        "--versioned",
        action="store_true",
        help="Create versioned snapshot"
    )
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help="Only update 'latest' file"
    )

    args = parser.parse_args()

    result = run_geojson_export(
        level=args.level,
        versioned=not args.latest_only
    )

    print(json.dumps(result, indent=2))
