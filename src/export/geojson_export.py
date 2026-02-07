"""
Maryland Viability Atlas - GeoJSON Export
Generates map-ready GeoJSON outputs with county boundaries and classifications

Data sources:
- Geometries: US Census TIGER/Line (via pygris)
- Synthesis: Database (final_synthesis_current table)

Outputs:
- exports/md_counties_latest.geojson (always current)
- exports/md_counties_{YYYYMMDD}.geojson (versioned snapshots)
"""

import geopandas as gpd
import pandas as pd
import json
import os
from datetime import datetime, timezone
from typing import Optional
import hashlib

from config.database import get_db, log_refresh
from config.settings import get_settings
from src.utils.logging import get_logger

_USE_DATABRICKS = get_settings().DATA_BACKEND == "databricks"

if not _USE_DATABRICKS:
    from sqlalchemy import text

logger = get_logger(__name__)
settings = get_settings()


def fetch_maryland_county_boundaries() -> gpd.GeoDataFrame:
    """
    Fetch Maryland county boundaries.

    When using Databricks, reads geometry_geojson from the md_counties Delta table.
    Otherwise, fetches from Census TIGER/Line via pygris.

    Returns:
        GeoDataFrame with county geometries
    """
    if _USE_DATABRICKS:
        return _fetch_boundaries_from_databricks()
    return _fetch_boundaries_from_pygris()


def _fetch_boundaries_from_databricks() -> gpd.GeoDataFrame:
    """Load county geometries stored as GeoJSON strings in Delta table."""
    from shapely.geometry import shape

    logger.info("Fetching county boundaries from Databricks Delta table")

    with get_db() as db:
        result = db.execute(
            "SELECT fips_code, county_name, geometry_geojson FROM md_counties"
        )
        rows = result.fetchall()

    if not rows:
        raise ValueError("No county records found in Databricks md_counties table")

    records = []
    for row in rows:
        geom = shape(json.loads(row.geometry_geojson)) if row.geometry_geojson else None
        records.append({
            'fips_code': row.fips_code,
            'county_name': row.county_name,
            'geometry': geom,
        })

    gdf = gpd.GeoDataFrame(records, crs='EPSG:4326')
    logger.info(f"Loaded {len(gdf)} county boundaries from Databricks")
    return gdf


def _fetch_boundaries_from_pygris() -> gpd.GeoDataFrame:
    """Fetch county boundaries from Census TIGER/Line via pygris."""
    logger.info("Fetching Maryland county boundaries from Census TIGER/Line")

    try:
        from pygris import counties

        md_counties = counties(state="MD", year=2023, cb=True)

        md_counties['GEOID'] = md_counties['GEOID'].astype(str).str.zfill(5)

        md_counties = md_counties.rename(columns={
            'GEOID': 'fips_code',
            'NAME': 'county_name',
            'ALAND': 'land_area_m2',
            'AWATER': 'water_area_m2'
        })

        if md_counties.crs != 'EPSG:4326':
            md_counties = md_counties.to_crs('EPSG:4326')

        md_counties = md_counties[['fips_code', 'county_name', 'geometry']]

        logger.info(f"Fetched {len(md_counties)} Maryland county boundaries")

        return md_counties

    except Exception as e:
        logger.error(f"Failed to fetch county boundaries: {e}", exc_info=True)
        raise


def _identify_top_strengths(layer_scores: dict, top_n: int = 2) -> list:
    valid_scores = {k: v for k, v in layer_scores.items() if pd.notna(v)}
    if not valid_scores:
        return []
    sorted_layers = sorted(valid_scores.items(), key=lambda x: x[1], reverse=True)
    return [name for name, _ in sorted_layers[:top_n]]


def _identify_top_weaknesses(layer_scores: dict, top_n: int = 2) -> list:
    valid_scores = {k: v for k, v in layer_scores.items() if pd.notna(v)}
    if not valid_scores:
        return []
    sorted_layers = sorted(valid_scores.items(), key=lambda x: x[1], reverse=False)
    return [name for name, _ in sorted_layers[:top_n]]


def _generate_explainability_payload(
    directional_class: str,
    confidence_class: str,
    risk_drag_score: float,
    layer_scores: dict
) -> dict:
    layer_names = {
        "employment_gravity": "Employment Gravity",
        "mobility_optionality": "Mobility Optionality",
        "school_trajectory": "School System Trajectory",
        "housing_elasticity": "Housing Elasticity",
        "demographic_momentum": "Demographic Momentum",
        "risk_drag": "Composite Risk Drag"
    }

    strengths = _identify_top_strengths(layer_scores, top_n=2)
    weaknesses = _identify_top_weaknesses(layer_scores, top_n=2)

    primary_strengths = [layer_names.get(s, s) for s in strengths]
    primary_weaknesses = [layer_names.get(w, w) for w in weaknesses]

    key_trends = []
    if directional_class == 'improving':
        key_trends.append("Multiple reinforcing structural tailwinds present")
    elif directional_class == 'at_risk':
        key_trends.append("Structural headwinds constraining growth capacity")
    else:
        key_trends.append("Balanced signals, mixed pressure directions")

    if confidence_class == 'strong':
        key_trends.append("High policy delivery reliability")
    elif confidence_class == 'fragile':
        key_trends.append("Low policy follow-through, high uncertainty")

    if pd.notna(risk_drag_score) and risk_drag_score >= 0.5:
        key_trends.append("Elevated environmental or infrastructure risk")

    return {
        "primary_strengths": primary_strengths,
        "primary_weaknesses": primary_weaknesses,
        "key_trends": key_trends
    }


def fetch_latest_synthesis() -> pd.DataFrame:
    """
    Fetch latest multi-year synthesis and layer scores from database.

    Returns:
        DataFrame with all classification and score data
    """
    logger.info("Fetching latest synthesis from database")

    synthesis_sql = """
        SELECT
            geoid AS fips_code,
            current_as_of_year AS data_year,
            final_grouping,
            directional_status,
            confidence_level,
            uncertainty_level,
            uncertainty_reasons,
            composite_score,
            employment_gravity_score,
            mobility_optionality_score,
            school_trajectory_score,
            housing_elasticity_score,
            demographic_momentum_score,
            risk_drag_score,
            classification_version,
            updated_at
        FROM final_synthesis_current
    """

    with get_db() as db:
        if _USE_DATABRICKS:
            result = db.execute(synthesis_sql)
            rows = result.fetchall()
            if rows:
                cols = [d[0] for d in result._cursor.description] if hasattr(result, '_cursor') else [
                    'fips_code', 'data_year', 'final_grouping', 'directional_status',
                    'confidence_level', 'uncertainty_level', 'uncertainty_reasons',
                    'composite_score', 'employment_gravity_score',
                    'mobility_optionality_score', 'school_trajectory_score',
                    'housing_elasticity_score', 'demographic_momentum_score',
                    'risk_drag_score', 'classification_version', 'updated_at'
                ]
                df = pd.DataFrame([list(r) for r in rows], columns=cols)
            else:
                df = pd.DataFrame()
        else:
            df = pd.read_sql(text(synthesis_sql), db.connection())

    if df.empty:
        logger.warning("No synthesis records found in database")
        return pd.DataFrame()

    # Backfill null layer scores from individual layer tables
    _BACKFILL_CONFIGS = {
        'employment_gravity_score': ('layer1_employment_gravity', 'economic_opportunity_index'),
        'mobility_optionality_score': ('layer2_mobility_optionality', 'mobility_optionality_index'),
        'school_trajectory_score': ('layer3_school_trajectory', 'education_opportunity_index'),
        'housing_elasticity_score': ('layer4_housing_elasticity', 'housing_opportunity_index'),
        'demographic_momentum_score': ('layer5_demographic_momentum', 'demographic_opportunity_index'),
        'risk_drag_score': ('layer6_risk_drag', 'risk_drag_index'),
    }
    for score_col, (table, primary_col) in _BACKFILL_CONFIGS.items():
        if score_col in df.columns and df[score_col].isna().any():
            try:
                with get_db() as db2:
                    backfill_sql = f"""
                        SELECT fips_code, {primary_col} AS val
                        FROM {table}
                        WHERE {primary_col} IS NOT NULL
                        ORDER BY data_year DESC
                    """
                    if _USE_DATABRICKS:
                        result = db2.execute(backfill_sql)
                        rows = result.fetchall()
                        backfill_df = pd.DataFrame(
                            [{'fips_code': r[0], 'val': r[1]} for r in rows]
                        ) if rows else pd.DataFrame()
                    else:
                        backfill_df = pd.read_sql(
                            text(backfill_sql), db2.connection()
                        )
                if not backfill_df.empty:
                    backfill_map = backfill_df.drop_duplicates('fips_code').set_index('fips_code')['val']
                    mask = df[score_col].isna()
                    df.loc[mask, score_col] = df.loc[mask, 'fips_code'].map(backfill_map)
                    filled = mask.sum() - df[score_col].isna().sum()
                    if filled > 0:
                        logger.info(f"Backfilled {filled} null values for {score_col}")
            except Exception as e:
                logger.warning(f"Could not backfill {score_col}: {e}")

    # Map V2 fields to V1-compatible property names for frontend
    df['synthesis_grouping'] = df['final_grouping']
    df['directional_class'] = df['directional_status']
    df['confidence_class'] = df['confidence_level']

    # Generate explainability fields (not stored in V2 table)
    explainability = []
    for _, row in df.iterrows():
        layer_scores = {
            "employment_gravity": row.get('employment_gravity_score'),
            "mobility_optionality": row.get('mobility_optionality_score'),
            "school_trajectory": row.get('school_trajectory_score'),
            "housing_elasticity": row.get('housing_elasticity_score'),
            "demographic_momentum": row.get('demographic_momentum_score'),
            "risk_drag": row.get('risk_drag_score')
        }
        payload = _generate_explainability_payload(
            directional_class=row.get('directional_status'),
            confidence_class=row.get('confidence_level'),
            risk_drag_score=row.get('risk_drag_score'),
            layer_scores=layer_scores
        )
        explainability.append(payload)

    explainability_df = pd.DataFrame(explainability)
    df = pd.concat([df.reset_index(drop=True), explainability_df], axis=1)

    logger.info(f"Fetched synthesis for {len(df)} counties")

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
    merged['last_updated'] = datetime.now(timezone.utc).isoformat()

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

    # Normalize common list-like fields
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
            # Coerce mixed/object columns to numeric before rounding to avoid object-dtype ufunc failures.
            numeric_series = pd.to_numeric(gdf[col], errors='coerce')
            if numeric_series.notna().any():
                gdf[col] = numeric_series.round(4)

    # Convert list/dict/object fields to JSON strings for GeoJSON compatibility
    import json

    def safe_json_dumps(x):
        if x is None:
            return None
        try:
            if isinstance(x, float) and pd.isna(x):
                return None
            return json.dumps(x)
        except (TypeError, ValueError):
            return None

    # Known array/json fields
    array_cols = [
        'primary_strengths', 'primary_weaknesses', 'key_trends',
        'uncertainty_reasons', 'per_layer_coverage', 'drivers', 'constraints',
        'coverage_summary'
    ]
    for col in array_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].apply(safe_json_dumps)

    # Fallback: stringify any remaining list/dict values in object columns
    object_cols = [c for c in gdf.columns if gdf[c].dtype == 'object']
    for col in object_cols:
        def coerce_obj(v):
            if isinstance(v, (list, dict, tuple)):
                return safe_json_dumps(v)
            return v
        gdf[col] = gdf[col].apply(coerce_obj)

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

    meta_json = json.dumps({
        "file_size_bytes": os.path.getsize(geojson_path),
        "export_tool": "geojson_export.py",
        "crs": "EPSG:4326"
    })

    with get_db() as db:
        if _USE_DATABRICKS:
            # Delete + insert (no ON CONFLICT in Databricks)
            db.execute(
                "DELETE FROM export_versions WHERE version = :version",
                {"version": version}
            )
            db.execute(
                """INSERT INTO export_versions
                   (version, export_date, data_year, geojson_path,
                    record_count, checksum, metadata)
                   VALUES (:version, :export_date, :data_year, :geojson_path,
                           :record_count, :checksum, :metadata)""",
                {
                    "version": version,
                    "export_date": datetime.now(timezone.utc).isoformat(),
                    "data_year": data_year,
                    "geojson_path": geojson_path,
                    "record_count": record_count,
                    "checksum": checksum,
                    "metadata": meta_json,
                }
            )
        else:
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
                "export_date": datetime.now(timezone.utc),
                "data_year": data_year,
                "geojson_path": geojson_path,
                "record_count": record_count,
                "checksum": checksum,
                "metadata": meta_json,
            })

            db.commit()


def _upload_to_blob_storage(local_path: str, blob_name: str):
    """Upload a file to Azure Blob Storage."""
    try:
        from azure.storage.blob import BlobServiceClient, ContentSettings

        blob_service = BlobServiceClient.from_connection_string(
            settings.AZURE_STORAGE_CONNECTION_STRING
        )
        container = blob_service.get_container_client(
            settings.AZURE_STORAGE_CONTAINER
        )

        # Ensure container exists
        try:
            container.get_container_properties()
        except Exception:
            container.create_container(public_access='blob')
            logger.info(f"Created blob container: {settings.AZURE_STORAGE_CONTAINER}")

        with open(local_path, 'rb') as data:
            container.upload_blob(
                name=blob_name,
                data=data,
                overwrite=True,
                content_settings=ContentSettings(
                    content_type='application/geo+json',
                    cache_control='public, max-age=3600'
                )
            )

        blob_url = f"{container.url}/{blob_name}"
        logger.info(f"Uploaded to Azure Blob Storage: {blob_url}")

    except ImportError:
        logger.warning("azure-storage-blob not installed, skipping blob upload")
    except Exception as e:
        logger.error(f"Failed to upload to Azure Blob Storage: {e}")


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

        # Fetch latest synthesis
        classifications_df = fetch_latest_synthesis()

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
            version = datetime.now(timezone.utc).strftime("%Y%m%d")
            versioned_path = os.path.join(
                settings.EXPORT_DIR,
                f"md_counties_{version}.geojson"
            )
            export_geojson(merged_gdf, versioned_path, indent=2)  # Readable for archive

            # Log version
            log_export_version(version, versioned_path, len(merged_gdf), data_year)

        # Upload to Azure Blob Storage if configured
        if settings.AZURE_STORAGE_CONNECTION_STRING:
            _upload_to_blob_storage(latest_path, "md_counties_latest.geojson")
            if versioned_path:
                blob_name = f"md_counties_{version}.geojson"
                _upload_to_blob_storage(versioned_path, blob_name)

        # Copy to frontend directory for local dev
        frontend_geojson = os.path.join("frontend", "md_counties_latest.geojson")
        if os.path.exists("frontend"):
            import shutil
            shutil.copy2(latest_path, frontend_geojson)
            logger.info(f"Copied latest GeoJSON to {frontend_geojson}")

        # Log success
        log_refresh(
            layer_name="geojson_export",
            data_source="final_synthesis_current",
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
            data_source="final_synthesis_current",
            status="failed",
            error_message=str(e)
        )

        raise


def main():
    """CLI entry point (also used by Databricks wheel task)."""
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


if __name__ == "__main__":
    main()
