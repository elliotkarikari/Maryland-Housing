"""
Maryland Viability Atlas - Layer 6: Risk Drag
Ingests environmental and infrastructure risk indicators

Signals Produced:
- Flood risk exposure
- Climate vulnerabilities
- Infrastructure deficiencies
"""

import sys
import pandas as pd
import numpy as np
import geopandas as gpd
import requests
from sqlalchemy import text
from typing import Optional

# Ensure project root is on sys.path when running as a script
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.data_sources import fetch_fema_nfhl, fetch_epa_ejscreen
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

NBI_SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Bridge_Inventory/FeatureServer/0"
EJSCREEN_YEAR = 2023


def _fetch_md_counties() -> gpd.GeoDataFrame:
    import pygris
    from pygris import counties

    md_counties = counties(state="MD", year=2023, cb=True)
    md_counties['GEOID'] = md_counties['GEOID'].astype(str).str.zfill(5)
    md_counties = md_counties.rename(columns={'GEOID': 'fips_code', 'NAME': 'county_name'})
    md_counties = md_counties[md_counties['fips_code'].isin(MD_COUNTY_FIPS.keys())]
    if md_counties.crs != 'EPSG:4326':
        md_counties = md_counties.to_crs('EPSG:4326')
    return md_counties[['fips_code', 'county_name', 'geometry']]


def _pick_env_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    for cand in candidates:
        for col in df.columns:
            lower = col.lower()
            if cand.lower() in lower and not any(k in lower for k in ['pct', 'pctl', 'percent']):
                return col
    return None


def _compute_sfha_metrics() -> pd.DataFrame:
    logger.info("Fetching FEMA NFHL SFHA polygons")
    counties = _fetch_md_counties()
    if counties.empty:
        return pd.DataFrame()

    counties_proj = counties.to_crs('EPSG:5070')
    results = []

    for _, county in counties.iterrows():
        fips = county['fips_code']
        geom = county['geometry']
        minx, miny, maxx, maxy = geom.bounds

        try:
            geojson = fetch_fema_nfhl("MD", geometry=(minx, miny, maxx, maxy))
            features = geojson.get("features", [])
        except Exception as e:
            logger.warning(f"FEMA NFHL failed for {fips}: {e}")
            features = []

        if not features:
            results.append({
                "fips_code": fips,
                "sfha_area_sq_mi": None,
                "sfha_pct_of_county": None
            })
            continue

        sfha_gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
        sfha_gdf = sfha_gdf[sfha_gdf.geometry.notna()].copy()
        if sfha_gdf.empty:
            results.append({
                "fips_code": fips,
                "sfha_area_sq_mi": None,
                "sfha_pct_of_county": None
            })
            continue

        sfha_proj = sfha_gdf.to_crs('EPSG:5070')
        county_geom = counties_proj[counties_proj['fips_code'] == fips].iloc[0]['geometry']
        clipped = gpd.clip(sfha_proj, county_geom)
        sfha_area_m2 = clipped.area.sum() if not clipped.empty else 0.0
        county_area_m2 = county_geom.area
        sfha_area_sq_mi = sfha_area_m2 / 2_589_988.110336
        pct = sfha_area_m2 / county_area_m2 if county_area_m2 else None

        results.append({
            "fips_code": fips,
            "sfha_area_sq_mi": sfha_area_sq_mi,
            "sfha_pct_of_county": pct
        })

    return pd.DataFrame(results)


def _compute_ejscreen_metrics(year: int = EJSCREEN_YEAR) -> pd.DataFrame:
    logger.info(f"Fetching EPA EJScreen data for {year}")
    df = fetch_epa_ejscreen(year=year)
    if df.empty or 'ID' not in df.columns:
        return pd.DataFrame()

    df['county_fips'] = df['ID'].astype(str).str[:5]
    df = df[df['county_fips'].isin(MD_COUNTY_FIPS.keys())]

    pm25_col = _pick_env_column(df, ["PM25"])
    ozone_col = _pick_env_column(df, ["OZONE"])
    haz_col = _pick_env_column(df, ["TSDF", "HAZWASTE"])
    traffic_col = _pick_env_column(df, ["TRAFFIC"])

    for col in [pm25_col, ozone_col, haz_col, traffic_col]:
        if col:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    agg_map = {}
    if pm25_col:
        agg_map[pm25_col] = 'mean'
    if ozone_col:
        agg_map[ozone_col] = 'mean'
    if haz_col:
        agg_map[haz_col] = 'mean'
    if traffic_col:
        agg_map[traffic_col] = 'mean'

    if not agg_map:
        return pd.DataFrame()

    agg = df.groupby('county_fips').agg(agg_map).reset_index()

    result = pd.DataFrame({
        "fips_code": agg['county_fips'],
        "pm25_avg": agg[pm25_col] if pm25_col else pd.NA,
        "ozone_avg": agg[ozone_col] if ozone_col else pd.NA,
        "proximity_hazwaste_score": agg[haz_col] if haz_col else pd.NA,
        "traffic_proximity_score": agg[traffic_col] if traffic_col else pd.NA,
    })

    return result


def _pick_field(fields: list[dict], candidates: list[str]) -> Optional[dict]:
    for cand in candidates:
        for field in fields:
            if field.get('name', '').lower() == cand.lower():
                return field
    for cand in candidates:
        for field in fields:
            if cand.lower() in field.get('name', '').lower():
                return field
    return None


def _fetch_nbi_bridge_metrics(state_fips: str = "24", state_abbr: str = "MD") -> pd.DataFrame:
    try:
        meta = requests.get(NBI_SERVICE_URL, params={"f": "json"}, timeout=60).json()
    except Exception as e:
        logger.warning(f"Failed to load NBI metadata: {e}")
        return pd.DataFrame()

    fields = meta.get("fields", [])
    if not fields:
        return pd.DataFrame()

    state_field = _pick_field(fields, ["state_fips", "state_code", "state", "state_num"])
    county_field = _pick_field(fields, ["county_fips", "county_code", "county"])
    deficient_field = _pick_field(fields, ["structurally_deficient", "structdef", "sd", "deficient"])

    if not state_field or not county_field or not deficient_field:
        logger.warning("NBI fields not detected for state/county/deficiency")
        return pd.DataFrame()

    state_field_name = state_field['name']
    county_field_name = county_field['name']
    deficient_field_name = deficient_field['name']

    if state_field.get("type") == "esriFieldTypeString":
        state_where = f"{state_field_name}='{state_fips}' OR {state_field_name}='{state_abbr}'"
    else:
        state_where = f"{state_field_name}={int(state_fips)}"

    records = []
    offset = 0
    page_size = 2000
    while True:
        params = {
            "where": state_where,
            "outFields": f"{state_field_name},{county_field_name},{deficient_field_name}",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size
        }
        resp = requests.get(f"{NBI_SERVICE_URL}/query", params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        feats = data.get("features", [])
        for feat in feats:
            attrs = feat.get("attributes", {})
            records.append(attrs)

        if not data.get("exceededTransferLimit") and len(feats) < page_size:
            break
        offset += page_size

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df[county_field_name] = df[county_field_name].astype(str).str.zfill(3)
    df['fips_code'] = state_fips.zfill(2) + df[county_field_name]
    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())].copy()

    def _is_deficient(val) -> bool:
        if pd.isna(val):
            return False
        if isinstance(val, str):
            return val.strip().upper() in {"Y", "YES", "1", "T", "TRUE"}
        try:
            return int(val) == 1
        except Exception:
            return False

    df['is_deficient'] = df[deficient_field_name].apply(_is_deficient)
    agg = df.groupby('fips_code', as_index=False).agg(
        bridges_total=('is_deficient', 'size'),
        bridges_structurally_deficient=('is_deficient', 'sum')
    )
    agg['bridges_deficient_pct'] = agg['bridges_structurally_deficient'] / agg['bridges_total']
    return agg


def calculate_risk_indicators(data_year: int = 2025) -> pd.DataFrame:
    """Calculate risk drag indicators."""
    sfha_df = _compute_sfha_metrics()
    ej_df = _compute_ejscreen_metrics()
    nbi_df = _fetch_nbi_bridge_metrics()

    if sfha_df.empty and ej_df.empty and nbi_df.empty:
        return pd.DataFrame()

    df = pd.DataFrame({"fips_code": list(MD_COUNTY_FIPS.keys())})
    df = df.merge(sfha_df, on='fips_code', how='left')
    df = df.merge(ej_df, on='fips_code', how='left')
    df = df.merge(nbi_df, on='fips_code', how='left')

    df['data_year'] = data_year
    df['sea_level_rise_exposure'] = pd.NA
    df['extreme_heat_days_annual'] = pd.NA

    risk_fields = [
        'sfha_pct_of_county',
        'pm25_avg',
        'ozone_avg',
        'proximity_hazwaste_score',
        'traffic_proximity_score',
        'bridges_deficient_pct'
    ]

    rank_components = []
    for field in risk_fields:
        if field in df.columns and df[field].notna().sum() >= 3:
            rank_components.append(df[field].rank(pct=True))

    df['risk_drag_index'] = pd.NA
    if rank_components:
        df['risk_drag_index'] = pd.concat(rank_components, axis=1).mean(axis=1)

    df.replace([np.inf, -np.inf], pd.NA, inplace=True)
    return df


def store_risk_data(df: pd.DataFrame):
    """Store risk drag data in database."""
    logger.info(f"Storing {len(df)} risk records")

    with get_db() as db:
        years = df['data_year'].unique().tolist()
        db.execute(text("DELETE FROM layer6_risk_drag WHERE data_year = ANY(:years)"),
                   {"years": years})

        insert_sql = text("""
            INSERT INTO layer6_risk_drag (
                fips_code, data_year,
                sfha_area_sq_mi, sfha_pct_of_county,
                sea_level_rise_exposure, extreme_heat_days_annual,
                pm25_avg, ozone_avg,
                proximity_hazwaste_score, traffic_proximity_score,
                bridges_total, bridges_structurally_deficient, bridges_deficient_pct,
                risk_drag_index
            ) VALUES (
                :fips_code, :data_year,
                :sfha_area_sq_mi, :sfha_pct_of_county,
                :sea_level_rise_exposure, :extreme_heat_days_annual,
                :pm25_avg, :ozone_avg,
                :proximity_hazwaste_score, :traffic_proximity_score,
                :bridges_total, :bridges_structurally_deficient, :bridges_deficient_pct,
                :risk_drag_index
            )
        """)

        for _, row in df.iterrows():
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ Risk data stored successfully")


def main():
    """Main execution for Layer 6 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 6: RISK DRAG INGESTION")
        logger.info("=" * 60)

        data_year = EJSCREEN_YEAR
        df = calculate_risk_indicators(data_year)

        if df.empty:
            logger.error("No risk data to store (real data not available)")
            log_refresh(
                layer_name="layer6_risk_drag",
                data_source="FEMA+EPA+NBI",
                status="failed",
                error_message="No records produced",
                metadata={"data_year": data_year}
            )
            return

        store_risk_data(df)

        log_refresh(
            layer_name="layer6_risk_drag",
            data_source="FEMA+EPA+NBI",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={"data_year": data_year}
        )

        logger.info("✓ Layer 6 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 6 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer6_risk_drag",
            data_source="FEMA+EPA+NBI",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
