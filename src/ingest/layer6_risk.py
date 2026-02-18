"""
Maryland Viability Atlas - Layer 6: Risk Drag
Ingests environmental and infrastructure risk indicators

Signals Produced:
- Flood risk exposure
- Climate vulnerabilities
- Infrastructure deficiencies
"""

import sys
from datetime import datetime

# Ensure project root is on sys.path when running as a script
from pathlib import Path
from typing import Any, Dict, List, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.database import get_db, log_refresh
from config.settings import MD_COUNTY_FIPS, get_settings
from src.utils.data_sources import fetch_epa_ejscreen, fetch_fema_nfhl
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

NBI_SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Bridge_Inventory/FeatureServer/0"
# Dynamic EJScreen year (data typically lags 1 year)
EJSCREEN_YEAR = datetime.now().year - 1


def _fetch_md_counties() -> gpd.GeoDataFrame:
    import pygris
    from pygris import counties

    # Use previous year for TIGER/Line data (current year not always available)
    tiger_year = datetime.now().year - 1
    try:
        md_counties = counties(state="MD", year=tiger_year, cb=True)
        md_counties["GEOID"] = md_counties["GEOID"].astype(str).str.zfill(5)
        md_counties = md_counties.rename(columns={"GEOID": "fips_code", "NAME": "county_name"})
        md_counties = md_counties[md_counties["fips_code"].isin(MD_COUNTY_FIPS.keys())]
        if md_counties.crs != "EPSG:4326":
            md_counties = md_counties.to_crs("EPSG:4326")
        return md_counties[["fips_code", "county_name", "geometry"]]
    except Exception as e:
        logger.warning(f"pygris TIGER fetch failed: {e}. Falling back to local county GeoJSON.")

    # Fallback to local GeoJSON exports
    export_dir = Path("exports")
    candidates = sorted(export_dir.glob("md_counties_*.geojson"))
    fallback_path = candidates[-1] if candidates else Path("exports/md_counties_latest.geojson")
    if not fallback_path.exists():
        raise RuntimeError("No local county GeoJSON available for fallback.")

    md_counties = gpd.read_file(fallback_path)
    if "fips_code" not in md_counties.columns:
        raise RuntimeError("Fallback GeoJSON missing fips_code column.")
    md_counties["fips_code"] = md_counties["fips_code"].astype(str).str.zfill(5)
    if "county_name" not in md_counties.columns and "NAME" in md_counties.columns:
        md_counties = md_counties.rename(columns={"NAME": "county_name"})
    if md_counties.crs != "EPSG:4326":
        md_counties = md_counties.to_crs("EPSG:4326")
    return md_counties[["fips_code", "county_name", "geometry"]]


def _pick_env_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    for cand in candidates:
        for col in df.columns:
            lower = col.lower()
            if cand.lower() in lower and not any(k in lower for k in ["pct", "pctl", "percent"]):
                return col
    return None


def _compute_sfha_metrics() -> pd.DataFrame:
    logger.info("Fetching FEMA NFHL SFHA polygons")
    if settings.FEMA_SKIP_NFHL:
        logger.warning("FEMA NFHL fetch disabled; skipping SFHA metrics")
        return pd.DataFrame(columns=["fips_code", "sfha_area_sq_mi", "sfha_pct_of_county"])
    counties = _fetch_md_counties()
    if counties.empty:
        return pd.DataFrame()

    counties_proj = counties.to_crs("EPSG:5070")
    county_geom_by_fips = counties_proj.set_index("fips_code")["geometry"].to_dict()
    results = []

    for county in counties[["fips_code", "geometry"]].to_dict(orient="records"):
        fips = county["fips_code"]
        geom = county["geometry"]
        minx, miny, maxx, maxy = geom.bounds

        try:
            geojson = fetch_fema_nfhl("MD", geometry=(minx, miny, maxx, maxy), max_attempts=2)
            features = geojson.get("features", [])
        except Exception as e:
            logger.warning(f"FEMA NFHL failed for {fips}: {e}")
            features = []

        if not features:
            results.append({"fips_code": fips, "sfha_area_sq_mi": None, "sfha_pct_of_county": None})
            continue

        sfha_gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
        sfha_gdf = sfha_gdf[sfha_gdf.geometry.notna()].copy()
        if sfha_gdf.empty:
            results.append({"fips_code": fips, "sfha_area_sq_mi": None, "sfha_pct_of_county": None})
            continue

        sfha_proj = sfha_gdf.to_crs("EPSG:5070")
        county_geom = county_geom_by_fips[fips]
        clipped = gpd.clip(sfha_proj, county_geom)
        sfha_area_m2 = clipped.area.sum() if not clipped.empty else 0.0
        county_area_m2 = county_geom.area
        sfha_area_sq_mi = sfha_area_m2 / 2_589_988.110336
        pct = sfha_area_m2 / county_area_m2 if county_area_m2 else None

        results.append(
            {"fips_code": fips, "sfha_area_sq_mi": sfha_area_sq_mi, "sfha_pct_of_county": pct}
        )

    return pd.DataFrame(results)


def _compute_ejscreen_metrics(year: int = EJSCREEN_YEAR) -> pd.DataFrame:
    logger.info(f"Fetching EPA EJScreen data for {year}")
    try:
        df = fetch_epa_ejscreen(year=year, prefer_zenodo=False)
    except Exception as e:
        logger.warning(f"EJScreen fetch failed; skipping pollution metrics: {e}")
        return pd.DataFrame(columns=["fips_code"])
    if df.empty or "ID" not in df.columns:
        return pd.DataFrame(columns=["fips_code"])

    df["county_fips"] = df["ID"].astype(str).str[:5]
    df = df[df["county_fips"].isin(MD_COUNTY_FIPS.keys())]

    pm25_col = _pick_env_column(df, ["PM25"])
    ozone_col = _pick_env_column(df, ["OZONE"])
    haz_col = _pick_env_column(df, ["TSDF", "HAZWASTE"])
    traffic_col = _pick_env_column(df, ["TRAFFIC"])

    for col in [pm25_col, ozone_col, haz_col, traffic_col]:
        if col:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    agg_map = {}
    if pm25_col:
        agg_map[pm25_col] = "mean"
    if ozone_col:
        agg_map[ozone_col] = "mean"
    if haz_col:
        agg_map[haz_col] = "mean"
    if traffic_col:
        agg_map[traffic_col] = "mean"

    if not agg_map:
        return pd.DataFrame()

    agg = df.groupby("county_fips").agg(agg_map).reset_index()

    result = pd.DataFrame(
        {
            "fips_code": agg["county_fips"],
            "pm25_avg": agg[pm25_col] if pm25_col else pd.NA,
            "ozone_avg": agg[ozone_col] if ozone_col else pd.NA,
            "proximity_hazwaste_score": agg[haz_col] if haz_col else pd.NA,
            "traffic_proximity_score": agg[traffic_col] if traffic_col else pd.NA,
        }
    )

    return result


def _pick_field(fields: list[dict], candidates: list[str]) -> Optional[dict]:
    for cand in candidates:
        for field in fields:
            name = field.get("name", "")
            alias = field.get("alias", "")
            if name.lower() == cand.lower() or alias.lower() == cand.lower():
                return field
    for cand in candidates:
        for field in fields:
            name = field.get("name", "")
            alias = field.get("alias", "")
            if cand.lower() in name.lower() or cand.lower() in alias.lower():
                return field
    return None


def _pick_fields(fields: list[dict], candidates: list[str]) -> list[str]:
    found = []
    for cand in candidates:
        for field in fields:
            name = field.get("name", "")
            alias = field.get("alias", "")
            if cand.lower() == name.lower() or cand.lower() == alias.lower():
                found.append(name)
    if found:
        return list(dict.fromkeys(found))
    for cand in candidates:
        for field in fields:
            name = field.get("name", "")
            alias = field.get("alias", "")
            if cand.lower() in name.lower() or cand.lower() in alias.lower():
                found.append(name)
    return list(dict.fromkeys(found))


def _fetch_nbi_bridge_metrics(state_fips: str = "24", state_abbr: str = "MD") -> pd.DataFrame:
    try:
        meta = requests.get(NBI_SERVICE_URL, params={"f": "json"}, timeout=60).json()
    except Exception as e:
        logger.warning(f"Failed to load NBI metadata: {e}")
        return pd.DataFrame()

    fields = meta.get("fields", [])
    if not fields:
        return pd.DataFrame()

    state_field = _pick_field(
        fields, ["state_fips", "state_code", "state", "state_num", "statefp", "state_id"]
    )
    county_field = _pick_field(
        fields, ["county_fips", "county_code", "county", "countyfp", "cnty_fips", "county_id"]
    )
    deficient_field = _pick_field(
        fields,
        [
            "structurally_deficient",
            "structdef",
            "struct_def",
            "sd",
            "deficient",
            "structural_deficiency",
            "structurally_deficient_ind",
        ],
    )

    condition_fields = []
    if not deficient_field:
        condition_fields = _pick_fields(
            fields,
            [
                "deck_cond",
                "superstructure_cond",
                "substructure_cond",
                "culvert_cond",
                "overall_cond",
                "structural_eval",
                "structural_rating",
            ],
        )

    if not state_field or not county_field:
        logger.warning("NBI fields not detected for state/county")
        return pd.DataFrame()
    if not deficient_field and not condition_fields:
        logger.warning("NBI fields not detected for deficiency or condition ratings")
        return pd.DataFrame()

    state_field_name = state_field["name"]
    county_field_name = county_field["name"]
    deficient_field_name = deficient_field["name"] if deficient_field else None

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
            "outFields": ",".join(
                [state_field_name, county_field_name]
                + ([deficient_field_name] if deficient_field_name else [])
                + condition_fields
            ),
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
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
    df["fips_code"] = state_fips.zfill(2) + df[county_field_name]
    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())].copy()

    if deficient_field_name:

        def _is_deficient(val) -> bool:
            if pd.isna(val):
                return False
            if isinstance(val, str):
                return val.strip().upper() in {"Y", "YES", "1", "T", "TRUE"}
            try:
                return int(val) == 1
            except Exception:
                return False

        df["is_deficient"] = df[deficient_field_name].apply(_is_deficient)
    else:
        for name in condition_fields:
            df[name] = pd.to_numeric(df[name], errors="coerce")
        df["is_deficient"] = df[condition_fields].le(4).any(axis=1).fillna(False)
    agg = df.groupby("fips_code", as_index=False).agg(
        bridges_total=("is_deficient", "size"),
        bridges_structurally_deficient=("is_deficient", "sum"),
    )
    agg["bridges_deficient_pct"] = agg["bridges_structurally_deficient"] / agg["bridges_total"]
    return agg


def calculate_risk_indicators(data_year: int = 2025) -> pd.DataFrame:
    """Calculate risk drag indicators."""
    sfha_df = _compute_sfha_metrics()
    ej_df = _compute_ejscreen_metrics(year=data_year)
    nbi_df = _fetch_nbi_bridge_metrics()

    if sfha_df.empty and ej_df.empty and nbi_df.empty:
        return pd.DataFrame()

    df = pd.DataFrame({"fips_code": list(MD_COUNTY_FIPS.keys())})
    df = df.merge(sfha_df, on="fips_code", how="left")
    df = df.merge(ej_df, on="fips_code", how="left")
    df = df.merge(nbi_df, on="fips_code", how="left")

    df["data_year"] = data_year
    df["sea_level_rise_exposure"] = pd.NA
    df["extreme_heat_days_annual"] = pd.NA

    required_cols = [
        "sfha_area_sq_mi",
        "sfha_pct_of_county",
        "pm25_avg",
        "ozone_avg",
        "proximity_hazwaste_score",
        "traffic_proximity_score",
        "bridges_total",
        "bridges_structurally_deficient",
        "bridges_deficient_pct",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    risk_fields = [
        "sfha_pct_of_county",
        "pm25_avg",
        "ozone_avg",
        "proximity_hazwaste_score",
        "traffic_proximity_score",
        "bridges_deficient_pct",
    ]

    rank_components = []
    for field in risk_fields:
        if field in df.columns and df[field].notna().sum() >= 3:
            rank_components.append(df[field].rank(pct=True))

    df["risk_drag_index"] = pd.NA
    if rank_components:
        df["risk_drag_index"] = pd.concat(rank_components, axis=1).mean(axis=1)

    df.replace([np.inf, -np.inf], pd.NA, inplace=True)
    return df


def store_risk_data(df: pd.DataFrame):
    """Store risk drag data in database."""
    logger.info(f"Storing {len(df)} risk records")

    with get_db() as db:
        years = df["data_year"].unique().tolist()
        db.execute(
            text("DELETE FROM layer6_risk_drag WHERE data_year = ANY(:years)"), {"years": years}
        )

        insert_sql = text(
            """
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
        """
        )

        rows = _build_risk_rows(df)
        if rows:
            db.execute(insert_sql, rows)

        db.commit()

    logger.info("✓ Risk data stored successfully")


def _build_risk_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        row_dict: Dict[str, Any] = {}
        for key, value in row.items():
            row_dict[key] = None if pd.isna(value) else value
        rows.append(row_dict)
    return rows


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
                metadata={"data_year": data_year},
            )
            return

        store_risk_data(df)

        log_refresh(
            layer_name="layer6_risk_drag",
            data_source="FEMA+EPA+NBI",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={"data_year": data_year},
        )

        logger.info("✓ Layer 6 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 6 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer6_risk_drag",
            data_source="FEMA+EPA+NBI",
            status="failed",
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    main()
