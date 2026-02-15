"""
Maryland Viability Atlas - API Routes
Endpoints for map data and metadata
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, NotRequired, Optional, TypedDict

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.database import DATABASE_BACKEND, get_db_session
from config.settings import MD_COUNTY_FIPS, get_settings
from src.utils.logging import get_logger

router = APIRouter()
settings = get_settings()
logger = get_logger(__name__)

COUNTIES_DEFAULT_GROUPING = "high_uncertainty"
COUNTIES_DEFAULT_DIRECTIONAL = "stable"
COUNTIES_DEFAULT_CONFIDENCE = "fragile"

_counties_geojson_cache: Optional[Dict[str, Any]] = None

LAYER_LATEST_SNAPSHOT_CONFIG: Dict[str, Dict[str, str]] = {
    "employment_gravity": {
        "table": "layer1_employment_gravity",
        "column": "economic_opportunity_index",
    },
    "mobility_optionality": {
        "table": "layer2_mobility_optionality",
        "column": "mobility_optionality_index",
    },
    "school_trajectory": {
        "table": "layer3_school_trajectory",
        "column": "education_opportunity_index",
    },
    "housing_elasticity": {
        "table": "layer4_housing_elasticity",
        "column": "housing_opportunity_index",
    },
    "demographic_momentum": {
        "table": "layer5_demographic_momentum",
        "column": "demographic_opportunity_index",
    },
    "risk_drag": {
        "table": "layer6_risk_drag",
        "column": "risk_drag_index",
    },
}


# Response models
class LayerFactor(BaseModel):
    """Individual factor contributing to a layer score"""

    name: str
    value: Optional[float] = None
    formatted_value: Optional[str] = None
    description: str
    weight: Optional[float] = None
    trend: Optional[str] = None  # 'up', 'down', 'stable', None
    trend_value: Optional[float] = None


class LayerDetail(BaseModel):
    """Detailed breakdown of a single layer"""

    layer_key: str
    display_name: str
    score: Optional[float]
    version: str  # 'v1', 'v2', etc.
    formula: str
    description: str
    factors: List[LayerFactor]
    momentum_slope: Optional[float] = None
    momentum_direction: Optional[str] = None
    data_year: int
    coverage_years: Optional[int] = None


class AreaDetail(BaseModel):
    """Detailed information for a single area"""

    fips_code: str
    county_name: str
    data_year: int
    directional_class: str
    confidence_class: str
    synthesis_grouping: str  # PRIMARY MAP LAYER - Final synthesis grouping
    composite_score: Optional[float]
    layer_scores: dict
    primary_strengths: List[str]
    primary_weaknesses: List[str]
    key_trends: List[str]
    last_updated: str


class RefreshStatus(BaseModel):
    """Latest data refresh status"""

    layer_name: str
    data_source: str
    refresh_date: str
    status: str
    records_processed: Optional[int]


class DataSource(BaseModel):
    """Data source documentation"""

    name: str
    agency: str
    url: str
    update_frequency: str
    latest_available: str


class CapabilitiesResponse(BaseModel):
    """Runtime capabilities available to clients."""

    chat_enabled: bool
    ai_enabled: bool
    api_version: str
    year_policy: Dict[str, Any]


class LayerFactorConfig(TypedDict):
    col: str
    name: str
    desc: str
    weight: Optional[float]
    invert: NotRequired[bool]


class LayerConfig(TypedDict):
    table: str
    display_name: str
    description: str
    version: str
    formula: str
    factors: List[LayerFactorConfig]


def _load_data_sources_registry() -> List[DataSource]:
    """Load data source metadata from config registry."""
    registry_path = os.path.join("config", "data_sources_registry.json")

    if not os.path.exists(registry_path):
        logger.warning(f"Data source registry not found at {registry_path}")
        return []

    with open(registry_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    sources: List[DataSource] = []
    for entry in payload:
        sources.append(
            DataSource(
                name=entry["name"],
                agency=entry["agency"],
                url=entry["url"],
                update_frequency=entry["update_frequency"],
                latest_available=entry["latest_available"],
            )
        )

    return sources


def _identify_top_strengths(layer_scores: dict, top_n: int = 2) -> List[str]:
    valid_scores = {k: v for k, v in layer_scores.items() if v is not None}
    if not valid_scores:
        return []
    sorted_layers = sorted(valid_scores.items(), key=lambda x: x[1], reverse=True)
    return [name for name, _ in sorted_layers[:top_n]]


def _identify_top_weaknesses(layer_scores: dict, top_n: int = 2) -> List[str]:
    valid_scores = {k: v for k, v in layer_scores.items() if v is not None}
    if not valid_scores:
        return []
    sorted_layers = sorted(valid_scores.items(), key=lambda x: x[1])
    return [name for name, _ in sorted_layers[:top_n]]


def _generate_explainability_payload(
    directional_class: str,
    confidence_class: str,
    risk_drag_score: Optional[float],
    layer_scores: dict,
) -> dict:
    layer_names = {
        "employment_gravity": "Employment Gravity",
        "mobility_optionality": "Mobility Optionality",
        "school_trajectory": "School System Trajectory",
        "housing_elasticity": "Housing Elasticity",
        "demographic_momentum": "Demographic Momentum",
    }

    strengths = _identify_top_strengths(layer_scores, top_n=2)
    weaknesses = _identify_top_weaknesses(layer_scores, top_n=2)

    primary_strengths = [layer_names.get(s, s) for s in strengths]
    primary_weaknesses = [layer_names.get(w, w) for w in weaknesses]

    key_trends = []
    if directional_class == "improving":
        key_trends.append("Multiple reinforcing structural tailwinds present")
    elif directional_class == "at_risk":
        key_trends.append("Structural headwinds constraining growth capacity")
    else:
        key_trends.append("Balanced signals, mixed pressure directions")

    if confidence_class == "strong":
        key_trends.append("High policy delivery reliability")
    elif confidence_class == "fragile":
        key_trends.append("Low policy follow-through, high uncertainty")

    if risk_drag_score is not None and risk_drag_score >= 0.5:
        key_trends.append("Elevated environmental or infrastructure risk")

    return {
        "primary_strengths": primary_strengths,
        "primary_weaknesses": primary_weaknesses,
        "key_trends": key_trends,
    }


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coalesce_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _parse_geometry(raw_geometry: Any) -> Optional[Dict[str, Any]]:
    if raw_geometry is None:
        return None

    if isinstance(raw_geometry, dict):
        return raw_geometry

    if isinstance(raw_geometry, str):
        try:
            parsed = json.loads(raw_geometry)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return None

    return None


def _parse_json_list(raw_value: Any) -> List[Any]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return raw_value
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _derive_directional_status(composite_score: Optional[float]) -> str:
    if composite_score is None:
        return COUNTIES_DEFAULT_DIRECTIONAL
    if composite_score >= 0.62:
        return "improving"
    if composite_score <= 0.38:
        return "at_risk"
    return "stable"


def _derive_confidence_level(layer_count: int) -> str:
    if layer_count >= 4:
        return "strong"
    if layer_count >= 2:
        return "conditional"
    return "fragile"


def _derive_synthesis_grouping(composite_score: Optional[float], directional_status: str) -> str:
    if composite_score is None:
        return COUNTIES_DEFAULT_GROUPING
    if directional_status == "improving":
        return "emerging_tailwinds" if composite_score >= 0.5 else "conditional_growth"
    if directional_status == "at_risk":
        return "at_risk_headwinds"
    return "stable_constrained"


def _fetch_latest_layer_snapshot(
    db: Session, geoid: str
) -> tuple[Dict[str, Optional[float]], List[int]]:
    layer_scores: Dict[str, Optional[float]] = {}
    year_candidates: List[int] = []

    for layer_key, spec in LAYER_LATEST_SNAPSHOT_CONFIG.items():
        table = spec["table"]
        column = spec["column"]
        latest_query = text(
            f"""
            SELECT data_year, {column} AS score
            FROM {table}
            WHERE fips_code = :geoid
            ORDER BY data_year DESC
            LIMIT 1
        """
        )
        try:
            row = db.execute(latest_query, {"geoid": geoid}).fetchone()
        except Exception as exc:
            logger.warning(
                "Skipping layer snapshot for %s on %s due to query error: %s",
                layer_key,
                geoid,
                exc,
            )
            row = None

        score = _safe_float(getattr(row, "score", None) if row else None)
        layer_scores[layer_key] = score

        year_value = _safe_int(getattr(row, "data_year", None) if row else None)
        if year_value is not None:
            year_candidates.append(year_value)

    return layer_scores, year_candidates


def _counties_geojson_query() -> str:
    if DATABASE_BACKEND == "databricks":
        geometry_expr = "mc.geometry_geojson"
    else:
        geometry_expr = "ST_AsGeoJSON(mc.geometry)"

    return f"""
        WITH l1_latest AS (
            SELECT fips_code, data_year AS l1_data_year, economic_opportunity_index AS l1_score
            FROM (
                SELECT
                    fips_code,
                    data_year,
                    economic_opportunity_index,
                    ROW_NUMBER() OVER (PARTITION BY fips_code ORDER BY data_year DESC) AS rn
                FROM layer1_employment_gravity
            ) t
            WHERE rn = 1
        ),
        l2_latest AS (
            SELECT fips_code, data_year AS l2_data_year, mobility_optionality_index AS l2_score
            FROM (
                SELECT
                    fips_code,
                    data_year,
                    mobility_optionality_index,
                    ROW_NUMBER() OVER (PARTITION BY fips_code ORDER BY data_year DESC) AS rn
                FROM layer2_mobility_optionality
            ) t
            WHERE rn = 1
        ),
        l3_latest AS (
            SELECT fips_code, data_year AS l3_data_year, education_opportunity_index AS l3_score
            FROM (
                SELECT
                    fips_code,
                    data_year,
                    education_opportunity_index,
                    ROW_NUMBER() OVER (PARTITION BY fips_code ORDER BY data_year DESC) AS rn
                FROM layer3_school_trajectory
            ) t
            WHERE rn = 1
        ),
        l4_latest AS (
            SELECT fips_code, data_year AS l4_data_year, housing_opportunity_index AS l4_score
            FROM (
                SELECT
                    fips_code,
                    data_year,
                    housing_opportunity_index,
                    ROW_NUMBER() OVER (PARTITION BY fips_code ORDER BY data_year DESC) AS rn
                FROM layer4_housing_elasticity
            ) t
            WHERE rn = 1
        ),
        l5_latest AS (
            SELECT fips_code, data_year AS l5_data_year, demographic_opportunity_index AS l5_score
            FROM (
                SELECT
                    fips_code,
                    data_year,
                    demographic_opportunity_index,
                    ROW_NUMBER() OVER (PARTITION BY fips_code ORDER BY data_year DESC) AS rn
                FROM layer5_demographic_momentum
            ) t
            WHERE rn = 1
        ),
        l6_latest AS (
            SELECT fips_code, data_year AS l6_data_year, risk_drag_index AS l6_score
            FROM (
                SELECT
                    fips_code,
                    data_year,
                    risk_drag_index,
                    ROW_NUMBER() OVER (PARTITION BY fips_code ORDER BY data_year DESC) AS rn
                FROM layer6_risk_drag
            ) t
            WHERE rn = 1
        )
        SELECT
            mc.fips_code,
            mc.county_name,
            {geometry_expr} AS geometry_geojson,
            mc.updated_at AS county_updated_at,
            fsc.current_as_of_year AS data_year,
            fsc.final_grouping,
            fsc.directional_status,
            fsc.confidence_level,
            fsc.uncertainty_level,
            fsc.uncertainty_reasons,
            fsc.composite_score,
            fsc.employment_gravity_score,
            fsc.mobility_optionality_score,
            fsc.school_trajectory_score,
            fsc.housing_elasticity_score,
            fsc.demographic_momentum_score,
            fsc.risk_drag_score,
            fsc.classification_version,
            fsc.updated_at AS synthesis_updated_at,
            l1.l1_data_year,
            l1.l1_score,
            l2.l2_data_year,
            l2.l2_score,
            l3.l3_data_year,
            l3.l3_score,
            l4.l4_data_year,
            l4.l4_score,
            l5.l5_data_year,
            l5.l5_score,
            l6.l6_data_year,
            l6.l6_score
        FROM md_counties mc
        LEFT JOIN final_synthesis_current fsc ON fsc.geoid = mc.fips_code
        LEFT JOIN l1_latest l1 ON l1.fips_code = mc.fips_code
        LEFT JOIN l2_latest l2 ON l2.fips_code = mc.fips_code
        LEFT JOIN l3_latest l3 ON l3.fips_code = mc.fips_code
        LEFT JOIN l4_latest l4 ON l4.fips_code = mc.fips_code
        LEFT JOIN l5_latest l5 ON l5.fips_code = mc.fips_code
        LEFT JOIN l6_latest l6 ON l6.fips_code = mc.fips_code
        ORDER BY mc.fips_code
    """


def _build_live_counties_geojson(rows: List[Any]) -> Dict[str, Any]:
    features: List[Dict[str, Any]] = []
    scored_count = 0

    for row in rows:
        geometry = _parse_geometry(getattr(row, "geometry_geojson", None))
        if not geometry:
            logger.warning(
                "Skipping county with missing/invalid geometry payload: %s",
                getattr(row, "fips_code", "unknown"),
            )
            continue

        layer_scores = {
            "employment_gravity": _safe_float(
                _coalesce_non_null(
                    getattr(row, "employment_gravity_score", None),
                    getattr(row, "l1_score", None),
                )
            ),
            "mobility_optionality": _safe_float(
                _coalesce_non_null(
                    getattr(row, "mobility_optionality_score", None),
                    getattr(row, "l2_score", None),
                )
            ),
            "school_trajectory": _safe_float(
                _coalesce_non_null(
                    getattr(row, "school_trajectory_score", None),
                    getattr(row, "l3_score", None),
                )
            ),
            "housing_elasticity": _safe_float(
                _coalesce_non_null(
                    getattr(row, "housing_elasticity_score", None),
                    getattr(row, "l4_score", None),
                )
            ),
            "demographic_momentum": _safe_float(
                _coalesce_non_null(
                    getattr(row, "demographic_momentum_score", None),
                    getattr(row, "l5_score", None),
                )
            ),
            "risk_drag": _safe_float(
                _coalesce_non_null(
                    getattr(row, "risk_drag_score", None),
                    getattr(row, "l6_score", None),
                )
            ),
        }

        has_scores = any(score is not None for score in layer_scores.values())
        if has_scores:
            scored_count += 1

        non_risk_scores = [
            score for key, score in layer_scores.items() if key != "risk_drag" and score is not None
        ]
        composite_from_layers = _mean(non_risk_scores)
        composite_score = _safe_float(getattr(row, "composite_score", None))
        if composite_score is None:
            composite_score = composite_from_layers

        directional_status = getattr(row, "directional_status", None)
        if not directional_status:
            directional_status = _derive_directional_status(composite_score)

        layer_count = len([score for score in layer_scores.values() if score is not None])
        confidence_level = getattr(row, "confidence_level", None)
        if not confidence_level:
            confidence_level = _derive_confidence_level(layer_count)

        synthesis_grouping = getattr(row, "final_grouping", None)
        if not synthesis_grouping:
            synthesis_grouping = _derive_synthesis_grouping(composite_score, directional_status)

        explainability = _generate_explainability_payload(
            directional_class=directional_status,
            confidence_class=confidence_level,
            risk_drag_score=layer_scores["risk_drag"],
            layer_scores=layer_scores,
        )
        if not has_scores:
            explainability["primary_strengths"] = []
            explainability["primary_weaknesses"] = []
            explainability["key_trends"] = [
                "Live Databricks feed active; synthesis refresh pending."
            ]

        year_candidates = [
            _safe_int(getattr(row, "data_year", None)),
            _safe_int(getattr(row, "l1_data_year", None)),
            _safe_int(getattr(row, "l2_data_year", None)),
            _safe_int(getattr(row, "l3_data_year", None)),
            _safe_int(getattr(row, "l4_data_year", None)),
            _safe_int(getattr(row, "l5_data_year", None)),
            _safe_int(getattr(row, "l6_data_year", None)),
        ]
        year_candidates = [year for year in year_candidates if year is not None]
        data_year_int = max(year_candidates) if year_candidates else None

        last_updated = getattr(row, "synthesis_updated_at", None) or getattr(
            row, "county_updated_at", None
        )
        if isinstance(last_updated, datetime):
            last_updated_iso = last_updated.isoformat()
        else:
            last_updated_iso = None

        feature = {
            "type": "Feature",
            "id": getattr(row, "fips_code", None),
            "geometry": geometry,
            "properties": {
                "fips_code": getattr(row, "fips_code", None),
                "geoid": getattr(row, "fips_code", None),
                "county_name": getattr(row, "county_name", None),
                "data_year": data_year_int,
                "synthesis_grouping": synthesis_grouping,
                "final_grouping": synthesis_grouping,
                "directional_class": directional_status,
                "directional_status": directional_status,
                "confidence_class": confidence_level,
                "confidence_level": confidence_level,
                "composite_score": composite_score,
                "employment_gravity_score": layer_scores["employment_gravity"],
                "mobility_optionality_score": layer_scores["mobility_optionality"],
                "school_trajectory_score": layer_scores["school_trajectory"],
                "housing_elasticity_score": layer_scores["housing_elasticity"],
                "demographic_momentum_score": layer_scores["demographic_momentum"],
                "risk_drag_score": layer_scores["risk_drag"],
                "classification_version": getattr(row, "classification_version", None)
                or "live-databricks",
                "uncertainty_level": getattr(row, "uncertainty_level", None),
                "uncertainty_reasons": _parse_json_list(getattr(row, "uncertainty_reasons", None)),
                "primary_strengths": explainability["primary_strengths"],
                "primary_weaknesses": explainability["primary_weaknesses"],
                "key_trends": explainability["key_trends"],
                "last_updated": last_updated_iso,
                "live_feed": True,
            },
        }
        features.append(feature)

    if not features:
        raise ValueError("No county geometries available in md_counties.")

    as_of_year_candidates = [
        feature["properties"]["data_year"]
        for feature in features
        if feature["properties"]["data_year"] is not None
    ]

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source": "databricks_live",
            "backend": DATABASE_BACKEND,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "county_count": len(features),
            "counties_with_scores": scored_count,
            "as_of_year": max(as_of_year_candidates) if as_of_year_candidates else None,
        },
    }


@router.get("/layers/counties/latest")
async def get_counties_geojson(db: Session = Depends(get_db_session)):
    """
    Get latest county-level GeoJSON directly from live database tables.

    Returns:
        GeoJSON FeatureCollection with all Maryland counties
    """
    global _counties_geojson_cache

    try:
        rows = db.execute(text(_counties_geojson_query())).fetchall()
        payload = _build_live_counties_geojson(rows)
        _counties_geojson_cache = payload

        return JSONResponse(
            content=payload,
            media_type="application/geo+json",
            headers={"Cache-Control": "no-store"},
        )
    except Exception as e:
        logger.error("Failed to build live county GeoJSON feed: %s", e, exc_info=True)
        if _counties_geojson_cache is not None:
            cached_payload = dict(_counties_geojson_cache)
            metadata = dict(cached_payload.get("metadata", {}))
            metadata["served_from_cache"] = True
            metadata["cache_reason"] = "live_query_failed"
            metadata["generated_at"] = datetime.now(timezone.utc).isoformat()
            cached_payload["metadata"] = metadata
            return JSONResponse(
                content=cached_payload,
                media_type="application/geo+json",
                headers={"Cache-Control": "no-store"},
            )
        raise HTTPException(status_code=503, detail="Live county feed unavailable")
    finally:
        db.close()


@router.get("/layers/counties/{version}")
async def get_counties_geojson_versioned(version: str):
    """
    Get versioned county GeoJSON snapshot

    Args:
        version: Version string (YYYYMMDD format, e.g., '20260128')

    Returns:
        GeoJSON FeatureCollection for specified version
    """
    geojson_path = os.path.join(settings.EXPORT_DIR, f"md_counties_{version}.geojson")

    if not os.path.exists(geojson_path):
        raise HTTPException(status_code=404, detail=f"Version {version} not found")

    return FileResponse(
        geojson_path, media_type="application/geo+json", filename=f"md_counties_{version}.geojson"
    )


@router.get("/areas/{geoid}", response_model=AreaDetail)
async def get_area_detail(geoid: str, db: Session = Depends(get_db_session)):
    """
    Get detailed information for a specific area

    Args:
        geoid: FIPS code (e.g., '24031' for Montgomery County)

    Returns:
        Detailed area information including scores and classifications
    """
    # Validate FIPS code
    if geoid not in MD_COUNTY_FIPS:
        raise HTTPException(status_code=404, detail=f"Unknown FIPS code: {geoid}")

    try:
        query = text(
            """
            SELECT
                fsc.geoid AS fips_code,
                fsc.current_as_of_year AS data_year,
                fsc.final_grouping,
                fsc.directional_status,
                fsc.confidence_level,
                fsc.composite_score,
                fsc.updated_at,
                fsc.employment_gravity_score,
                fsc.mobility_optionality_score,
                fsc.school_trajectory_score,
                fsc.housing_elasticity_score,
                fsc.demographic_momentum_score,
                fsc.risk_drag_score
            FROM final_synthesis_current fsc
            WHERE fsc.geoid = :geoid
        """
        )

        result = db.execute(query, {"geoid": geoid}).fetchone()
        fallback_layer_scores, fallback_years = _fetch_latest_layer_snapshot(db, geoid)

        if result:
            layer_scores = {
                "employment_gravity": _safe_float(
                    _coalesce_non_null(
                        result.employment_gravity_score, fallback_layer_scores["employment_gravity"]
                    )
                ),
                "mobility_optionality": _safe_float(
                    _coalesce_non_null(
                        result.mobility_optionality_score,
                        fallback_layer_scores["mobility_optionality"],
                    )
                ),
                "school_trajectory": _safe_float(
                    _coalesce_non_null(
                        result.school_trajectory_score, fallback_layer_scores["school_trajectory"]
                    )
                ),
                "housing_elasticity": _safe_float(
                    _coalesce_non_null(
                        result.housing_elasticity_score, fallback_layer_scores["housing_elasticity"]
                    )
                ),
                "demographic_momentum": _safe_float(
                    _coalesce_non_null(
                        result.demographic_momentum_score,
                        fallback_layer_scores["demographic_momentum"],
                    )
                ),
                "risk_drag": _safe_float(
                    _coalesce_non_null(result.risk_drag_score, fallback_layer_scores["risk_drag"])
                ),
            }
        else:
            layer_scores = fallback_layer_scores

        layer_count = len([score for score in layer_scores.values() if score is not None])
        non_risk_scores = [
            score for key, score in layer_scores.items() if key != "risk_drag" and score is not None
        ]

        composite_score = _safe_float(getattr(result, "composite_score", None) if result else None)
        if composite_score is None:
            composite_score = _mean(non_risk_scores)

        directional_class = getattr(result, "directional_status", None) if result else None
        if not directional_class:
            directional_class = _derive_directional_status(composite_score)

        confidence_class = getattr(result, "confidence_level", None) if result else None
        if not confidence_class:
            confidence_class = _derive_confidence_level(layer_count)

        synthesis_grouping = getattr(result, "final_grouping", None) if result else None
        if not synthesis_grouping:
            synthesis_grouping = _derive_synthesis_grouping(composite_score, directional_class)

        data_year_candidates: List[int] = list(fallback_years)
        if result and _safe_int(result.data_year) is not None:
            data_year_candidates.append(int(result.data_year))
        data_year = max(data_year_candidates) if data_year_candidates else settings.PREDICT_TO_YEAR

        explainability = _generate_explainability_payload(
            directional_class=directional_class,
            confidence_class=confidence_class,
            risk_drag_score=layer_scores["risk_drag"],
            layer_scores=layer_scores,
        )
        if layer_count == 0:
            explainability["primary_strengths"] = []
            explainability["primary_weaknesses"] = []
            explainability["key_trends"] = [
                "No county-layer scores are ingested yet for this county."
            ]

        if result and isinstance(result.updated_at, datetime):
            last_updated = result.updated_at.isoformat()
        else:
            last_updated = datetime.now(timezone.utc).isoformat()

        return AreaDetail(
            fips_code=geoid,
            county_name=MD_COUNTY_FIPS[geoid],
            data_year=data_year,
            directional_class=directional_class,
            confidence_class=confidence_class,
            synthesis_grouping=synthesis_grouping,
            composite_score=composite_score,
            layer_scores=layer_scores,
            primary_strengths=explainability["primary_strengths"],
            primary_weaknesses=explainability["primary_weaknesses"],
            key_trends=explainability["key_trends"],
            last_updated=last_updated,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch area detail: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()


# Layer configuration for factor breakdown
LAYER_CONFIGS: Dict[str, LayerConfig] = {
    "employment_gravity": {
        "table": "layer1_employment_gravity",
        "display_name": "Economic Opportunity",
        "description": "Measures access to high-wage jobs and economic diversification",
        "version": "v2",
        "formula": "0.40 × diversification + 0.60 × accessibility",
        "factors": [
            {
                "col": "economic_opportunity_index",
                "name": "Economic Opportunity Index",
                "desc": "Combined v1+v2 composite score",
                "weight": 1.0,
            },
            {
                "col": "economic_accessibility_score",
                "name": "Job Accessibility",
                "desc": "High-wage jobs reachable within 45 min",
                "weight": 0.60,
            },
            {
                "col": "employment_diversification_score",
                "name": "Employment Diversification",
                "desc": "Sector diversity and wage quality",
                "weight": 0.40,
            },
            {
                "col": "high_wage_jobs_accessible_45min",
                "name": "High-Wage Jobs (45 min)",
                "desc": "Jobs earning >$40k accessible",
                "weight": None,
            },
            {
                "col": "wage_quality_ratio",
                "name": "Wage Quality Ratio",
                "desc": "High-wage to low-wage job ratio",
                "weight": None,
            },
            {
                "col": "sector_diversity_entropy",
                "name": "Sector Diversity",
                "desc": "Shannon entropy across industries",
                "weight": None,
            },
        ],
    },
    "mobility_optionality": {
        "table": "layer2_mobility_optionality",
        "display_name": "Mobility Options",
        "description": "Measures transportation accessibility and mode options",
        "version": "v2",
        "formula": "0.60 × transit + 0.25 × walk + 0.15 × bike",
        "factors": [
            {
                "col": "mobility_optionality_index",
                "name": "Mobility Index",
                "desc": "Combined multimodal accessibility",
                "weight": 1.0,
            },
            {
                "col": "transit_accessibility_score",
                "name": "Transit Accessibility",
                "desc": "Jobs reachable by transit (45 min)",
                "weight": 0.60,
            },
            {
                "col": "walk_accessibility_score",
                "name": "Walk Accessibility",
                "desc": "Jobs reachable by walking (30 min)",
                "weight": 0.25,
            },
            {
                "col": "bike_accessibility_score",
                "name": "Bike Accessibility",
                "desc": "Jobs reachable by bike (30 min)",
                "weight": 0.15,
            },
            {
                "col": "transit_car_accessibility_ratio",
                "name": "Transit vs Car Ratio",
                "desc": "Transit competitiveness vs driving",
                "weight": None,
            },
            {
                "col": "mode_count",
                "name": "Mode Count",
                "desc": "Available transportation modes",
                "weight": None,
            },
        ],
    },
    "school_trajectory": {
        "table": "layer3_school_trajectory",
        "display_name": "Education Access",
        "description": "Measures access to quality schools and educational opportunity",
        "version": "v2",
        "formula": "0.40 × supply + 0.60 × accessibility",
        "factors": [
            {
                "col": "education_opportunity_index",
                "name": "Education Opportunity Index",
                "desc": "Combined v1+v2 composite score",
                "weight": 1.0,
            },
            {
                "col": "education_accessibility_score",
                "name": "Education Accessibility",
                "desc": "Quality school access composite",
                "weight": 0.60,
            },
            {
                "col": "school_supply_score",
                "name": "School Supply",
                "desc": "Enrollment and school density",
                "weight": 0.40,
            },
            {
                "col": "avg_high_quality_accessible_30min",
                "name": "Quality Schools (30 min)",
                "desc": "Above-median schools accessible",
                "weight": None,
            },
            {
                "col": "prek_accessibility_score",
                "name": "Pre-K Access",
                "desc": "Early childhood program availability",
                "weight": None,
            },
            {
                "col": "avg_proficiency",
                "name": "Avg Proficiency",
                "desc": "Average ELA/Math proficiency",
                "weight": None,
            },
        ],
    },
    "housing_elasticity": {
        "table": "layer4_housing_elasticity",
        "display_name": "Housing Affordability",
        "description": "Measures housing supply responsiveness and affordability burden",
        "version": "v2",
        "formula": "0.40 × elasticity + 0.60 × affordability",
        "factors": [
            {
                "col": "housing_opportunity_index",
                "name": "Housing Opportunity Index",
                "desc": "Combined v1+v2 composite score",
                "weight": 1.0,
            },
            {
                "col": "housing_affordability_score",
                "name": "Affordability Score",
                "desc": "Cost burden and affordable stock",
                "weight": 0.60,
            },
            {
                "col": "housing_elasticity_index",
                "name": "Supply Elasticity",
                "desc": "Permit activity and responsiveness",
                "weight": 0.40,
            },
            {
                "col": "cost_burdened_pct",
                "name": "Cost Burdened %",
                "desc": "Households paying >30% on housing",
                "weight": None,
                "invert": True,
            },
            {
                "col": "affordable_units_pct",
                "name": "Affordable Units %",
                "desc": "Units affordable to low income",
                "weight": None,
            },
            {
                "col": "price_to_income_ratio",
                "name": "Price-to-Income",
                "desc": "Median home value / income",
                "weight": None,
                "invert": True,
            },
        ],
    },
    "demographic_momentum": {
        "table": "layer5_demographic_momentum",
        "display_name": "Demographic Health",
        "description": "Measures population dynamics, equity, and migration patterns",
        "version": "v2",
        "formula": "0.30 × static + 0.40 × equity + 0.30 × migration",
        "factors": [
            {
                "col": "demographic_opportunity_index",
                "name": "Demographic Opportunity Index",
                "desc": "Combined v1-v3 composite score",
                "weight": 1.0,
            },
            {
                "col": "equity_score",
                "name": "Equity Score",
                "desc": "Segregation and family viability",
                "weight": 0.40,
            },
            {
                "col": "static_demographic_score",
                "name": "Static Demographics",
                "desc": "Population structure",
                "weight": 0.30,
            },
            {
                "col": "migration_dynamics_score",
                "name": "Migration Dynamics",
                "desc": "Net migration and growth",
                "weight": 0.30,
            },
            {
                "col": "racial_diversity_index",
                "name": "Diversity Index",
                "desc": "Shannon entropy diversity",
                "weight": None,
            },
            {
                "col": "net_migration_rate",
                "name": "Net Migration Rate",
                "desc": "Inflow minus outflow rate",
                "weight": None,
            },
        ],
    },
    "risk_drag": {
        "table": "layer6_risk_drag",
        "display_name": "Risk & Vulnerability",
        "description": "Measures environmental hazards, climate risk, and community vulnerability",
        "version": "v2",
        "formula": "0.40 × static + 0.60 × modern_vulnerability",
        "factors": [
            {
                "col": "risk_drag_index",
                "name": "Risk Drag Index",
                "desc": "Combined v1+v2 risk score",
                "weight": 1.0,
            },
            {
                "col": "modern_vulnerability_score",
                "name": "Modern Vulnerability",
                "desc": "Climate + social vulnerability",
                "weight": 0.60,
            },
            {
                "col": "static_risk_score",
                "name": "Static Risk",
                "desc": "Flood, pollution, infrastructure",
                "weight": 0.40,
            },
            {
                "col": "climate_projection_score",
                "name": "Climate Projection",
                "desc": "SLR + heat vulnerability",
                "weight": None,
            },
            {
                "col": "social_vulnerability_index",
                "name": "Social Vulnerability",
                "desc": "CDC SVI composite",
                "weight": None,
            },
            {
                "col": "sfha_pct_of_county",
                "name": "Flood Zone %",
                "desc": "Special Flood Hazard Area",
                "weight": None,
            },
        ],
    },
}


def _get_trend_direction(slope: Optional[float]) -> Optional[str]:
    """Convert slope to trend direction"""
    if slope is None:
        return None
    if slope > 0.01:
        return "up"
    elif slope < -0.01:
        return "down"
    return "stable"


@router.get("/areas/{geoid}/layers/{layer_key}", response_model=LayerDetail)
async def get_layer_detail(geoid: str, layer_key: str, db: Session = Depends(get_db_session)):
    """
    Get detailed factor breakdown for a specific layer

    Args:
        geoid: FIPS code (e.g., '24031' for Montgomery County)
        layer_key: Layer identifier (employment_gravity, mobility_optionality, etc.)

    Returns:
        Detailed layer information with factor breakdown and trends
    """
    if geoid not in MD_COUNTY_FIPS:
        raise HTTPException(status_code=404, detail=f"Unknown FIPS code: {geoid}")

    if layer_key not in LAYER_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Unknown layer: {layer_key}")

    config = LAYER_CONFIGS[layer_key]

    try:
        # Build column list for query
        factor_cols = [f["col"] for f in config["factors"]]
        col_list = ", ".join(
            [f'"{c}"' if c != "data_year" else c for c in factor_cols + ["data_year"]]
        )

        # Query layer table
        layer_query = text(
            f"""
            SELECT {col_list}
            FROM {config["table"]}
            WHERE fips_code = :geoid
            ORDER BY data_year DESC
            LIMIT 1
        """
        )

        layer_result = db.execute(layer_query, {"geoid": geoid}).fetchone()

        if not layer_result:
            raise HTTPException(status_code=404, detail=f"No {layer_key} data for {geoid}")

        # Query timeseries features for momentum
        ts_query = text(
            """
            SELECT
                momentum_slope,
                momentum_percent_change,
                coverage_years,
                level_latest,
                level_baseline
            FROM layer_timeseries_features
            WHERE geoid = :geoid AND layer_name = :layer_name
            ORDER BY as_of_year DESC
            LIMIT 1
        """
        )

        ts_result = db.execute(ts_query, {"geoid": geoid, "layer_name": layer_key}).fetchone()

        # Build factors list
        factors = []
        for factor_config in config["factors"]:
            col = factor_config["col"]
            value = getattr(layer_result, col, None) if layer_result else None

            # Format the value for display
            formatted = None
            if value is not None:
                if "pct" in col.lower() or "ratio" in col.lower():
                    formatted = f"{value * 100:.1f}%" if value < 1 else f"{value:.1f}%"
                elif "index" in col.lower() or "score" in col.lower():
                    formatted = f"{value:.3f}"
                elif isinstance(value, float):
                    formatted = f"{value:,.0f}" if value > 100 else f"{value:.2f}"
                else:
                    formatted = str(value)

            # Determine trend from timeseries if available
            trend = None
            trend_value = None
            if ts_result and factor_config.get("weight") == 1.0:  # Main index
                trend = _get_trend_direction(ts_result.momentum_slope)
                trend_value = ts_result.momentum_percent_change

            factors.append(
                LayerFactor(
                    name=factor_config["name"],
                    value=float(value) if value is not None else None,
                    formatted_value=formatted,
                    description=factor_config["desc"],
                    weight=factor_config.get("weight"),
                    trend=trend,
                    trend_value=float(trend_value) if trend_value is not None else None,
                )
            )

        # Get the main score
        main_score = None
        for f in factors:
            if f.weight == 1.0:
                main_score = f.value
                break

        return LayerDetail(
            layer_key=layer_key,
            display_name=config["display_name"],
            score=main_score,
            version=config["version"],
            formula=config["formula"],
            description=config["description"],
            factors=factors,
            momentum_slope=(
                float(ts_result.momentum_slope) if ts_result and ts_result.momentum_slope else None
            ),
            momentum_direction=(
                _get_trend_direction(ts_result.momentum_slope) if ts_result else None
            ),
            data_year=layer_result.data_year if layer_result else 2025,
            coverage_years=ts_result.coverage_years if ts_result else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch layer detail: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()


@router.get("/metadata/refresh", response_model=List[RefreshStatus])
async def get_latest_refresh_status(
    db: Session = Depends(get_db_session), limit: int = Query(default=10, le=50)
):
    """
    Get latest data refresh status for all layers

    Args:
        limit: Maximum number of refresh records to return

    Returns:
        List of recent refresh operations
    """
    try:
        query = text(
            """
            SELECT DISTINCT ON (layer_name)
                layer_name,
                data_source,
                refresh_date,
                status,
                records_processed
            FROM data_refresh_log
            ORDER BY layer_name, refresh_date DESC
            LIMIT :limit
        """
        )

        results = db.execute(query, {"limit": limit}).fetchall()

        return [
            RefreshStatus(
                layer_name=r.layer_name,
                data_source=r.data_source,
                refresh_date=r.refresh_date.isoformat(),
                status=r.status,
                records_processed=r.records_processed,
            )
            for r in results
        ]

    except Exception as e:
        logger.error(f"Failed to fetch refresh status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()


@router.get("/metadata/sources", response_model=List[DataSource])
async def get_data_sources():
    """
    Get documentation for all data sources

    Returns:
        List of data source metadata
    """
    sources = _load_data_sources_registry()
    if not sources:
        raise HTTPException(status_code=500, detail="Data source registry is unavailable")
    return sources


@router.get("/metadata/capabilities", response_model=CapabilitiesResponse)
async def get_capabilities() -> CapabilitiesResponse:
    """
    Expose runtime feature capabilities for frontend gating and diagnostics.
    """
    return CapabilitiesResponse(
        chat_enabled=bool(settings.AI_ENABLED and settings.OPENAI_API_KEY),
        ai_enabled=bool(settings.AI_ENABLED),
        api_version=settings.API_VERSION,
        year_policy={
            "lodes_latest_year": settings.LODES_LATEST_YEAR,
            "lodes_lag_years": settings.LODES_LAG_YEARS,
            "acs_latest_year": settings.ACS_LATEST_YEAR,
            "acs_geography_max_year": settings.ACS_GEOGRAPHY_MAX_YEAR,
            "nces_observed_max_year": settings.NCES_OBSERVED_MAX_YEAR,
            "predict_to_year": settings.PREDICT_TO_YEAR,
        },
    )


@router.get("/metadata/classifications")
async def get_classification_definitions():
    """
    Get definitions for classification categories

    Returns:
        Classification definitions and thresholds
    """
    from src.processing import multiyear_classification as myc

    return {
        "directional_status": {
            "improving": {
                "definition": "Multiple reinforcing structural tailwinds present",
                "criteria": f"≥{myc.THRESHOLD_IMPROVING_MIN_LAYERS} layers above {myc.THRESHOLD_IMPROVING_HIGH} AND ≤1 below {myc.THRESHOLD_IMPROVING_LOW} (with positive momentum if available)",
            },
            "stable": {
                "definition": "Balanced signals, mixed pressure directions",
                "criteria": "Does not meet criteria for Improving or At Risk",
            },
            "at_risk": {
                "definition": "Structural headwinds constraining growth capacity",
                "criteria": f"≥{myc.THRESHOLD_AT_RISK_COUNT} layers below {myc.THRESHOLD_AT_RISK_LOW} OR severe risk drag (≥{myc.THRESHOLD_RISK_DRAG_SEVERE})",
            },
        },
        "confidence_overlay": {
            "strong": {
                "definition": "High evidence coverage across layers",
                "criteria": f"Average coverage ≥ {myc.COVERAGE_STRONG} years",
            },
            "conditional": {
                "definition": "Partial evidence coverage across layers",
                "criteria": f"Average coverage ≥ {myc.COVERAGE_CONDITIONAL} years",
            },
            "fragile": {
                "definition": "Sparse evidence coverage across layers",
                "criteria": f"Average coverage < {myc.COVERAGE_CONDITIONAL} years",
            },
        },
        "disclaimer": "This tool does NOT predict housing prices or provide investment recommendations. "
        "Classifications indicate directional pressure under current policy trends.",
    }


@router.get("/counties")
async def list_counties():
    """
    List all Maryland counties with FIPS codes

    Returns:
        Dict mapping FIPS codes to county names
    """
    return MD_COUNTY_FIPS
