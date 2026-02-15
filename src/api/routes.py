"""
Maryland Viability Atlas - API Routes
Endpoints for map data and metadata
"""

import json
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.database import get_db_session
from config.settings import MD_COUNTY_FIPS, get_settings
from src.utils.logging import get_logger

router = APIRouter()
settings = get_settings()
logger = get_logger(__name__)


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


@router.get("/layers/counties/latest")
async def get_counties_geojson():
    """
    Get latest county-level GeoJSON

    Returns:
        GeoJSON FeatureCollection with all Maryland counties
    """
    geojson_path = os.path.join(settings.EXPORT_DIR, "md_counties_latest.geojson")

    if not os.path.exists(geojson_path):
        raise HTTPException(
            status_code=404, detail="GeoJSON export not found. Run export pipeline first."
        )

    return FileResponse(
        geojson_path, media_type="application/geo+json", filename="md_counties_latest.geojson"
    )


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

        if not result:
            raise HTTPException(status_code=404, detail=f"No data found for FIPS code {geoid}")

        layer_scores = {
            "employment_gravity": result.employment_gravity_score,
            "mobility_optionality": result.mobility_optionality_score,
            "school_trajectory": result.school_trajectory_score,
            "housing_elasticity": result.housing_elasticity_score,
            "demographic_momentum": result.demographic_momentum_score,
            "risk_drag": result.risk_drag_score,
        }

        explainability = _generate_explainability_payload(
            directional_class=result.directional_status,
            confidence_class=result.confidence_level,
            risk_drag_score=result.risk_drag_score,
            layer_scores=layer_scores,
        )

        return AreaDetail(
            fips_code=result.fips_code,
            county_name=MD_COUNTY_FIPS[geoid],
            data_year=result.data_year,
            directional_class=result.directional_status,
            confidence_class=result.confidence_level,
            synthesis_grouping=result.final_grouping,
            composite_score=result.composite_score,
            layer_scores=layer_scores,
            primary_strengths=explainability["primary_strengths"],
            primary_weaknesses=explainability["primary_weaknesses"],
            key_trends=explainability["key_trends"],
            last_updated=result.updated_at.isoformat() if result.updated_at else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch area detail: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()


# Layer configuration for factor breakdown
LAYER_CONFIGS = {
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
