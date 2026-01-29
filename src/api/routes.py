"""
Maryland Viability Atlas - API Routes
Endpoints for map data and metadata
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime
import os
import json

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db_session
from src.utils.logging import get_logger

router = APIRouter()
settings = get_settings()
logger = get_logger(__name__)


# Response models
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
            status_code=404,
            detail="GeoJSON export not found. Run export pipeline first."
        )

    return FileResponse(
        geojson_path,
        media_type="application/geo+json",
        filename="md_counties_latest.geojson"
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
        raise HTTPException(
            status_code=404,
            detail=f"Version {version} not found"
        )

    return FileResponse(
        geojson_path,
        media_type="application/geo+json",
        filename=f"md_counties_{version}.geojson"
    )


@router.get("/areas/{geoid}", response_model=AreaDetail)
async def get_area_detail(
    geoid: str,
    db: Session = Depends(get_db_session)
):
    """
    Get detailed information for a specific area

    Args:
        geoid: FIPS code (e.g., '24031' for Montgomery County)

    Returns:
        Detailed area information including scores and classifications
    """
    # Validate FIPS code
    if geoid not in MD_COUNTY_FIPS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown FIPS code: {geoid}"
        )

    try:
        # Fetch classification and scores
        query = text("""
            SELECT
                cc.fips_code,
                cc.data_year,
                cc.directional_class,
                cc.confidence_class,
                cc.synthesis_grouping,
                cc.composite_score,
                cc.primary_strengths,
                cc.primary_weaknesses,
                cc.key_trends,
                cc.updated_at,
                ls.employment_gravity_score,
                ls.mobility_optionality_score,
                ls.school_trajectory_score,
                ls.housing_elasticity_score,
                ls.demographic_momentum_score,
                ls.risk_drag_score
            FROM county_classifications cc
            LEFT JOIN layer_scores ls
                ON cc.fips_code = ls.fips_code
                AND cc.data_year = ls.data_year
            WHERE cc.fips_code = :geoid
                AND cc.data_year = (
                    SELECT MAX(data_year) FROM county_classifications WHERE fips_code = :geoid
                )
        """)

        result = db.execute(query, {"geoid": geoid}).fetchone()

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for FIPS code {geoid}"
            )

        # Build response
        return AreaDetail(
            fips_code=result.fips_code,
            county_name=MD_COUNTY_FIPS[geoid],
            data_year=result.data_year,
            directional_class=result.directional_class,
            confidence_class=result.confidence_class,
            synthesis_grouping=result.synthesis_grouping,
            composite_score=result.composite_score,
            layer_scores={
                "employment_gravity": result.employment_gravity_score,
                "mobility_optionality": result.mobility_optionality_score,
                "school_trajectory": result.school_trajectory_score,
                "housing_elasticity": result.housing_elasticity_score,
                "demographic_momentum": result.demographic_momentum_score,
                "risk_drag": result.risk_drag_score
            },
            primary_strengths=result.primary_strengths or [],
            primary_weaknesses=result.primary_weaknesses or [],
            key_trends=result.key_trends or [],
            last_updated=result.updated_at.isoformat() if result.updated_at else None
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch area detail: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()


@router.get("/metadata/refresh", response_model=List[RefreshStatus])
async def get_latest_refresh_status(
    db: Session = Depends(get_db_session),
    limit: int = Query(default=10, le=50)
):
    """
    Get latest data refresh status for all layers

    Args:
        limit: Maximum number of refresh records to return

    Returns:
        List of recent refresh operations
    """
    try:
        query = text("""
            SELECT DISTINCT ON (layer_name)
                layer_name,
                data_source,
                refresh_date,
                status,
                records_processed
            FROM data_refresh_log
            ORDER BY layer_name, refresh_date DESC
            LIMIT :limit
        """)

        results = db.execute(query, {"limit": limit}).fetchall()

        return [
            RefreshStatus(
                layer_name=r.layer_name,
                data_source=r.data_source,
                refresh_date=r.refresh_date.isoformat(),
                status=r.status,
                records_processed=r.records_processed
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
    # This could be loaded from docs/DATA_SOURCES.md or a database table
    # For now, return key sources
    return [
        DataSource(
            name="US Census LEHD/LODES",
            agency="US Census Bureau",
            url="https://lehd.ces.census.gov/data/",
            update_frequency="Annual",
            latest_available="2021"
        ),
        DataSource(
            name="BLS QCEW",
            agency="Bureau of Labor Statistics",
            url="https://www.bls.gov/cew/",
            update_frequency="Quarterly",
            latest_available="2025 Q3"
        ),
        DataSource(
            name="USASpending.gov",
            agency="US Treasury",
            url="https://www.usaspending.gov/",
            update_frequency="Daily",
            latest_available="Current"
        ),
        DataSource(
            name="Census ACS 5-Year",
            agency="US Census Bureau",
            url="https://www.census.gov/programs-surveys/acs",
            update_frequency="Annual",
            latest_available="2019-2023"
        ),
        DataSource(
            name="IRS County Migration",
            agency="IRS Statistics of Income",
            url="https://www.irs.gov/statistics/soi-tax-stats-migration-data",
            update_frequency="Annual",
            latest_available="2021-2022"
        ),
        DataSource(
            name="FEMA National Flood Hazard Layer",
            agency="FEMA",
            url="https://msc.fema.gov/",
            update_frequency="Rolling updates",
            latest_available="Current"
        ),
        DataSource(
            name="EPA EJScreen",
            agency="EPA",
            url="https://www.epa.gov/ejscreen",
            update_frequency="Annual",
            latest_available="2023"
        )
    ]


@router.get("/metadata/classifications")
async def get_classification_definitions():
    """
    Get definitions for classification categories

    Returns:
        Classification definitions and thresholds
    """
    return {
        "directional_status": {
            "improving": {
                "definition": "Multiple reinforcing structural tailwinds present",
                "criteria": f"≥{settings.THRESHOLD_IMPROVING_MIN_LAYERS} layers above {settings.THRESHOLD_IMPROVING_HIGH} AND none below {settings.THRESHOLD_IMPROVING_LOW}"
            },
            "stable": {
                "definition": "Balanced signals, mixed pressure directions",
                "criteria": "Does not meet criteria for Improving or At Risk"
            },
            "at_risk": {
                "definition": "Structural headwinds constraining growth capacity",
                "criteria": f"≥{settings.THRESHOLD_AT_RISK_COUNT} layers below {settings.THRESHOLD_AT_RISK_LOW} OR severe risk drag with ≥1 layer below {settings.THRESHOLD_AT_RISK_WITH_DRAG}"
            }
        },
        "confidence_overlay": {
            "strong": {
                "definition": "High policy delivery reliability",
                "criteria": f"Policy persistence score ≥ {settings.CONFIDENCE_STRONG_MIN}"
            },
            "conditional": {
                "definition": "Mixed track record, administrative dependency",
                "criteria": f"{settings.CONFIDENCE_CONDITIONAL_MIN} ≤ score < {settings.CONFIDENCE_STRONG_MIN}"
            },
            "fragile": {
                "definition": "Low policy follow-through, high uncertainty",
                "criteria": f"Policy persistence score < {settings.CONFIDENCE_CONDITIONAL_MIN}"
            }
        },
        "disclaimer": "This tool does NOT predict housing prices or provide investment recommendations. "
                     "Classifications indicate directional pressure under current policy trends."
    }


@router.get("/counties")
async def list_counties():
    """
    List all Maryland counties with FIPS codes

    Returns:
        Dict mapping FIPS codes to county names
    """
    return MD_COUNTY_FIPS
