"""
Maryland Viability Atlas - FastAPI Backend
Serves county synthesis data and GeoJSON for map visualization
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import text

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.database import get_db
from config.settings import MD_COUNTY_FIPS

app = FastAPI(
    title="Maryland Viability Atlas API",
    description="API for Maryland county growth and viability synthesis data",
    version="2.0.0"
)

# CORS middleware for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files (frontend)
FRONTEND_DIR = PROJECT_ROOT / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# Response Models
class LayerScores(BaseModel):
    employment_gravity: Optional[float] = None
    mobility_optionality: Optional[float] = None
    school_trajectory: Optional[float] = None
    housing_elasticity: Optional[float] = None
    demographic_momentum: Optional[float] = None
    risk_drag: Optional[float] = None


class CountyDetail(BaseModel):
    fips_code: str
    county_name: str
    data_year: int

    # Classification
    synthesis_grouping: str
    directional_class: str
    confidence_class: str
    composite_score: float
    classification_version: str

    # Layer scores
    layer_scores: LayerScores

    # Narratives
    primary_strengths: List[str]
    primary_weaknesses: List[str]
    key_trends: List[str]

    # Metadata
    last_updated: datetime


# Helper Functions
def _format_score(value) -> Optional[float]:
    """Convert Decimal to float, handle None."""
    if value is None:
        return None
    return float(value)


def _parse_array_field(value) -> List[str]:
    """Parse PostgreSQL array field to Python list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    # Handle string representation of array
    if isinstance(value, str):
        value = value.strip('{}')
        if not value:
            return []
        return [s.strip().strip('"') for s in value.split(',')]
    return []


# Endpoints
@app.get("/")
async def root():
    """Serve the main map interface."""
    index_path = FRONTEND_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Frontend not found")
    return FileResponse(index_path)


@app.get("/api")
async def api_info():
    """API information endpoint."""
    return {
        "name": "Maryland Viability Atlas API",
        "version": "2.0.0",
        "endpoints": {
            "counties": "/api/v1/layers/counties/latest",
            "county_detail": "/api/v1/areas/{fips_code}"
        }
    }


@app.get("/api/v1/layers/counties/latest")
async def get_counties_geojson():
    """
    Return GeoJSON with all Maryland counties and their synthesis classifications.
    This endpoint serves the static GeoJSON file.
    """
    geojson_path = PROJECT_ROOT / "frontend" / "md_counties_latest.geojson"

    if not geojson_path.exists():
        raise HTTPException(status_code=404, detail="GeoJSON file not found")

    return FileResponse(
        path=geojson_path,
        media_type="application/json",
        filename="md_counties_latest.geojson"
    )


@app.get("/api/v1/areas/{fips_code}", response_model=CountyDetail)
async def get_county_detail(fips_code: str):
    """
    Get detailed synthesis data for a specific county.

    Args:
        fips_code: 5-digit FIPS code (e.g., "24001" for Allegany County)

    Returns:
        CountyDetail: Complete county synthesis information
    """
    # Validate FIPS code
    if fips_code not in MD_COUNTY_FIPS:
        raise HTTPException(
            status_code=404,
            detail=f"County with FIPS code {fips_code} not found"
        )

    with get_db() as db:
        # Query final_synthesis table
        query = text("""
            SELECT
                geoid as fips_code,
                county_name,
                data_year,
                synthesis_grouping,
                directional_class,
                confidence_class,
                composite_score,
                classification_version,
                employment_gravity_score,
                mobility_optionality_score,
                school_trajectory_score,
                housing_elasticity_score,
                demographic_momentum_score,
                risk_drag_score,
                primary_strengths,
                primary_weaknesses,
                key_trends,
                last_updated
            FROM final_synthesis
            WHERE geoid = :fips_code
        """)

        result = db.execute(query, {"fips_code": fips_code})
        row = result.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"No synthesis data found for county {fips_code}"
            )

        # Build response
        county_detail = CountyDetail(
            fips_code=row.fips_code,
            county_name=row.county_name or MD_COUNTY_FIPS.get(fips_code, "Unknown"),
            data_year=row.data_year,
            synthesis_grouping=row.synthesis_grouping,
            directional_class=row.directional_class,
            confidence_class=row.confidence_class,
            composite_score=_format_score(row.composite_score),
            classification_version=row.classification_version or "v2.0-multiyear",
            layer_scores=LayerScores(
                employment_gravity=_format_score(row.employment_gravity_score),
                mobility_optionality=_format_score(row.mobility_optionality_score),
                school_trajectory=_format_score(row.school_trajectory_score),
                housing_elasticity=_format_score(row.housing_elasticity_score),
                demographic_momentum=_format_score(row.demographic_momentum_score),
                risk_drag=_format_score(row.risk_drag_score)
            ),
            primary_strengths=_parse_array_field(row.primary_strengths),
            primary_weaknesses=_parse_array_field(row.primary_weaknesses),
            key_trends=_parse_array_field(row.key_trends),
            last_updated=row.last_updated or datetime.now()
        )

        return county_detail


@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint."""
    try:
        with get_db() as db:
            db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
