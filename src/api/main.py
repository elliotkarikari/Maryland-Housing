"""
Maryland Viability Atlas - FastAPI Application
Read-only API for serving map data and metadata
"""

import os
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session

from config.database import get_db_session, test_connection
from config.settings import get_settings
from src.api.chat_routes import router as chat_router
from src.api.routes import router
from src.utils.logging import setup_logging

settings = get_settings()
logger = setup_logging("api")


def _parse_cors_allow_origins(raw: str) -> list[str]:
    origins = [origin.strip() for origin in (raw or "").split(",") if origin.strip()]
    return origins or ["http://localhost:3000", "http://127.0.0.1:3000"]


# Initialize FastAPI app
app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description=settings.API_DESCRIPTION,
    docs_url="/docs" if settings.DEBUG else None,  # Disable in production
    redoc_url="/redoc" if settings.DEBUG else None,
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_allow_origins(settings.CORS_ALLOW_ORIGINS),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router, prefix="/api/v1")
app.include_router(chat_router, prefix="/api/v1")


@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    logger.info(f"Starting {settings.API_TITLE} v{settings.API_VERSION}")
    logger.info(f"Environment: {settings.ENVIRONMENT}")

    # Test database connection
    if not test_connection():
        logger.error("Database connection failed on startup")
        # Don't raise - allow app to start but health check will fail


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    logger.info("Shutting down API")


@app.get("/")
async def root():
    """
    Root endpoint - API information
    """
    return {
        "name": settings.API_TITLE,
        "version": settings.API_VERSION,
        "description": settings.API_DESCRIPTION,
        "docs": "/docs" if settings.DEBUG else "disabled in production",
        "endpoints": {
            "health": "/health",
            "counties_geojson": "/api/v1/layers/counties/latest",
            "area_detail": "/api/v1/areas/{geoid}",
            "chat": "/api/v1/chat",
            "data_sources": "/api/v1/metadata/sources",
            "capabilities": "/api/v1/metadata/capabilities",
            "latest_refresh": "/api/v1/metadata/refresh",
        },
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint for Railway/monitoring
    """
    try:
        db_healthy = test_connection()

        # Check if latest GeoJSON exists
        geojson_path = os.path.join(settings.EXPORT_DIR, "md_counties_latest.geojson")
        geojson_exists = os.path.exists(geojson_path)

        status = "healthy" if (db_healthy and geojson_exists) else "degraded"

        return {
            "status": status,
            "database": "connected" if db_healthy else "disconnected",
            "geojson_export": "available" if geojson_exists else "missing",
            "environment": settings.ENVIRONMENT,
        }

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
    )
