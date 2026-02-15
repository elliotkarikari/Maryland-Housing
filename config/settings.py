"""
Maryland Viability Atlas - Application Settings
Manages environment variables and configuration using Pydantic
"""

import os
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Required:
        - DATABASE_URL
        - MAPBOX_ACCESS_TOKEN
        - CENSUS_API_KEY

    Optional:
        - BLS_API_KEY (improves rate limits)
        - SENTRY_DSN (error tracking)
    """

    # Database
    DATABASE_URL: str

    # External APIs
    MAPBOX_ACCESS_TOKEN: str
    CENSUS_API_KEY: str
    BLS_API_KEY: Optional[str] = None

    # Application
    ENVIRONMENT: str = "production"
    LOG_LEVEL: str = "INFO"
    DEBUG: bool = False

    # Error tracking (optional)
    SENTRY_DSN: Optional[str] = None

    # Data refresh settings
    AUTO_REFRESH_ENABLED: bool = True
    REFRESH_TIMEZONE: str = "America/New_York"

    # API settings
    API_TITLE: str = "Maryland Growth & Family Viability Atlas API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "Spatial analytics API for Maryland directional growth signals"
    CORS_ALLOW_ORIGINS: str = "http://localhost:3000,http://127.0.0.1:3000"

    # Data backend routing
    # databricks = primary ingestion backend
    # postgres = local/legacy fallback
    DATA_BACKEND: str = "databricks"
    DATABRICKS_SQLALCHEMY_URL: Optional[str] = None
    DATABRICKS_SERVER_HOSTNAME: Optional[str] = None
    DATABRICKS_HTTP_PATH: Optional[str] = None
    DATABRICKS_ACCESS_TOKEN: Optional[str] = None
    DATABRICKS_CATALOG: str = "hive_metastore"
    DATABRICKS_SCHEMA: str = "default"

    # Rate limiting (requests per minute)
    CENSUS_API_RATE_LIMIT: int = 8  # Conservative: 500/day = ~8/min
    BLS_API_RATE_LIMIT: int = 8
    USASPENDING_RATE_LIMIT: int = 60  # No documented limit, be respectful

    # Data sources
    LEHD_BASE_URL: str = "https://lehd.ces.census.gov/data/lodes/LODES8"
    BLS_API_BASE_URL: str = "https://api.bls.gov/publicAPI/v2"
    USASPENDING_API_URL: str = "https://api.usaspending.gov/api/v2"
    CENSUS_API_BASE_URL: str = "https://api.census.gov/data"
    FEMA_NFHL_URL: str = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
    FEMA_NFHL_URL_FALLBACK: str = (
        "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer"
    )
    FEMA_NFHL_FEATURE_URL: str = (
        "https://services.arcgis.com/2gdL2gxYNFY2TOUb/arcgis/rest/services/FEMA_National_Flood_Hazard_Layer/FeatureServer/0"
    )
    FEMA_SKIP_NFHL: bool = True
    EPA_EJSCREEN_URL: str = "https://gaftp.epa.gov/EJSCREEN"
    EPA_EJSCREEN_ZENODO_URL: Optional[str] = (
        "https://zenodo.org/records/14767363/files/2023.zip?download=1"
    )
    NOAA_SLR_DATA_URL: Optional[str] = None
    NOAA_SLR_VECTOR_URLS: Optional[str] = (
        "https://coast.noaa.gov/slrdata/Sea_Level_Rise_Vectors/MD/MD_East_slr_final_dist_HalfFoot.zip;"
        "https://coast.noaa.gov/slrdata/Sea_Level_Rise_Vectors/MD/MD_West_slr_final_dist_HalfFoot.zip"
    )
    CDC_HEAT_DATA_URL: Optional[str] = None
    NLCD_LAND_COVER_URL: Optional[str] = None
    NCES_CCD_PRELIM_URL: Optional[str] = (
        "https://prod-ies-dm-migration.s3.us-gov-west-1.amazonaws.com/nces/asset_builder_data/"
        "2025/08/2025046%20Preliminary%20Data%20Release%20CCD%20Nonfiscal_0.zip"
    )
    HUD_USER_API_TOKEN: Optional[str] = None
    HUD_FMR_DATA_URL: Optional[str] = None
    HUD_FMR_DATA_PATH: Optional[str] = None
    HUD_LIHTC_DATA_URL: Optional[str] = None
    HUD_LIHTC_DATA_PATH: Optional[str] = None
    USPS_VACANCY_DATA_URL: Optional[str] = None
    USPS_VACANCY_DATA_PATH: Optional[str] = None
    USPS_ZIP_COUNTY_CROSSWALK_URL: Optional[str] = None
    USPS_ZIP_COUNTY_CROSSWALK_PATH: Optional[str] = None
    CENSUS_ZIP_COUNTY_CROSSWALK_URL: Optional[str] = None
    HUD_USPS_API_URL: Optional[str] = None
    LOW_VACANCY_COUNTIES_URL: Optional[str] = None
    LOW_VACANCY_COUNTIES_PATH: Optional[str] = None
    CENSUS_QWI_DATA_URL: Optional[str] = None
    CENSUS_QWI_DATA_PATH: Optional[str] = None
    CENSUS_QWI_DATASET: str = "timeseries/qwi/sa"

    # Maryland state FIPS code
    MD_STATE_FIPS: str = "24"

    # Data years (current defaults - will be dynamic in production)
    LODES_LATEST_YEAR: int = 2022
    ACS_LATEST_YEAR: int = 2024
    ACS_GEOGRAPHY_MAX_YEAR: int = 2022
    NCES_OBSERVED_MAX_YEAR: int = 2024
    LODES_LAG_YEARS: int = 2

    # Prediction alignment settings
    PREDICT_TO_YEAR: int = 2025
    PREDICTION_MIN_YEARS: int = 3
    PREDICTION_MAX_EXTRAP_YEARS: int = 2
    USE_EFFECTIVE_VALUES: bool = False

    # File storage
    EXPORT_DIR: str = "exports"
    LOG_DIR: str = "logs"

    # Classification thresholds (directional status)
    THRESHOLD_IMPROVING_MIN_LAYERS: int = 3  # Layers above high threshold
    THRESHOLD_IMPROVING_HIGH: float = 0.6  # High performance threshold
    THRESHOLD_IMPROVING_LOW: float = 0.3  # Minimum acceptable (none below this)

    THRESHOLD_AT_RISK_COUNT: int = 2  # Layers below low threshold
    THRESHOLD_AT_RISK_LOW: float = 0.3
    THRESHOLD_AT_RISK_WITH_DRAG: float = 0.4  # If severe risk drag present

    # Risk drag penalty floor (caps over-penalization)
    RISK_DRAG_PENALTY_FLOOR: float = 0.5

    # Confidence thresholds (policy persistence)
    CONFIDENCE_STRONG_MIN: float = 0.67
    CONFIDENCE_CONDITIONAL_MIN: float = 0.34
    # Below CONDITIONAL_MIN = Fragile

    # AI settings
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_MODEL: str = "gpt-5.1-mini"  # Default OpenAI model
    OPENAI_MAX_TOKENS: int = 4000
    AI_ENABLED: bool = False  # Set True if API key available

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=True, extra="ignore"
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Create directories if they don't exist
        os.makedirs(self.EXPORT_DIR, exist_ok=True)
        os.makedirs(self.LOG_DIR, exist_ok=True)


@lru_cache()
def get_settings() -> Settings:
    """
    Returns cached settings instance.
    Uses lru_cache to avoid re-reading .env on every call.
    """
    return Settings()


# Maryland county FIPS codes for validation
MD_COUNTY_FIPS = {
    "24001": "Allegany County",
    "24003": "Anne Arundel County",
    "24005": "Baltimore County",
    "24009": "Calvert County",
    "24011": "Caroline County",
    "24013": "Carroll County",
    "24015": "Cecil County",
    "24017": "Charles County",
    "24019": "Dorchester County",
    "24021": "Frederick County",
    "24023": "Garrett County",
    "24025": "Harford County",
    "24027": "Howard County",
    "24029": "Kent County",
    "24031": "Montgomery County",
    "24033": "Prince George's County",
    "24035": "Queen Anne's County",
    "24037": "St. Mary's County",
    "24039": "Somerset County",
    "24041": "Talbot County",
    "24043": "Washington County",
    "24045": "Wicomico County",
    "24047": "Worcester County",
    "24510": "Baltimore City",
}

# NAICS sector codes for LODES CNS (Census Nomenclature System)
LODES_SECTOR_CODES = {
    "CNS01": "Agriculture, Forestry, Fishing and Hunting",
    "CNS02": "Mining, Quarrying, and Oil and Gas Extraction",
    "CNS03": "Utilities",
    "CNS04": "Construction",
    "CNS05": "Manufacturing",
    "CNS06": "Wholesale Trade",
    "CNS07": "Retail Trade",
    "CNS08": "Transportation and Warehousing",
    "CNS09": "Information",
    "CNS10": "Finance and Insurance",
    "CNS11": "Real Estate and Rental and Leasing",
    "CNS12": "Professional, Scientific, and Technical Services",
    "CNS13": "Management of Companies and Enterprises",
    "CNS14": "Administrative Support and Waste Management",
    "CNS15": "Educational Services",
    "CNS16": "Health Care and Social Assistance",
    "CNS17": "Arts, Entertainment, and Recreation",
    "CNS18": "Accommodation and Food Services",
    "CNS19": "Other Services (except Public Administration)",
    "CNS20": "Public Administration",
}

# Stable sectors for employment gravity calculation
STABLE_SECTORS = ["CNS15", "CNS16", "CNS20"]  # Education, Health, Public Admin
