"""Shared year selection policy for pipeline and ingestion modules."""

from __future__ import annotations

from datetime import datetime

from config.settings import get_settings

settings = get_settings()


def current_year() -> int:
    return datetime.now().year


def pipeline_default_year() -> int:
    """Default as-of year for pipeline execution."""
    return min(settings.PREDICT_TO_YEAR, current_year())


def lodes_year_for_data_year(data_year: int) -> int:
    """LODES generally lags; clamp to configured latest."""
    return min(data_year - settings.LODES_LAG_YEARS, settings.LODES_LATEST_YEAR)


def acs_year_for_data_year(data_year: int, lag_years: int = 1) -> int:
    """ACS year used for derived indicators with configurable lag."""
    return min(data_year - lag_years, settings.ACS_LATEST_YEAR)


def acs_geography_year(year: int) -> int:
    """Some tract/crosswalk geographies are only available through this cap."""
    return min(year, settings.ACS_GEOGRAPHY_MAX_YEAR)


def nces_observed_year(year: int) -> int:
    """NCES observed data upper bound."""
    return min(year, settings.NCES_OBSERVED_MAX_YEAR)


def layer3_default_data_year() -> int:
    """Layer 3 default target year."""
    return pipeline_default_year()


def layer5_default_data_year() -> int:
    """Layer 5 default target year based on ACS geography availability."""
    return min(settings.ACS_GEOGRAPHY_MAX_YEAR + 1, current_year())
