"""
Pytest configuration and shared fixtures for Maryland Housing Atlas tests.
"""

import pytest
import pandas as pd
from typing import Dict, Any


# Sample Maryland county FIPS codes for testing
SAMPLE_FIPS_CODES = [
    "24031",  # Montgomery County
    "24033",  # Prince George's County
    "24003",  # Anne Arundel County
    "24005",  # Baltimore County
    "24510",  # Baltimore City
]


@pytest.fixture
def sample_fips_codes() -> list:
    """Return sample FIPS codes for testing."""
    return SAMPLE_FIPS_CODES


@pytest.fixture
def sample_county_data() -> pd.DataFrame:
    """Generate sample county data for testing."""
    return pd.DataFrame({
        "fips_code": SAMPLE_FIPS_CODES,
        "county_name": [
            "Montgomery County",
            "Prince George's County",
            "Anne Arundel County",
            "Baltimore County",
            "Baltimore City",
        ],
        "population": [1062061, 967201, 588261, 854535, 585708],
        "data_year": [2023] * 5,
    })


@pytest.fixture
def sample_layer_scores() -> Dict[str, float]:
    """Generate sample layer scores for testing."""
    return {
        "employment_gravity": 0.72,
        "mobility_optionality": 0.65,
        "school_trajectory": 0.78,
        "housing_elasticity": 0.45,
        "demographic_momentum": 0.58,
        "risk_drag": 0.22,
    }


@pytest.fixture
def sample_synthesis_data() -> Dict[str, Any]:
    """Generate sample synthesis classification data."""
    return {
        "fips_code": "24031",
        "county_name": "Montgomery County",
        "synthesis_grouping": "emerging_tailwinds",
        "directional_class": "improving",
        "confidence_class": "strong",
        "composite_score": 0.68,
        "data_year": 2023,
    }


@pytest.fixture
def empty_dataframe() -> pd.DataFrame:
    """Return an empty DataFrame for edge case testing."""
    return pd.DataFrame()


# Database fixtures (uncomment when database tests are needed)
# @pytest.fixture(scope="session")
# def db_session():
#     """Create a test database session."""
#     from config.database import get_db
#     with get_db() as db:
#         yield db


# API fixtures (uncomment when API tests are needed)
# @pytest.fixture
# def test_client():
#     """Create a test client for API testing."""
#     from fastapi.testclient import TestClient
#     from src.api.main import app
#     return TestClient(app)
