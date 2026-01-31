"""
Regression tests for Layer 6 dynamic year calculation fixes.

These tests verify that:
1. ACS_GEOGRAPHY_MAX_YEAR is dynamically calculated
2. CDC SVI fetch defaults to current year - 1
3. EJScreen fetch defaults to current year - 1
4. Synthetic data is properly flagged with is_synthetic=True
"""

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
import pandas as pd


class TestDynamicYearCalculation:
    """Test that hardcoded years have been replaced with dynamic calculations."""

    def test_acs_geography_max_year_is_dynamic(self):
        """ACS_GEOGRAPHY_MAX_YEAR should be current year - 2."""
        from src.ingest.layer6_risk_vulnerability import ACS_GEOGRAPHY_MAX_YEAR

        expected_year = datetime.now().year - 2
        assert ACS_GEOGRAPHY_MAX_YEAR == expected_year, (
            f"ACS_GEOGRAPHY_MAX_YEAR should be {expected_year}, got {ACS_GEOGRAPHY_MAX_YEAR}"
        )

    def test_ejscreen_year_is_dynamic_v1(self):
        """EJSCREEN_YEAR in v1 module should be current year - 1."""
        from src.ingest.layer6_risk import EJSCREEN_YEAR

        expected_year = datetime.now().year - 1
        assert EJSCREEN_YEAR == expected_year, (
            f"EJSCREEN_YEAR should be {expected_year}, got {EJSCREEN_YEAR}"
        )

    def test_cdc_svi_default_year_is_none(self):
        """fetch_cdc_svi_data should default year to None (then calculate dynamically)."""
        from src.ingest.layer6_risk_vulnerability import fetch_cdc_svi_data
        import inspect

        sig = inspect.signature(fetch_cdc_svi_data)
        year_param = sig.parameters.get('year')

        assert year_param is not None, "year parameter should exist"
        assert year_param.default is None, (
            f"year default should be None, got {year_param.default}"
        )

    def test_ejscreen_fetch_default_year_is_none(self):
        """fetch_expanded_ejscreen_data should default year to None."""
        from src.ingest.layer6_risk_vulnerability import fetch_expanded_ejscreen_data
        import inspect

        sig = inspect.signature(fetch_expanded_ejscreen_data)
        year_param = sig.parameters.get('year')

        assert year_param is not None, "year parameter should exist"
        assert year_param.default is None, (
            f"year default should be None, got {year_param.default}"
        )


class TestSyntheticDataFlagging:
    """Test that synthetic data is properly flagged."""

    def test_synthetic_svi_data_has_is_synthetic_flag(self):
        """Synthetic SVI data should have is_synthetic=True column."""
        from src.ingest.layer6_risk_vulnerability import _generate_synthetic_svi_data

        df = _generate_synthetic_svi_data()

        assert 'is_synthetic' in df.columns, "is_synthetic column should exist"
        assert df['is_synthetic'].all(), "All rows should have is_synthetic=True"

    def test_synthetic_svi_data_covers_all_counties(self):
        """Synthetic SVI data should cover all 24 MD counties."""
        from src.ingest.layer6_risk_vulnerability import _generate_synthetic_svi_data
        from config.settings import MD_COUNTY_FIPS

        df = _generate_synthetic_svi_data()

        assert len(df) == len(MD_COUNTY_FIPS), (
            f"Should have {len(MD_COUNTY_FIPS)} rows, got {len(df)}"
        )

        for fips in MD_COUNTY_FIPS.keys():
            assert fips in df['fips_code'].values, f"Missing county {fips}"

    def test_synthetic_data_has_valid_vulnerability_scores(self):
        """Synthetic SVI scores should be in valid 0-1 range."""
        from src.ingest.layer6_risk_vulnerability import _generate_synthetic_svi_data

        df = _generate_synthetic_svi_data()

        score_cols = [
            'socioeconomic_vulnerability',
            'household_vulnerability',
            'minority_language_vulnerability',
            'housing_transport_vulnerability',
            'social_vulnerability_index'
        ]

        for col in score_cols:
            assert col in df.columns, f"Missing column {col}"
            assert df[col].between(0, 1).all(), f"{col} values should be in [0, 1]"


class TestYearCalculationEdgeCases:
    """Test edge cases for year calculations."""

    def test_acs_max_year_never_exceeds_current(self):
        """ACS max year should never be greater than current year."""
        from src.ingest.layer6_risk_vulnerability import _get_acs_max_year

        max_year = _get_acs_max_year()
        current_year = datetime.now().year

        assert max_year < current_year, (
            f"ACS max year {max_year} should be less than current year {current_year}"
        )

    def test_acs_max_year_is_reasonable(self):
        """ACS max year should be within reasonable range (last 5 years)."""
        from src.ingest.layer6_risk_vulnerability import _get_acs_max_year

        max_year = _get_acs_max_year()
        current_year = datetime.now().year

        assert current_year - max_year <= 5, (
            f"ACS max year {max_year} is too old (> 5 years from current)"
        )
        assert current_year - max_year >= 1, (
            f"ACS max year {max_year} should be at least 1 year behind"
        )
