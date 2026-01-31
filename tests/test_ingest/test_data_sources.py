"""
Regression tests for data source utilities.

These tests verify that:
1. BLS QCEW fetch uses requests with timeout (not pd.read_csv directly)
2. Timeout errors are properly caught and logged
3. Rate limiters are properly applied
"""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import requests


class TestBLSQCEWTimeout:
    """Test that BLS QCEW fetch has proper timeout handling."""

    @patch('src.utils.data_sources.requests.get')
    def test_bls_qcew_uses_requests_with_timeout(self, mock_get):
        """BLS QCEW fetch should use requests.get with timeout."""
        from src.utils.data_sources import fetch_bls_qcew

        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "area_fips,own_code,industry_code\n24001,0,10\n"
        mock_get.return_value = mock_response

        # Call with minimal area to speed up test
        fetch_bls_qcew(year=2024, quarter=1, area_codes=["001"])

        # Verify requests.get was called with timeout
        mock_get.assert_called()
        call_kwargs = mock_get.call_args[1]
        assert 'timeout' in call_kwargs, "requests.get should be called with timeout"
        assert call_kwargs['timeout'] == 60, "timeout should be 60 seconds"

    @patch('src.utils.data_sources.requests.get')
    def test_bls_qcew_handles_timeout_gracefully(self, mock_get):
        """BLS QCEW fetch should handle timeout without crashing."""
        from src.utils.data_sources import fetch_bls_qcew

        # Mock timeout exception
        mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")

        # Should not raise, should return empty DataFrame
        result = fetch_bls_qcew(year=2024, quarter=1, area_codes=["001"])

        assert isinstance(result, pd.DataFrame)
        assert result.empty, "Should return empty DataFrame on timeout"

    @patch('src.utils.data_sources.requests.get')
    def test_bls_qcew_handles_connection_error(self, mock_get):
        """BLS QCEW fetch should handle connection errors gracefully."""
        from src.utils.data_sources import fetch_bls_qcew

        # Mock connection error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")

        # Should not raise, should return empty DataFrame
        result = fetch_bls_qcew(year=2024, quarter=1, area_codes=["001"])

        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestRateLimiterConfiguration:
    """Test that rate limiters are properly configured."""

    def test_census_rate_limiter_exists(self):
        """Census rate limiter should be configured."""
        from src.utils.data_sources import census_limiter

        assert census_limiter is not None
        assert hasattr(census_limiter, 'calls_per_minute')
        assert census_limiter.calls_per_minute > 0

    def test_bls_rate_limiter_exists(self):
        """BLS rate limiter should be configured."""
        from src.utils.data_sources import bls_limiter

        assert bls_limiter is not None
        assert hasattr(bls_limiter, 'calls_per_minute')
        assert bls_limiter.calls_per_minute > 0

    def test_usaspending_rate_limiter_exists(self):
        """USASpending rate limiter should be configured."""
        from src.utils.data_sources import usaspending_limiter

        assert usaspending_limiter is not None
        assert hasattr(usaspending_limiter, 'calls_per_minute')
        assert usaspending_limiter.calls_per_minute > 0


class TestDataSourceImports:
    """Test that io module is properly imported for StringIO."""

    def test_io_module_available(self):
        """io module should be imported for StringIO usage."""
        import src.utils.data_sources as ds

        # Verify io is used in the module (by checking for StringIO usage pattern)
        import inspect
        source = inspect.getsource(ds.fetch_bls_qcew)

        assert 'StringIO' in source or 'io.StringIO' in source, (
            "fetch_bls_qcew should use io.StringIO for parsing CSV response"
        )
