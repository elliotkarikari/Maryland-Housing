import numpy as np
import pandas as pd

from src.ingest.layer5_demographic_equity import (
    _build_county_demographic_rows,
    _build_tract_demographic_rows,
    compute_segregation_indices,
)


def test_build_tract_demographic_rows_coerces_missing_values():
    df = pd.DataFrame(
        {
            "tract_geoid": ["24001000100"],
            "fips_code": ["24001"],
            "total_population": [np.nan],
            "pop_25_44": [120],
            "median_family_income": [np.nan],
            "demographic_opportunity_score": [0.65],
        }
    )

    rows = _build_tract_demographic_rows(df=df, data_year=2025, acs_year=2023)
    row = rows[0]

    assert row["tract"] == "24001000100"
    assert row["fips"] == "24001"
    assert row["pop"] == 0
    assert row["age_25_44"] == 120
    assert row["median_income"] is None
    assert row["composite"] == 0.65
    assert row["acs_year"] == 2023


def test_build_county_demographic_rows_handles_pct_and_nullable_rates():
    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "pop_total": [1000, 0],
            "pop_age_25_44": [250, 10],
            "net_migration_rate": [0.02, np.nan],
            "inflow_rate": [0.03, np.nan],
            "outflow_rate": [0.01, np.nan],
            "demographic_opportunity_index": [0.7, 0.2],
        }
    )

    rows = _build_county_demographic_rows(df=df, data_year=2025, acs_year=2023)

    assert len(rows) == 2
    assert rows[0]["fips"] == "24001"
    assert rows[0]["pop_25_44_pct"] == 0.25
    assert rows[0]["net_rate"] == 0.02
    assert rows[1]["fips"] == "24003"
    assert rows[1]["pop_25_44_pct"] == 0.0
    assert rows[1]["net_rate"] is None
    assert rows[1]["inflow_rate"] is None
    assert rows[1]["outflow_rate"] is None


def test_compute_segregation_indices_handles_valid_and_invalid_counties():
    df = pd.DataFrame(
        {
            "tract_geoid": ["24001000100", "24001000200", "24003000100"],
            "fips_code": ["24001", "24001", "24003"],
            "pop_black_alone": [50.0, 0.0, 0.0],
            "pop_hispanic": [0.0, 50.0, 0.0],
            "pop_white_alone": [50.0, 50.0, 100.0],
            "total_population": [100.0, 100.0, 100.0],
        }
    )

    result = compute_segregation_indices(df)
    county_24001 = result.loc[result["fips_code"] == "24001"].iloc[0]
    county_24003 = result.loc[result["fips_code"] == "24003"].iloc[0]

    assert county_24001["dissimilarity_index"] == 0.0
    assert county_24001["exposure_index"] == 0.5
    assert county_24001["isolation_index"] == 0.5

    assert county_24003["dissimilarity_index"] == 0.0
    assert county_24003["exposure_index"] == 0.5
    assert county_24003["isolation_index"] == 0.5
