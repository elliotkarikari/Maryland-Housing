import numpy as np
import pandas as pd

from src.ingest.layer4_housing_affordability import (
    _build_county_housing_update_rows,
    _build_tract_housing_rows,
)


def test_build_county_housing_update_rows_computes_opportunity_index():
    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "housing_affordability_score": [0.7, np.nan],
            "rent_to_income_ratio": [0.35, 0.45],
            "fmr_2br_to_income": [0.3, 12.0],  # second should be clipped to None by bounds
        }
    )

    rows = _build_county_housing_update_rows(
        df=df,
        data_year=2025,
        acs_year=2023,
        elasticity_scores={"24001": 0.5, "24003": 0.4},
    )

    assert len(rows) == 2
    assert rows[0]["fips_code"] == "24001"
    assert rows[0]["opportunity_index"] == 0.62  # 0.4*0.5 + 0.6*0.7
    assert rows[1]["fips_code"] == "24003"
    assert np.isnan(rows[1]["opportunity_index"])  # preserves prior NaN behavior
    assert rows[1]["fmr_2br_to_income"] is None


def test_build_tract_housing_rows_coerces_missing_and_bounds():
    df = pd.DataFrame(
        {
            "tract_geoid": ["24001000100"],
            "fips_code": ["24001"],
            "total_housing_units": [np.nan],
            "median_gross_rent": [-1],  # invalid -> None
            "price_to_income_ratio": [-5],  # out of bounds -> None
            "rent_to_income_ratio": [0.4],
            "housing_plus_transport_pct": [2.0],  # out of bounds -> None
            "housing_affordability_score": [0.55],
            "population": [1000],
        }
    )

    rows = _build_tract_housing_rows(df=df, data_year=2025, acs_year=2023)
    row = rows[0]

    assert row["tract_geoid"] == "24001000100"
    assert row["total_units"] == 0
    assert row["rent"] is None
    assert row["pti"] is None
    assert row["rti"] == 0.4
    assert row["ht_pct"] is None
    assert row["affordability_score"] == 0.55
    assert row["acs_year"] == 2023
