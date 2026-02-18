import numpy as np
import pandas as pd

from src.ingest.layer3_education_accessibility import (
    _bool_or_default,
    _build_county_education_rows,
)


def test_build_county_education_rows_coerces_missing_values():
    df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "total_schools": [np.nan],
            "schools_with_prek": [4],
            "avg_schools_accessible_15min": [0.5],
            "avg_schools_accessible_30min": [np.nan],
            "avg_ela_proficiency": [np.nan],
            "avg_math_proficiency": [42.0],
            "education_opportunity_index": [np.nan],
        }
    )

    rows = _build_county_education_rows(df=df, data_year=2025, nces_year=2024, acs_year=2023)

    assert len(rows) == 1
    row = rows[0]
    assert row["fips"] == "24001"
    assert row["total_schools"] == 0
    assert row["schools_with_prek"] == 4
    assert row["avg_15"] == 0.5
    assert row["avg_30"] == 0.0
    assert row["ela_prof"] is None
    assert row["math_prof"] == 42.0
    assert row["opportunity_index"] == 0.0
    assert row["nces_year"] == 2024
    assert row["acs_year"] == 2023


def test_bool_or_default_handles_text_and_missing():
    assert _bool_or_default("true", False) is True
    assert _bool_or_default("0", True) is False
    assert _bool_or_default(np.nan, True) is True
