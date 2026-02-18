from contextlib import contextmanager

import numpy as np
import pandas as pd

from src.ingest.layer3_schools import (
    _build_school_rows,
    calculate_school_indicators,
    store_school_data,
)


def test_calculate_school_indicators_computes_3yr_change(monkeypatch):
    enrollment_df = pd.DataFrame(
        {
            "fips_code": ["24001", "24001", "24001"],
            "data_year": [2020, 2023, 2024],
            "total_enrollment": [100.0, 130.0, 135.0],
            "schools_total": [3, 3, 3],
        }
    )
    monkeypatch.setattr(
        "src.ingest.layer3_schools._build_enrollment_timeseries",
        lambda latest_year=None: enrollment_df,
    )

    result = calculate_school_indicators(latest_year=2024)
    row_2023 = result[result["data_year"] == 2023].iloc[0]
    row_2024 = result[result["data_year"] == 2024].iloc[0]

    assert float(row_2023["enrollment_3yr_change_pct"]) == 30.0
    assert pd.isna(row_2024["enrollment_3yr_change_pct"])


def test_build_school_rows_coerces_missing_values():
    df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2024],
            "total_enrollment": [np.nan],
            "schools_total": [10],
            "enrollment_3yr_change_pct": [np.nan],
            "enrollment_momentum_score": [0.6],
        }
    )
    rows = _build_school_rows(df)
    row = rows[0]
    assert row["fips_code"] == "24001"
    assert row["data_year"] == 2024
    assert row["total_enrollment"] is None
    assert row["schools_total"] == 10
    assert row["enrollment_3yr_change_pct"] is None
    assert row["enrollment_momentum_score"] == 0.6


def test_store_school_data_uses_batched_execute(monkeypatch):
    class FakeDB:
        def __init__(self):
            self.executions = []
            self.commit_calls = 0

        def execute(self, sql, params=None):
            self.executions.append((sql, params))

        def commit(self):
            self.commit_calls += 1

    fake_db = FakeDB()

    @contextmanager
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr("src.ingest.layer3_schools.get_db", fake_get_db)

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2024, 2024],
            "total_enrollment": [1000, 900],
            "schools_total": [10, 8],
            "enrollment_3yr_change_pct": [5.0, np.nan],
            "enrollment_momentum_score": [0.7, 0.3],
        }
    )

    store_school_data(df)

    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 2

    _, delete_params = fake_db.executions[0]
    assert delete_params is None

    _, insert_params = fake_db.executions[1]
    assert isinstance(insert_params, list)
    assert len(insert_params) == 2
    assert insert_params[1]["enrollment_3yr_change_pct"] is None
