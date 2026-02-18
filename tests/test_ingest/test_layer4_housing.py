from contextlib import contextmanager

import numpy as np
import pandas as pd

from src.ingest.layer4_housing import _apply_housing_trends, _build_housing_rows, store_housing_data


def test_apply_housing_trends_computes_price_and_permit_deltas():
    combined = pd.DataFrame(
        {
            "fips_code": ["24001", "24001", "24001", "24003", "24003"],
            "data_year": [2018, 2021, 2023, 2021, 2023],
            "price_to_income_ratio": [3.0, 3.4, 3.8, 2.8, 2.7],
            "permits_per_1000_households": [10.0, 9.0, 12.0, 5.0, 4.0],
        }
    )

    result = _apply_housing_trends(combined)
    row_24001_2023 = result[(result["fips_code"] == "24001") & (result["data_year"] == 2023)].iloc[
        0
    ]
    row_24003_2023 = result[(result["fips_code"] == "24003") & (result["data_year"] == 2023)].iloc[
        0
    ]

    assert float(row_24001_2023["price_to_income_5yr_change"]) == 0.8
    assert row_24001_2023["permits_3yr_trend"] == "increasing"
    assert pd.isna(row_24003_2023["price_to_income_5yr_change"])
    assert row_24003_2023["permits_3yr_trend"] == "decreasing"


def test_build_housing_rows_coerces_optional_fields():
    df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2023],
            "permits_total": [np.nan],
            "price_to_income_ratio": [3.2],
            "permits_3yr_trend": [pd.NA],
            "supply_responsiveness_score": [0.6],
            "housing_elasticity_index": [0.7],
        }
    )

    rows = _build_housing_rows(df)
    row = rows[0]
    assert row["fips_code"] == "24001"
    assert row["data_year"] == 2023
    assert row["permits_total"] is None
    assert row["permits_3yr_trend"] is None
    assert row["price_to_income_ratio"] == 3.2
    assert row["supply_responsiveness_score"] == 0.6
    assert row["housing_elasticity_index"] == 0.7


def test_store_housing_data_uses_batched_execute(monkeypatch):
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

    monkeypatch.setattr("src.ingest.layer4_housing.get_db", fake_get_db)

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2023, 2023],
            "price_to_income_ratio": [3.2, 2.9],
            "supply_responsiveness_score": [0.6, 0.4],
            "housing_elasticity_index": [0.7, 0.3],
        }
    )

    store_housing_data(df)

    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 1
    _, params = fake_db.executions[0]
    assert isinstance(params, list)
    assert len(params) == 2
