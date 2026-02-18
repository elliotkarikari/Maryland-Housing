from contextlib import contextmanager

import numpy as np
import pandas as pd

from src.ingest.layer6_risk import _build_risk_rows, store_risk_data


def test_build_risk_rows_replaces_nan_with_none():
    df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2024],
            "sfha_area_sq_mi": [np.nan],
            "risk_drag_index": [0.42],
        }
    )

    rows = _build_risk_rows(df)
    row = rows[0]
    assert row["fips_code"] == "24001"
    assert row["data_year"] == 2024
    assert row["sfha_area_sq_mi"] is None
    assert row["risk_drag_index"] == 0.42


def test_store_risk_data_uses_batched_execute(monkeypatch):
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

    monkeypatch.setattr("src.ingest.layer6_risk.get_db", fake_get_db)

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2024, 2024],
            "risk_drag_index": [0.5, np.nan],
            "sfha_area_sq_mi": [1.2, 0.9],
            "sfha_pct_of_county": [0.1, 0.07],
            "sea_level_rise_exposure": [np.nan, np.nan],
            "extreme_heat_days_annual": [np.nan, np.nan],
            "pm25_avg": [7.1, 6.8],
            "ozone_avg": [35.0, 33.0],
            "proximity_hazwaste_score": [0.2, 0.1],
            "traffic_proximity_score": [0.3, 0.25],
            "bridges_total": [100, 80],
            "bridges_structurally_deficient": [5, 4],
            "bridges_deficient_pct": [0.05, 0.05],
        }
    )

    store_risk_data(df)

    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 2
    _, delete_params = fake_db.executions[0]
    assert delete_params == {"years": [2024]}

    _, insert_params = fake_db.executions[1]
    assert isinstance(insert_params, list)
    assert len(insert_params) == 2
    assert insert_params[1]["risk_drag_index"] is None
