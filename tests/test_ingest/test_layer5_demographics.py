from contextlib import contextmanager

import numpy as np
import pandas as pd

from src.ingest.layer5_demographics import (
    _apply_momentum_features,
    apply_vacancy_predictions,
    store_demographic_data,
)


def test_store_demographic_data_uses_batched_execute(monkeypatch):
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

    monkeypatch.setattr("src.ingest.layer5_demographics.get_db", fake_get_db)

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2023, 2023],
            "pop_total": [1000, np.nan],
        }
    )

    store_demographic_data(df)

    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 2
    _, delete_params = fake_db.executions[0]
    assert delete_params == {"years": [2023]}

    _, batch_params = fake_db.executions[1]
    assert isinstance(batch_params, list)
    assert len(batch_params) == 2
    assert batch_params[1]["pop_total"] is None


def test_apply_vacancy_predictions_only_fills_eligible_future_rows(monkeypatch):
    monkeypatch.setattr(
        "src.ingest.layer5_demographics.settings.PREDICT_TO_YEAR", 2025, raising=False
    )
    monkeypatch.setattr(
        "src.ingest.layer5_demographics.settings.PREDICTION_MIN_YEARS", 2, raising=False
    )
    monkeypatch.setattr(
        "src.ingest.layer5_demographics.settings.PREDICTION_MAX_EXTRAP_YEARS", 2, raising=False
    )

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24001", "24001", "24001"],
            "data_year": [2020, 2021, 2022, 2024],
            "vacancy_rate": [0.10, 0.20, np.nan, np.nan],
            "vacancy_source": [pd.NA, pd.NA, pd.NA, pd.NA],
        }
    )

    result = apply_vacancy_predictions(df)

    row_2022 = result.loc[result["data_year"] == 2022].iloc[0]
    row_2024 = result.loc[result["data_year"] == 2024].iloc[0]

    assert bool(row_2022["vacancy_predicted"]) is True
    assert row_2022["vacancy_pred_method"] in {"theil_sen", "linear_trend"}
    assert row_2022["vacancy_pred_years"] == 1
    assert 0 <= float(row_2022["vacancy_rate_pred"]) <= 1
    assert row_2022["vacancy_source"] == "predicted"

    assert bool(row_2024["vacancy_predicted"]) is False
    assert pd.isna(row_2024["vacancy_rate_pred"])
    assert pd.isna(row_2024["vacancy_pred_method"])
    assert pd.isna(row_2024["vacancy_pred_years"])


def test_apply_momentum_features_requires_exact_reference_years():
    combined = pd.DataFrame(
        {
            "fips_code": ["24001", "24001", "24001"],
            "data_year": [2020, 2022, 2023],
            "pop_age_25_44": [100.0, 130.0, 140.0],
            "households_total": [50.0, 60.0, 66.0],
        }
    )

    result = _apply_momentum_features(combined)
    row_2022 = result.loc[result["data_year"] == 2022].iloc[0]
    row_2023 = result.loc[result["data_year"] == 2023].iloc[0]

    assert float(row_2022["working_age_momentum"]) == 30.0
    assert pd.isna(row_2022["household_formation_change"])
    assert pd.isna(row_2023["working_age_momentum"])
    assert float(row_2023["household_formation_change"]) == 10.0
