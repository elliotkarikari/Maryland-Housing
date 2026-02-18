from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest

from src.processing.feature_registry import FEATURES_BY_LAYER
from src.processing.scoring import (
    calculate_all_layer_scores,
    calculate_composite_score,
    calculate_layer_score,
    store_layer_scores,
)


def test_calculate_layer_score_weighted():
    features = FEATURES_BY_LAYER["mobility_optionality"]
    cols = [f"{f.name}_normalized" for f in features]
    weights = [f.weight for f in features]

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2024, 2024],
            cols[0]: [0.2, 0.4],
            cols[1]: [0.6, 0.8],
            cols[2]: [1.0, 0.0],
        }
    )

    scores, coverage = calculate_layer_score(df, "mobility_optionality", use_weights=True)

    expected_first = np.average([0.2, 0.6, 1.0], weights=weights)
    expected_second = np.average([0.4, 0.8, 0.0], weights=weights)

    assert scores.iloc[0] == pytest.approx(expected_first)
    assert scores.iloc[1] == pytest.approx(expected_second)
    assert coverage.iloc[0] == pytest.approx(1.0)
    assert coverage.iloc[1] == pytest.approx(1.0)


def test_calculate_layer_score_missing_features():
    df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2024]})
    scores, coverage = calculate_layer_score(df, "mobility_optionality")

    assert scores.isna().all()
    assert (coverage == 0.0).all()


def test_calculate_layer_score_partial_coverage():
    features = FEATURES_BY_LAYER["mobility_optionality"]
    cols = [f"{f.name}_normalized" for f in features]
    weights = [f.weight for f in features]

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2024, 2024],
            cols[0]: [0.2, np.nan],
            cols[1]: [np.nan, np.nan],
            cols[2]: [1.0, np.nan],
        }
    )

    scores, coverage = calculate_layer_score(df, "mobility_optionality", use_weights=True)

    expected_first = np.average([0.2, 1.0], weights=[weights[0], weights[2]])
    assert scores.iloc[0] == pytest.approx(expected_first)
    assert coverage.iloc[0] == pytest.approx(2 / 3)

    assert pd.isna(scores.iloc[1])
    assert coverage.iloc[1] == pytest.approx(0.0)


def test_calculate_all_layer_scores():
    mobility_features = FEATURES_BY_LAYER["mobility_optionality"]
    employment_features = FEATURES_BY_LAYER["employment_gravity"]

    mobility_df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2024, 2024],
            f"{mobility_features[0].name}_normalized": [0.2, 0.4],
            f"{mobility_features[1].name}_normalized": [0.6, 0.8],
            f"{mobility_features[2].name}_normalized": [1.0, 0.0],
        }
    )

    employment_df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2024, 2024],
            f"{employment_features[0].name}_normalized": [0.3, 0.7],
            f"{employment_features[1].name}_normalized": [0.5, 0.9],
        }
    )

    normalized_layers = {
        "mobility_optionality": mobility_df,
        "employment_gravity": employment_df,
    }

    result = calculate_all_layer_scores(normalized_layers)

    assert not result.empty
    assert "mobility_optionality_score" in result.columns
    assert "mobility_optionality_coverage" in result.columns
    assert "employment_gravity_score" in result.columns
    assert "employment_gravity_coverage" in result.columns


def test_calculate_composite_score_with_risk_drag():
    df = pd.DataFrame(
        {
            "employment_gravity_score": [0.8, 0.4],
            "mobility_optionality_score": [0.6, 0.4],
            "school_trajectory_score": [0.7, 0.4],
            "housing_elasticity_score": [0.9, 0.4],
            "demographic_momentum_score": [0.5, 0.4],
            "risk_drag_score": [0.2, 0.5],
        }
    )

    result = calculate_composite_score(df, include_risk_drag=True)

    composite_first = np.mean([0.8, 0.6, 0.7, 0.9, 0.5]) * (1 - 0.2)
    composite_second = np.mean([0.4, 0.4, 0.4, 0.4, 0.4]) * (1 - 0.5)

    assert result.iloc[0] == pytest.approx(composite_first)
    assert result.iloc[1] == pytest.approx(composite_second)


def test_store_layer_scores_uses_batched_execute(monkeypatch):
    class FakeDB:
        def __init__(self):
            self.executions = []
            self.commit_calls = 0

        def execute(self, sql, params):
            self.executions.append((sql, params))

        def commit(self):
            self.commit_calls += 1

    fake_db = FakeDB()

    @contextmanager
    def fake_get_db():
        yield fake_db

    monkeypatch.setattr("src.processing.scoring.get_db", fake_get_db)
    monkeypatch.setattr("src.processing.scoring.table_name", lambda t: t)

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "employment_gravity_score": [0.8, np.nan],
            "mobility_optionality_score": [0.7, 0.6],
            "school_trajectory_score": [0.5, 0.4],
            "housing_elasticity_score": [0.9, 0.2],
            "demographic_momentum_score": [0.1, 0.3],
            "risk_drag_score": [0.2, 0.5],
            "composite_raw": [0.55, 0.31],
            "composite_normalized": [0.9, 0.2],
        }
    )

    store_layer_scores(df, data_year=2024)

    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 1
    _, params = fake_db.executions[0]
    assert isinstance(params, list)
    assert len(params) == 2
    assert params[0]["fips_code"] == "24001"
    assert params[0]["data_year"] == 2024
    assert params[1]["employment_gravity_score"] is None
