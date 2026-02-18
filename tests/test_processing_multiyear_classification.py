from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest

from src.processing.multiyear_classification import (
    MOMENTUM_POSITIVE_THRESHOLD,
    classify_confidence_level,
    classify_directional_status,
    compute_composite_score,
    determine_final_grouping,
    store_final_synthesis,
)


def test_classify_directional_status_improving_with_momentum():
    row = pd.Series(
        {
            "employment_gravity_score": 0.7,
            "mobility_optionality_score": 0.8,
            "school_trajectory_score": 0.65,
            "housing_elasticity_score": 0.72,
            "demographic_momentum_score": 0.5,
            "risk_drag_score": 0.2,
            "employment_gravity_momentum": MOMENTUM_POSITIVE_THRESHOLD + 0.1,
            "mobility_optionality_momentum": MOMENTUM_POSITIVE_THRESHOLD + 0.05,
        }
    )

    assert classify_directional_status(row) == "improving"


def test_classify_directional_status_at_risk_with_negative_momentum():
    row = pd.Series(
        {
            "employment_gravity_score": 0.6,
            "mobility_optionality_score": 0.6,
            "school_trajectory_score": 0.6,
            "housing_elasticity_score": 0.6,
            "demographic_momentum_score": 0.6,
            "risk_drag_score": 0.2,
            "employment_gravity_momentum": 0.2,
            "mobility_optionality_momentum": 0.3,
        }
    )

    assert classify_directional_status(row) == "at_risk"


def test_classify_confidence_level_strong_and_fragile():
    strong_row = pd.Series(
        {
            "employment_coverage": 5,
            "mobility_coverage": 5,
            "schools_coverage": 5,
            "housing_coverage": 5,
        }
    )
    fragile_row = pd.Series(
        {
            "employment_coverage": 2,
            "mobility_coverage": 2,
            "schools_coverage": 2,
            "housing_coverage": 2,
        }
    )

    assert classify_confidence_level(strong_row)[0] == "strong"
    assert classify_confidence_level(fragile_row)[0] == "fragile"


def test_compute_composite_score_with_risk_penalty():
    row = pd.Series(
        {
            "employment_gravity_score": 0.6,
            "mobility_optionality_score": 0.6,
            "school_trajectory_score": 0.6,
            "housing_elasticity_score": 0.6,
            "demographic_momentum_score": 0.6,
            "risk_drag_score": 0.2,
        }
    )

    expected = 0.6 * (1 - 0.2)
    assert compute_composite_score(row) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("directional", "confidence", "reasons", "expected"),
    [
        ("stable", "fragile", ["sparse_coverage"], "high_uncertainty"),
        ("at_risk", "conditional", [], "at_risk_headwinds"),
        ("improving", "strong", [], "emerging_tailwinds"),
        ("improving", "conditional", ["some_layers_sparse"], "conditional_growth"),
        ("stable", "conditional", [], "stable_constrained"),
    ],
)
def test_determine_final_grouping(directional, confidence, reasons, expected):
    assert determine_final_grouping(directional, confidence, reasons, 0.5) == expected


def test_store_final_synthesis_uses_batched_execute(monkeypatch):
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

    monkeypatch.setattr("src.processing.multiyear_classification.get_db", fake_get_db)

    df = pd.DataFrame(
        {
            "geoid": ["24001", "24003"],
            "current_as_of_year": [2025, 2025],
            "final_grouping": ["emerging_tailwinds", "high_uncertainty"],
            "directional_status": ["improving", "stable"],
            "confidence_level": ["strong", "fragile"],
            "uncertainty_reasons": [[], ["sparse_coverage", "some_layers_sparse"]],
            "composite_score": [0.74, np.nan],
            "risk_drag_score": [0.2, np.nan],
            "employment_gravity_score": [0.8, 0.3],
            "mobility_optionality_score": [0.7, 0.2],
            "school_trajectory_score": [0.75, 0.25],
            "housing_elasticity_score": [0.7, 0.2],
            "demographic_momentum_score": [0.6, 0.2],
        }
    )

    store_final_synthesis(df)

    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 2
    _, delete_params = fake_db.executions[0]
    assert delete_params is None

    _, insert_params = fake_db.executions[1]
    assert isinstance(insert_params, list)
    assert len(insert_params) == 2
    assert insert_params[0]["uncertainty_level"] == "low"
    assert insert_params[1]["uncertainty_level"] == "high"
    assert insert_params[1]["composite_score"] is None
    assert insert_params[0]["classification_version"] == "v2.0-multiyear"
