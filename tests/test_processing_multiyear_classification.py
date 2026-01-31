import numpy as np
import pandas as pd
import pytest

from src.processing.multiyear_classification import (
    classify_directional_status,
    classify_confidence_level,
    compute_composite_score,
    determine_final_grouping,
    MOMENTUM_POSITIVE_THRESHOLD,
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
