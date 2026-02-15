import numpy as np
import pandas as pd
import pytest

from src.processing.multiyear_scoring import (
    WEIGHTS_FULL,
    WEIGHTS_NO_MOMENTUM,
    calculate_missingness_penalty,
    compute_composite_scores,
    normalize_layer_features,
    percentile_normalize,
)


def test_percentile_normalize_handles_nan():
    series = pd.Series([10, np.nan, 30], index=["a", "b", "c"])
    result = percentile_normalize(series)

    assert np.isnan(result.loc["b"])
    assert result.loc["a"] == pytest.approx(0.5)
    assert result.loc["c"] == pytest.approx(1.0)


def test_calculate_missingness_penalty_thresholds():
    assert calculate_missingness_penalty(5) == pytest.approx(0.0)
    assert calculate_missingness_penalty(4) == pytest.approx(0.1)
    assert calculate_missingness_penalty(2) == pytest.approx(0.6)


def test_normalize_layer_features_percentiles_per_layer():
    df = pd.DataFrame(
        {
            "geoid": ["1", "2", "1", "2"],
            "layer_name": ["employment", "employment", "risk", "risk"],
            "level_latest": [10, 20, 5, 15],
            "momentum_slope": [0.1, 0.2, np.nan, np.nan],
            "stability_consistency": [0.4, 0.6, 0.2, 0.8],
        }
    )

    result = normalize_layer_features(df)

    employment_mask = result["layer_name"] == "employment"
    risk_mask = result["layer_name"] == "risk"

    assert result.loc[employment_mask, "layer_level_score"].tolist() == [0.5, 1.0]
    assert result.loc[risk_mask, "layer_level_score"].tolist() == [0.5, 1.0]
    assert result.loc[employment_mask, "layer_momentum_score"].tolist() == [0.5, 1.0]
    assert result.loc[risk_mask, "layer_momentum_score"].isna().all()


def test_compute_composite_scores_weights_and_penalty():
    df = pd.DataFrame(
        {
            "layer_level_score": [0.8, 0.6, 0.7],
            "layer_momentum_score": [0.6, np.nan, 0.5],
            "layer_stability_score": [0.4, np.nan, np.nan],
            "coverage_years": [5, 2, 4],
        }
    )

    result = compute_composite_scores(df)

    # Row 0: full weights, no penalty
    expected_full = (
        WEIGHTS_FULL["level"] * 0.8
        + WEIGHTS_FULL["momentum"] * 0.6
        + WEIGHTS_FULL["stability"] * 0.4
    )
    assert result.loc[0, "layer_overall_score"] == pytest.approx(expected_full)
    assert result.loc[0, "weights_used"] == WEIGHTS_FULL

    # Row 1: level only, penalty applied (coverage 2 -> penalty 0.6)
    expected_level_only = 0.6 * (1 - 0.5 * 0.6)
    assert result.loc[1, "layer_overall_score"] == pytest.approx(expected_level_only)
    assert result.loc[1, "weights_used"] == WEIGHTS_NO_MOMENTUM

    # Row 2: level + momentum, penalty (coverage 4 -> penalty 0.1)
    expected_partial = (0.625 * 0.7 + 0.375 * 0.5) * (1 - 0.5 * 0.1)
    assert result.loc[2, "layer_overall_score"] == pytest.approx(expected_partial)
