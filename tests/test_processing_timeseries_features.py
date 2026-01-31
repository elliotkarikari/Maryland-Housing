import numpy as np
import pandas as pd
import pytest

import src.processing.timeseries_features as tf


def test_compute_robust_slope_linear_series():
    years = np.array([2020, 2021, 2022], dtype=float)
    values = np.array([1.0, 2.0, 3.0], dtype=float)

    slope, mad = tf.compute_robust_slope(years, values)

    assert slope == pytest.approx(1.0)
    assert mad == pytest.approx(0.0)


def test_compute_stability_metrics_basic():
    values = np.array([1.0, 2.0, 3.0, 4.0], dtype=float)
    result = tf.compute_stability_metrics(values)

    assert result["volatility"] == pytest.approx(1.5)
    assert result["consistency"] == pytest.approx(1.0)
    assert result["persistence"] == 3
    assert result["cv"] == pytest.approx(np.std(values, ddof=1) / np.mean(values))


def test_compute_layer_timeseries_features_insufficient_data(monkeypatch):
    data = pd.DataFrame({"year": [2023, 2025], "value": [10.0, 12.0]})

    monkeypatch.setattr(tf, "extract_timeseries_data", lambda *args, **kwargs: data)

    result = tf.compute_layer_timeseries_features(
        geoid="24001",
        layer_name="employment_gravity",
        layer_table="layer1_employment_gravity",
        metric_column="economic_opportunity_index",
        window_size=5,
        as_of_year=2025,
    )

    assert result["coverage_years"] == 2
    assert result["computation_method"] == "insufficient_data"
    assert np.isnan(result["momentum_slope"])
    assert result["data_gaps"] == [2021, 2022, 2024]


def test_compute_layer_timeseries_features_full_window(monkeypatch):
    data = pd.DataFrame(
        {
            "year": [2021, 2022, 2023, 2024, 2025],
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )

    monkeypatch.setattr(tf, "extract_timeseries_data", lambda *args, **kwargs: data)

    result = tf.compute_layer_timeseries_features(
        geoid="24001",
        layer_name="employment_gravity",
        layer_table="layer1_employment_gravity",
        metric_column="economic_opportunity_index",
        window_size=5,
        as_of_year=2025,
    )

    assert result["coverage_years"] == 5
    assert result["computation_method"] == "theil_sen"
    assert result["level_latest"] == pytest.approx(5.0)
    assert result["level_baseline"] == pytest.approx(1.0)
    assert result["stability_persistence"] == 4
