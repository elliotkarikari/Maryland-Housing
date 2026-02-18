from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest

from src.processing.feature_registry import Directionality, FeatureDefinition, NormMethod
from src.processing.normalization import (
    minmax_normalize,
    normalize_feature,
    percentile_normalize,
    robust_zscore_normalize,
    store_normalized_features,
)


def test_percentile_normalize_positive():
    values = pd.Series([10, 20, 30], index=["a", "b", "c"])
    result = percentile_normalize(values, Directionality.POSITIVE)

    assert result.loc["a"] == pytest.approx(1 / 3)
    assert result.loc["b"] == pytest.approx(2 / 3)
    assert result.loc["c"] == pytest.approx(1.0)


def test_percentile_normalize_negative_inverts():
    values = pd.Series([10, 20, 30], index=["a", "b", "c"])
    result = percentile_normalize(values, Directionality.NEGATIVE)

    assert result.loc["a"] == pytest.approx(2 / 3)
    assert result.loc["b"] == pytest.approx(1 / 3)
    assert result.loc["c"] == pytest.approx(0.0)


def test_robust_zscore_zero_iqr_returns_midpoint():
    values = pd.Series([5, 5, 5], index=["a", "b", "c"])
    result = robust_zscore_normalize(values, Directionality.POSITIVE)

    assert result.nunique() == 1
    assert result.iloc[0] == pytest.approx(0.5)


def test_minmax_no_variation_returns_midpoint():
    values = pd.Series([1, 1, 1], index=["a", "b", "c"])
    result = minmax_normalize(values, Directionality.POSITIVE)

    assert result.nunique() == 1
    assert result.iloc[0] == pytest.approx(0.5)


def test_normalize_feature_missing_column_returns_nan():
    df = pd.DataFrame({"other": [1, 2, 3]}, index=[0, 1, 2])
    feature = FeatureDefinition(
        name="test_feature",
        layer="test_layer",
        source_table="test_table",
        source_column="missing_col",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="unit",
        description="desc",
    )

    result = normalize_feature(df, feature)
    assert result.isna().all()


def test_normalize_feature_insufficient_values():
    df = pd.DataFrame({"value": [1.0, np.nan, 2.0, np.nan]}, index=[0, 1, 2, 3])
    feature = FeatureDefinition(
        name="test_feature",
        layer="test_layer",
        source_table="test_table",
        source_column="value",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="unit",
        description="desc",
    )

    result = normalize_feature(df, feature)
    assert result.isna().all()


def test_normalize_feature_success_with_nan_padding():
    df = pd.DataFrame({"value": [1.0, 2.0, 3.0, np.nan]}, index=[0, 1, 2, 3])
    feature = FeatureDefinition(
        name="test_feature",
        layer="test_layer",
        source_table="test_table",
        source_column="value",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="unit",
        description="desc",
    )

    result = normalize_feature(df, feature)

    assert result.isna().sum() == 1
    assert result.loc[0] == pytest.approx(1 / 3)
    assert result.loc[1] == pytest.approx(2 / 3)
    assert result.loc[2] == pytest.approx(1.0)


def test_store_normalized_features_uses_batched_execute(monkeypatch):
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

    monkeypatch.setattr("src.processing.normalization.get_db", fake_get_db)
    monkeypatch.setattr("src.processing.normalization.table_name", lambda t: t)

    normalized_layers = {
        "layer_a": pd.DataFrame(
            {
                "fips_code": ["24001", "24003"],
                "data_year": [2024, 2024],
                "feature_a_normalized": [0.1, np.nan],
            }
        ),
        "layer_b": pd.DataFrame(
            {
                "fips_code": ["24001", "24003"],
                "data_year": [2024, 2024],
                "feature_b_normalized": [0.2, 0.3],
            }
        ),
    }

    store_normalized_features(normalized_layers, data_year=2024)

    assert fake_db.commit_calls == 2
    assert len(fake_db.executions) == 2
    _, batched_params = fake_db.executions[1]
    assert isinstance(batched_params, list)
    assert len(batched_params) == 3

    observed = {
        (row["fips_code"], row["feature_name"], row["normalized_value"]) for row in batched_params
    }
    assert observed == {
        ("24001", "feature_a", 0.1),
        ("24001", "feature_b", 0.2),
        ("24003", "feature_b", 0.3),
    }
