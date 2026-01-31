import pytest

from src.processing.feature_registry import (
    FEATURES_BY_LAYER,
    get_feature,
    get_primary_features,
    get_ai_dependent_features,
)


def test_get_feature_unknown_raises():
    with pytest.raises(ValueError):
        get_feature("does_not_exist")


def test_get_primary_features_matches_layer_max_weight():
    primary = get_primary_features()

    assert len(primary) == len(FEATURES_BY_LAYER)

    for feature in primary:
        layer_features = FEATURES_BY_LAYER[feature.layer]
        max_weight = max(f.weight for f in layer_features)
        assert feature.weight == max_weight


def test_get_ai_dependent_features_only_ai():
    ai_features = get_ai_dependent_features()

    assert all(f.requires_ai for f in ai_features)
