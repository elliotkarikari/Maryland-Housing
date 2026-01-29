"""
Tests for classification logic

Run with: pytest tests/test_classification.py -v
"""

import pytest
import pandas as pd
import numpy as np
from src.processing.classification import (
    classify_directional_status,
    classify_confidence,
    identify_top_strengths,
    identify_top_weaknesses
)
from config.settings import get_settings

settings = get_settings()


class TestDirectionalClassification:
    """Test directional status classification"""

    def test_improving_classification(self):
        """Test that counties with 3+ high-performing layers are 'improving'"""
        # 4 layers above 0.6, none below 0.3
        layer_scores = pd.Series({
            'employment': 0.7,
            'mobility': 0.8,
            'school': 0.65,
            'housing': 0.75,
            'demographic': 0.5
        })
        risk_drag = 0.2

        result = classify_directional_status(layer_scores, risk_drag)
        assert result == 'improving'

    def test_at_risk_classification(self):
        """Test that counties with 2+ low-performing layers are 'at_risk'"""
        # 2 layers below 0.3
        layer_scores = pd.Series({
            'employment': 0.2,
            'mobility': 0.25,
            'school': 0.5,
            'housing': 0.6,
            'demographic': 0.4
        })
        risk_drag = 0.3

        result = classify_directional_status(layer_scores, risk_drag)
        assert result == 'at_risk'

    def test_stable_classification(self):
        """Test that counties not meeting improving/at_risk criteria are 'stable'"""
        # Mixed scores, not extreme
        layer_scores = pd.Series({
            'employment': 0.5,
            'mobility': 0.4,
            'school': 0.6,
            'housing': 0.45,
            'demographic': 0.55
        })
        risk_drag = 0.3

        result = classify_directional_status(layer_scores, risk_drag)
        assert result == 'stable'

    def test_high_risk_drag_override(self):
        """Test that severe risk drag can trigger 'at_risk' classification"""
        # Moderate scores but high risk drag
        layer_scores = pd.Series({
            'employment': 0.5,
            'mobility': 0.35,  # Below 0.4 threshold
            'school': 0.5,
            'housing': 0.5,
            'demographic': 0.5
        })
        risk_drag = 0.75  # Severe risk drag (>= 0.7)

        result = classify_directional_status(layer_scores, risk_drag)
        assert result == 'at_risk'


class TestConfidenceClassification:
    """Test confidence overlay classification"""

    def test_strong_confidence(self):
        """Test strong confidence classification"""
        result = classify_confidence(0.7)
        assert result == 'strong'

    def test_conditional_confidence(self):
        """Test conditional confidence classification"""
        result = classify_confidence(0.5)
        assert result == 'conditional'

    def test_fragile_confidence(self):
        """Test fragile confidence classification"""
        result = classify_confidence(0.2)
        assert result == 'fragile'

    def test_missing_data_default(self):
        """Test that missing policy data defaults to 'conditional'"""
        result = classify_confidence(np.nan)
        assert result == 'conditional'


class TestExplainability:
    """Test explainability functions"""

    def test_identify_strengths(self):
        """Test identification of top performing layers"""
        layer_scores = {
            'employment': 0.8,
            'mobility': 0.9,  # Top
            'school': 0.85,   # Second
            'housing': 0.6,
            'demographic': 0.7
        }

        strengths = identify_top_strengths(layer_scores, top_n=2)

        assert len(strengths) == 2
        assert 'mobility' in strengths
        assert 'school' in strengths

    def test_identify_weaknesses(self):
        """Test identification of weakest performing layers"""
        layer_scores = {
            'employment': 0.3,  # Bottom
            'mobility': 0.4,    # Second bottom
            'school': 0.7,
            'housing': 0.6,
            'demographic': 0.8
        }

        weaknesses = identify_top_weaknesses(layer_scores, top_n=2)

        assert len(weaknesses) == 2
        assert 'employment' in weaknesses
        assert 'mobility' in weaknesses

    def test_handles_nan_values(self):
        """Test that NaN values are filtered out"""
        layer_scores = {
            'employment': 0.5,
            'mobility': np.nan,
            'school': 0.7,
            'housing': np.nan,
            'demographic': 0.6
        }

        strengths = identify_top_strengths(layer_scores, top_n=2)

        # Should only consider non-NaN values
        assert len(strengths) <= 2
        assert 'mobility' not in strengths
        assert 'housing' not in strengths


class TestThresholdSensitivity:
    """Test sensitivity to threshold changes"""

    def test_threshold_boundary_improving(self):
        """Test classification at improving threshold boundary"""
        # County just at threshold
        layer_scores_at_threshold = pd.Series({
            'employment': 0.6,  # Exactly at threshold
            'mobility': 0.6,
            'school': 0.6,
            'housing': 0.5,
            'demographic': 0.5
        })

        result = classify_directional_status(layer_scores_at_threshold, 0.2)
        assert result == 'improving'  # Should meet criteria

        # County just below threshold
        layer_scores_below = pd.Series({
            'employment': 0.59,  # Just below
            'mobility': 0.6,
            'school': 0.6,
            'housing': 0.5,
            'demographic': 0.5
        })

        result_below = classify_directional_status(layer_scores_below, 0.2)
        assert result_below == 'stable'  # Should not meet criteria

    def test_threshold_boundary_at_risk(self):
        """Test classification at at_risk threshold boundary"""
        # Exactly at threshold
        layer_scores_at = pd.Series({
            'employment': 0.3,  # Exactly at threshold
            'mobility': 0.3,    # Exactly at threshold
            'school': 0.5,
            'housing': 0.5,
            'demographic': 0.5
        })

        # At threshold means NOT below, so should be stable
        result = classify_directional_status(layer_scores_at, 0.2)
        assert result == 'stable'

        # Just below threshold
        layer_scores_below = pd.Series({
            'employment': 0.29,
            'mobility': 0.29,
            'school': 0.5,
            'housing': 0.5,
            'demographic': 0.5
        })

        result_below = classify_directional_status(layer_scores_below, 0.2)
        assert result_below == 'at_risk'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
