import pandas as pd

from src.export.geojson_export import _build_explainability_df


def test_build_explainability_df_returns_expected_columns():
    df = pd.DataFrame(
        {
            "directional_status": ["improving", "at_risk"],
            "confidence_level": ["strong", "fragile"],
            "risk_drag_score": [0.3, 0.8],
            "employment_gravity_score": [0.8, 0.2],
            "mobility_optionality_score": [0.7, 0.1],
            "school_trajectory_score": [0.6, 0.3],
            "housing_elasticity_score": [0.9, 0.2],
            "demographic_momentum_score": [0.5, 0.4],
        }
    )

    out = _build_explainability_df(df)

    assert len(out) == 2
    assert {"primary_strengths", "primary_weaknesses", "key_trends"} <= set(out.columns)
    assert isinstance(out.loc[0, "primary_strengths"], list)
    assert isinstance(out.loc[0, "primary_weaknesses"], list)
    assert isinstance(out.loc[0, "key_trends"], list)
    assert "High policy delivery reliability" in out.loc[0, "key_trends"]
    assert "Low policy follow-through, high uncertainty" in out.loc[1, "key_trends"]
