import pandas as pd

from src.ingest.layer2_accessibility import _enrich_county_with_general_flows


def test_enrich_county_with_general_flows_blends_accessibility_and_flow_score():
    county_df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "multimodal_accessibility_score": [0.4, 0.6],
        }
    )
    flow_summary = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "acs_flow_year": [2022, 2022],
            "general_nonmovers": [60000, 50000],
            "general_inflow_total": [2100, 4200],
            "general_outflow_total": [1700, 1500],
            "general_net_flow": [400, 2700],
            "general_inflow_rate": [0.034, 0.078],
            "general_outflow_rate": [0.028, 0.028],
            "general_net_flow_rate": [0.006, 0.05],
            "general_flow_score": [0.5, 1.0],
            "mobility_flow_method": [
                "v2-accessibility+acs-net-flow",
                "v2-accessibility+acs-net-flow",
            ],
        }
    )

    result = _enrich_county_with_general_flows(county_df, flow_summary, flow_year=2022).set_index(
        "fips_code"
    )

    assert result.loc["24001", "general_inflow_total"] == 2100
    assert result.loc["24003", "general_inflow_total"] == 4200
    assert result.loc["24001", "mobility_optionality_method"] == "v2-accessibility+acs-net-flow"

    # Score rank with two counties should be 0.5 for lower and 1.0 for higher.
    expected_24001 = 0.85 * 0.4 + 0.15 * 0.5
    expected_24003 = 0.85 * 0.6 + 0.15 * 1.0
    assert result.loc["24001", "mobility_optionality_index"] == expected_24001
    assert result.loc["24003", "mobility_optionality_index"] == expected_24003


def test_enrich_county_with_general_flows_falls_back_to_accessibility_only_when_missing_flows():
    county_df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "multimodal_accessibility_score": [0.55],
        }
    )

    result = _enrich_county_with_general_flows(county_df, pd.DataFrame(), flow_year=2022)

    assert result.loc[0, "mobility_optionality_index"] == 0.55
    assert result.loc[0, "mobility_optionality_method"] == "v2-accessibility-only"
    assert result.loc[0, "general_inflow_total"] == 0
