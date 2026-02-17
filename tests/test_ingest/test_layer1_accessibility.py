import pandas as pd
import pytest

from src.ingest.layer1_economic_accessibility import (
    aggregate_to_county,
    compute_economic_accessibility,
    compute_sector_diversity,
    fetch_lodes_od_county_flows,
)


def test_aggregate_to_county_keeps_max_diagnostic_and_adds_weighted_primary():
    tract_df = pd.DataFrame(
        {
            "tract_geoid": ["24001000100", "24001000200"],
            "fips_code": ["24001", "24001"],
            "total_jobs": [200, 300],
            "high_wage_jobs": [100, 200],
            "mid_wage_jobs": [60, 70],
            "low_wage_jobs": [40, 30],
            "high_wage_jobs_accessible_45min": [100, 1000],
            "high_wage_jobs_accessible_30min": [50, 500],
            "total_jobs_accessible_45min": [200, 2000],
            "total_jobs_accessible_30min": [100, 1000],
            "economic_accessibility_score": [0.2, 0.9],
            "job_market_reach_score": [0.2, 0.9],
            "wage_quality_ratio": [0.5, 0.5],
            "job_quality_index": [0.2, 0.9],
            "upward_mobility_score": [0.2, 0.9],
            "sector_diversity_entropy": [1.2, 2.0],
            "labor_force_participation": [0.65, 0.7],
            "population": [900, 100],
            "working_age_pop": [500, 50],
            "area_sq_mi": [1.5, 1.0],
            "accessibility_method": ["haversine_proxy", "haversine_proxy"],
            "accessibility_threshold_30_min": [30, 30],
            "accessibility_threshold_45_min": [45, 45],
            "accessibility_proxy_distance_30_km": [20.0, 20.0],
            "accessibility_proxy_distance_45_km": [35.0, 35.0],
        }
    )

    county_df = aggregate_to_county(tract_df)
    assert len(county_df) == 1
    row = county_df.iloc[0]

    # Max fields are retained as frontier diagnostics.
    assert row["high_wage_jobs_accessible_45min"] == 1000
    assert row["total_jobs_accessible_45min"] == 2000

    # Primary weighted fields.
    assert row["high_wage_jobs_accessible_45min_weighted_mean"] == pytest.approx(190.0)
    assert row["total_jobs_accessible_45min_weighted_mean"] == pytest.approx(380.0)
    assert row["high_wage_jobs_accessible_45min_weighted_median"] == pytest.approx(100.0)
    assert row["total_jobs_accessible_45min_weighted_median"] == pytest.approx(200.0)

    # Score inputs now derive from weighted accessibility means.
    assert row["wage_quality_ratio"] == pytest.approx(0.5)
    assert row["economic_accessibility_score"] == pytest.approx(1.0)
    assert row["accessibility_method"] == "haversine_proxy"


def test_compute_economic_accessibility_proxy_threshold_override():
    tract_jobs = pd.DataFrame(
        {
            "tract_geoid": ["24001000100", "24001000200"],
            "fips_code": ["24001", "24001"],
            "total_jobs": [200, 100],
            "high_wage_jobs": [100, 50],
        }
    )
    tract_centroids = pd.DataFrame(
        {
            "tract_geoid": ["24001000100", "24001000200"],
            "fips_code": ["24001", "24001"],
            "centroid_lon": [0.0, 0.1],  # ~11.1km apart at equator
            "centroid_lat": [0.0, 0.0],
            "area_sq_mi": [1.0, 1.0],
        }
    )

    out = compute_economic_accessibility(
        tract_jobs=tract_jobs,
        tract_centroids=tract_centroids,
        mode="proxy",
        threshold_30_min=30,
        threshold_45_min=45,
        proxy_distance_30_km=5.0,
        proxy_distance_45_km=15.0,
    )
    out = out.set_index("tract_geoid")

    # At 5km, each tract can only reach itself.
    assert out.loc["24001000100", "high_wage_jobs_accessible_30min"] == 100
    assert out.loc["24001000200", "high_wage_jobs_accessible_30min"] == 50

    # At 15km, both tracts can reach both destinations.
    assert out.loc["24001000100", "high_wage_jobs_accessible_45min"] == 150
    assert out.loc["24001000200", "high_wage_jobs_accessible_45min"] == 150
    assert out.loc["24001000100", "accessibility_method"] == "haversine_proxy"


def test_compute_sector_diversity_adds_stable_sector_share():
    tract_jobs = pd.DataFrame(
        {
            "total_jobs": [100],
            "CNS09": [65],
            "CNS15": [20],
            "CNS16": [10],
            "CNS20": [5],
        }
    )
    out = compute_sector_diversity(tract_jobs)
    assert out.loc[0, "stable_sector_jobs"] == 35
    assert out.loc[0, "stable_sector_share"] == pytest.approx(0.35)


def test_fetch_lodes_od_county_flows_derives_working_age_and_capture_metrics(tmp_path, monkeypatch):
    od_csv = tmp_path / "od.csv"
    od_csv.write_text(
        "\n".join(
            [
                "w_geocode,h_geocode,year,S000,SA01,SA02,SA03,SE01,SE02,SE03,SI01,SI02,SI03",
                # 24001 resident + same-county worker
                "240010001001001,240010001001000,2022,10,2,6,2,2,4,4,3,4,3",
                # 24001 resident commuting to 24003
                "240030001001001,240010001001000,2022,5,1,3,1,1,2,2,2,1,2",
                # 24003 resident commuting to 24001
                "240010001001001,240030001001000,2022,7,1,4,2,1,3,3,2,3,2",
                # Non-target year should be ignored
                "240010001001001,240010001001000,2021,9,2,4,3,2,3,4,3,3,3",
            ]
        )
        + "\n"
    )

    monkeypatch.setattr(
        "src.ingest.layer1_economic_accessibility.settings.LODES_OD_DATA_PATH", str(od_csv)
    )
    monkeypatch.setattr("src.ingest.layer1_economic_accessibility.settings.LODES_OD_DATA_URL", None)
    monkeypatch.setattr("src.ingest.layer1_economic_accessibility.settings.LODES_OD_CHUNK_SIZE", 2)

    out = fetch_lodes_od_county_flows(2022).set_index("fips_code")

    row_24001 = out.loc["24001"]
    assert row_24001["od_resident_workers"] == 15
    assert row_24001["od_live_work_same_county"] == 10
    assert row_24001["od_outbound_workers"] == 5
    assert row_24001["od_inbound_workers"] == 7
    assert row_24001["od_net_commuter_flow"] == 2
    assert row_24001["od_working_age_resident_workers"] == 12
    assert row_24001["od_working_age_live_work_same_county"] == 8
    assert row_24001["od_working_age_share"] == pytest.approx(12 / 15)
    assert row_24001["od_local_capture_rate"] == pytest.approx(10 / 15)
    assert row_24001["od_working_age_local_capture_rate"] == pytest.approx(8 / 12)
    assert row_24001["od_high_wage_resident_workers"] == 6
    assert row_24001["od_high_wage_live_work_same_county"] == 4
    assert row_24001["od_high_wage_share"] == pytest.approx(6 / 15)
    assert row_24001["od_high_wage_local_capture_rate"] == pytest.approx(4 / 6)

    row_24003 = out.loc["24003"]
    assert row_24003["od_resident_workers"] == 7
    assert row_24003["od_live_work_same_county"] == 0
    assert row_24003["od_outbound_workers"] == 7
    assert row_24003["od_inbound_workers"] == 5
    assert row_24003["od_net_commuter_flow"] == -2
