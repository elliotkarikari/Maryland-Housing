from datetime import date
from pathlib import Path
import sys

import pandas as pd

import src.ingest.layer1_economic_accessibility as layer1
import src.ingest.layer2_accessibility as layer2
import src.ingest.layer3_education_accessibility as layer3edu
import src.ingest.layer3_schools as layer3schools
import src.ingest.layer4_housing as layer4
import src.ingest.layer4_housing_affordability as layer4aff
import src.ingest.layer5_demographics as layer5demo
import src.ingest.layer5_demographic_equity as layer5eq
import src.ingest.layer6_risk as layer6
import src.ingest.layer6_risk_vulnerability as layer6v
import src.ingest.policy_persistence as policy


class DummyScalar:
    def scalar(self):
        return None


class DummyDB:
    def execute(self, *args, **kwargs):
        return DummyScalar()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_layer1_pipeline(monkeypatch):
    tract_df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2025],
            "total_jobs": [100],
            "high_wage_jobs": [10],
        }
    )
    county_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2025]})

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer1, "calculate_economic_opportunity_indicators", lambda **kwargs: (tract_df, county_df))
    monkeypatch.setattr(layer1, "store_tract_economic_opportunity", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer1, "store_county_economic_opportunity", lambda *args, **kwargs: None)
    monkeypatch.setattr(layer1, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(layer1, "apply_predictions_to_table", lambda *args, **kwargs: None)

    layer1.run_layer1_v2_ingestion(data_year=2025, multi_year=False, store_data=True)
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer2_pipeline(monkeypatch, tmp_path):
    tract_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2025]})
    county_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2025]})

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer2, "calculate_accessibility_indicators", lambda **kwargs: (tract_df, county_df))
    monkeypatch.setattr(layer2, "store_tract_accessibility", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer2, "store_county_accessibility", lambda *args, **kwargs: None)
    monkeypatch.setattr(layer2, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(layer2, "check_r5py_available", lambda: False)
    monkeypatch.setattr(layer2, "apply_predictions_to_table", lambda *args, **kwargs: None)

    feed = layer2.GTFSFeedInfo(
        name="test",
        path=Path(tmp_path / "feed.zip"),
        agency="test",
        feed_date=date(2024, 1, 1),
        file_hash="abc",
        source_url="https://example.com/feed.zip",
        fetch_date="2026-02-15",
    )
    monkeypatch.setattr(layer2, "download_gtfs_feeds", lambda *args, **kwargs: [feed])

    layer2.run_layer2_v2_ingestion(data_year=2025, use_r5=False, store_data=True)
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer3_education_pipeline(monkeypatch):
    tract_df = pd.DataFrame(
        {"fips_code": ["24001"], "data_year": [2025], "education_opportunity_score": [0.7]}
    )
    county_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2025]})
    schools_df = pd.DataFrame({"school_id": [1]})

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer3edu, "calculate_education_accessibility_indicators", lambda **kwargs: (tract_df, county_df, schools_df))
    monkeypatch.setattr(layer3edu, "store_school_directory", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer3edu, "store_tract_education_accessibility", lambda *args, **kwargs: None)
    monkeypatch.setattr(layer3edu, "store_county_education_accessibility", lambda *args, **kwargs: None)
    monkeypatch.setattr(layer3edu, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(layer3edu, "apply_predictions_to_table", lambda *args, **kwargs: None)

    layer3edu.run_layer3_v2_ingestion(data_year=2025, multi_year=False, store_data=True)
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer3_school_pipeline_main(monkeypatch):
    df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2025],
            "total_enrollment": [1000],
            "schools_total": [10],
        }
    )
    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer3schools, "calculate_school_indicators", lambda **kwargs: df)
    monkeypatch.setattr(layer3schools, "store_school_data", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer3schools, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(sys, "argv", ["prog", "--year", "2025"])

    layer3schools.main()
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer4_housing_pipeline(monkeypatch):
    acs_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2021]})
    permits_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2021]})

    def fake_calc(df, year):
        df = df.copy()
        df["data_year"] = year
        df["price_to_income_ratio"] = 2.5
        df["permits_per_1000_households"] = 1.2
        return df[["fips_code", "data_year", "price_to_income_ratio", "permits_per_1000_households"]]

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer4, "fetch_acs_housing_data", lambda **kwargs: acs_df)
    monkeypatch.setattr(layer4, "fetch_bps_permits", lambda **kwargs: permits_df)
    monkeypatch.setattr(layer4, "calculate_housing_indicators", fake_calc)
    monkeypatch.setattr(layer4, "store_housing_data", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer4, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))

    layer4.run_layer4_ingestion(data_year=2021, multi_year=False)
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer4_affordability_pipeline(monkeypatch):
    tract_df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2025],
            "cost_burdened_pct": [0.3],
            "housing_affordability_score": [0.6],
        }
    )
    county_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2025]})

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer4aff, "get_db", lambda: DummyDB())
    monkeypatch.setattr(layer4aff, "calculate_housing_affordability_indicators", lambda **kwargs: (tract_df, county_df))
    monkeypatch.setattr(layer4aff, "store_tract_housing_affordability", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer4aff, "store_county_housing_affordability", lambda *args, **kwargs: None)
    monkeypatch.setattr(layer4aff, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(layer4aff, "apply_predictions_to_table", lambda *args, **kwargs: None)

    layer4aff.run_layer4_v2_ingestion(data_year=2025, multi_year=False, store_data=True)
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer5_demographics_pipeline(monkeypatch):
    acs_df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2023],
            "pop_age_25_44": [1000],
            "households_total": [400],
            "pop_age_25_44_pct": [0.3],
            "households_family_with_children": [100],
        }
    )

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer5demo, "calculate_demographic_indicators", lambda year: acs_df)
    monkeypatch.setattr(layer5demo, "fetch_irs_migration_by_year", lambda: {})
    monkeypatch.setattr(layer5demo, "store_demographic_data", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer5demo, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(sys, "argv", ["prog", "--year", "2023", "--single-year"])

    layer5demo.main()
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer5_equity_pipeline(monkeypatch):
    tract_df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "data_year": [2024],
            "equity_score": [0.5],
            "demographic_opportunity_score": [0.6],
        }
    )
    county_df = pd.DataFrame({"fips_code": ["24001"], "data_year": [2024]})

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer5eq, "calculate_demographic_equity_indicators", lambda **kwargs: (tract_df, county_df))
    monkeypatch.setattr(layer5eq, "store_tract_demographic_equity", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer5eq, "store_county_demographic_equity", lambda *args, **kwargs: None)
    monkeypatch.setattr(layer5eq, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(layer5eq, "apply_predictions_to_table", lambda *args, **kwargs: None)

    layer5eq.run_layer5_v2_ingestion(data_year=2024, multi_year=False, store_data=True)
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer6_risk_pipeline(monkeypatch):
    df = pd.DataFrame({"fips_code": ["24001"], "risk_drag_index": [0.4]})

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer6, "calculate_risk_indicators", lambda *args, **kwargs: df)
    monkeypatch.setattr(layer6, "store_risk_data", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer6, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))

    layer6.main()
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_layer6_vulnerability_pipeline(monkeypatch):
    df = pd.DataFrame(
        {
            "fips_code": ["24001"],
            "risk_drag_index": [0.4],
            "static_risk_score": [0.2],
            "modern_vulnerability_score": [0.3],
        }
    )

    calls = {"store": 0, "refresh": 0}

    monkeypatch.setattr(layer6v, "compute_risk_vulnerability", lambda *args, **kwargs: df)
    monkeypatch.setattr(layer6v, "store_risk_vulnerability_data", lambda *args, **kwargs: calls.__setitem__("store", calls["store"] + 1))
    monkeypatch.setattr(layer6v, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(layer6v, "apply_predictions_to_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(sys, "argv", ["prog", "--year", "2025"])

    layer6v.main()
    assert calls["store"] == 1
    assert calls["refresh"] == 1


def test_policy_persistence_pipeline_no_ai(monkeypatch):
    federal_df = pd.DataFrame(
        {"fips_code": ["24001"], "federal_awards_yoy_consistency": [0.8]}
    )

    calls = {"merge": 0, "refresh": 0}

    monkeypatch.setattr(policy, "fetch_usaspending_consistency", lambda **kwargs: federal_df)
    monkeypatch.setattr(policy, "merge_and_store_policy_persistence", lambda *args, **kwargs: calls.__setitem__("merge", calls["merge"] + 1))
    monkeypatch.setattr(policy, "log_refresh", lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(policy.settings, "AI_ENABLED", False, raising=False)

    policy.run_policy_persistence_ingestion(data_year=2025, include_ai=False)
    assert calls["merge"] == 1
    assert calls["refresh"] == 1
