from contextlib import contextmanager

import pandas as pd

import src.ingest.policy_persistence as policy


def test_merge_and_store_policy_persistence_uses_batched_execute(monkeypatch):
    class FakeDB:
        def __init__(self):
            self.executions = []
            self.commit_calls = 0

        def execute(self, sql, params=None):
            self.executions.append((sql, params))

        def commit(self):
            self.commit_calls += 1

    fake_db = FakeDB()
    confidence_calls = []

    @contextmanager
    def fake_get_db():
        yield fake_db

    def fake_calculate_confidence_score(federal_consistency, cip_follow_through, has_cip_data):
        confidence_calls.append((federal_consistency, cip_follow_through, has_cip_data))
        return {"confidence_score": 0.6}

    monkeypatch.setattr(policy, "get_db", fake_get_db)
    monkeypatch.setattr(policy, "calculate_confidence_score", fake_calculate_confidence_score)
    monkeypatch.setattr(policy, "classify_confidence", lambda score: "conditional")

    federal_df = pd.DataFrame({"fips_code": ["24001"], "federal_awards_yoy_consistency": [0.8]})
    cip_df = pd.DataFrame({"fips_code": ["24001"], "cip_follow_through_rate": [0.7]})

    policy.merge_and_store_policy_persistence(federal_df=federal_df, cip_df=cip_df, data_year=2025)

    assert len(confidence_calls) == len(policy.MD_COUNTY_FIPS)
    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 1

    _, params = fake_db.executions[0]
    assert isinstance(params, list)
    assert len(params) == len(policy.MD_COUNTY_FIPS)

    row_24001 = next(row for row in params if row["fips_code"] == "24001")
    assert row_24001["federal_awards_yoy_consistency"] == 0.8
    assert row_24001["cip_follow_through_rate"] == 0.7
    assert row_24001["confidence_score"] == 0.6
    assert row_24001["confidence_class"] == "conditional"
    assert row_24001["data_year"] == 2025
