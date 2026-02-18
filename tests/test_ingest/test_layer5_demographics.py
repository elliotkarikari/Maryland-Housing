from contextlib import contextmanager

import numpy as np
import pandas as pd

from src.ingest.layer5_demographics import store_demographic_data


def test_store_demographic_data_uses_batched_execute(monkeypatch):
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

    monkeypatch.setattr("src.ingest.layer5_demographics.get_db", fake_get_db)

    df = pd.DataFrame(
        {
            "fips_code": ["24001", "24003"],
            "data_year": [2023, 2023],
            "pop_total": [1000, np.nan],
        }
    )

    store_demographic_data(df)

    assert fake_db.commit_calls == 1
    assert len(fake_db.executions) == 2
    _, delete_params = fake_db.executions[0]
    assert delete_params == {"years": [2023]}

    _, batch_params = fake_db.executions[1]
    assert isinstance(batch_params, list)
    assert len(batch_params) == 2
    assert batch_params[1]["pop_total"] is None
