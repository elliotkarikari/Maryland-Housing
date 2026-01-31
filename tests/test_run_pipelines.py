import sys

import pandas as pd
import pytest

import src.run_pipeline as run_pipeline
import src.run_multiyear_pipeline as run_multiyear


def test_run_pipeline_export_only(monkeypatch):
    calls = {"multi": 0, "export": 0}

    monkeypatch.setattr(run_pipeline, "test_connection", lambda: True)
    monkeypatch.setattr(
        run_pipeline,
        "run_multiyear_pipeline",
        lambda **kwargs: calls.__setitem__("multi", calls["multi"] + 1),
    )
    monkeypatch.setattr(
        run_pipeline,
        "run_geojson_export",
        lambda **kwargs: {"record_count": 1, "latest_path": "exports/md_counties_latest.geojson"},
    )
    monkeypatch.setattr(sys, "argv", ["prog", "--export-only"])

    with pytest.raises(SystemExit) as exc:
        run_pipeline.main()

    assert exc.value.code == 0
    assert calls["multi"] == 0


def test_run_pipeline_no_export(monkeypatch):
    calls = {"multi": 0, "export": 0}

    monkeypatch.setattr(run_pipeline, "test_connection", lambda: True)

    def fake_multi(**kwargs):
        calls["multi"] += 1
        return True

    def fake_export(**kwargs):
        calls["export"] += 1
        return {"record_count": 1, "latest_path": "exports/md_counties_latest.geojson"}

    monkeypatch.setattr(run_pipeline, "run_multiyear_pipeline", fake_multi)
    monkeypatch.setattr(run_pipeline, "run_geojson_export", fake_export)
    monkeypatch.setattr(sys, "argv", ["prog", "--no-export"])

    with pytest.raises(SystemExit) as exc:
        run_pipeline.main()

    assert exc.value.code == 0
    assert calls["multi"] == 1
    assert calls["export"] == 0


def test_run_pipeline_prereq_fail(monkeypatch):
    monkeypatch.setattr(run_pipeline, "test_connection", lambda: False)
    monkeypatch.setattr(sys, "argv", ["prog"])

    with pytest.raises(SystemExit) as exc:
        run_pipeline.main()

    assert exc.value.code == 1


def test_run_multiyear_pipeline_success(monkeypatch):
    calls = {"timeseries": 0, "scores": 0, "store": 0}

    monkeypatch.setattr(
        run_multiyear,
        "compute_all_timeseries_features",
        lambda **kwargs: calls.__setitem__("timeseries", calls["timeseries"] + 1) or 10,
    )
    monkeypatch.setattr(
        run_multiyear,
        "compute_all_layer_scores",
        lambda **kwargs: pd.DataFrame({"geoid": ["24001"]}),
    )
    monkeypatch.setattr(
        run_multiyear,
        "classify_all_counties",
        lambda **kwargs: pd.DataFrame(
            {
                "geoid": ["24001"],
                "final_grouping": ["stable_constrained"],
                "directional_status": ["stable"],
                "confidence_level": ["conditional"],
            }
        ),
    )
    monkeypatch.setattr(
        run_multiyear,
        "store_final_synthesis",
        lambda df: calls.__setitem__("store", calls["store"] + 1),
    )

    assert run_multiyear.run_pipeline(as_of_year=2025) is True
    assert calls["timeseries"] == 1
    assert calls["store"] == 1


def test_run_multiyear_pipeline_skip_flags(monkeypatch):
    calls = {"timeseries": 0, "scores": 0, "store": 0}

    monkeypatch.setattr(
        run_multiyear,
        "compute_all_timeseries_features",
        lambda **kwargs: calls.__setitem__("timeseries", calls["timeseries"] + 1) or 10,
    )
    monkeypatch.setattr(
        run_multiyear,
        "compute_all_layer_scores",
        lambda **kwargs: calls.__setitem__("scores", calls["scores"] + 1) or pd.DataFrame({"geoid": ["24001"]}),
    )
    monkeypatch.setattr(
        run_multiyear,
        "classify_all_counties",
        lambda **kwargs: pd.DataFrame(
            {
                "geoid": ["24001"],
                "final_grouping": ["stable_constrained"],
                "directional_status": ["stable"],
                "confidence_level": ["conditional"],
            }
        ),
    )
    monkeypatch.setattr(
        run_multiyear,
        "store_final_synthesis",
        lambda df: calls.__setitem__("store", calls["store"] + 1),
    )

    assert run_multiyear.run_pipeline(as_of_year=2025, skip_timeseries=True, skip_scoring=True) is True
    assert calls["timeseries"] == 0
    assert calls["scores"] == 0
    assert calls["store"] == 1


def test_run_multiyear_pipeline_no_classifications(monkeypatch):
    monkeypatch.setattr(run_multiyear, "compute_all_timeseries_features", lambda **kwargs: 5)
    monkeypatch.setattr(run_multiyear, "compute_all_layer_scores", lambda **kwargs: pd.DataFrame({"geoid": ["24001"]}))
    monkeypatch.setattr(run_multiyear, "classify_all_counties", lambda **kwargs: pd.DataFrame())

    assert run_multiyear.run_pipeline(as_of_year=2025) is False
