from datetime import datetime
from types import SimpleNamespace

from fastapi.testclient import TestClient

import src.api.main as api_main
from src.api.routes import get_db_session


class DummyResult:
    def __init__(self, fetchone_value=None, fetchall_value=None):
        self._fetchone_value = fetchone_value
        self._fetchall_value = fetchall_value

    def fetchone(self):
        return self._fetchone_value

    def fetchall(self):
        return self._fetchall_value or []


class DummySession:
    def __init__(self, results):
        self._results = list(results)
        self.closed = False

    def execute(self, *args, **kwargs):
        return self._results.pop(0)

    def close(self):
        self.closed = True


class AttrDict(SimpleNamespace):
    def __getattr__(self, name):
        return None


def _client_with_db(results):
    api_main.app.dependency_overrides[get_db_session] = lambda: DummySession(results)
    return TestClient(api_main.app)


def test_root_endpoint():
    client = TestClient(api_main.app)
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.json()
    assert "name" in body
    assert "version" in body


def test_health_endpoint(monkeypatch, tmp_path):
    monkeypatch.setattr(api_main, "test_connection", lambda: True)
    monkeypatch.setattr(api_main.os.path, "exists", lambda path: True)
    monkeypatch.setattr(api_main.settings, "EXPORT_DIR", str(tmp_path), raising=False)

    client = TestClient(api_main.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


def test_geojson_latest_file(monkeypatch, tmp_path):
    geo_path = tmp_path / "md_counties_latest.geojson"
    geo_path.write_text('{"type":"FeatureCollection","features":[]}')
    monkeypatch.setattr(api_main.settings, "EXPORT_DIR", str(tmp_path), raising=False)

    client = TestClient(api_main.app)
    resp = client.get("/api/v1/layers/counties/latest")
    assert resp.status_code == 200


def test_geojson_versioned_file(monkeypatch, tmp_path):
    geo_path = tmp_path / "md_counties_20260131.geojson"
    geo_path.write_text('{"type":"FeatureCollection","features":[]}')
    monkeypatch.setattr(api_main.settings, "EXPORT_DIR", str(tmp_path), raising=False)

    client = TestClient(api_main.app)
    resp = client.get("/api/v1/layers/counties/20260131")
    assert resp.status_code == 200


def test_area_detail_endpoint():
    result = AttrDict(
        fips_code="24001",
        data_year=2025,
        final_grouping="stable_constrained",
        directional_status="stable",
        confidence_level="conditional",
        composite_score=0.55,
        updated_at=datetime(2025, 1, 1, 0, 0, 0),
        employment_gravity_score=0.6,
        mobility_optionality_score=0.5,
        school_trajectory_score=0.4,
        housing_elasticity_score=0.7,
        demographic_momentum_score=0.5,
        risk_drag_score=0.2,
    )

    client = _client_with_db([DummyResult(fetchone_value=result)])
    resp = client.get("/api/v1/areas/24001")
    assert resp.status_code == 200
    body = resp.json()
    assert body["fips_code"] == "24001"
    assert "primary_strengths" in body
    assert "primary_weaknesses" in body


def test_layer_detail_endpoint():
    layer_result = AttrDict(
        data_year=2025,
        economic_opportunity_index=0.7,
        economic_accessibility_score=0.6,
        employment_diversification_score=0.5,
        high_wage_jobs_accessible_45min=1000,
        wage_quality_ratio=1.2,
        sector_diversity_shannon=2.1,
    )
    ts_result = AttrDict(
        momentum_slope=0.02,
        momentum_percent_change=0.1,
        coverage_years=5,
    )

    client = _client_with_db(
        [
            DummyResult(fetchone_value=layer_result),
            DummyResult(fetchone_value=ts_result),
        ]
    )

    resp = client.get("/api/v1/areas/24001/layers/employment_gravity")
    assert resp.status_code == 200
    body = resp.json()
    assert body["layer_key"] == "employment_gravity"
    assert body["score"] == 0.7


def test_refresh_status_endpoint():
    refresh_rows = [
        AttrDict(
            layer_name="layer1_employment_gravity",
            data_source="LODES",
            refresh_date=datetime(2025, 1, 2, 0, 0, 0),
            status="success",
            records_processed=100,
        )
    ]

    client = _client_with_db([DummyResult(fetchall_value=refresh_rows)])
    resp = client.get("/api/v1/metadata/refresh?limit=1")
    assert resp.status_code == 200
    assert len(resp.json()) == 1
