from contextlib import contextmanager
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


class DummyScalarResult:
    def __init__(self, scalar_value):
        self._scalar_value = scalar_value

    def scalar(self):
        return self._scalar_value


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
    monkeypatch.setattr(api_main.settings, "EXPORT_DIR", str(tmp_path), raising=False)

    class HealthSession:
        def execute(self, *args, **kwargs):
            return DummyScalarResult(24)

    @contextmanager
    def fake_get_db():
        yield HealthSession()

    monkeypatch.setattr(api_main, "get_db", fake_get_db)

    client = TestClient(api_main.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"
    assert resp.json()["county_count"] == 24


def test_geojson_latest_live_feed():
    rows = [
        AttrDict(
            fips_code="24001",
            county_name="Allegany County",
            geometry_geojson='{"type":"Polygon","coordinates":[[[-78,39],[-77.9,39],[-77.9,39.1],[-78,39]]]}',
            county_updated_at=None,
            data_year=2025,
            final_grouping="stable_constrained",
            directional_status="stable",
            confidence_level="conditional",
            uncertainty_level="low",
            uncertainty_reasons='["none"]',
            composite_score=0.55,
            employment_gravity_score=0.6,
            mobility_optionality_score=0.5,
            school_trajectory_score=0.4,
            housing_elasticity_score=0.7,
            demographic_momentum_score=0.5,
            risk_drag_score=0.2,
            classification_version="v2.0",
            synthesis_updated_at=datetime(2025, 1, 1, 0, 0, 0),
        )
    ]

    client = _client_with_db([DummyResult(fetchall_value=rows)])
    resp = client.get("/api/v1/layers/counties/latest")
    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == 1
    props = body["features"][0]["properties"]
    assert props["fips_code"] == "24001"
    assert props["live_feed"] is True


def test_geojson_latest_progressive_scores_from_layer_tables():
    rows = [
        AttrDict(
            fips_code="24003",
            county_name="Anne Arundel County",
            geometry_geojson='{"type":"Polygon","coordinates":[[[-76.8,39],[-76.7,39],[-76.7,39.1],[-76.8,39]]]}',
            county_updated_at=None,
            data_year=None,
            final_grouping=None,
            directional_status=None,
            confidence_level=None,
            uncertainty_level=None,
            uncertainty_reasons=None,
            composite_score=None,
            employment_gravity_score=None,
            mobility_optionality_score=None,
            school_trajectory_score=None,
            housing_elasticity_score=None,
            demographic_momentum_score=None,
            risk_drag_score=None,
            classification_version=None,
            synthesis_updated_at=None,
            l1_data_year=2022,
            l1_score=0.71,
            l2_data_year=2022,
            l2_score=0.50,
            l3_data_year=2024,
            l3_score=0.62,
            l4_data_year=2023,
            l4_score=0.55,
            l5_data_year=None,
            l5_score=None,
            l6_data_year=None,
            l6_score=None,
        )
    ]

    client = _client_with_db([DummyResult(fetchall_value=rows)])
    resp = client.get("/api/v1/layers/counties/latest")
    assert resp.status_code == 200
    body = resp.json()
    props = body["features"][0]["properties"]

    assert props["county_name"] == "Anne Arundel County"
    assert props["data_year"] == 2024
    assert abs(props["composite_score"] - 0.595) < 1e-6
    assert props["directional_class"] == "stable"
    assert props["confidence_class"] == "strong"
    assert props["synthesis_grouping"] == "stable_constrained"
    assert props["employment_gravity_score"] == 0.71
    assert props["mobility_optionality_score"] == 0.5
    assert props["school_trajectory_score"] == 0.62
    assert props["housing_elasticity_score"] == 0.55
    assert props["demographic_momentum_score"] is None


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


def test_area_detail_progressive_fallback_without_synthesis_row():
    # Query order:
    # 1) final_synthesis_current lookup
    # 2-7) latest row per layer table (employment, mobility, school, housing, demographic, risk)
    client = _client_with_db(
        [
            DummyResult(fetchone_value=None),
            DummyResult(fetchone_value=AttrDict(data_year=2022, score=0.8)),
            DummyResult(fetchone_value=AttrDict(data_year=None, score=None)),
            DummyResult(fetchone_value=AttrDict(data_year=2024, score=0.6)),
            DummyResult(fetchone_value=AttrDict(data_year=2023, score=0.4)),
            DummyResult(fetchone_value=AttrDict(data_year=None, score=None)),
            DummyResult(fetchone_value=AttrDict(data_year=None, score=None)),
        ]
    )

    resp = client.get("/api/v1/areas/24021")
    assert resp.status_code == 200
    body = resp.json()
    assert body["fips_code"] == "24021"
    assert body["data_year"] == 2024
    assert abs(body["composite_score"] - 0.6) < 1e-6
    assert body["directional_class"] == "stable"
    assert body["confidence_class"] == "conditional"
    assert body["synthesis_grouping"] == "stable_constrained"
    assert body["layer_scores"]["employment_gravity"] == 0.8
    assert body["layer_scores"]["school_trajectory"] == 0.6
    assert body["layer_scores"]["housing_elasticity"] == 0.4


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
