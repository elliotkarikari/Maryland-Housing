import io
import zipfile
from types import SimpleNamespace

import pandas as pd
import pytest

import src.utils.data_sources as ds


class DummyResponse:
    def __init__(self, *, status_code=200, text="", content=b"", json_data=None, ok=True):
        self.status_code = status_code
        self.text = text
        self.content = content
        self._json_data = json_data
        self.ok = ok
        self.headers = {"content-length": str(len(content))}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data

    def iter_content(self, chunk_size=8192):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]


def test_rate_limiter_sleeps_when_called_too_fast(monkeypatch):
    limiter = ds.RateLimiter(calls_per_minute=60)  # 1 call/sec
    calls = []
    sleeps = []

    def target():
        calls.append("ok")
        return "done"

    times = iter([100.0, 100.0, 100.1, 100.1])
    monkeypatch.setattr(ds.time, "time", lambda: next(times))
    monkeypatch.setattr(ds.time, "sleep", lambda s: sleeps.append(s))

    wrapped = limiter(target)
    wrapped()
    wrapped()

    assert calls == ["ok", "ok"]
    assert sleeps and sleeps[0] == pytest.approx(0.9)


def test_read_csv_from_bytes_handles_plain_and_zip():
    csv_bytes = b"ID,VAL\n24001,1\n24003,2\n"

    plain_df = ds._read_csv_from_bytes(csv_bytes, dtype={"ID": str})
    assert list(plain_df.columns) == ["ID", "VAL"]
    assert plain_df["ID"].tolist() == ["24001", "24003"]

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as zf:
        zf.writestr("data.csv", csv_bytes)

    zip_df = ds._read_csv_from_bytes(buffer.getvalue(), dtype={"ID": str})
    assert list(zip_df.columns) == ["ID", "VAL"]
    assert zip_df["ID"].tolist() == ["24001", "24003"]


def test_candidate_ejscreen_urls_contains_expected_variants():
    urls = ds._candidate_ejscreen_urls("https://example.com", 2023)
    assert any("EJSCREEN_2023_StatePct.csv" in url for url in urls)


def test_discover_ejscreen_urls_parses_listing(monkeypatch):
    html = """
    <a href="EJSCREEN_2023_StatePct.csv">csv</a>
    <a href="EJSCREEN_2023_StatePct.zip">zip</a>
    <a href="not_a_match.txt">ignore</a>
    <a href="https://cdn.example.com/EJSCREEN_2023_StatePct_with_AS_CNMI_GU_VI.csv">abs</a>
    """

    def fake_get(url, timeout=60):
        return DummyResponse(status_code=200, text=html, ok=True)

    monkeypatch.setattr(ds.requests, "get", fake_get)

    urls = ds._discover_ejscreen_urls("https://example.com", 2023)
    assert "https://example.com/2023/EJSCREEN_2023_StatePct.csv" in urls
    assert "https://example.com/2023/EJSCREEN_2023_StatePct.zip" in urls
    assert "https://cdn.example.com/EJSCREEN_2023_StatePct_with_AS_CNMI_GU_VI.csv" in urls


def test_fetch_epa_ejscreen_success(monkeypatch):
    csv_bytes = b"ID,VAL\n24001,1\n11001,2\n"

    monkeypatch.setattr(ds, "_candidate_ejscreen_urls", lambda base, year: ["https://example.com/file.csv"])
    monkeypatch.setattr(ds, "_discover_ejscreen_urls", lambda base, year: [])
    monkeypatch.setattr(ds.requests, "get", lambda url, timeout=120: DummyResponse(status_code=200, content=csv_bytes))

    df = ds.fetch_epa_ejscreen(year=2023, lookback_years=0)
    assert df["ID"].tolist() == ["24001"]
    assert df["ejscreen_year"].iloc[0] == 2023


def test_fetch_census_data_success(monkeypatch):
    json_payload = [
        ["NAME", "B01001_001E", "state", "county"],
        ["Allegany County, MD", "100", "24", "001"],
    ]

    monkeypatch.setattr(
        ds.requests,
        "get",
        lambda url, params=None, timeout=30: DummyResponse(status_code=200, json_data=json_payload),
    )

    df = ds.fetch_census_data.__wrapped__(
        dataset="acs/acs5",
        variables=["B01001_001E"],
        geography="county:*",
        year=2023,
    )

    assert df.iloc[0]["NAME"] == "Allegany County, MD"


def test_fetch_usaspending_county_success(monkeypatch):
    payload = {"results": [{"county_name": "Allegany", "amount": 123.45}]}

    monkeypatch.setattr(
        ds.requests,
        "post",
        lambda url, json=None, timeout=60: DummyResponse(status_code=200, json_data=payload),
    )

    df = ds.fetch_usaspending_county.__wrapped__("2023-01-01", "2023-12-31")
    assert df.iloc[0]["county_name"] == "Allegany"


def test_fetch_lodes_wac_success(monkeypatch):
    expected = pd.DataFrame({"w_geocode": ["24001"], "C000": [10]})

    def fake_read_csv(url, compression=None, dtype=None):
        return expected

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    df = ds.fetch_lodes_wac(state="md", year=2021, job_type="JT00")
    assert df.equals(expected)


def test_download_file_success(monkeypatch, tmp_path):
    content = b"hello-world"
    file_path = tmp_path / "file.bin"

    monkeypatch.setattr(
        ds.requests,
        "get",
        lambda url, stream=True, timeout=300: DummyResponse(status_code=200, content=content),
    )

    assert ds.download_file("https://example.com/file.bin", str(file_path))
    assert file_path.read_bytes() == content
