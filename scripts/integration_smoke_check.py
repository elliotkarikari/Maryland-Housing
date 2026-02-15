#!/usr/bin/env python3
"""Integration smoke checks using real runtime secrets."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import requests
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.database import DATABASE_BACKEND, get_db, test_connection
from config.settings import get_settings

TIMEOUT_SECONDS = 30
PLACEHOLDER_TOKENS = {"test-mapbox-token", "test-census-key", "changeme", "your_api_key_here"}


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def _safe_env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def check_required_env() -> CheckResult:
    backend = DATABASE_BACKEND
    required = ["MAPBOX_ACCESS_TOKEN", "CENSUS_API_KEY"]
    if backend == "databricks":
        required.extend(
            ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN"]
        )
    else:
        required.append("DATABASE_URL")

    missing = [name for name in required if not _safe_env(name)]
    if missing:
        return CheckResult(
            name="required_env",
            ok=False,
            detail=f"Missing required env vars: {', '.join(sorted(missing))}",
        )

    placeholder_hits = []
    for name in ("MAPBOX_ACCESS_TOKEN", "CENSUS_API_KEY"):
        if _safe_env(name).lower() in PLACEHOLDER_TOKENS:
            placeholder_hits.append(name)
    if placeholder_hits:
        return CheckResult(
            name="required_env",
            ok=False,
            detail=f"Placeholder values detected for: {', '.join(sorted(placeholder_hits))}",
        )

    return CheckResult(name="required_env", ok=True, detail=f"backend={backend}")


def check_database_connection() -> CheckResult:
    if not test_connection():
        return CheckResult(
            name="database_connection",
            ok=False,
            detail="test_connection() returned False",
        )

    try:
        with get_db() as db:
            value = db.execute(text("SELECT 1")).scalar()
        if value != 1:
            return CheckResult(
                name="database_connection",
                ok=False,
                detail=f"Expected SELECT 1 == 1, got {value!r}",
            )
    except Exception as exc:
        return CheckResult(
            name="database_connection",
            ok=False,
            detail=f"Query check failed: {exc}",
        )

    return CheckResult(name="database_connection", ok=True, detail="SELECT 1 succeeded")


def check_live_county_feed_tables() -> CheckResult:
    try:
        with get_db() as db:
            county_count = int(db.execute(text("SELECT COUNT(*) FROM md_counties")).scalar() or 0)
            synthesis_count = int(
                db.execute(text("SELECT COUNT(*) FROM final_synthesis_current")).scalar() or 0
            )
    except Exception as exc:
        return CheckResult(
            name="live_county_feed_tables",
            ok=False,
            detail=f"Failed to query county feed tables: {exc}",
        )

    if county_count <= 0:
        return CheckResult(
            name="live_county_feed_tables",
            ok=False,
            detail="md_counties has no geometry rows; live county feed cannot render",
        )

    return CheckResult(
        name="live_county_feed_tables",
        ok=True,
        detail=f"md_counties={county_count}, final_synthesis_current={synthesis_count}",
    )


def check_census_api_key() -> CheckResult:
    settings = get_settings()
    url = f"{settings.CENSUS_API_BASE_URL}/2023/acs/acs5"
    params = {"get": "NAME", "for": "state:24", "key": settings.CENSUS_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        if response.status_code != 200:
            return CheckResult(
                name="census_api",
                ok=False,
                detail=f"HTTP {response.status_code}: {response.text[:200]}",
            )
        payload = response.json()
        if not isinstance(payload, list) or len(payload) < 2:
            return CheckResult(
                name="census_api",
                ok=False,
                detail=f"Unexpected payload shape: {type(payload).__name__}",
            )
    except Exception as exc:
        return CheckResult(name="census_api", ok=False, detail=str(exc))

    return CheckResult(name="census_api", ok=True, detail="ACS request returned data")


def check_mapbox_token() -> CheckResult:
    settings = get_settings()
    url = "https://api.mapbox.com/styles/v1/mapbox/streets-v12"
    params = {"access_token": settings.MAPBOX_ACCESS_TOKEN}

    try:
        response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        if response.status_code != 200:
            return CheckResult(
                name="mapbox_token",
                ok=False,
                detail=f"HTTP {response.status_code}: {response.text[:200]}",
            )
        payload = response.json()
        if payload.get("id") != "streets-v12":
            return CheckResult(
                name="mapbox_token",
                ok=False,
                detail=f"Unexpected style id: {payload.get('id')!r}",
            )
    except Exception as exc:
        return CheckResult(name="mapbox_token", ok=False, detail=str(exc))

    return CheckResult(name="mapbox_token", ok=True, detail="Style API request succeeded")


def run_checks() -> list[CheckResult]:
    env_result = check_required_env()
    results = [env_result]
    if not env_result.ok:
        return results

    checks: list[Callable[[], CheckResult]] = [
        check_database_connection,
        check_live_county_feed_tables,
        check_census_api_key,
        check_mapbox_token,
    ]
    results.extend(check() for check in checks)
    return results


def main() -> int:
    results = run_checks()
    ok = all(result.ok for result in results)

    print(json.dumps([result.__dict__ for result in results], indent=2))
    if not ok:
        failed = [result.name for result in results if not result.ok]
        print(f"Integration smoke checks failed: {', '.join(failed)}")
        return 1

    print("Integration smoke checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
