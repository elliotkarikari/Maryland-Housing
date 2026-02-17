"""
Maryland Viability Atlas - Layer 1: Economic Opportunity Accessibility (v2)
Modern accessibility-based economic analysis using LODES wage-filtered data.

This module computes travel time-based economic accessibility metrics that measure
actual ability to reach high-wage jobs and economic opportunities, replacing simple
job counts with policy-relevant opportunity access analysis.

Data Sources:
- LODES WAC: Workplace Area Characteristics with wage segments (SE01/SE02/SE03)
- LODES RAC: Residence Area Characteristics for labor force
- ACS 5-Year: Demographics, income, labor force participation
- Opportunity Insights: Upward mobility predictors (optional)

Core Metrics:
- High-wage jobs accessible (30min, 45min)
- Total jobs accessible (30min, 45min)
- Economic accessibility score (normalized)
- Wage quality ratio (high-wage / total accessible)
- Upward mobility composite

Author: Maryland Viability Atlas Team
Date: 2026-01-29
Version: 2.0
"""

import hashlib
import os
import re
import sys
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.database import get_db, log_refresh, table_name as db_table_name
from config.settings import MD_COUNTY_FIPS, get_settings
from src.utils.data_sources import download_file
from src.utils.logging import get_logger
from src.utils.prediction_utils import apply_predictions_to_table

logger = get_logger(__name__)
settings = get_settings()

L1_TRACT_TABLE = db_table_name("layer1_economic_opportunity_tract")
L1_COUNTY_TABLE = db_table_name("layer1_employment_gravity")

# =============================================================================
# CONFIGURATION
# =============================================================================

CACHE_DIR = Path("data/cache/economic_v2")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

LODES_CACHE_DIR = CACHE_DIR / "lodes"
LODES_CACHE_DIR.mkdir(exist_ok=True)

ACS_CACHE_DIR = CACHE_DIR / "acs"
ACS_CACHE_DIR.mkdir(exist_ok=True)

QWI_CACHE_DIR = CACHE_DIR / "qwi"
QWI_CACHE_DIR.mkdir(exist_ok=True)

# LODES wage segments
# SE01: $1,250/month or less (~$15k/year)
# SE02: $1,251/month to $3,333/month (~$15k-$40k/year)
# SE03: More than $3,333/month (>$40k/year) - "high wage"
WAGE_SEGMENTS = {
    "SE01": "low_wage",  # <$15k/year
    "SE02": "mid_wage",  # $15k-$40k/year
    "SE03": "high_wage",  # >$40k/year
}

# County-level composite weights
# Local strength = v1 employment diversification score
# Regional access = v2 economic accessibility score
LOCAL_STRENGTH_WEIGHT = 0.4
REGIONAL_ACCESS_WEIGHT = 0.6
QWI_BLEND_WEIGHT = 0.15

# Accessibility mode options
ACCESSIBILITY_MODE_AUTO = "auto"
ACCESSIBILITY_MODE_NETWORK = "network"
ACCESSIBILITY_MODE_PROXY = "proxy"
# Allow table, schema.table, or catalog.schema.table identifiers.
SAFE_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")

# Sectors for diversity analysis (NAICS 2-digit via CNS codes)
NAICS_SECTORS = {
    "CNS01": "Agriculture",
    "CNS02": "Mining",
    "CNS03": "Utilities",
    "CNS04": "Construction",
    "CNS05": "Manufacturing",
    "CNS06": "Wholesale",
    "CNS07": "Retail",
    "CNS08": "Transportation",
    "CNS09": "Information",
    "CNS10": "Finance",
    "CNS11": "Real Estate",
    "CNS12": "Professional/Tech",
    "CNS13": "Management",
    "CNS14": "Admin Support",
    "CNS15": "Education",
    "CNS16": "Healthcare",
    "CNS17": "Arts/Entertainment",
    "CNS18": "Accommodation/Food",
    "CNS19": "Other Services",
    "CNS20": "Public Admin",
}

# High-wage intensive sectors (typically pay above median)
HIGH_WAGE_SECTORS = ["CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS20"]

# =============================================================================
# DATA ACQUISITION
# =============================================================================


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _find_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    for cand in candidates:
        if cand in columns:
            return cand
    for cand in candidates:
        for col in columns:
            if cand in col:
                return col
    return None


def _resolve_data_path(
    local_path: Optional[str], url: Optional[str], cache_dir: Path, filename: str
) -> Optional[Path]:
    if local_path:
        path = Path(local_path)
        if path.exists():
            return path
        logger.warning(f"Provided data path not found: {local_path}")
    if url:
        cache_dir.mkdir(parents=True, exist_ok=True)
        target = cache_dir / filename
        if not target.exists():
            ok = download_file(url, str(target))
            if not ok:
                logger.warning(f"Failed to download data from {url}")
                return None
        return target
    return None


def _fetch_qwi_api(data_year: int) -> pd.DataFrame:
    base_url = settings.CENSUS_QWI_DATA_URL
    if not base_url:
        base_url = f"{settings.CENSUS_API_BASE_URL.rstrip('/')}/{settings.CENSUS_QWI_DATASET}"
    if not settings.CENSUS_API_KEY:
        logger.warning("Census API key missing; cannot fetch QWI API")
        return pd.DataFrame()

    current_year = datetime.utcnow().year
    start_year = min(data_year, current_year - 1)
    target_years = list(range(start_year, max(start_year - 3, 1990) - 1, -1))

    get_variants = ["Emp,HirA,Sep", "Emp,HirA,SepA"]
    base_params = {
        # Explicit defaults to avoid overly large queries and missing required predicates.
        "sex": "0",  # both sexes
        "agegrp": "A00",  # all ages
        "ownercode": "A00",  # all ownership
        "seasonadj": "U",  # unadjusted
    }

    for year in target_years:
        for quarter in [4, 3, 2, 1]:
            for get_fields in get_variants:
                params = {
                    "get": get_fields,
                    "for": "county:*",
                    "in": "state:24",
                    "year": str(year),
                    "quarter": str(quarter),
                    "key": settings.CENSUS_API_KEY,
                    **base_params,
                }
                try:
                    resp = requests.get(base_url, params=params, timeout=30)
                    if resp.status_code != 200:
                        warn = False
                        if resp.status_code in (401, 403):
                            warn = True
                        elif resp.status_code == 400 and get_fields == "Emp,HirA,Sep":
                            warn = True
                        if warn:
                            logger.warning(
                                "QWI API request failed",
                                extra={
                                    "status_code": resp.status_code,
                                    "year": year,
                                    "quarter": quarter,
                                    "url": resp.url,
                                },
                            )
                        continue
                    data = resp.json()
                    if not data or len(data) < 2:
                        continue
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df = _normalize_columns(df)
                    df["fips_code"] = df["state"].astype(str).str.zfill(2) + df["county"].astype(
                        str
                    ).str.zfill(3)
                    df["emp"] = pd.to_numeric(df.get("emp"), errors="coerce")
                    df["hira"] = pd.to_numeric(df.get("hira"), errors="coerce")
                    sep_col = df.get("sep")
                    if sep_col is None:
                        sep_col = df.get("sepa")
                    df["sep"] = pd.to_numeric(sep_col, errors="coerce")
                    df["qwi_year"] = year
                    df["qwi_quarter"] = quarter

                    cache_path = QWI_CACHE_DIR / f"qwi_{year}_q{quarter}.csv"
                    try:
                        df.to_csv(cache_path, index=False)
                    except Exception:
                        pass

                    logger.info(f"Fetched QWI API data for {year} Q{quarter} ({get_fields})")
                    return df
                except Exception:
                    continue

    logger.warning("QWI API returned no data for requested years")
    return pd.DataFrame()


def fetch_qwi_by_county(data_year: int) -> pd.DataFrame:
    """
    Fetch Census QWI data and aggregate to Maryland counties.

    Supports either a local CSV (settings.CENSUS_QWI_DATA_PATH) or a URL
    (settings.CENSUS_QWI_DATA_URL). Expected columns are flexible; common
    variants for hires/separations/turnover are detected.
    """
    if settings.CENSUS_QWI_DATA_URL and "api.census.gov" in settings.CENSUS_QWI_DATA_URL:
        api_df = _fetch_qwi_api(data_year)
        if api_df.empty:
            return pd.DataFrame()
        df = api_df
    else:
        source_path = _resolve_data_path(
            settings.CENSUS_QWI_DATA_PATH,
            settings.CENSUS_QWI_DATA_URL,
            QWI_CACHE_DIR,
            f"qwi_{data_year}.csv",
        )

        if source_path is None:
            logger.warning("QWI data source not configured; skipping QWI enrichment")
            return pd.DataFrame()

        try:
            df = pd.read_csv(source_path, dtype=str, low_memory=False)
        except Exception as e:
            logger.warning(f"Failed to read QWI data {source_path}: {e}")
            return pd.DataFrame()

        df = _normalize_columns(df)
    columns = list(df.columns)

    year_col = _find_col(columns, ["year", "yr", "time"])
    qwi_year = data_year
    if year_col:
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
        if df[year_col].notna().any():
            if (df[year_col] == data_year).any():
                qwi_year = data_year
                df = df[df[year_col] == data_year]
            else:
                qwi_year = int(df[year_col].max())
                df = df[df[year_col] == qwi_year]

    fips_col = _find_col(columns, ["fips_code", "fips", "county_fips", "geoid", "geo_id"])
    if fips_col:
        df["fips_code"] = df[fips_col].astype(str).str.zfill(5)
    else:
        state_col = _find_col(columns, ["state", "state_fips", "st"])
        county_col = _find_col(columns, ["county", "county_fips", "cnty"])
        if state_col and county_col:
            df["fips_code"] = df[state_col].astype(str).str.zfill(2) + df[county_col].astype(
                str
            ).str.zfill(3)
        else:
            logger.warning("QWI data missing FIPS columns; skipping QWI enrichment")
            return pd.DataFrame()

    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())]
    if df.empty:
        logger.warning("QWI data contains no Maryland counties after filtering")
        return pd.DataFrame()

    emp_col = _find_col(columns, ["emp", "employment", "emp_total", "employed"])
    hires_col = _find_col(columns, ["hira", "hires", "hire"])
    seps_col = _find_col(columns, ["sep", "sepa", "separations", "separation"])
    hire_rate_col = _find_col(columns, ["hire_rate", "hira_rate", "hire_rt"])
    sep_rate_col = _find_col(columns, ["separation_rate", "sepa_rate", "sep_rate"])
    turnover_col = _find_col(columns, ["turnover", "turnovr", "turnover_rate", "turno"])

    if emp_col:
        df[emp_col] = pd.to_numeric(df[emp_col], errors="coerce")
    if hires_col:
        df[hires_col] = pd.to_numeric(df[hires_col], errors="coerce")
    if seps_col:
        df[seps_col] = pd.to_numeric(df[seps_col], errors="coerce")
    if hire_rate_col:
        df[hire_rate_col] = pd.to_numeric(df[hire_rate_col], errors="coerce")
    if sep_rate_col:
        df[sep_rate_col] = pd.to_numeric(df[sep_rate_col], errors="coerce")
    if turnover_col:
        df[turnover_col] = pd.to_numeric(df[turnover_col], errors="coerce")

    # Derive rates if needed
    df["qwi_emp_total"] = df[emp_col] if emp_col else pd.NA
    df["qwi_hires"] = df[hires_col] if hires_col else pd.NA
    df["qwi_separations"] = df[seps_col] if seps_col else pd.NA

    if hire_rate_col:
        df["qwi_hire_rate"] = df[hire_rate_col]
    elif emp_col and hires_col:
        df["qwi_hire_rate"] = df["qwi_hires"] / df["qwi_emp_total"]
    else:
        df["qwi_hire_rate"] = pd.NA

    if sep_rate_col:
        df["qwi_separation_rate"] = df[sep_rate_col]
    elif emp_col and seps_col:
        df["qwi_separation_rate"] = df["qwi_separations"] / df["qwi_emp_total"]
    else:
        df["qwi_separation_rate"] = pd.NA

    if turnover_col:
        df["qwi_turnover_rate"] = df[turnover_col]
    elif df["qwi_hire_rate"].notna().any() and df["qwi_separation_rate"].notna().any():
        df["qwi_turnover_rate"] = df["qwi_hire_rate"] + df["qwi_separation_rate"]
    else:
        df["qwi_turnover_rate"] = pd.NA

    df["qwi_net_job_growth_rate"] = pd.NA
    if df["qwi_hire_rate"].notna().any() and df["qwi_separation_rate"].notna().any():
        df["qwi_net_job_growth_rate"] = df["qwi_hire_rate"] - df["qwi_separation_rate"]
    elif emp_col and hires_col and seps_col:
        df["qwi_net_job_growth_rate"] = (df["qwi_hires"] - df["qwi_separations"]) / df[
            "qwi_emp_total"
        ]

    def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
        mask = values.notna() & weights.notna() & (weights > 0)
        if not mask.any():
            return np.nan
        return float((values[mask] * weights[mask]).sum() / weights[mask].sum())

    grouped = []
    for fips_code, sub in df.groupby("fips_code"):
        emp_total = sub["qwi_emp_total"].sum(min_count=1)
        hires_total = sub["qwi_hires"].sum(min_count=1)
        seps_total = sub["qwi_separations"].sum(min_count=1)

        if emp_col and emp_total and emp_total > 0:
            hire_rate = _weighted_mean(sub["qwi_hire_rate"], sub["qwi_emp_total"])
            sep_rate = _weighted_mean(sub["qwi_separation_rate"], sub["qwi_emp_total"])
            turnover_rate = _weighted_mean(sub["qwi_turnover_rate"], sub["qwi_emp_total"])
        else:
            hire_rate = sub["qwi_hire_rate"].mean()
            sep_rate = sub["qwi_separation_rate"].mean()
            turnover_rate = sub["qwi_turnover_rate"].mean()

        net_growth = np.nan
        if pd.notna(hire_rate) and pd.notna(sep_rate):
            net_growth = hire_rate - sep_rate
        elif pd.notna(emp_total) and emp_total and pd.notna(hires_total) and pd.notna(seps_total):
            net_growth = (hires_total - seps_total) / emp_total

        grouped.append(
            {
                "fips_code": fips_code,
                "qwi_emp_total": int(emp_total) if pd.notna(emp_total) else None,
                "qwi_hires": int(hires_total) if pd.notna(hires_total) else None,
                "qwi_separations": int(seps_total) if pd.notna(seps_total) else None,
                "qwi_hire_rate": hire_rate,
                "qwi_separation_rate": sep_rate,
                "qwi_turnover_rate": turnover_rate,
                "qwi_net_job_growth_rate": net_growth,
                "qwi_year": qwi_year,
            }
        )

    qwi_df = pd.DataFrame(grouped)
    logger.info(f"Loaded QWI records for {len(qwi_df)} counties (year={qwi_year})")
    return qwi_df


def _resolve_table_name(table_name: Optional[str]) -> Optional[str]:
    if not table_name:
        return None
    if not SAFE_TABLE_NAME_RE.match(table_name):
        logger.warning(f"Ignoring unsafe table name: {table_name}")
        return None
    return table_name


def _fetch_lodes_od_county_flows_from_table(od_year: int, table_name: str) -> pd.DataFrame:
    table_name = _resolve_table_name(table_name)
    if table_name is None:
        return pd.DataFrame()
    table_ref = table_name if "." in table_name else db_table_name(table_name)

    sql = text(
        f"""
        WITH base AS (
            SELECT
                h_county,
                w_county,
                SUM(COALESCE(s000, 0)) AS s000,
                SUM(COALESCE(sa02, 0) + COALESCE(sa03, 0)) AS working_age_workers,
                SUM(COALESCE(se03, 0)) AS high_wage_workers
            FROM {table_ref}
            WHERE data_year = :od_year
              AND h_county LIKE '24%'
              AND w_county LIKE '24%'
            GROUP BY h_county, w_county
        ),
        resident AS (
            SELECT
                h_county AS fips_code,
                SUM(s000) AS od_resident_workers,
                SUM(working_age_workers) AS od_working_age_resident_workers,
                SUM(high_wage_workers) AS od_high_wage_resident_workers
            FROM base
            GROUP BY h_county
        ),
        same_county AS (
            SELECT
                h_county AS fips_code,
                SUM(s000) AS od_live_work_same_county,
                SUM(working_age_workers) AS od_working_age_live_work_same_county,
                SUM(high_wage_workers) AS od_high_wage_live_work_same_county
            FROM base
            WHERE h_county = w_county
            GROUP BY h_county
        ),
        outbound AS (
            SELECT
                h_county AS fips_code,
                SUM(s000) AS od_outbound_workers
            FROM base
            WHERE h_county <> w_county
            GROUP BY h_county
        ),
        inbound AS (
            SELECT
                w_county AS fips_code,
                SUM(s000) AS od_inbound_workers
            FROM base
            WHERE h_county <> w_county
            GROUP BY w_county
        )
        SELECT
            r.fips_code,
            CAST(:od_year AS INT) AS od_year,
            r.od_resident_workers,
            COALESCE(i.od_inbound_workers, 0) AS od_inbound_workers,
            COALESCE(o.od_outbound_workers, 0) AS od_outbound_workers,
            COALESCE(s.od_live_work_same_county, 0) AS od_live_work_same_county,
            COALESCE(s.od_working_age_live_work_same_county, 0) AS od_working_age_live_work_same_county,
            COALESCE(s.od_high_wage_live_work_same_county, 0) AS od_high_wage_live_work_same_county,
            r.od_working_age_resident_workers,
            r.od_high_wage_resident_workers
        FROM resident r
        LEFT JOIN inbound i ON i.fips_code = r.fips_code
        LEFT JOIN outbound o ON o.fips_code = r.fips_code
        LEFT JOIN same_county s ON s.fips_code = r.fips_code
        """
    )

    try:
        with get_db() as db:
            rows = db.execute(sql, {"od_year": int(od_year)}).fetchall()
    except Exception:
        logger.info(
            "LODES OD raw table query unavailable; falling back to CSV-based OD aggregation",
            exc_info=True,
        )
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(row._mapping) for row in rows])
    if df.empty:
        return df

    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())].copy()
    if df.empty:
        return df

    df["od_net_commuter_flow"] = df["od_inbound_workers"] - df["od_outbound_workers"]
    df["od_local_capture_rate"] = np.where(
        df["od_resident_workers"] > 0,
        df["od_live_work_same_county"] / df["od_resident_workers"],
        np.nan,
    )
    df["od_working_age_share"] = np.where(
        df["od_resident_workers"] > 0,
        df["od_working_age_resident_workers"] / df["od_resident_workers"],
        np.nan,
    )
    df["od_working_age_local_capture_rate"] = np.where(
        df["od_working_age_resident_workers"] > 0,
        df["od_working_age_live_work_same_county"] / df["od_working_age_resident_workers"],
        np.nan,
    )
    df["od_high_wage_share"] = np.where(
        df["od_resident_workers"] > 0,
        df["od_high_wage_resident_workers"] / df["od_resident_workers"],
        np.nan,
    )
    df["od_high_wage_local_capture_rate"] = np.where(
        df["od_high_wage_resident_workers"] > 0,
        df["od_high_wage_live_work_same_county"] / df["od_high_wage_resident_workers"],
        np.nan,
    )

    logger.info(
        f"Loaded LODES OD county metrics from table {table_name} for year={od_year} "
        f"({len(df)} counties)"
    )
    return df


def fetch_lodes_od_county_flows(od_year: int) -> pd.DataFrame:
    """
    Aggregate LODES OD flows to county-level commute indicators.

    Expected OD schema (OnTheMap OD download):
      w_geocode, h_geocode, year, S000, SA01, SA02, SA03, SE01, SE02, SE03, ...

    Working-age commuters follow the project rule:
      working_age = SA02 + SA03  (ages 30-54 and 55+)
    """
    table_name = _resolve_table_name(getattr(settings, "LODES_OD_TABLE", None))
    if table_name:
        table_df = _fetch_lodes_od_county_flows_from_table(od_year=od_year, table_name=table_name)
        if not table_df.empty:
            return table_df

    source_path = _resolve_data_path(
        settings.LODES_OD_DATA_PATH,
        settings.LODES_OD_DATA_URL,
        LODES_CACHE_DIR,
        f"md_od_JT00_{od_year}.csv",
    )
    if source_path is None:
        logger.info("LODES OD source not configured; skipping OD commute-flow enrichment")
        return pd.DataFrame()

    required_cols = {"w_geocode", "h_geocode", "year", "s000", "sa02", "sa03", "se03"}
    numeric_cols = ["s000", "sa02", "sa03", "se03"]
    county_keys = set(MD_COUNTY_FIPS.keys())
    chunk_size = max(10000, int(settings.LODES_OD_CHUNK_SIZE or 500000))

    try:
        reader = pd.read_csv(
            source_path,
            dtype={"w_geocode": str, "h_geocode": str},
            usecols=lambda c: str(c).strip().lower() in required_cols,
            chunksize=chunk_size,
            low_memory=False,
        )
    except Exception as exc:
        logger.warning(f"Failed to read LODES OD data from {source_path}: {exc}")
        return pd.DataFrame()

    def _empty_metrics() -> Dict[str, int]:
        return {
            "od_resident_workers": 0,
            "od_inbound_workers": 0,
            "od_outbound_workers": 0,
            "od_live_work_same_county": 0,
            "od_working_age_resident_workers": 0,
            "od_working_age_live_work_same_county": 0,
            "od_high_wage_resident_workers": 0,
            "od_high_wage_live_work_same_county": 0,
        }

    county_metrics: Dict[str, Dict[str, int]] = {}
    total_rows = 0
    year_rows = 0

    for chunk in reader:
        chunk = _normalize_columns(chunk)
        total_rows += len(chunk)

        if "year" in chunk.columns:
            chunk["year"] = pd.to_numeric(chunk["year"], errors="coerce")
            chunk = chunk[chunk["year"] == int(od_year)]
        if chunk.empty:
            continue
        year_rows += len(chunk)

        chunk["w_geocode"] = chunk["w_geocode"].astype(str).str.zfill(15)
        chunk["h_geocode"] = chunk["h_geocode"].astype(str).str.zfill(15)
        chunk["w_county"] = chunk["w_geocode"].str[:5]
        chunk["h_county"] = chunk["h_geocode"].str[:5]

        chunk = chunk[
            chunk["h_county"].isin(county_keys) & chunk["w_county"].isin(county_keys)
        ].copy()
        if chunk.empty:
            continue

        for col in numeric_cols:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce").fillna(0)

        chunk["working_age_workers"] = chunk["sa02"] + chunk["sa03"]
        chunk["high_wage_workers"] = chunk["se03"]

        resident = chunk.groupby("h_county")[
            ["s000", "working_age_workers", "high_wage_workers"]
        ].sum()
        for fips_code, vals in resident.iterrows():
            metrics = county_metrics.setdefault(fips_code, _empty_metrics())
            metrics["od_resident_workers"] += int(vals["s000"])
            metrics["od_working_age_resident_workers"] += int(vals["working_age_workers"])
            metrics["od_high_wage_resident_workers"] += int(vals["high_wage_workers"])

        same_county = chunk[chunk["h_county"] == chunk["w_county"]]
        if not same_county.empty:
            same_agg = same_county.groupby("h_county")[
                ["s000", "working_age_workers", "high_wage_workers"]
            ].sum()
            for fips_code, vals in same_agg.iterrows():
                metrics = county_metrics.setdefault(fips_code, _empty_metrics())
                metrics["od_live_work_same_county"] += int(vals["s000"])
                metrics["od_working_age_live_work_same_county"] += int(vals["working_age_workers"])
                metrics["od_high_wage_live_work_same_county"] += int(vals["high_wage_workers"])

        cross_county = chunk[chunk["h_county"] != chunk["w_county"]]
        if not cross_county.empty:
            outbound = cross_county.groupby("h_county")["s000"].sum()
            inbound = cross_county.groupby("w_county")["s000"].sum()

            for fips_code, val in outbound.items():
                metrics = county_metrics.setdefault(fips_code, _empty_metrics())
                metrics["od_outbound_workers"] += int(val)

            for fips_code, val in inbound.items():
                metrics = county_metrics.setdefault(fips_code, _empty_metrics())
                metrics["od_inbound_workers"] += int(val)

    if not county_metrics:
        logger.warning(
            "LODES OD data had no Maryland county rows for requested year",
            extra={"od_year": od_year, "source_path": str(source_path)},
        )
        return pd.DataFrame()

    records = []
    for fips_code, metrics in county_metrics.items():
        resident = metrics["od_resident_workers"]
        resident_working_age = metrics["od_working_age_resident_workers"]
        resident_high_wage = metrics["od_high_wage_resident_workers"]

        records.append(
            {
                "fips_code": fips_code,
                "od_year": int(od_year),
                **metrics,
                "od_net_commuter_flow": metrics["od_inbound_workers"]
                - metrics["od_outbound_workers"],
                "od_local_capture_rate": (
                    metrics["od_live_work_same_county"] / resident if resident > 0 else np.nan
                ),
                "od_working_age_share": (
                    resident_working_age / resident if resident > 0 else np.nan
                ),
                "od_working_age_local_capture_rate": (
                    metrics["od_working_age_live_work_same_county"] / resident_working_age
                    if resident_working_age > 0
                    else np.nan
                ),
                "od_high_wage_share": (resident_high_wage / resident if resident > 0 else np.nan),
                "od_high_wage_local_capture_rate": (
                    metrics["od_high_wage_live_work_same_county"] / resident_high_wage
                    if resident_high_wage > 0
                    else np.nan
                ),
            }
        )

    od_df = pd.DataFrame(records).sort_values("fips_code").reset_index(drop=True)
    logger.info(
        "Loaded LODES OD county metrics",
        extra={
            "source_path": str(source_path),
            "od_year": od_year,
            "rows_total": total_rows,
            "rows_matched_year": year_rows,
            "counties": len(od_df),
        },
    )
    return od_df


def download_lodes_wac_segments(year: int) -> pd.DataFrame:
    """
    Download LODES Workplace Area Characteristics with wage segments.

    Wage segments require downloading 4 separate files and merging:
    - S000: Total jobs + sectors
    - SE01: Low wage jobs (<$1250/month = <$15k/year)
    - SE02: Mid wage jobs ($1251-$3333/month = $15k-$40k/year)
    - SE03: High wage jobs (>$3333/month = >$40k/year)

    Args:
        year: LODES year (typically 2 years behind current)

    Returns:
        DataFrame with jobs by tract and wage segment
    """
    cache_path = LODES_CACHE_DIR / f"md_wac_segments_{year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached LODES WAC: {cache_path}")
        df = pd.read_csv(cache_path, dtype={"w_geocode": str, "tract_geoid": str, "fips_code": str})
        # Ensure proper string formatting
        if "tract_geoid" in df.columns:
            df["tract_geoid"] = df["tract_geoid"].astype(str)
            df["fips_code"] = df["fips_code"].astype(str)
        df["source_url"] = "https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/"
        df["fetch_date"] = datetime.utcnow().date().isoformat()
        df["is_real"] = True
        return df

    logger.info(f"Downloading LODES WAC with wage segments for {year}...")

    try:
        # Download S000 (total jobs and sectors)
        url_s000 = (
            f"https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_S000_JT00_{year}.csv.gz"
        )
        logger.info(f"  Downloading S000 (all jobs)...")
        df = pd.read_csv(url_s000, compression="gzip", dtype={"w_geocode": str})

        # Keep relevant columns from S000
        columns_to_keep = [
            "w_geocode",
            "C000",
            "CNS01",
            "CNS02",
            "CNS03",
            "CNS04",
            "CNS05",
            "CNS06",
            "CNS07",
            "CNS08",
            "CNS09",
            "CNS10",
            "CNS11",
            "CNS12",
            "CNS13",
            "CNS14",
            "CNS15",
            "CNS16",
            "CNS17",
            "CNS18",
            "CNS19",
            "CNS20",
        ]
        available_cols = [c for c in columns_to_keep if c in df.columns]
        df = df[available_cols].copy()

        # Download and merge wage segments
        for seg_code, seg_name in [("SE01", "low"), ("SE02", "mid"), ("SE03", "high")]:
            url_seg = f"https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_{seg_code}_JT00_{year}.csv.gz"
            logger.info(f"  Downloading {seg_code} ({seg_name} wage)...")
            df_seg = pd.read_csv(url_seg, compression="gzip", dtype={"w_geocode": str})

            # Keep only w_geocode and C000 (job count for this segment)
            df_seg = df_seg[["w_geocode", "C000"]].copy()
            df_seg = df_seg.rename(columns={"C000": seg_code})

            # Merge with main dataframe
            df = df.merge(df_seg, on="w_geocode", how="left")

        # Fill NaN values with 0
        df["SE01"] = df["SE01"].fillna(0).astype(int)
        df["SE02"] = df["SE02"].fillna(0).astype(int)
        df["SE03"] = df["SE03"].fillna(0).astype(int)

        # Extract tract GEOID and ensure string formatting
        df["w_geocode"] = df["w_geocode"].astype(str).str.zfill(15)  # Block codes are 15 digits
        df["tract_geoid"] = df["w_geocode"].str[:11]
        df["fips_code"] = df["w_geocode"].str[:5]

        # Filter to Maryland (state FIPS 24)
        df = df[df["fips_code"].str.startswith("24")]

        df["source_url"] = (
            f"https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/"
            f"md_wac_S000_JT00_{year}.csv.gz; "
            f"https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_SE01_JT00_{year}.csv.gz; "
            f"https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_SE02_JT00_{year}.csv.gz; "
            f"https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_SE03_JT00_{year}.csv.gz"
        )
        df["fetch_date"] = datetime.utcnow().date().isoformat()
        df["is_real"] = True

        # Cache
        df.to_csv(cache_path, index=False)

        logger.info(f"✓ Downloaded LODES WAC: {len(df)} blocks, {df['C000'].sum():,} total jobs")
        logger.info(
            f"   Low wage: {df['SE01'].sum():,}, Mid wage: {df['SE02'].sum():,}, High wage: {df['SE03'].sum():,}"
        )
        return df

    except Exception as e:
        logger.error(f"Failed to download LODES WAC: {e}")
        raise


def download_lodes_rac(year: int) -> pd.DataFrame:
    """
    Download LODES Residence Area Characteristics.

    RAC shows where workers live, complementing WAC which shows where they work.

    Args:
        year: LODES year

    Returns:
        DataFrame with worker residence by tract
    """
    cache_path = LODES_CACHE_DIR / f"md_rac_{year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached LODES RAC: {cache_path}")
        df = pd.read_csv(cache_path, dtype={"h_geocode": str})
        df["source_url"] = (
            f"https://lehd.ces.census.gov/data/lodes/LODES8/md/rac/md_rac_S000_JT00_{year}.csv.gz"
        )
        df["fetch_date"] = datetime.utcnow().date().isoformat()
        df["is_real"] = True
        return df

    url = f"https://lehd.ces.census.gov/data/lodes/LODES8/md/rac/md_rac_S000_JT00_{year}.csv.gz"

    logger.info(f"Downloading LODES RAC for {year}...")

    try:
        df = pd.read_csv(url, compression="gzip", dtype={"h_geocode": str})

        # Keep relevant columns
        columns_to_keep = ["h_geocode", "C000", "SE01", "SE02", "SE03"]
        available_cols = [c for c in columns_to_keep if c in df.columns]
        df = df[available_cols].copy()

        # Extract tract GEOID
        df["tract_geoid"] = df["h_geocode"].str[:11]
        df["fips_code"] = df["h_geocode"].str[:5]

        # Filter to Maryland
        df = df[df["fips_code"].str.startswith("24")]

        df["source_url"] = (
            f"https://lehd.ces.census.gov/data/lodes/LODES8/md/rac/md_rac_S000_JT00_{year}.csv.gz"
        )
        df["fetch_date"] = datetime.utcnow().date().isoformat()
        df["is_real"] = True

        # Cache
        df.to_csv(cache_path, index=False)

        logger.info(f"✓ Downloaded LODES RAC: {len(df)} blocks")
        return df

    except Exception as e:
        logger.error(f"Failed to download LODES RAC: {e}")
        raise


def aggregate_lodes_to_tract(wac_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate block-level LODES data to tract level.

    Args:
        wac_df: Block-level WAC DataFrame

    Returns:
        Tract-level aggregated DataFrame
    """
    # Ensure tract_geoid and fips_code are strings
    wac_df["tract_geoid"] = wac_df["tract_geoid"].astype(str)
    wac_df["fips_code"] = wac_df["fips_code"].astype(str)

    # Numeric columns to sum
    sum_cols = [
        "C000",
        "SE01",
        "SE02",
        "SE03",
        "CNS01",
        "CNS02",
        "CNS03",
        "CNS04",
        "CNS05",
        "CNS06",
        "CNS07",
        "CNS08",
        "CNS09",
        "CNS10",
        "CNS11",
        "CNS12",
        "CNS13",
        "CNS14",
        "CNS15",
        "CNS16",
        "CNS17",
        "CNS18",
        "CNS19",
        "CNS20",
    ]

    available_sum = [c for c in sum_cols if c in wac_df.columns]

    tract_agg = wac_df.groupby(["tract_geoid", "fips_code"])[available_sum].sum().reset_index()

    # Rename for clarity
    tract_agg = tract_agg.rename(
        columns={
            "C000": "total_jobs",
            "SE01": "low_wage_jobs",
            "SE02": "mid_wage_jobs",
            "SE03": "high_wage_jobs",
        }
    )

    logger.info(f"Aggregated to {len(tract_agg)} tracts")
    return tract_agg


def fetch_tract_centroids(year: int = 2020) -> gpd.GeoDataFrame:
    """
    Fetch Maryland census tract centroids and areas.

    Args:
        year: Census year

    Returns:
        GeoDataFrame with tract centroids
    """
    cache_path = CACHE_DIR / f"md_tract_centroids_{year}.csv"

    if cache_path.exists():
        logger.info("Using cached tract centroids")
        df = pd.read_csv(cache_path, dtype={"tract_geoid": str, "fips_code": str})
        return df

    try:
        import pygris

        tracts = pygris.tracts(state="MD", year=year, cb=True)
        tracts = tracts.to_crs("EPSG:4326")

        # Compute centroids
        tracts_proj = tracts.to_crs("EPSG:3857")
        tracts["centroid_lon"] = tracts_proj.geometry.centroid.to_crs("EPSG:4326").x
        tracts["centroid_lat"] = tracts_proj.geometry.centroid.to_crs("EPSG:4326").y
        tracts["area_sq_mi"] = tracts_proj.geometry.area / 2.59e6

        # Rename columns and ensure string types
        tracts["tract_geoid"] = tracts["GEOID"].astype(str).str.zfill(11)  # Ensure 11-digit format
        tracts["fips_code"] = (
            tracts["STATEFP"].astype(str) + tracts["COUNTYFP"].astype(str)
        ).str.zfill(5)

        # Filter to valid counties
        tracts = tracts[tracts["fips_code"].isin(MD_COUNTY_FIPS.keys())]

        # Keep essential columns
        result = tracts[
            ["tract_geoid", "fips_code", "centroid_lon", "centroid_lat", "area_sq_mi"]
        ].copy()

        # Ensure string types are preserved
        result["tract_geoid"] = result["tract_geoid"].astype(str)
        result["fips_code"] = result["fips_code"].astype(str)

        # Cache
        result.to_csv(cache_path, index=False)

        logger.info(f"✓ Loaded {len(result)} tract centroids")
        return result

    except Exception as e:
        logger.error(f"Failed to fetch tract centroids: {e}")
        raise


def fetch_acs_demographics(year: int) -> pd.DataFrame:
    """
    Fetch ACS demographic data for tracts (population, labor force, etc.)

    Args:
        year: ACS 5-year end year

    Returns:
        DataFrame with tract demographics
    """
    cache_path = ACS_CACHE_DIR / f"md_acs_demo_{year}.csv"

    if cache_path.exists():
        logger.info(f"Using cached ACS demographics: {cache_path}")
        return pd.read_csv(cache_path, dtype={"tract_geoid": str, "fips_code": str})

    try:
        from census import Census

        c = Census(settings.CENSUS_API_KEY)

        # ACS variables
        variables = (
            "B01003_001E",  # Total population
            "B01001_011E",
            "B01001_012E",
            "B01001_013E",
            "B01001_014E",  # Male 25-44
            "B01001_015E",
            "B01001_016E",
            "B01001_017E",  # Male 45-64
            "B01001_035E",
            "B01001_036E",
            "B01001_037E",
            "B01001_038E",  # Female 25-44
            "B01001_039E",
            "B01001_040E",
            "B01001_041E",  # Female 45-64
            "B23025_003E",  # In labor force
            "B23025_002E",  # Labor force total
        )

        data = c.acs5.state_county_tract(
            variables, state_fips="24", county_fips="*", tract="*", year=year
        )

        df = pd.DataFrame(data)
        df["tract_geoid"] = df["state"] + df["county"] + df["tract"]
        df["fips_code"] = df["state"] + df["county"]

        # Compute working age population (25-64)
        male_25_64_cols = [
            "B01001_011E",
            "B01001_012E",
            "B01001_013E",
            "B01001_014E",
            "B01001_015E",
            "B01001_016E",
            "B01001_017E",
        ]
        female_25_64_cols = [
            "B01001_035E",
            "B01001_036E",
            "B01001_037E",
            "B01001_038E",
            "B01001_039E",
            "B01001_040E",
            "B01001_041E",
        ]

        for col in male_25_64_cols + female_25_64_cols:
            if col not in df.columns:
                df[col] = 0

        df["working_age_pop"] = (
            df[male_25_64_cols].fillna(0).sum(axis=1) + df[female_25_64_cols].fillna(0).sum(axis=1)
        ).astype(int)

        # Labor force participation
        df["labor_force"] = pd.to_numeric(df.get("B23025_003E", 0), errors="coerce").fillna(0)
        df["labor_force_total"] = pd.to_numeric(df.get("B23025_002E", 0), errors="coerce").fillna(0)
        df["labor_force_participation"] = np.where(
            df["labor_force_total"] > 0, df["labor_force"] / df["labor_force_total"], 0
        )

        df = df.rename(columns={"B01003_001E": "population"})
        df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)

        result = df[
            [
                "tract_geoid",
                "fips_code",
                "population",
                "working_age_pop",
                "labor_force_participation",
            ]
        ].copy()

        # Cache
        result.to_csv(cache_path, index=False)

        logger.info(f"✓ Loaded ACS demographics for {len(result)} tracts")
        return result

    except Exception as e:
        logger.error(f"Failed to fetch ACS demographics: {e}")
        # Return empty with expected columns
        return pd.DataFrame(
            columns=[
                "tract_geoid",
                "fips_code",
                "population",
                "working_age_pop",
                "labor_force_participation",
            ]
        )


# =============================================================================
# ACCESSIBILITY COMPUTATION
# =============================================================================


def _resolve_accessibility_thresholds(
    threshold_30_min: Optional[int] = None,
    threshold_45_min: Optional[int] = None,
    proxy_distance_30_km: Optional[float] = None,
    proxy_distance_45_km: Optional[float] = None,
) -> Dict[str, float]:
    t30_min = int(threshold_30_min or settings.LAYER1_THRESHOLD_30_MINUTES)
    t45_min = int(threshold_45_min or settings.LAYER1_THRESHOLD_45_MINUTES)
    d30_km = float(
        proxy_distance_30_km
        if proxy_distance_30_km is not None
        else settings.LAYER1_PROXY_DISTANCE_30_KM
    )
    d45_km = float(
        proxy_distance_45_km
        if proxy_distance_45_km is not None
        else settings.LAYER1_PROXY_DISTANCE_45_KM
    )

    if t30_min <= 0 or t45_min <= 0:
        raise ValueError("Accessibility minute thresholds must be positive")
    if d30_km <= 0 or d45_km <= 0:
        raise ValueError("Accessibility proxy distance thresholds must be positive")
    if t30_min > t45_min:
        raise ValueError("30-minute threshold cannot exceed 45-minute threshold")
    if d30_km > d45_km:
        raise ValueError("30-minute proxy distance cannot exceed 45-minute proxy distance")

    return {
        "threshold_30_min": t30_min,
        "threshold_45_min": t45_min,
        "proxy_distance_30_km": d30_km,
        "proxy_distance_45_km": d45_km,
    }


def _check_r5py_available() -> bool:
    try:
        import r5py  # noqa: F401

        return True
    except (ImportError, Exception) as exc:
        if "java" in str(exc).lower():
            logger.warning("Layer 1 network mode requires Java 11+ for r5py")
        return False


def _load_network_inputs():
    from src.ingest.layer2_accessibility import download_gtfs_feeds, download_maryland_osm

    osm_path = download_maryland_osm()
    gtfs_feeds = download_gtfs_feeds()
    if not gtfs_feeds:
        raise RuntimeError("No GTFS feeds available for Layer 1 network accessibility")
    return osm_path, gtfs_feeds


def _default_departure_time() -> datetime:
    return datetime(
        2026,
        1,
        27,  # Tuesday
        settings.LAYER1_NETWORK_DEPARTURE_HOUR,
        settings.LAYER1_NETWORK_DEPARTURE_MINUTE,
    )


def _compute_accessibility_network(
    df: pd.DataFrame,
    threshold_30_min: int,
    threshold_45_min: int,
) -> pd.DataFrame:
    import r5py

    logger.info("Computing Layer 1 accessibility with network OD matrix (drive+transit)...")
    osm_path, gtfs_feeds = _load_network_inputs()
    network = r5py.TransportNetwork(
        osm_pbf=str(osm_path),
        gtfs=[str(feed.path) for feed in gtfs_feeds],
    )

    origins = gpd.GeoDataFrame(
        df[["tract_geoid", "fips_code"]].copy(),
        geometry=gpd.points_from_xy(df["centroid_lon"], df["centroid_lat"]),
        crs="EPSG:4326",
    ).rename(columns={"tract_geoid": "id"})
    destinations = origins[["id", "geometry"]].copy()
    departure_time = _default_departure_time()

    max_minutes = max(threshold_30_min, threshold_45_min)
    drive = r5py.TravelTimeMatrixComputer(
        network,
        origins=origins,
        destinations=destinations,
        departure=departure_time,
        transport_modes=[r5py.TransportMode.CAR],
        max_time=timedelta(minutes=max_minutes),
    ).compute_travel_times()

    transit = r5py.TravelTimeMatrixComputer(
        network,
        origins=origins,
        destinations=destinations,
        departure=departure_time,
        departure_time_window=timedelta(hours=1),
        transport_modes=[r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK],
        max_time=timedelta(minutes=max_minutes),
    ).compute_travel_times()

    drive_df = drive[["from_id", "to_id", "travel_time"]].rename(
        columns={"travel_time": "travel_time_drive"}
    )
    transit_df = transit[["from_id", "to_id", "travel_time"]].rename(
        columns={"travel_time": "travel_time_transit"}
    )
    drive_df["from_id"] = drive_df["from_id"].astype(str)
    drive_df["to_id"] = drive_df["to_id"].astype(str)
    transit_df["from_id"] = transit_df["from_id"].astype(str)
    transit_df["to_id"] = transit_df["to_id"].astype(str)

    od = drive_df.merge(transit_df, on=["from_id", "to_id"], how="outer")
    od["travel_time"] = od[["travel_time_drive", "travel_time_transit"]].min(axis=1, skipna=True)
    od = od.drop(columns=["travel_time_drive", "travel_time_transit"])

    jobs_lookup = (
        df[["tract_geoid", "high_wage_jobs", "total_jobs"]]
        .rename(columns={"tract_geoid": "to_id"})
        .copy()
    )
    jobs_lookup["to_id"] = jobs_lookup["to_id"].astype(str)
    od = od.merge(jobs_lookup, on="to_id", how="left")
    od["high_wage_jobs"] = od["high_wage_jobs"].fillna(0)
    od["total_jobs"] = od["total_jobs"].fillna(0)

    within_30 = od[(od["travel_time"].notna()) & (od["travel_time"] <= threshold_30_min)]
    within_45 = od[(od["travel_time"].notna()) & (od["travel_time"] <= threshold_45_min)]

    agg_30 = (
        within_30.groupby("from_id")[["high_wage_jobs", "total_jobs"]]
        .sum()
        .rename(
            columns={
                "high_wage_jobs": "high_wage_jobs_accessible_30min",
                "total_jobs": "total_jobs_accessible_30min",
            }
        )
    )
    agg_45 = (
        within_45.groupby("from_id")[["high_wage_jobs", "total_jobs"]]
        .sum()
        .rename(
            columns={
                "high_wage_jobs": "high_wage_jobs_accessible_45min",
                "total_jobs": "total_jobs_accessible_45min",
            }
        )
    )

    enriched = df.merge(
        agg_30,
        left_on="tract_geoid",
        right_index=True,
        how="left",
    ).merge(
        agg_45,
        left_on="tract_geoid",
        right_index=True,
        how="left",
    )

    for col in [
        "high_wage_jobs_accessible_30min",
        "total_jobs_accessible_30min",
        "high_wage_jobs_accessible_45min",
        "total_jobs_accessible_45min",
    ]:
        enriched[col] = enriched[col].fillna(0).round().astype(int)

    enriched["accessibility_method"] = "network_od_drive_transit"
    enriched["accessibility_threshold_30_min"] = int(threshold_30_min)
    enriched["accessibility_threshold_45_min"] = int(threshold_45_min)
    enriched["accessibility_proxy_distance_30_km"] = np.nan
    enriched["accessibility_proxy_distance_45_km"] = np.nan
    return enriched


def _compute_accessibility_proxy(
    df: pd.DataFrame,
    distance_30_km: float,
    distance_45_km: float,
    threshold_30_min: int,
    threshold_45_min: int,
) -> pd.DataFrame:
    logger.info("Computing Layer 1 accessibility with haversine proxy model...")

    n_tracts = len(df)
    lons = df["centroid_lon"].values
    lats = df["centroid_lat"].values
    high_wage = df["high_wage_jobs"].values if "high_wage_jobs" in df else np.zeros(n_tracts)
    total = df["total_jobs"].values if "total_jobs" in df else np.zeros(n_tracts)

    high_wage_45 = np.zeros(n_tracts)
    high_wage_30 = np.zeros(n_tracts)
    total_45 = np.zeros(n_tracts)
    total_30 = np.zeros(n_tracts)

    def haversine_matrix(lon1, lat1, lon2, lat2):
        R = 6371
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        dlat = np.radians(lat2 - lat1)
        dlon = np.radians(lon2 - lon1)
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
        c = 2 * np.arcsin(np.sqrt(a))
        return R * c

    batch_size = 100
    logger.info(f"Computing proxy accessibility for {n_tracts} tracts...")
    for i in range(0, n_tracts, batch_size):
        batch_end = min(i + batch_size, n_tracts)
        for j in range(i, batch_end):
            distances = haversine_matrix(lons[j], lats[j], lons, lats)
            mask_30 = distances <= distance_30_km
            mask_45 = distances <= distance_45_km
            high_wage_30[j] = high_wage[mask_30].sum()
            total_30[j] = total[mask_30].sum()
            high_wage_45[j] = high_wage[mask_45].sum()
            total_45[j] = total[mask_45].sum()
        if (batch_end) % 500 == 0:
            logger.info(f"  Processed {batch_end}/{n_tracts} tracts")

    enriched = df.copy()
    enriched["high_wage_jobs_accessible_45min"] = high_wage_45.astype(int)
    enriched["high_wage_jobs_accessible_30min"] = high_wage_30.astype(int)
    enriched["total_jobs_accessible_45min"] = total_45.astype(int)
    enriched["total_jobs_accessible_30min"] = total_30.astype(int)
    enriched["accessibility_method"] = "haversine_proxy"
    enriched["accessibility_threshold_30_min"] = int(threshold_30_min)
    enriched["accessibility_threshold_45_min"] = int(threshold_45_min)
    enriched["accessibility_proxy_distance_30_km"] = float(distance_30_km)
    enriched["accessibility_proxy_distance_45_km"] = float(distance_45_km)
    return enriched


def compute_economic_accessibility(
    tract_jobs: pd.DataFrame,
    tract_centroids: pd.DataFrame,
    mode: Optional[str] = None,
    threshold_30_min: Optional[int] = None,
    threshold_45_min: Optional[int] = None,
    proxy_distance_30_km: Optional[float] = None,
    proxy_distance_45_km: Optional[float] = None,
) -> pd.DataFrame:
    """
    Compute Layer 1 accessibility metrics using network OD matrices when available,
    with deterministic proxy fallback.
    """
    thresholds = _resolve_accessibility_thresholds(
        threshold_30_min=threshold_30_min,
        threshold_45_min=threshold_45_min,
        proxy_distance_30_km=proxy_distance_30_km,
        proxy_distance_45_km=proxy_distance_45_km,
    )
    requested_mode = (mode or settings.LAYER1_ACCESSIBILITY_MODE or ACCESSIBILITY_MODE_AUTO).lower()
    if requested_mode not in {
        ACCESSIBILITY_MODE_AUTO,
        ACCESSIBILITY_MODE_NETWORK,
        ACCESSIBILITY_MODE_PROXY,
    }:
        raise ValueError(f"Unsupported Layer 1 accessibility mode: {requested_mode}")

    tract_centroids = tract_centroids.copy()
    tract_jobs = tract_jobs.copy()
    tract_centroids["tract_geoid"] = tract_centroids["tract_geoid"].astype(str)
    tract_centroids["fips_code"] = tract_centroids["fips_code"].astype(str)
    tract_jobs["tract_geoid"] = tract_jobs["tract_geoid"].astype(str)
    tract_jobs["fips_code"] = tract_jobs["fips_code"].astype(str)

    df = tract_centroids.merge(tract_jobs, on=["tract_geoid", "fips_code"], how="inner").fillna(0)
    logger.info(f"Merged {len(df)} tracts with job and location data")

    if requested_mode in {ACCESSIBILITY_MODE_AUTO, ACCESSIBILITY_MODE_NETWORK}:
        if _check_r5py_available():
            try:
                df = _compute_accessibility_network(
                    df,
                    threshold_30_min=int(thresholds["threshold_30_min"]),
                    threshold_45_min=int(thresholds["threshold_45_min"]),
                )
            except Exception:
                if requested_mode == ACCESSIBILITY_MODE_NETWORK:
                    raise
                logger.warning(
                    "Layer 1 network OD computation failed; falling back to haversine proxy",
                    exc_info=True,
                )
                df = _compute_accessibility_proxy(
                    df,
                    distance_30_km=float(thresholds["proxy_distance_30_km"]),
                    distance_45_km=float(thresholds["proxy_distance_45_km"]),
                    threshold_30_min=int(thresholds["threshold_30_min"]),
                    threshold_45_min=int(thresholds["threshold_45_min"]),
                )
        else:
            if requested_mode == ACCESSIBILITY_MODE_NETWORK:
                raise RuntimeError("Layer 1 network mode requested but r5py/Java is unavailable")
            logger.info("Layer 1 network mode unavailable; using haversine proxy")
            df = _compute_accessibility_proxy(
                df,
                distance_30_km=float(thresholds["proxy_distance_30_km"]),
                distance_45_km=float(thresholds["proxy_distance_45_km"]),
                threshold_30_min=int(thresholds["threshold_30_min"]),
                threshold_45_min=int(thresholds["threshold_45_min"]),
            )
    else:
        df = _compute_accessibility_proxy(
            df,
            distance_30_km=float(thresholds["proxy_distance_30_km"]),
            distance_45_km=float(thresholds["proxy_distance_45_km"]),
            threshold_30_min=int(thresholds["threshold_30_min"]),
            threshold_45_min=int(thresholds["threshold_45_min"]),
        )

    regional_high_wage = df["high_wage_jobs"].sum()
    regional_total = df["total_jobs"].sum()
    df["pct_regional_high_wage_accessible"] = np.where(
        regional_high_wage > 0, df["high_wage_jobs_accessible_45min"] / regional_high_wage, 0
    )
    df["pct_regional_jobs_accessible"] = np.where(
        regional_total > 0, df["total_jobs_accessible_45min"] / regional_total, 0
    )
    df["wage_quality_ratio"] = np.where(
        df["total_jobs_accessible_45min"] > 0,
        df["high_wage_jobs_accessible_45min"] / df["total_jobs_accessible_45min"],
        0,
    )
    logger.info("✓ Economic accessibility computed")
    return df


def compute_sector_diversity(tract_jobs: pd.DataFrame) -> pd.DataFrame:
    """
    Compute sector diversity metrics for each tract.

    Args:
        tract_jobs: DataFrame with sector-level job counts

    Returns:
        DataFrame with diversity metrics added
    """
    sector_cols = [f"CNS{i:02d}" for i in range(1, 21)]
    available_sectors = [c for c in sector_cols if c in tract_jobs.columns]
    stable_sector_cols = [c for c in ["CNS15", "CNS16", "CNS20"] if c in tract_jobs.columns]

    if not available_sectors:
        tract_jobs["sector_diversity_entropy"] = 0
        tract_jobs["high_wage_sector_concentration"] = 0
        tract_jobs["stable_sector_jobs"] = 0
        tract_jobs["stable_sector_share"] = 0.0
        return tract_jobs

    def shannon_entropy(row):
        jobs = row[available_sectors].values.astype(float)
        total = jobs.sum()
        if total == 0:
            return 0
        probs = jobs / total
        probs = probs[probs > 0]  # Avoid log(0)
        return -np.sum(probs * np.log2(probs))

    def hhi_concentration(row):
        # HHI for high-wage sectors only
        hw_sectors = [c for c in HIGH_WAGE_SECTORS if c in available_sectors]
        if not hw_sectors:
            return 0
        jobs = row[hw_sectors].values.astype(float)
        total = jobs.sum()
        if total == 0:
            return 0
        shares = jobs / total
        return np.sum(shares**2)

    tract_jobs["sector_diversity_entropy"] = tract_jobs.apply(shannon_entropy, axis=1)
    tract_jobs["high_wage_sector_concentration"] = tract_jobs.apply(hhi_concentration, axis=1)
    if stable_sector_cols:
        tract_jobs["stable_sector_jobs"] = tract_jobs[stable_sector_cols].fillna(0).sum(axis=1)
    else:
        tract_jobs["stable_sector_jobs"] = 0
    tract_jobs["stable_sector_share"] = np.where(
        tract_jobs["total_jobs"] > 0,
        tract_jobs["stable_sector_jobs"] / tract_jobs["total_jobs"],
        0.0,
    )

    return tract_jobs


def normalize_accessibility_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize accessibility metrics to 0-1 scores.

    Uses percentile ranking within Maryland for comparability.

    Args:
        df: DataFrame with raw accessibility counts

    Returns:
        DataFrame with normalized scores
    """
    # Economic accessibility score (primary metric)
    # Based on high-wage jobs accessible within 45 min
    df["economic_accessibility_score"] = df["high_wage_jobs_accessible_45min"].rank(pct=True)

    # Job market reach score (total jobs accessible)
    df["job_market_reach_score"] = df["total_jobs_accessible_45min"].rank(pct=True)

    # Job quality index (weighted by wage quality ratio)
    df["job_quality_index"] = 0.7 * df["economic_accessibility_score"] + 0.3 * df[
        "wage_quality_ratio"
    ].rank(pct=True)

    # Upward mobility score (composite)
    # For now, use economic accessibility as proxy
    # Can be enhanced with Opportunity Insights data
    df["upward_mobility_score"] = df["economic_accessibility_score"]

    return df


# =============================================================================
# AGGREGATION
# =============================================================================


def aggregate_to_county(tract_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tract-level metrics to county level.

    Uses population-weighted statistics for primary accessibility metrics.
    Max-access fields are retained as frontier diagnostics.

    Args:
        tract_df: DataFrame with tract-level metrics

    Returns:
        DataFrame with county-level metrics
    """
    tract_df = tract_df.copy()

    # Prepare population weights
    tract_df["population"] = tract_df["population"].fillna(0)
    tract_df["pop_weight"] = tract_df.groupby("fips_code")["population"].transform(
        lambda x: x / x.sum() if x.sum() > 0 else 1 / len(x)
    )

    # Weighted averages for scores
    score_cols = [
        "economic_accessibility_score",
        "job_market_reach_score",
        "wage_quality_ratio",
        "job_quality_index",
        "upward_mobility_score",
        "sector_diversity_entropy",
        "labor_force_participation",
    ]

    weighted_cols = {}
    for col in score_cols:
        if col in tract_df.columns:
            tract_df[f"{col}_weighted"] = tract_df[col] * tract_df["pop_weight"]
            weighted_cols[f"{col}_weighted"] = (f"{col}_weighted", "sum")

    # Weighted mean accessibility counts used as primary county accessibility anchors
    accessibility_count_cols = [
        "high_wage_jobs_accessible_45min",
        "high_wage_jobs_accessible_30min",
        "total_jobs_accessible_45min",
        "total_jobs_accessible_30min",
    ]
    for col in accessibility_count_cols:
        if col in tract_df.columns:
            tract_df[f"{col}_weighted"] = tract_df[col] * tract_df["pop_weight"]
            weighted_cols[f"{col}_weighted_mean"] = (f"{col}_weighted", "sum")

    # Aggregation spec
    agg_spec = {
        # Sum job counts
        "total_jobs": ("total_jobs", "sum"),
        "high_wage_jobs": ("high_wage_jobs", "sum"),
        "mid_wage_jobs": ("mid_wage_jobs", "sum"),
        "low_wage_jobs": ("low_wage_jobs", "sum"),
        # Frontier diagnostics (best-tract accessibility in county)
        "high_wage_jobs_accessible_45min": ("high_wage_jobs_accessible_45min", "max"),
        "high_wage_jobs_accessible_30min": ("high_wage_jobs_accessible_30min", "max"),
        "total_jobs_accessible_45min": ("total_jobs_accessible_45min", "max"),
        "total_jobs_accessible_30min": ("total_jobs_accessible_30min", "max"),
        # Sum population
        "population": ("population", "sum"),
        "working_age_pop": ("working_age_pop", "sum"),
        # Count tracts
        "tract_count": ("tract_geoid", "count"),
        # Area
        "area_sq_mi": ("area_sq_mi", "sum"),
    }
    if "stable_sector_jobs" in tract_df.columns:
        agg_spec["stable_sector_jobs"] = ("stable_sector_jobs", "sum")

    # Add weighted columns to spec
    for new_col, (src_col, func) in weighted_cols.items():
        if src_col in tract_df.columns:
            agg_spec[new_col] = (src_col, func)

    county_agg = tract_df.groupby("fips_code").agg(**agg_spec).reset_index()

    # Rename weighted columns back
    for col in score_cols:
        weighted_name = f"{col}_weighted"
        if weighted_name in county_agg.columns:
            county_agg[col] = county_agg[weighted_name]
            county_agg = county_agg.drop(columns=[weighted_name])

    def _weighted_median(values: np.ndarray, weights: np.ndarray) -> float:
        mask = np.isfinite(values) & np.isfinite(weights) & (weights >= 0)
        values = values[mask]
        weights = weights[mask]
        if len(values) == 0:
            return np.nan
        if np.sum(weights) <= 0:
            return float(np.nanmedian(values))
        order = np.argsort(values)
        values = values[order]
        weights = weights[order]
        cdf = np.cumsum(weights) / np.sum(weights)
        idx = int(np.searchsorted(cdf, 0.5, side="left"))
        return float(values[min(idx, len(values) - 1)])

    # Add weighted median accessibility counts for county summaries.
    grouped = tract_df.groupby("fips_code")
    for col in accessibility_count_cols:
        if col not in tract_df.columns:
            continue
        median_series = grouped.apply(
            lambda g: _weighted_median(
                g[col].to_numpy(dtype=float),
                g["population"].to_numpy(dtype=float),
            )
        )
        county_agg = county_agg.merge(
            median_series.rename(f"{col}_weighted_median"),
            on="fips_code",
            how="left",
        )

    # Primary county accessibility scores use weighted mean accessibility counts.
    hw_mean_col = "high_wage_jobs_accessible_45min_weighted_mean"
    total_mean_col = "total_jobs_accessible_45min_weighted_mean"
    if hw_mean_col in county_agg.columns and total_mean_col in county_agg.columns:
        county_agg["economic_accessibility_score"] = county_agg[hw_mean_col].rank(pct=True)
        county_agg["job_market_reach_score"] = county_agg[total_mean_col].rank(pct=True)
        county_agg["wage_quality_ratio"] = np.where(
            county_agg[total_mean_col] > 0,
            county_agg[hw_mean_col] / county_agg[total_mean_col],
            0,
        )
        county_agg["job_quality_index"] = 0.7 * county_agg["economic_accessibility_score"] + 0.3 * (
            county_agg["wage_quality_ratio"].rank(pct=True)
        )
        county_agg["upward_mobility_score"] = county_agg["economic_accessibility_score"]

    if "stable_sector_jobs" in county_agg.columns:
        county_agg["stable_sector_share"] = np.where(
            county_agg["total_jobs"] > 0,
            county_agg["stable_sector_jobs"] / county_agg["total_jobs"],
            np.nan,
        )

    # Compute regional percentages
    regional_high_wage = county_agg["high_wage_jobs"].sum()
    regional_total = county_agg["total_jobs"].sum()

    county_agg["pct_regional_high_wage_accessible"] = np.where(
        regional_high_wage > 0, county_agg["high_wage_jobs"] / regional_high_wage, 0
    )

    county_agg["pct_regional_jobs_accessible"] = np.where(
        regional_total > 0, county_agg["total_jobs"] / regional_total, 0
    )

    # Entrepreneurship density (establishments per 1000 pop)
    # This would need BLS QCEW data - set to None for now
    county_agg["entrepreneurship_density"] = None

    # Accessibility provenance and thresholds.
    for numeric_col in [
        "accessibility_threshold_30_min",
        "accessibility_threshold_45_min",
        "accessibility_proxy_distance_30_km",
        "accessibility_proxy_distance_45_km",
    ]:
        if numeric_col in tract_df.columns:
            series = tract_df.groupby("fips_code")[numeric_col].median().rename(numeric_col)
            county_agg = county_agg.merge(series, on="fips_code", how="left")
    if "accessibility_method" in tract_df.columns:
        method_series = tract_df.groupby("fips_code")["accessibility_method"].agg(
            lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0]
        )
        county_agg = county_agg.merge(
            method_series.rename("accessibility_method"),
            on="fips_code",
            how="left",
        )

    return county_agg


# =============================================================================
# DATABASE STORAGE
# =============================================================================


def store_tract_economic_opportunity(
    df: pd.DataFrame, data_year: int, lodes_year: int, acs_year: int
):
    """
    Store tract-level economic opportunity data in database.

    Args:
        df: DataFrame with tract metrics
        data_year: Year to associate with this data
        lodes_year: Year of LODES data used
        acs_year: Year of ACS data used
    """
    logger.info(f"Storing {len(df)} tract economic opportunity records")

    with get_db() as db:
        # Clear existing data for this year
        db.execute(
            text(
                f"""
            DELETE FROM {L1_TRACT_TABLE}
            WHERE data_year = :data_year
        """
            ),
            {"data_year": data_year},
        )

        # Insert new records
        for _, row in df.iterrows():
            econ_score = row.get("economic_accessibility_score", None)
            if econ_score is not None and pd.isna(econ_score):
                econ_score = None
            elif econ_score is not None:
                econ_score = float(econ_score)

            db.execute(
                text(
                    f"""
                INSERT INTO {L1_TRACT_TABLE} (
                    tract_geoid, fips_code, data_year,
                    total_jobs, high_wage_jobs, mid_wage_jobs, low_wage_jobs,
                    high_wage_jobs_accessible_45min, high_wage_jobs_accessible_30min,
                    total_jobs_accessible_45min, total_jobs_accessible_30min,
                    economic_accessibility_score, job_market_reach_score, wage_quality_ratio,
                    pct_regional_high_wage_accessible, pct_regional_jobs_accessible,
                    sector_diversity_entropy, high_wage_sector_concentration,
                    upward_mobility_score, job_quality_index,
                    tract_population, tract_working_age_pop, labor_force_participation,
                    lodes_year, acs_year
                ) VALUES (
                    :tract_geoid, :fips_code, :data_year,
                    :total_jobs, :high_wage_jobs, :mid_wage_jobs, :low_wage_jobs,
                    :high_wage_45, :high_wage_30, :total_45, :total_30,
                    :econ_score, :market_score, :wage_ratio,
                    :pct_hw, :pct_total,
                    :entropy, :concentration,
                    :mobility, :quality,
                    :population, :working_age, :lfp,
                    :lodes_year, :acs_year
                )
            """
                ),
                {
                    "tract_geoid": row["tract_geoid"],
                    "fips_code": row["fips_code"],
                    "data_year": data_year,
                    "total_jobs": int(row.get("total_jobs", 0)),
                    "high_wage_jobs": int(row.get("high_wage_jobs", 0)),
                    "mid_wage_jobs": int(row.get("mid_wage_jobs", 0)),
                    "low_wage_jobs": int(row.get("low_wage_jobs", 0)),
                    "high_wage_45": int(row.get("high_wage_jobs_accessible_45min", 0)),
                    "high_wage_30": int(row.get("high_wage_jobs_accessible_30min", 0)),
                    "total_45": int(row.get("total_jobs_accessible_45min", 0)),
                    "total_30": int(row.get("total_jobs_accessible_30min", 0)),
                    "econ_score": econ_score,
                    "market_score": float(row.get("job_market_reach_score", 0)),
                    "wage_ratio": float(row.get("wage_quality_ratio", 0)),
                    "pct_hw": float(row.get("pct_regional_high_wage_accessible", 0)),
                    "pct_total": float(row.get("pct_regional_jobs_accessible", 0)),
                    "entropy": float(row.get("sector_diversity_entropy", 0)),
                    "concentration": float(row.get("high_wage_sector_concentration", 0)),
                    "mobility": float(row.get("upward_mobility_score", 0)),
                    "quality": float(row.get("job_quality_index", 0)),
                    "population": int(row.get("population", 0)),
                    "working_age": int(row.get("working_age_pop", 0)),
                    "lfp": float(row.get("labor_force_participation", 0)),
                    "lodes_year": lodes_year,
                    "acs_year": acs_year,
                },
            )

        db.commit()

    logger.info("✓ Tract economic opportunity data stored")


def store_county_economic_opportunity(
    df: pd.DataFrame, data_year: int, lodes_year: int, acs_year: int
):
    """
    Store county-level economic opportunity data.

    Updates the existing layer1_employment_gravity table with new accessibility metrics.

    Args:
        df: DataFrame with county metrics
        data_year: Year for this data
        lodes_year: Year of LODES data
        acs_year: Year of ACS data
    """
    logger.info(f"Updating {len(df)} county economic opportunity records")
    use_databricks_backend = (settings.DATA_BACKEND or "").strip().lower() == "databricks"

    with get_db() as db:
        local_strength_scores = {}
        local_result = db.execute(
            text(
                f"""
            SELECT fips_code,
                   employment_diversification_score,
                   sector_diversity_entropy,
                   stable_sector_share
            FROM {L1_COUNTY_TABLE}
            WHERE data_year = :data_year
        """
            ),
            {"data_year": data_year},
        )
        for db_row in local_result.fetchall():
            fips_code, v1_score, entropy, stable_share = db_row
            local_strength = None

            if entropy is not None and stable_share is not None:
                try:
                    entropy_score = float(entropy) / np.log2(20)
                    local_strength = 0.7 * entropy_score + 0.3 * float(stable_share)
                except (TypeError, ValueError):
                    local_strength = None

            if local_strength is None and v1_score is not None:
                try:
                    local_strength = float(v1_score)
                except (TypeError, ValueError):
                    local_strength = None

            if local_strength is not None:
                local_strength = float(max(0.0, min(1.0, float(local_strength))))

            local_strength_scores[fips_code] = local_strength

        for _, row in df.iterrows():
            if use_databricks_backend:
                db.execute(
                    text(
                        f"""
                        INSERT INTO {L1_COUNTY_TABLE} (fips_code, data_year)
                        SELECT :fips_code, :data_year
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {L1_COUNTY_TABLE}
                            WHERE fips_code = :fips_code AND data_year = :data_year
                        )
                        """
                    ),
                    {
                        "fips_code": row["fips_code"],
                        "data_year": data_year,
                    },
                )
            else:
                db.execute(
                    text(
                        f"""
                        INSERT INTO {L1_COUNTY_TABLE} (fips_code, data_year)
                        VALUES (:fips_code, :data_year)
                        ON CONFLICT (fips_code, data_year) DO NOTHING
                        """
                    ),
                    {
                        "fips_code": row["fips_code"],
                        "data_year": data_year,
                    },
                )

            local_strength = None
            row_entropy = row.get("sector_diversity_entropy")
            row_stable_share = row.get("stable_sector_share")
            if pd.notna(row_entropy) and pd.notna(row_stable_share):
                try:
                    entropy_score = float(row_entropy) / np.log2(20)
                    local_strength = 0.7 * entropy_score + 0.3 * float(row_stable_share)
                except (TypeError, ValueError):
                    local_strength = None

            if local_strength is None:
                local_strength = local_strength_scores.get(row["fips_code"])
                if local_strength is not None and pd.isna(local_strength):
                    local_strength = None
                elif local_strength is not None:
                    local_strength = float(local_strength)

            if local_strength is not None:
                local_strength = float(max(0.0, min(1.0, float(local_strength))))

            econ_score = row.get("economic_accessibility_score", None)
            if econ_score is not None and pd.isna(econ_score):
                econ_score = None
            elif econ_score is not None:
                econ_score = float(econ_score)

            qwi_score = row.get("qwi_net_job_growth_score", None)
            if qwi_score is not None and pd.isna(qwi_score):
                qwi_score = None
            elif qwi_score is not None:
                qwi_score = float(qwi_score)

            if local_strength is None and econ_score is None:
                base_index = None
            elif local_strength is None:
                base_index = econ_score
            elif econ_score is None:
                base_index = local_strength
            else:
                base_index = (
                    LOCAL_STRENGTH_WEIGHT * local_strength + REGIONAL_ACCESS_WEIGHT * econ_score
                )

            if qwi_score is None:
                opportunity_index = base_index
            elif base_index is None:
                opportunity_index = qwi_score
            else:
                opportunity_index = (
                    1 - QWI_BLEND_WEIGHT
                ) * base_index + QWI_BLEND_WEIGHT * qwi_score
            if opportunity_index is not None and pd.notna(opportunity_index):
                opportunity_index = float(opportunity_index)

            # Update existing records with new accessibility columns
            db.execute(
                text(
                    f"""
                UPDATE {L1_COUNTY_TABLE}
                SET
                    total_jobs = :total_jobs,
                    high_wage_jobs = :high_wage_jobs,
                    mid_wage_jobs = :mid_wage_jobs,
                    low_wage_jobs = :low_wage_jobs,
                    high_wage_jobs_accessible_45min = :high_wage_45,
                    high_wage_jobs_accessible_30min = :high_wage_30,
                    total_jobs_accessible_45min = :total_45,
                    total_jobs_accessible_30min = :total_30,
                    high_wage_jobs_accessible_45min_weighted_mean = :high_wage_45_weighted_mean,
                    high_wage_jobs_accessible_45min_weighted_median = :high_wage_45_weighted_median,
                    total_jobs_accessible_45min_weighted_mean = :total_45_weighted_mean,
                    total_jobs_accessible_45min_weighted_median = :total_45_weighted_median,
                    high_wage_jobs_accessible_30min_weighted_mean = :high_wage_30_weighted_mean,
                    high_wage_jobs_accessible_30min_weighted_median = :high_wage_30_weighted_median,
                    total_jobs_accessible_30min_weighted_mean = :total_30_weighted_mean,
                    total_jobs_accessible_30min_weighted_median = :total_30_weighted_median,
                    economic_accessibility_score = :econ_score,
                    job_market_reach_score = :market_score,
                    wage_quality_ratio = :wage_ratio,
                    pct_regional_high_wage_accessible = :pct_hw,
                    pct_regional_jobs_accessible = :pct_total,
                    sector_diversity_entropy = :sector_diversity_entropy,
                    stable_sector_share = :stable_sector_share,
                    high_wage_sector_concentration = :concentration,
                    upward_mobility_score = :mobility,
                    job_quality_index = :quality,
                    entrepreneurship_density = :entrepreneurship_density,
                    qwi_emp_total = :qwi_emp_total,
                    qwi_hires = :qwi_hires,
                    qwi_separations = :qwi_separations,
                    qwi_hire_rate = :qwi_hire_rate,
                    qwi_separation_rate = :qwi_separation_rate,
                    qwi_turnover_rate = :qwi_turnover_rate,
                    qwi_net_job_growth_rate = :qwi_net_job_growth_rate,
                    qwi_year = :qwi_year,
                    od_year = :od_year,
                    od_resident_workers = :od_resident_workers,
                    od_inbound_workers = :od_inbound_workers,
                    od_outbound_workers = :od_outbound_workers,
                    od_live_work_same_county = :od_live_work_same_county,
                    od_net_commuter_flow = :od_net_commuter_flow,
                    od_local_capture_rate = :od_local_capture_rate,
                    od_working_age_resident_workers = :od_working_age_resident_workers,
                    od_working_age_live_work_same_county = :od_working_age_live_work_same_county,
                    od_working_age_share = :od_working_age_share,
                    od_working_age_local_capture_rate = :od_working_age_local_capture_rate,
                    od_high_wage_resident_workers = :od_high_wage_resident_workers,
                    od_high_wage_live_work_same_county = :od_high_wage_live_work_same_county,
                    od_high_wage_share = :od_high_wage_share,
                    od_high_wage_local_capture_rate = :od_high_wage_local_capture_rate,
                    employment_diversification_score = COALESCE(:local_strength, employment_diversification_score),
                    economic_opportunity_index = :opportunity_index,
                    working_age_pop = :working_age,
                    labor_force_participation = :lfp,
                    accessibility_method = :accessibility_method,
                    accessibility_threshold_30_min = :accessibility_threshold_30_min,
                    accessibility_threshold_45_min = :accessibility_threshold_45_min,
                    accessibility_proxy_distance_30_km = :accessibility_proxy_distance_30_km,
                    accessibility_proxy_distance_45_km = :accessibility_proxy_distance_45_km,
                    lodes_year = :lodes_year,
                    acs_year = :acs_year,
                    accessibility_version = 'v2-accessibility',
                    updated_at = CURRENT_TIMESTAMP
                WHERE fips_code = :fips_code AND data_year = :data_year
            """
                ),
                {
                    "fips_code": row["fips_code"],
                    "data_year": data_year,
                    "total_jobs": int(row.get("total_jobs", 0)),
                    "high_wage_jobs": int(row.get("high_wage_jobs", 0)),
                    "mid_wage_jobs": int(row.get("mid_wage_jobs", 0)),
                    "low_wage_jobs": int(row.get("low_wage_jobs", 0)),
                    "high_wage_45": int(row.get("high_wage_jobs_accessible_45min", 0)),
                    "high_wage_30": int(row.get("high_wage_jobs_accessible_30min", 0)),
                    "total_45": int(row.get("total_jobs_accessible_45min", 0)),
                    "total_30": int(row.get("total_jobs_accessible_30min", 0)),
                    "high_wage_45_weighted_mean": (
                        float(row.get("high_wage_jobs_accessible_45min_weighted_mean"))
                        if pd.notna(row.get("high_wage_jobs_accessible_45min_weighted_mean"))
                        else None
                    ),
                    "high_wage_45_weighted_median": (
                        float(row.get("high_wage_jobs_accessible_45min_weighted_median"))
                        if pd.notna(row.get("high_wage_jobs_accessible_45min_weighted_median"))
                        else None
                    ),
                    "total_45_weighted_mean": (
                        float(row.get("total_jobs_accessible_45min_weighted_mean"))
                        if pd.notna(row.get("total_jobs_accessible_45min_weighted_mean"))
                        else None
                    ),
                    "total_45_weighted_median": (
                        float(row.get("total_jobs_accessible_45min_weighted_median"))
                        if pd.notna(row.get("total_jobs_accessible_45min_weighted_median"))
                        else None
                    ),
                    "high_wage_30_weighted_mean": (
                        float(row.get("high_wage_jobs_accessible_30min_weighted_mean"))
                        if pd.notna(row.get("high_wage_jobs_accessible_30min_weighted_mean"))
                        else None
                    ),
                    "high_wage_30_weighted_median": (
                        float(row.get("high_wage_jobs_accessible_30min_weighted_median"))
                        if pd.notna(row.get("high_wage_jobs_accessible_30min_weighted_median"))
                        else None
                    ),
                    "total_30_weighted_mean": (
                        float(row.get("total_jobs_accessible_30min_weighted_mean"))
                        if pd.notna(row.get("total_jobs_accessible_30min_weighted_mean"))
                        else None
                    ),
                    "total_30_weighted_median": (
                        float(row.get("total_jobs_accessible_30min_weighted_median"))
                        if pd.notna(row.get("total_jobs_accessible_30min_weighted_median"))
                        else None
                    ),
                    "econ_score": float(row.get("economic_accessibility_score", 0)),
                    "market_score": float(row.get("job_market_reach_score", 0)),
                    "wage_ratio": float(row.get("wage_quality_ratio", 0)),
                    "pct_hw": float(row.get("pct_regional_high_wage_accessible", 0)),
                    "pct_total": float(row.get("pct_regional_jobs_accessible", 0)),
                    "sector_diversity_entropy": (
                        float(row.get("sector_diversity_entropy"))
                        if pd.notna(row.get("sector_diversity_entropy"))
                        else None
                    ),
                    "stable_sector_share": (
                        float(row.get("stable_sector_share"))
                        if pd.notna(row.get("stable_sector_share"))
                        else None
                    ),
                    "concentration": float(row.get("high_wage_sector_concentration", 0)),
                    "mobility": float(row.get("upward_mobility_score", 0)),
                    "quality": float(row.get("job_quality_index", 0)),
                    "entrepreneurship_density": (
                        float(row.get("entrepreneurship_density"))
                        if pd.notna(row.get("entrepreneurship_density"))
                        else None
                    ),
                    "local_strength": local_strength,
                    "opportunity_index": opportunity_index,
                    "qwi_emp_total": (
                        int(row.get("qwi_emp_total"))
                        if pd.notna(row.get("qwi_emp_total"))
                        else None
                    ),
                    "qwi_hires": (
                        int(row.get("qwi_hires")) if pd.notna(row.get("qwi_hires")) else None
                    ),
                    "qwi_separations": (
                        int(row.get("qwi_separations"))
                        if pd.notna(row.get("qwi_separations"))
                        else None
                    ),
                    "qwi_hire_rate": (
                        float(row.get("qwi_hire_rate"))
                        if pd.notna(row.get("qwi_hire_rate"))
                        else None
                    ),
                    "qwi_separation_rate": (
                        float(row.get("qwi_separation_rate"))
                        if pd.notna(row.get("qwi_separation_rate"))
                        else None
                    ),
                    "qwi_turnover_rate": (
                        float(row.get("qwi_turnover_rate"))
                        if pd.notna(row.get("qwi_turnover_rate"))
                        else None
                    ),
                    "qwi_net_job_growth_rate": (
                        float(row.get("qwi_net_job_growth_rate"))
                        if pd.notna(row.get("qwi_net_job_growth_rate"))
                        else None
                    ),
                    "qwi_year": int(row.get("qwi_year")) if pd.notna(row.get("qwi_year")) else None,
                    "od_year": int(row.get("od_year")) if pd.notna(row.get("od_year")) else None,
                    "od_resident_workers": (
                        int(row.get("od_resident_workers"))
                        if pd.notna(row.get("od_resident_workers"))
                        else None
                    ),
                    "od_inbound_workers": (
                        int(row.get("od_inbound_workers"))
                        if pd.notna(row.get("od_inbound_workers"))
                        else None
                    ),
                    "od_outbound_workers": (
                        int(row.get("od_outbound_workers"))
                        if pd.notna(row.get("od_outbound_workers"))
                        else None
                    ),
                    "od_live_work_same_county": (
                        int(row.get("od_live_work_same_county"))
                        if pd.notna(row.get("od_live_work_same_county"))
                        else None
                    ),
                    "od_net_commuter_flow": (
                        int(row.get("od_net_commuter_flow"))
                        if pd.notna(row.get("od_net_commuter_flow"))
                        else None
                    ),
                    "od_local_capture_rate": (
                        float(row.get("od_local_capture_rate"))
                        if pd.notna(row.get("od_local_capture_rate"))
                        else None
                    ),
                    "od_working_age_resident_workers": (
                        int(row.get("od_working_age_resident_workers"))
                        if pd.notna(row.get("od_working_age_resident_workers"))
                        else None
                    ),
                    "od_working_age_live_work_same_county": (
                        int(row.get("od_working_age_live_work_same_county"))
                        if pd.notna(row.get("od_working_age_live_work_same_county"))
                        else None
                    ),
                    "od_working_age_share": (
                        float(row.get("od_working_age_share"))
                        if pd.notna(row.get("od_working_age_share"))
                        else None
                    ),
                    "od_working_age_local_capture_rate": (
                        float(row.get("od_working_age_local_capture_rate"))
                        if pd.notna(row.get("od_working_age_local_capture_rate"))
                        else None
                    ),
                    "od_high_wage_resident_workers": (
                        int(row.get("od_high_wage_resident_workers"))
                        if pd.notna(row.get("od_high_wage_resident_workers"))
                        else None
                    ),
                    "od_high_wage_live_work_same_county": (
                        int(row.get("od_high_wage_live_work_same_county"))
                        if pd.notna(row.get("od_high_wage_live_work_same_county"))
                        else None
                    ),
                    "od_high_wage_share": (
                        float(row.get("od_high_wage_share"))
                        if pd.notna(row.get("od_high_wage_share"))
                        else None
                    ),
                    "od_high_wage_local_capture_rate": (
                        float(row.get("od_high_wage_local_capture_rate"))
                        if pd.notna(row.get("od_high_wage_local_capture_rate"))
                        else None
                    ),
                    "working_age": int(row.get("working_age_pop", 0)),
                    "lfp": float(row.get("labor_force_participation", 0)),
                    "accessibility_method": (
                        str(row.get("accessibility_method"))
                        if pd.notna(row.get("accessibility_method"))
                        else None
                    ),
                    "accessibility_threshold_30_min": (
                        int(row.get("accessibility_threshold_30_min"))
                        if pd.notna(row.get("accessibility_threshold_30_min"))
                        else None
                    ),
                    "accessibility_threshold_45_min": (
                        int(row.get("accessibility_threshold_45_min"))
                        if pd.notna(row.get("accessibility_threshold_45_min"))
                        else None
                    ),
                    "accessibility_proxy_distance_30_km": (
                        float(row.get("accessibility_proxy_distance_30_km"))
                        if pd.notna(row.get("accessibility_proxy_distance_30_km"))
                        else None
                    ),
                    "accessibility_proxy_distance_45_km": (
                        float(row.get("accessibility_proxy_distance_45_km"))
                        if pd.notna(row.get("accessibility_proxy_distance_45_km"))
                        else None
                    ),
                    "lodes_year": lodes_year,
                    "acs_year": acs_year,
                },
            )

        db.commit()

    logger.info("✓ County economic opportunity data updated")


# =============================================================================
# MAIN EXECUTION
# =============================================================================


def calculate_economic_opportunity_indicators(
    data_year: int = None,
    lodes_year: int = None,
    acs_year: int = None,
    accessibility_mode: Optional[str] = None,
    threshold_30_min: Optional[int] = None,
    threshold_45_min: Optional[int] = None,
    proxy_distance_30_km: Optional[float] = None,
    proxy_distance_45_km: Optional[float] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main function to calculate economic opportunity indicators.

    Args:
        data_year: Year to associate with this data (default: current year)
        lodes_year: LODES year to use (default: data_year - 2)
        acs_year: ACS year to use (default: data_year - 2)

    Returns:
        Tuple of (tract_df, county_df) with economic opportunity metrics
    """
    data_year = data_year or datetime.now().year
    lodes_year = lodes_year or min(data_year - 2, settings.LODES_LATEST_YEAR)
    acs_year = acs_year or min(data_year - 2, settings.ACS_LATEST_YEAR)

    logger.info("=" * 60)
    logger.info("LAYER 1 v2: ECONOMIC OPPORTUNITY ACCESSIBILITY ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Data year: {data_year}")
    logger.info(f"LODES year: {lodes_year}")
    logger.info(f"ACS year: {acs_year}")

    # Step 1: Download LODES data
    logger.info("\n[1/6] Downloading LODES data with wage segments...")
    wac_df = download_lodes_wac_segments(lodes_year)

    # Step 2: Aggregate to tract level
    logger.info("\n[2/6] Aggregating to tract level...")
    tract_jobs = aggregate_lodes_to_tract(wac_df)

    # Step 3: Compute sector diversity
    logger.info("\n[3/6] Computing sector diversity...")
    tract_jobs = compute_sector_diversity(tract_jobs)

    # Step 4: Get tract centroids
    logger.info("\n[4/6] Loading tract centroids...")
    tract_centroids = fetch_tract_centroids()

    # Step 5: Compute accessibility
    logger.info("\n[5/6] Computing economic accessibility...")
    tract_df = compute_economic_accessibility(
        tract_jobs,
        tract_centroids,
        mode=accessibility_mode,
        threshold_30_min=threshold_30_min,
        threshold_45_min=threshold_45_min,
        proxy_distance_30_km=proxy_distance_30_km,
        proxy_distance_45_km=proxy_distance_45_km,
    )

    # Merge with ACS demographics
    logger.info("\n[6/6] Merging ACS demographics...")
    acs_df = fetch_acs_demographics(acs_year)
    if not acs_df.empty:
        tract_df = tract_df.merge(
            acs_df[["tract_geoid", "population", "working_age_pop", "labor_force_participation"]],
            on="tract_geoid",
            how="left",
        )

    # Fill missing demographics
    tract_df["population"] = tract_df.get("population", 0).fillna(0).astype(int)
    tract_df["working_age_pop"] = tract_df.get("working_age_pop", 0).fillna(0).astype(int)
    tract_df["labor_force_participation"] = tract_df.get("labor_force_participation", 0).fillna(0)

    # Normalize scores
    tract_df = normalize_accessibility_scores(tract_df)

    # Aggregate to county
    county_df = aggregate_to_county(tract_df)

    # Optional: QWI enrichment for job dynamics
    qwi_df = fetch_qwi_by_county(data_year)
    if not qwi_df.empty:
        county_df = county_df.merge(qwi_df, on="fips_code", how="left")
        if "qwi_net_job_growth_rate" in county_df.columns:
            growth = county_df["qwi_net_job_growth_rate"]
            valid_mask = growth.notna()
            county_df["qwi_net_job_growth_score"] = pd.NA
            if valid_mask.sum() >= 3:
                county_df.loc[valid_mask, "qwi_net_job_growth_score"] = growth[valid_mask].rank(
                    pct=True
                )

    # Optional: LODES OD enrichment for county commute flow / capture metrics.
    od_df = fetch_lodes_od_county_flows(lodes_year)
    if not od_df.empty:
        county_df = county_df.merge(od_df, on="fips_code", how="left")

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("ECONOMIC OPPORTUNITY ANALYSIS COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Tracts analyzed: {len(tract_df)}")
    logger.info(f"Counties: {len(county_df)}")
    logger.info(f"Total jobs: {tract_df['total_jobs'].sum():,}")
    logger.info(f"High-wage jobs: {tract_df['high_wage_jobs'].sum():,}")
    logger.info(f"Avg accessibility score: {tract_df['economic_accessibility_score'].mean():.3f}")

    return tract_df, county_df


def run_layer1_v2_ingestion(
    data_year: int = None,
    multi_year: bool = True,
    store_data: bool = True,
    window_years: int = 5,
    predict_to_year: Optional[int] = None,
    accessibility_mode: Optional[str] = None,
    threshold_30_min: Optional[int] = None,
    threshold_45_min: Optional[int] = None,
    proxy_distance_30_km: Optional[float] = None,
    proxy_distance_45_km: Optional[float] = None,
):
    """
    Run complete Layer 1 v2 ingestion pipeline.

    Args:
        data_year: End year for analysis window (default: latest available)
        multi_year: If True, run a multi-year window ending at the latest available year
        store_data: Whether to store results in database
        window_years: Window size for multi-year ingestion (default: 5)
    """
    latest_available_year = min(settings.LODES_LATEST_YEAR, settings.ACS_LATEST_YEAR) + 2
    end_year = data_year or latest_available_year
    if end_year > latest_available_year:
        logger.warning(
            f"Requested end year {end_year} exceeds latest available data year "
            f"{latest_available_year}. Using {latest_available_year}."
        )
        end_year = latest_available_year

    try:
        if multi_year:
            start_year = end_year - window_years + 1
            years_to_fetch = list(range(start_year, end_year + 1))
            logger.info(
                "Starting Layer 1 v2 MULTI-YEAR ingestion for years "
                f"{years_to_fetch[0]}-{years_to_fetch[-1]}"
            )
        else:
            years_to_fetch = [end_year]
            logger.info(f"Starting Layer 1 v2 single-year ingestion for {end_year}")

        total_records = 0
        failed_years = []
        last_tract_df = pd.DataFrame()
        last_county_df = pd.DataFrame()

        for year in years_to_fetch:
            lodes_year = min(year - 2, settings.LODES_LATEST_YEAR)
            acs_year = min(year - 2, settings.ACS_LATEST_YEAR)

            logger.info("=" * 70)
            logger.info(f"Processing year {year}")
            logger.info("=" * 70)

            try:
                tract_df, county_df = calculate_economic_opportunity_indicators(
                    data_year=year,
                    lodes_year=lodes_year,
                    acs_year=acs_year,
                    accessibility_mode=accessibility_mode,
                    threshold_30_min=threshold_30_min,
                    threshold_45_min=threshold_45_min,
                    proxy_distance_30_km=proxy_distance_30_km,
                    proxy_distance_45_km=proxy_distance_45_km,
                )

                if store_data and not tract_df.empty:
                    store_tract_economic_opportunity(tract_df, year, lodes_year, acs_year)
                    store_county_economic_opportunity(county_df, year, lodes_year, acs_year)

                    log_refresh(
                        layer_name="layer1_employment_gravity",
                        data_source="LODES+ACS (v2 accessibility)",
                        status="success",
                        records_processed=len(tract_df),
                        records_inserted=len(tract_df) + len(county_df),
                        metadata={
                            "data_year": year,
                            "lodes_year": lodes_year,
                            "acs_year": acs_year,
                            "version": "v2-accessibility",
                            "accessibility_mode": accessibility_mode
                            or settings.LAYER1_ACCESSIBILITY_MODE,
                            "threshold_30_min": threshold_30_min
                            or settings.LAYER1_THRESHOLD_30_MINUTES,
                            "threshold_45_min": threshold_45_min
                            or settings.LAYER1_THRESHOLD_45_MINUTES,
                            "proxy_distance_30_km": (
                                proxy_distance_30_km
                                if proxy_distance_30_km is not None
                                else settings.LAYER1_PROXY_DISTANCE_30_KM
                            ),
                            "proxy_distance_45_km": (
                                proxy_distance_45_km
                                if proxy_distance_45_km is not None
                                else settings.LAYER1_PROXY_DISTANCE_45_KM
                            ),
                            "tracts": len(tract_df),
                            "counties": len(county_df),
                            "total_jobs": int(tract_df["total_jobs"].sum()),
                            "high_wage_jobs": int(tract_df["high_wage_jobs"].sum()),
                            "od_counties": (
                                int(county_df["od_resident_workers"].notna().sum())
                                if "od_resident_workers" in county_df.columns
                                else 0
                            ),
                            "od_year": (
                                int(county_df["od_year"].dropna().iloc[0])
                                if "od_year" in county_df.columns
                                and county_df["od_year"].notna().any()
                                else None
                            ),
                        },
                    )

                total_records += len(tract_df)
                last_tract_df = tract_df
                last_county_df = county_df
                logger.info(f"✓ Year {year} complete: {len(tract_df)} tract records")

            except Exception as e:
                logger.error(f"✗ Year {year} ingestion failed: {e}", exc_info=True)
                failed_years.append(year)
                continue

        logger.info("=" * 70)
        if multi_year:
            logger.info("MULTI-YEAR INGESTION SUMMARY")
            logger.info(
                f"  Years requested: {years_to_fetch[0]}-{years_to_fetch[-1]} "
                f"({len(years_to_fetch)} years)"
            )
            logger.info(f"  Years successful: {len(years_to_fetch) - len(failed_years)}")
            logger.info(
                f"  Years failed: {len(failed_years)} {failed_years if failed_years else ''}"
            )
            logger.info(f"  Total tract records stored: {total_records}")
        else:
            logger.info(f"Single-year ingestion {'succeeded' if not failed_years else 'failed'}")

        if failed_years and len(failed_years) == len(years_to_fetch):
            raise Exception(f"All years failed: {failed_years}")

        if store_data:
            target_year = predict_to_year or settings.PREDICT_TO_YEAR
            apply_predictions_to_table(
                table="layer1_employment_gravity",
                metric_col="economic_opportunity_index",
                target_year=target_year,
                clip=(0.0, 1.0),
            )

        logger.info("✓ Layer 1 v2 ingestion complete")
        return last_tract_df, last_county_df

    except Exception as e:
        logger.error(f"Layer 1 v2 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer1_employment_gravity",
            data_source="LODES+ACS (v2 accessibility)",
            status="failed",
            error_message=str(e),
        )
        raise


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Layer 1 v2: Economic Opportunity Accessibility Analysis"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="End year for window (default: latest available year)",
    )
    parser.add_argument(
        "--single-year",
        action="store_true",
        help="Fetch only single year (default: multi-year window)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Calculate but do not store results")
    parser.add_argument(
        "--predict-to-year",
        type=int,
        default=None,
        help="Predict missing years up to target year (default: settings.PREDICT_TO_YEAR)",
    )
    parser.add_argument(
        "--accessibility-mode",
        choices=[ACCESSIBILITY_MODE_AUTO, ACCESSIBILITY_MODE_NETWORK, ACCESSIBILITY_MODE_PROXY],
        default=None,
        help="Accessibility impedance mode (default: settings.LAYER1_ACCESSIBILITY_MODE)",
    )
    parser.add_argument(
        "--threshold-30-min",
        type=int,
        default=None,
        help="Accessibility threshold for 30-minute reachability scenario",
    )
    parser.add_argument(
        "--threshold-45-min",
        type=int,
        default=None,
        help="Accessibility threshold for 45-minute reachability scenario",
    )
    parser.add_argument(
        "--proxy-distance-30-km",
        type=float,
        default=None,
        help="Proxy distance (km) used for 30-minute threshold in haversine mode",
    )
    parser.add_argument(
        "--proxy-distance-45-km",
        type=float,
        default=None,
        help="Proxy distance (km) used for 45-minute threshold in haversine mode",
    )

    args = parser.parse_args()

    run_layer1_v2_ingestion(
        data_year=args.year,
        multi_year=not args.single_year,
        store_data=not args.dry_run,
        predict_to_year=args.predict_to_year,
        accessibility_mode=args.accessibility_mode,
        threshold_30_min=args.threshold_30_min,
        threshold_45_min=args.threshold_45_min,
        proxy_distance_30_km=args.proxy_distance_30_km,
        proxy_distance_45_km=args.proxy_distance_45_km,
    )


if __name__ == "__main__":
    main()
