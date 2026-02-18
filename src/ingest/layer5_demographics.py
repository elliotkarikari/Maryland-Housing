"""
Maryland Viability Atlas - Layer 5: Demographic Momentum
Ingests migration patterns and household formation signals

Signals Produced:
- Working-age population trends
- Migration flows
- Household formation rates
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

try:
    from scipy.stats import theilslopes
except Exception:  # pragma: no cover - optional dependency
    theilslopes = None

# Ensure project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.database import get_db, log_refresh
from config.settings import MD_COUNTY_FIPS, get_settings
from src.utils.data_sources import download_file, fetch_census_data
from src.utils.db_bulk import execute_batch
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

CACHE_DIR = Path("data/cache/demographics")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOW_VACANCY_CACHE_DIR = CACHE_DIR / "low_vacancy"
LOW_VACANCY_CACHE_DIR.mkdir(parents=True, exist_ok=True)

ACS_DEMOGRAPHIC_VARIABLES = {
    "B01001_001E": "pop_total",
    "B01001_010E": "pop_m_25_29",
    "B01001_011E": "pop_m_30_34",
    "B01001_012E": "pop_m_35_39",
    "B01001_013E": "pop_m_40_44",
    "B01001_034E": "pop_f_25_29",
    "B01001_035E": "pop_f_30_34",
    "B01001_036E": "pop_f_35_39",
    "B01001_037E": "pop_f_40_44",
    "B11001_001E": "households_total",
    "B11001_002E": "households_family",
    "B11005_003E": "households_family_with_children",
}

IRS_YEAR_RANGES = ["1718", "1819", "1920", "2021", "2122"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _find_col(columns: list[str], candidates: list[str]) -> Optional[str]:
    for cand in candidates:
        if cand in columns:
            return cand
    for cand in candidates:
        for col in columns:
            if cand in col:
                return col
    return None


def _download_irs_flow(year_range: str, flow: str) -> Path:
    filename = f"irs_{flow}_{year_range}.csv"
    target = CACHE_DIR / filename
    if not target.exists():
        url = f"https://www.irs.gov/pub/irs-soi/county{flow}{year_range}.csv"
        logger.info(f"Downloading IRS {flow} data for {year_range}")
        ok = download_file(url, str(target))
        if not ok:
            raise RuntimeError(f"Failed to download IRS {flow} file for {year_range}")
    return target


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


def _read_census_crosswalk(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, sep="|", dtype=str, encoding="utf-8-sig", low_memory=False)
    except Exception as e:
        logger.warning(f"Failed to read Census crosswalk {path}: {e}")
        return pd.DataFrame()

    df = _normalize_columns(df)
    if "geoid_zcta5_20" not in df.columns or "geoid_county_20" not in df.columns:
        logger.warning("Census crosswalk missing expected GEOID columns")
        return pd.DataFrame()

    df = df[["geoid_zcta5_20", "geoid_county_20", "arealand_part"]].copy()
    df = df.rename(
        columns={
            "geoid_zcta5_20": "zip",
            "geoid_county_20": "county_fips",
            "arealand_part": "res_ratio",
        }
    )
    df["zip"] = df["zip"].astype(str).str.zfill(5)
    df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)
    df["res_ratio"] = pd.to_numeric(df["res_ratio"], errors="coerce").fillna(0)
    total = df.groupby("zip")["res_ratio"].transform("sum")
    df["res_ratio"] = df["res_ratio"] / total.replace({0: pd.NA})
    df["res_ratio"] = df["res_ratio"].fillna(0)
    return df[["zip", "county_fips", "res_ratio"]]


def _load_zip_crosswalk(zip_codes: list[str]) -> pd.DataFrame:
    if settings.USPS_ZIP_COUNTY_CROSSWALK_PATH:
        try:
            path = Path(settings.USPS_ZIP_COUNTY_CROSSWALK_PATH)
            if path.exists():
                if path.suffix.lower() in {".txt", ".dat"}:
                    return _read_census_crosswalk(path)
                return pd.read_csv(path, dtype=str, low_memory=False)
        except Exception as e:
            logger.warning(
                f"Failed to read ZIP crosswalk {settings.USPS_ZIP_COUNTY_CROSSWALK_PATH}: {e}"
            )
            return pd.DataFrame()

    if settings.CENSUS_ZIP_COUNTY_CROSSWALK_URL:
        target = CACHE_DIR / "census_zcta_county20.txt"
        if not target.exists():
            ok = download_file(settings.CENSUS_ZIP_COUNTY_CROSSWALK_URL, str(target))
            if ok:
                return _read_census_crosswalk(target)
            logger.warning("Failed to download Census ZIP→county crosswalk")
        else:
            return _read_census_crosswalk(target)

    return pd.DataFrame()


def _parse_low_vacancy_year(path: Path) -> Optional[int]:
    name = path.name
    for token in name.replace("_", "-").split("-"):
        if token.isdigit() and len(token) == 4:
            return int(token)
    digits = "".join(ch for ch in name if ch.isdigit())
    if len(digits) >= 4:
        return int(digits[:4])
    return None


def _extract_low_vacancy_link(html_path: Path, base_url: str) -> Optional[str]:
    try:
        html = html_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

    matches = re.findall(r"href=[\"\\\']([^\"\\\']+\\.(?:xlsx?|csv))", html, flags=re.IGNORECASE)
    if not matches:
        return None

    # Prefer links that look like low vacancy dataset
    for link in matches:
        if "lowvac" in link.lower() or "vac" in link.lower():
            return urljoin(base_url, link)
    return urljoin(base_url, matches[0])


def fetch_low_vacancy_counties() -> pd.DataFrame:
    """
    Fetch HUD low-vacancy county list (FY Excel).
    Returns county-level low vacancy indicator and unit counts.
    """
    source_path = _resolve_data_path(
        settings.LOW_VACANCY_COUNTIES_PATH,
        settings.LOW_VACANCY_COUNTIES_URL,
        LOW_VACANCY_CACHE_DIR,
        "low_vacancy_counties.xlsx",
    )

    if source_path is None:
        logger.warning("Low vacancy county source not configured; skipping")
        return pd.DataFrame()

    try:
        df = pd.read_excel(source_path)
    except Exception as e:
        source_url = settings.LOW_VACANCY_COUNTIES_URL
        if source_url and source_url.lower().endswith(".html"):
            link = _extract_low_vacancy_link(source_path, source_url)
            if link:
                filename = Path(urlparse(link).path).name or "low_vacancy_counties.xlsx"
                target = LOW_VACANCY_CACHE_DIR / filename
                if not target.exists():
                    ok = download_file(link, str(target))
                    if not ok:
                        logger.warning("Failed to download low vacancy dataset from link")
                        return pd.DataFrame()
                try:
                    df = pd.read_excel(target)
                    source_path = target
                except Exception as e2:
                    logger.warning(f"Failed to read low vacancy file {target}: {e2}")
                    return pd.DataFrame()
            else:
                logger.warning("Could not find dataset link on low vacancy HTML page")
                return pd.DataFrame()
        else:
            logger.warning(f"Failed to read low vacancy file {source_path}: {e}")
            return pd.DataFrame()

    df = _normalize_columns(df)
    columns = list(df.columns)
    fips_col = _find_col(columns, ["county fips", "county_fips", "fips", "fips_code"])
    units_col = _find_col(columns, ["number units", "number_units", "units"])
    occupied_col = _find_col(columns, ["occupied units", "occupied_units"])
    percent_col = _find_col(columns, ["percent occupied", "percent_occupied", "pct_occupied"])

    if not fips_col:
        logger.warning("Low vacancy file missing county FIPS column")
        return pd.DataFrame()

    fy_year = _parse_low_vacancy_year(Path(source_path)) or datetime.utcnow().year

    df["fips_code"] = pd.to_numeric(df[fips_col], errors="coerce")
    df["fips_code"] = df["fips_code"].dropna().astype(int).astype(str).str.zfill(5)
    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())]
    if df.empty:
        return pd.DataFrame()

    df["low_vacancy_units"] = pd.to_numeric(df[units_col], errors="coerce") if units_col else pd.NA
    df["low_vacancy_occupied_units"] = (
        pd.to_numeric(df[occupied_col], errors="coerce") if occupied_col else pd.NA
    )
    df["low_vacancy_percent_occupied"] = (
        pd.to_numeric(df[percent_col], errors="coerce") if percent_col else pd.NA
    )
    df["low_vacancy_fy"] = fy_year
    df["low_vacancy_county_flag"] = True
    df["source_url"] = settings.LOW_VACANCY_COUNTIES_URL or str(source_path)
    df["fetch_date"] = datetime.utcnow().date().isoformat()
    df["is_real"] = True

    return df[
        [
            "fips_code",
            "low_vacancy_county_flag",
            "low_vacancy_units",
            "low_vacancy_occupied_units",
            "low_vacancy_percent_occupied",
            "low_vacancy_fy",
            "source_url",
            "fetch_date",
            "is_real",
        ]
    ].drop_duplicates()


def merge_low_vacancy_counts(combined: pd.DataFrame, low_vacancy_df: pd.DataFrame) -> pd.DataFrame:
    if low_vacancy_df.empty:
        combined["low_vacancy_county_flag"] = pd.NA
        combined["low_vacancy_units"] = pd.NA
        combined["low_vacancy_occupied_units"] = pd.NA
        combined["low_vacancy_percent_occupied"] = pd.NA
        combined["low_vacancy_fy"] = pd.NA
        return combined

    low_vacancy_df = low_vacancy_df.drop(
        columns=[
            col for col in ["source_url", "fetch_date", "is_real"] if col in low_vacancy_df.columns
        ]
    )

    merged = combined.merge(
        low_vacancy_df,
        left_on=["fips_code", "data_year"],
        right_on=["fips_code", "low_vacancy_fy"],
        how="left",
    )

    missing_vacancy = merged["vacancy_rate"].isna()
    if missing_vacancy.any():
        percent = merged["low_vacancy_percent_occupied"]
        derived_rate = (1 - (percent / 100)).clip(lower=0, upper=1)
        derived_vacant = merged["low_vacancy_units"] - merged["low_vacancy_occupied_units"]

        merged.loc[missing_vacancy, "vacancy_rate"] = derived_rate
        merged.loc[missing_vacancy, "total_addresses"] = merged.loc[
            missing_vacancy, "low_vacancy_units"
        ]
        merged.loc[missing_vacancy, "vacant_addresses"] = derived_vacant
        merged.loc[missing_vacancy, "vacancy_source"] = "lowvactpv"

    return merged


def apply_vacancy_predictions(combined: pd.DataFrame) -> pd.DataFrame:
    combined = combined.copy()
    combined["vacancy_rate_pred"] = pd.NA
    combined["vacancy_predicted"] = False
    combined["vacancy_pred_method"] = pd.NA
    combined["vacancy_pred_years"] = pd.NA

    target_year = settings.PREDICT_TO_YEAR
    min_years = settings.PREDICTION_MIN_YEARS
    max_extrap = settings.PREDICTION_MAX_EXTRAP_YEARS

    for fips in combined["fips_code"].unique():
        sub_mask = combined["fips_code"] == fips
        sub = combined.loc[sub_mask]
        history = sub[sub["vacancy_rate"].notna()]
        if len(history) < min_years:
            continue

        years = history["data_year"].astype(float).values
        rates = history["vacancy_rate"].astype(float).values

        try:
            if theilslopes is not None:
                slope, intercept, *_ = theilslopes(rates, years)
                method = "theil_sen"
            else:
                slope, intercept = np.polyfit(years, rates, 1)
                method = "linear_trend"
        except Exception:
            continue

        max_year = float(np.max(years))
        if max_year >= target_year:
            continue

        end_year = min(target_year, max_year + max_extrap)
        year_series = combined["data_year"].astype(float)
        prediction_mask = (
            sub_mask
            & combined["vacancy_rate"].isna()
            & (year_series > max_year)
            & (year_series <= end_year)
        )
        if not prediction_mask.any():
            continue

        predict_years = year_series.loc[prediction_mask]
        predictions = (slope * predict_years + intercept).clip(lower=0.0, upper=1.0).astype(float)
        combined.loc[prediction_mask, "vacancy_rate_pred"] = predictions
        combined.loc[prediction_mask, "vacancy_predicted"] = True
        combined.loc[prediction_mask, "vacancy_pred_method"] = method
        combined.loc[prediction_mask, "vacancy_pred_years"] = (predict_years - max_year).astype(int)
        missing_source_mask = prediction_mask & combined["vacancy_source"].isna()
        combined.loc[missing_source_mask, "vacancy_source"] = "predicted"

    return combined


def _apply_momentum_features(combined: pd.DataFrame) -> pd.DataFrame:
    combined = combined.copy()

    pop_lookup = combined[["fips_code", "data_year", "pop_age_25_44"]].rename(
        columns={"data_year": "lookup_year", "pop_age_25_44": "pop_age_25_44_baseline"}
    )
    household_lookup = combined[["fips_code", "data_year", "households_total"]].rename(
        columns={"data_year": "lookup_year", "households_total": "households_total_baseline"}
    )

    combined["baseline_year"] = combined["data_year"] - 2
    combined["prior_year"] = combined["data_year"] - 1
    combined = combined.merge(
        pop_lookup,
        left_on=["fips_code", "baseline_year"],
        right_on=["fips_code", "lookup_year"],
        how="left",
    ).drop(columns=["lookup_year"])
    combined = combined.merge(
        household_lookup,
        left_on=["fips_code", "prior_year"],
        right_on=["fips_code", "lookup_year"],
        how="left",
    ).drop(columns=["lookup_year"])

    pop_baseline = combined["pop_age_25_44_baseline"]
    pop_current = combined["pop_age_25_44"]
    pop_valid = pop_baseline.notna() & pop_current.notna() & (pop_baseline != 0)
    combined["working_age_momentum"] = pd.NA
    combined.loc[pop_valid, "working_age_momentum"] = (
        (pop_current.loc[pop_valid] - pop_baseline.loc[pop_valid])
        / pop_baseline.loc[pop_valid]
        * 100
    )

    household_baseline = combined["households_total_baseline"]
    household_current = combined["households_total"]
    household_valid = (
        household_baseline.notna() & household_current.notna() & (household_baseline != 0)
    )
    combined["household_formation_change"] = pd.NA
    combined.loc[household_valid, "household_formation_change"] = (
        (household_current.loc[household_valid] - household_baseline.loc[household_valid])
        / household_baseline.loc[household_valid]
        * 100
    )

    return combined.drop(
        columns=[
            "baseline_year",
            "prior_year",
            "pop_age_25_44_baseline",
            "households_total_baseline",
        ]
    )


def _fetch_crosswalk_from_api(zip_codes: list[str]) -> pd.DataFrame:
    crosswalk_api_url = settings.USPS_ZIP_COUNTY_CROSSWALK_URL or settings.HUD_USPS_API_URL
    if not crosswalk_api_url:
        return pd.DataFrame()
    if not settings.HUD_USER_API_TOKEN:
        logger.warning("HUD API token missing; cannot fetch USPS crosswalk API")
        return pd.DataFrame()

    base_url = crosswalk_api_url.rstrip("/")
    headers = {"Authorization": f"Bearer {settings.HUD_USER_API_TOKEN}"}
    rows = []

    for zip_code in zip_codes:
        params = {"type": 3, "query": zip_code}
        try:
            resp = requests.get(f"{base_url}/crosswalk", headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            data = None
            if isinstance(payload, dict):
                data = payload.get("data") or payload.get("Data") or payload.get("results")
            if not isinstance(data, list):
                continue
            for item in data:
                if not isinstance(item, dict):
                    continue
                rows.append(item)
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


def fetch_usps_vacancy_by_county(target_years: list[int]) -> pd.DataFrame:
    """
    Fetch USPS vacancy data (via HUD) and aggregate to county.

    Requires either:
    - settings.USPS_VACANCY_DATA_PATH (local CSV), or
    - settings.USPS_VACANCY_DATA_URL (downloadable CSV)
    and optionally settings.USPS_ZIP_COUNTY_CROSSWALK_PATH for ZIP → county mapping.
    """
    vacancy_url = settings.USPS_VACANCY_DATA_URL
    if vacancy_url and not vacancy_url.lower().endswith((".csv", ".txt", ".zip", ".gz", ".xlsx")):
        logger.warning("USPS vacancy URL is not a direct data file; skipping USPS enrichment")
        vacancy_url = None

    source_path = _resolve_data_path(
        settings.USPS_VACANCY_DATA_PATH, vacancy_url, CACHE_DIR, "usps_vacancy.csv"
    )

    if source_path is None:
        logger.warning("USPS vacancy data source not configured; skipping USPS enrichment")
        return pd.DataFrame()

    try:
        df = pd.read_csv(source_path, dtype=str, low_memory=False)
    except Exception as e:
        logger.warning(f"Failed to read USPS vacancy data {source_path}: {e}")
        return pd.DataFrame()

    df = _normalize_columns(df)
    columns = list(df.columns)

    year_col = _find_col(columns, ["year", "yr", "time"])
    data_year = max(target_years) if target_years else datetime.utcnow().year
    if year_col:
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
        if (df[year_col] == data_year).any():
            df = df[df[year_col] == data_year]
        elif df[year_col].notna().any():
            data_year = int(df[year_col].max())
            df = df[df[year_col] == data_year]

    total_col = _find_col(columns, ["total_units", "total", "total_addresses", "tot"])
    vacant_col = _find_col(columns, ["vacant", "vacant_units", "vacant_addresses"])
    if not total_col or not vacant_col:
        logger.warning("USPS vacancy data missing total/vacant columns; skipping USPS enrichment")
        return pd.DataFrame()

    total_col = total_col
    vacant_col = vacant_col
    df[total_col] = pd.to_numeric(df[total_col], errors="coerce")
    df[vacant_col] = pd.to_numeric(df[vacant_col], errors="coerce")

    fips_col = _find_col(columns, ["fips", "fips_code", "county_fips", "geoid"])
    if fips_col:
        df["fips_code"] = df[fips_col].astype(str).str.zfill(5)
    else:
        zip_col = _find_col(columns, ["zip", "zipcode", "zip5"])
        if not zip_col:
            logger.warning("USPS vacancy data missing ZIP codes and FIPS; skipping USPS enrichment")
            return pd.DataFrame()

        crosswalk = _load_zip_crosswalk(df[zip_col].astype(str).str.zfill(5).unique().tolist())
        if crosswalk is None or crosswalk.empty:
            if settings.HUD_USER_API_TOKEN and (
                settings.USPS_ZIP_COUNTY_CROSSWALK_URL or settings.HUD_USPS_API_URL
            ):
                crosswalk = _fetch_crosswalk_from_api(
                    df[zip_col].astype(str).str.zfill(5).unique().tolist()
                )
            if crosswalk is None or crosswalk.empty:
                logger.warning("USPS ZIP→county crosswalk not configured; skipping USPS enrichment")
                return pd.DataFrame()

        if crosswalk is None or crosswalk.empty:
            logger.warning("USPS ZIP→county crosswalk empty; skipping USPS enrichment")
            return pd.DataFrame()

        crosswalk = _normalize_columns(crosswalk)
        cw_cols = list(crosswalk.columns)
        cw_zip = _find_col(cw_cols, ["zip", "zipcode", "zip5"])
        cw_fips = _find_col(cw_cols, ["county_fips", "fips", "fips_code", "geoid"])
        if not cw_zip or not cw_fips:
            logger.warning("Crosswalk missing ZIP or county FIPS columns; skipping USPS enrichment")
            return pd.DataFrame()

        ratio_col = _find_col(cw_cols, ["tot_ratio", "res_ratio", "ratio", "weight"])
        crosswalk[cw_zip] = crosswalk[cw_zip].astype(str).str.zfill(5)
        crosswalk[cw_fips] = crosswalk[cw_fips].astype(str).str.zfill(5)

        df[zip_col] = df[zip_col].astype(str).str.zfill(5)
        df = df.merge(
            crosswalk[[cw_zip, cw_fips] + ([ratio_col] if ratio_col else [])],
            left_on=zip_col,
            right_on=cw_zip,
            how="left",
        )

        df["fips_code"] = df[cw_fips]
        if ratio_col:
            df["zip_weight"] = pd.to_numeric(df[ratio_col], errors="coerce")
        else:
            df["zip_weight"] = 1.0
        df["zip_weight"] = df["zip_weight"].fillna(1.0)

        df[total_col] = df[total_col] * df["zip_weight"]
        df[vacant_col] = df[vacant_col] * df["zip_weight"]

    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())]
    if df.empty:
        logger.warning("USPS vacancy data contains no Maryland counties after filtering")
        return pd.DataFrame()

    agg = df.groupby("fips_code", as_index=False)[[total_col, vacant_col]].sum(min_count=1)
    agg = agg.rename(columns={total_col: "total_addresses", vacant_col: "vacant_addresses"})
    agg["vacancy_rate"] = np.where(
        agg["total_addresses"] > 0, agg["vacant_addresses"] / agg["total_addresses"], pd.NA
    )
    agg["data_year"] = data_year
    agg["source_url"] = vacancy_url or str(source_path)
    agg["fetch_date"] = datetime.utcnow().date().isoformat()
    agg["is_real"] = True
    return agg


def merge_usps_vacancy(combined: pd.DataFrame, usps_df: pd.DataFrame) -> pd.DataFrame:
    if usps_df.empty:
        combined["total_addresses"] = pd.NA
        combined["vacant_addresses"] = pd.NA
        combined["vacancy_rate"] = pd.NA
        combined["vacancy_source"] = pd.NA
        return combined

    merged = combined.merge(usps_df, on=["fips_code", "data_year"], how="left")
    merged["vacancy_source"] = (
        merged["vacancy_source"] if "vacancy_source" in merged.columns else pd.NA
    )
    merged.loc[merged["vacancy_rate"].notna(), "vacancy_source"] = "usps"
    return merged


def _load_irs_flow(year_range: str, flow: str) -> pd.DataFrame:
    path = _download_irs_flow(year_range, flow)
    try:
        df = pd.read_csv(path, dtype=str)
    except UnicodeDecodeError:
        df = pd.read_csv(path, dtype=str, encoding="latin1")
    return _normalize_columns(df)


def _aggregate_irs_flow(df: pd.DataFrame, flow: str) -> pd.DataFrame:
    columns = list(df.columns)
    if flow == "inflow":
        state_col = _find_col(columns, ["y2_statefips", "y2_state_fips", "statefips_dest"])
        county_col = _find_col(columns, ["y2_countyfips", "y2_county_fips", "countyfips_dest"])
        other_col = _find_col(columns, ["y1_countyfips", "y1_county_fips"])
    else:
        state_col = _find_col(columns, ["y1_statefips", "y1_state_fips", "statefips_orig"])
        county_col = _find_col(columns, ["y1_countyfips", "y1_county_fips", "countyfips_orig"])
        other_col = _find_col(columns, ["y2_countyfips", "y2_county_fips"])

    n1_col = _find_col(columns, ["n1", "num_returns", "returns"])
    n2_col = _find_col(columns, ["n2", "num_exemptions", "exemptions"])

    if not state_col or not county_col or not n1_col or not n2_col:
        raise RuntimeError("IRS migration file missing required columns")

    df[state_col] = df[state_col].astype(str).str.zfill(2)
    df[county_col] = df[county_col].astype(str).str.zfill(3)

    # Filter to Maryland counties and exclude aggregate rows
    df = df[df[state_col] == "24"].copy()
    df = df[df[county_col] != "000"]
    if other_col:
        df = df[df[other_col].astype(str).str.zfill(3) != "000"]

    df["fips_code"] = df[state_col] + df[county_col]
    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())]

    df[n1_col] = pd.to_numeric(df[n1_col], errors="coerce")
    df[n2_col] = pd.to_numeric(df[n2_col], errors="coerce")

    agg = df.groupby("fips_code", as_index=False)[[n1_col, n2_col]].sum()
    agg = agg.rename(columns={n1_col: f"{flow}_households", n2_col: f"{flow}_exemptions"})
    return agg


def fetch_irs_migration_by_year() -> dict[int, pd.DataFrame]:
    """Fetch IRS inflow/outflow data across available year ranges."""
    results: dict[int, pd.DataFrame] = {}
    for year_range in IRS_YEAR_RANGES:
        try:
            inflow = _aggregate_irs_flow(_load_irs_flow(year_range, "inflow"), "inflow")
            outflow = _aggregate_irs_flow(_load_irs_flow(year_range, "outflow"), "outflow")
            df = inflow.merge(outflow, on="fips_code", how="outer")

            data_year = 2000 + int(year_range[2:])
            df["data_year"] = data_year
            df["source_url"] = (
                f"https://www.irs.gov/pub/irs-soi/countyinflow{year_range}.csv; "
                f"https://www.irs.gov/pub/irs-soi/countyoutflow{year_range}.csv"
            )
            df["fetch_date"] = datetime.utcnow().date().isoformat()
            df["is_real"] = True
            results[data_year] = df
            logger.info(f"Loaded IRS migration data for {year_range} ({data_year})")
        except Exception as e:
            logger.warning(f"Skipping IRS migration {year_range}: {e}")
            continue
    return results


def fetch_acs_demographic_data(data_year: int) -> pd.DataFrame:
    logger.info(f"Fetching ACS demographic data for {data_year}")
    df = fetch_census_data(
        dataset="acs/acs5",
        variables=list(ACS_DEMOGRAPHIC_VARIABLES.keys()),
        geography="county:*",
        state="24",
        year=data_year,
    )

    if df.empty:
        return pd.DataFrame()

    df["fips_code"] = "24" + df["county"].str.zfill(3)
    for acs_var, col_name in ACS_DEMOGRAPHIC_VARIABLES.items():
        if acs_var in df.columns:
            df[col_name] = pd.to_numeric(df[acs_var], errors="coerce")

    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())]
    cols_to_keep = ["fips_code"] + list(ACS_DEMOGRAPHIC_VARIABLES.values())
    for col in ["source_url", "fetch_date", "is_real"]:
        if col in df.columns:
            cols_to_keep.append(col)
    return df[[c for c in cols_to_keep if c in df.columns]]


def calculate_demographic_indicators(data_year: int = 2023) -> pd.DataFrame:
    """Calculate demographic momentum indicators."""
    acs_df = fetch_acs_demographic_data(data_year)
    if acs_df.empty:
        return pd.DataFrame()

    acs_df["data_year"] = data_year

    acs_df["pop_age_25_44"] = (
        acs_df["pop_m_25_29"]
        + acs_df["pop_m_30_34"]
        + acs_df["pop_m_35_39"]
        + acs_df["pop_m_40_44"]
        + acs_df["pop_f_25_29"]
        + acs_df["pop_f_30_34"]
        + acs_df["pop_f_35_39"]
        + acs_df["pop_f_40_44"]
    )
    acs_df["pop_age_25_44_pct"] = acs_df["pop_age_25_44"] / acs_df["pop_total"]

    return acs_df


def store_demographic_data(df: pd.DataFrame):
    """Store demographic momentum data in database."""
    logger.info(f"Storing {len(df)} demographic records")

    with get_db() as db:
        years = df["data_year"].unique().tolist()
        db.execute(
            text("DELETE FROM layer5_demographic_momentum WHERE data_year = ANY(:years)"),
            {"years": years},
        )

        insert_sql = text(
            """
            INSERT INTO layer5_demographic_momentum (
                fips_code, data_year,
                pop_total, pop_age_25_44, pop_age_25_44_pct,
                households_total, households_family, households_family_with_children,
                inflow_households, outflow_households, net_migration_households,
                inflow_exemptions, outflow_exemptions, net_migration_persons,
                total_addresses, vacant_addresses, vacancy_rate, vacancy_source,
                vacancy_rate_pred, vacancy_predicted, vacancy_pred_method, vacancy_pred_years,
                low_vacancy_county_flag, low_vacancy_units, low_vacancy_occupied_units,
                low_vacancy_percent_occupied, low_vacancy_fy,
                family_household_inflow_rate, working_age_momentum,
                household_formation_change, demographic_momentum_score
            ) VALUES (
                :fips_code, :data_year,
                :pop_total, :pop_age_25_44, :pop_age_25_44_pct,
                :households_total, :households_family, :households_family_with_children,
                :inflow_households, :outflow_households, :net_migration_households,
                :inflow_exemptions, :outflow_exemptions, :net_migration_persons,
                :total_addresses, :vacant_addresses, :vacancy_rate, :vacancy_source,
                :vacancy_rate_pred, :vacancy_predicted, :vacancy_pred_method, :vacancy_pred_years,
                :low_vacancy_county_flag, :low_vacancy_units, :low_vacancy_occupied_units,
                :low_vacancy_percent_occupied, :low_vacancy_fy,
                :family_household_inflow_rate, :working_age_momentum,
                :household_formation_change, :demographic_momentum_score
            )
        """
        )

        rows = df.to_dict(orient="records")
        execute_batch(db, insert_sql, rows, chunk_size=1000)

        db.commit()

    logger.info("✓ Demographic data stored successfully")


def main():
    """Main execution for Layer 5 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 5: DEMOGRAPHIC MOMENTUM INGESTION")
        logger.info("=" * 60)

        parser = argparse.ArgumentParser(description="Ingest Layer 5 Demographic data")
        parser.add_argument("--year", type=int, help="Latest ACS year to fetch (default: latest)")
        parser.add_argument(
            "--single-year",
            action="store_true",
            help="Fetch only single year (default: multi-year)",
        )
        args = parser.parse_args()

        current_year = datetime.utcnow().year
        latest_year = args.year or min(settings.ACS_LATEST_YEAR, current_year)
        if latest_year > settings.ACS_LATEST_YEAR:
            logger.warning(
                f"Requested year {latest_year} exceeds ACS latest year {settings.ACS_LATEST_YEAR}. "
                f"Using {settings.ACS_LATEST_YEAR}."
            )
            latest_year = settings.ACS_LATEST_YEAR
        if args.single_year:
            years_to_fetch = [latest_year]
        else:
            years_to_fetch = list(range(latest_year - 4, latest_year + 1))

        irs_by_year = fetch_irs_migration_by_year()
        all_years = []

        for year in years_to_fetch:
            acs_df = calculate_demographic_indicators(year)
            if acs_df.empty:
                logger.warning(f"No ACS demographic data for {year}")
                continue

            irs_df = irs_by_year.get(year)
            if irs_df is not None and not irs_df.empty:
                merged = acs_df.merge(irs_df, on=["fips_code", "data_year"], how="left")
            else:
                merged = acs_df.copy()
                merged["inflow_households"] = pd.NA
                merged["outflow_households"] = pd.NA
                merged["inflow_exemptions"] = pd.NA
                merged["outflow_exemptions"] = pd.NA

            merged["net_migration_households"] = (
                merged["inflow_households"] - merged["outflow_households"]
            )
            merged["net_migration_persons"] = (
                merged["inflow_exemptions"] - merged["outflow_exemptions"]
            )

            merged["family_household_inflow_rate"] = pd.NA
            if "households_family_with_children" in merged.columns:
                merged["family_household_inflow_rate"] = (
                    merged["inflow_households"] / merged["households_family_with_children"]
                )
                merged.loc[
                    merged["households_family_with_children"] == 0, "family_household_inflow_rate"
                ] = pd.NA

            all_years.append(merged)

        if not all_years:
            logger.error("No demographic data to store (real data not available)")
            log_refresh(
                layer_name="layer5_demographic_momentum",
                data_source="ACS+IRS",
                status="failed",
                error_message="No ACS/IRS records produced",
                metadata={"years_requested": years_to_fetch},
            )
            return

        combined = pd.concat(all_years, ignore_index=True)

        # Working-age momentum (3-year change) and household formation change (YoY)
        combined = _apply_momentum_features(combined)

        # Composite demographic momentum score (percentile of available signals)
        combined["demographic_momentum_score"] = pd.NA
        for year in combined["data_year"].unique():
            year_mask = combined["data_year"] == year
            sub = combined.loc[year_mask].copy()

            components = []
            if sub["net_migration_households"].notna().sum() >= 3:
                components.append(sub["net_migration_households"].rank(pct=True))
            if sub["working_age_momentum"].notna().sum() >= 3:
                components.append(sub["working_age_momentum"].rank(pct=True))
            if sub["household_formation_change"].notna().sum() >= 3:
                components.append(sub["household_formation_change"].rank(pct=True))
            if sub["pop_age_25_44_pct"].notna().sum() >= 3:
                components.append(sub["pop_age_25_44_pct"].rank(pct=True))

            if components:
                combined.loc[year_mask, "demographic_momentum_score"] = pd.concat(
                    components, axis=1
                ).mean(axis=1)

        # USPS vacancy (if configured)
        usps_df = fetch_usps_vacancy_by_county(years_to_fetch)
        combined = merge_usps_vacancy(combined, usps_df)
        low_vacancy_df = fetch_low_vacancy_counties()
        combined = merge_low_vacancy_counts(combined, low_vacancy_df)
        combined = apply_vacancy_predictions(combined)
        combined.replace([np.inf, -np.inf], pd.NA, inplace=True)

        store_demographic_data(combined)

        log_refresh(
            layer_name="layer5_demographic_momentum",
            data_source="ACS+IRS",
            status="success",
            records_processed=len(combined),
            records_inserted=len(combined),
            metadata={"years": sorted(combined["data_year"].unique().tolist())},
        )

        logger.info("✓ Layer 5 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 5 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer5_demographic_momentum",
            data_source="ACS+IRS",
            status="failed",
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    main()
