"""
Maryland Viability Atlas - Layer 3: School System Trajectory
Ingests school enrollment trends using NCES CCD Student Membership data.

Data Sources:
- NCES CCD Student Membership files (052 series) - actual enrollment counts
- MSDE enrollment exports (marylandedu) - preferred source

Signals Produced:
- Total enrollment (LEA -> county)
- Enrollment 3-year change percent
- Enrollment momentum score (percentile across counties)

Note: Capital investment and capacity strain require CIP data and remain NULL.
Enforces MAX 5-year lookback from current date (January 29, 2026).
"""

import sys
import zipfile
import re
import argparse
import io
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import numpy as np
from sqlalchemy import text
import requests

# Ensure project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.ingest.write_mode import is_append_mode
from src.utils.data_sources import download_file
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

CACHE_DIR = Path("data/cache/schools")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# NCES CCD Data page - contains direct download links
CCD_LEA_BROWSE_URL = "https://nces.ed.gov/ccd/pau_rev.asp"
CCD_BASE_URL = "https://nces.ed.gov/ccd/"
MSDE_REPORTCARD_DOWNLOAD_BASE_URL = "https://reportcard.msde.maryland.gov/Download"


def _normalize_colname(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def _parse_year_from_text(value: str) -> Optional[int]:
    if value is None:
        return None
    text = str(value)
    years = [int(y) for y in re.findall(r"(?:19|20)\d{2}", text)]
    if years:
        return max(years)
    pair = re.findall(r"\b(\d{2})(\d{2})\b", text)
    if pair:
        end_year = int(pair[-1][1])
        return 2000 + end_year if end_year < 90 else 1900 + end_year
    return None


def _load_json_mapping(value: Optional[str], path: Optional[Path]) -> Dict[int, str]:
    if value:
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return {int(k): str(v) for k, v in parsed.items() if str(k).isdigit() and str(v)}
        except Exception:
            return {}
    if path and path.exists():
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                return {int(k): str(v) for k, v in parsed.items() if str(k).isdigit() and str(v)}
        except Exception:
            return {}
    return {}


def _load_msde_enrollment_manifest() -> Dict[int, str]:
    configured = settings.MSDE_ENROLLMENT_MANIFEST_PATH
    default_path = Path("config/msde_enrollment_manifest.json")
    path = Path(configured) if configured else default_path
    if path.exists():
        return _load_json_mapping(settings.MSDE_ENROLLMENT_FILE_MAP, path)
    return _load_json_mapping(settings.MSDE_ENROLLMENT_FILE_MAP, None)


def _load_nces_membership_manifest() -> Dict[int, str]:
    configured = settings.NCES_MEMBERSHIP_MANIFEST_PATH
    default_path = Path("config/nces_membership_manifest.json")
    path = Path(configured) if configured else default_path
    if not path.exists():
        return {}
    return _load_json_mapping(None, path)


def _github_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "maryland-housing-ingest",
    }
    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _list_github_msde_files(latest_year: Optional[int] = None) -> List[Tuple[str, str, Optional[int]]]:
    repo = settings.MSDE_ENROLLMENT_GITHUB_REPO
    path = settings.MSDE_ENROLLMENT_GITHUB_PATH.strip("/")
    branch = settings.MSDE_ENROLLMENT_GITHUB_BRANCH or "main"
    if not repo or not path:
        return []

    api_url = f"https://api.github.com/repos/{repo}/contents/{path}"
    try:
        resp = requests.get(api_url, headers=_github_headers(), params={"ref": branch}, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"GitHub MSDE listing failed ({resp.status_code})")
            return []
        payload = resp.json()
    except Exception as e:
        logger.warning(f"GitHub MSDE listing failed: {e}")
        return []

    if not isinstance(payload, list):
        return []

    allowed_ext = (".csv", ".csv.gz", ".zip", ".xlsx", ".xls")
    scored_by_year: Dict[int, Tuple[int, str, str]] = {}
    for item in payload:
        if item.get("type") != "file":
            continue
        name = str(item.get("name", ""))
        lower = name.lower()
        if not lower.endswith(allowed_ext):
            continue
        if not any(token in lower for token in ["enroll", "member"]):
            continue
        year = _parse_year_from_text(name)
        if year is None:
            continue
        if latest_year and year < latest_year - 6:
            continue
        download_url = item.get("download_url")
        if not download_url:
            continue
        score = 0
        if "enrollment" in lower:
            score += 3
        if "by_race" in lower:
            score += 2
        if "race" in lower:
            score += 1
        existing = scored_by_year.get(year)
        if existing is None or score > existing[0]:
            scored_by_year[year] = (score, name, download_url)

    files: List[Tuple[str, str, Optional[int]]] = []
    for year, (_, name, download_url) in sorted(scored_by_year.items(), reverse=True):
        files.append((name, download_url, year))
    return files


def _download_msde_enrollment_files(latest_year: Optional[int] = None) -> List[Path]:
    if settings.MSDE_ENROLLMENT_PATH:
        path = Path(settings.MSDE_ENROLLMENT_PATH)
        if path.exists():
            return [path]
        logger.warning(f"MSDE_ENROLLMENT_PATH not found: {path}")

    if settings.MSDE_ENROLLMENT_URL:
        url = settings.MSDE_ENROLLMENT_URL
        filename = Path(urlparse(url).path).name or "msde_enrollment.csv"
        target = CACHE_DIR / filename
        if not target.exists():
            ok = download_file(url, str(target))
            if not ok:
                raise RuntimeError(f"Failed to download MSDE enrollment source: {url}")
        return [target]

    # First use local cached exports if present.
    cached = sorted(CACHE_DIR.glob("Enrollment_By_Race_*.csv"))
    if cached:
        return cached

    manifest = _load_msde_enrollment_manifest()
    if manifest:
        base_url = settings.MSDE_ENROLLMENT_RAW_BASE_URL.rstrip("/")
        paths: List[Path] = []
        for _, source in sorted(manifest.items()):
            source_url = source if source.startswith("http") else f"{base_url}/{source}"
            filename = Path(urlparse(source_url).path).name or "msde_enrollment.csv"
            target = CACHE_DIR / filename
            if not target.exists():
                ok = download_file(source_url, str(target))
                if not ok:
                    continue
            paths.append(target)
        if paths:
            return paths

    if not settings.MSDE_ENROLLMENT_AUTO_DISCOVER:
        return []

    github_files = _list_github_msde_files(latest_year=latest_year)
    paths = []
    for name, url, _ in github_files:
        target = CACHE_DIR / name
        if not target.exists():
            ok = download_file(url, str(target))
            if not ok:
                continue
        paths.append(target)
    return paths


def _read_msde_enrollment_file(source_path: Path) -> pd.DataFrame:
    suffix = source_path.suffix.lower()
    if suffix in [".csv", ".txt", ".gz"]:
        return pd.read_csv(source_path, compression="infer", dtype=str, low_memory=False)
    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(source_path, dtype=str)
    if suffix == ".zip":
        with zipfile.ZipFile(source_path, "r") as zf:
            members = [n for n in zf.namelist() if n.lower().endswith((".csv", ".csv.gz", ".xlsx", ".xls"))]
            if not members:
                raise RuntimeError(f"No tabular file found in {source_path.name}")
            member = members[0]
            with zf.open(member) as f:
                if member.lower().endswith(".csv"):
                    return pd.read_csv(f, dtype=str, low_memory=False)
                if member.lower().endswith(".csv.gz"):
                    return pd.read_csv(io.BytesIO(f.read()), compression="gzip", dtype=str, low_memory=False)
                return pd.read_excel(f, dtype=str)
    raise RuntimeError(f"Unsupported MSDE file type: {source_path.suffix}")


def _normalize_name(name: str) -> str:
    """Normalize county name for matching"""
    if not isinstance(name, str):
        return ""
    n = name.lower()
    for token in ["public schools", "public school system", "public school",
                  "school district", "county", "city", "board of education"]:
        n = n.replace(token, " ")
    n = n.replace("'", " ").replace("-", " ")
    n = " ".join(n.split())
    return n


def _build_county_name_map() -> Dict[str, str]:
    """Build mapping from normalized county names to FIPS codes"""
    mapping = {}
    for fips, name in MD_COUNTY_FIPS.items():
        mapping[_normalize_name(name.replace("County", "").replace("City", ""))] = fips
    return mapping


def _resolve_membership_zip_url(year: int) -> Optional[str]:
    """
    Resolve the Student Membership ZIP download URL for a given school year.

    Uses CCD 052 series (Student Membership) instead of 029 series (Directory).

    Args:
        year: School year end (e.g., 2022 for 2021-22)

    Returns:
        Full download URL or None if not found
    """
    manifest = _load_nces_membership_manifest()
    if manifest:
        manifest_url = manifest.get(year)
        if manifest_url:
            if manifest_url.startswith("http"):
                logger.info(f"Resolved NCES membership URL for {year} from manifest")
                return manifest_url
            logger.warning(f"Invalid NCES manifest URL for {year}: {manifest_url}")
            return None
        logger.info(f"No NCES manifest URL for {year}; falling back to NCES page scrape")

    try:
        resp = requests.get(CCD_LEA_BROWSE_URL, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to load CCD browse page: {e}")
        return None

    # Convert year to YYZZ format (e.g., 2022 -> 2122 for 2021-22)
    prev_year = year - 1
    yy_format = f"{str(prev_year)[-2:]}{str(year)[-2:]}"

    # Look for Student Membership files: Data/zip/ccd_lea_052_2122_*.zip
    # Pattern matches membership series (052)
    pattern = rf"Data/zip/ccd_lea_052_{yy_format}_[^\"'<>]+\.zip"
    matches = re.findall(pattern, resp.text, flags=re.IGNORECASE)

    if not matches:
        logger.warning(f"No Student Membership ZIP found for {year} (pattern: 052_{yy_format})")
        return None

    # Take first match and construct full URL
    relative_path = matches[0]
    full_url = CCD_BASE_URL + relative_path

    logger.info(f"Resolved Student Membership URL for {year}: {full_url}")
    return full_url


def _download_membership_file(year: int) -> Path:
    """
    Download CCD Student Membership file for a given year.

    Args:
        year: School year end (e.g., 2022 for 2021-22)

    Returns:
        Path to downloaded ZIP file
    """
    target = CACHE_DIR / f"ccd_membership_{year}.zip"
    if not target.exists():
        zip_url = _resolve_membership_zip_url(year)
        if not zip_url:
            raise RuntimeError(f"No Student Membership file found for year {year}")
        logger.info(f"Downloading Student Membership file for {year}")
        ok = download_file(zip_url, str(target))
        if not ok:
            raise RuntimeError(f"Failed to download Student Membership file for {year}")
    return target


def _read_membership_zip(zip_path: Path) -> pd.DataFrame:
    """Read membership data from ZIP file with encoding fallback"""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        candidates = [n for n in zf.namelist() if n.lower().endswith(('.csv', '.txt'))]
        if not candidates:
            raise RuntimeError(f"No data file found in {zip_path.name}")

        # Prefer LEA-level membership files
        preferred = [n for n in candidates if "lea" in n.lower() or "agency" in n.lower()]
        data_name = preferred[0] if preferred else candidates[0]

        # Try multiple encodings (NCES files vary)
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None

        for encoding in encodings_to_try:
            try:
                with zf.open(data_name) as f:
                    df = pd.read_csv(f, dtype=str, encoding=encoding, low_memory=False)
                    logger.info(f"Successfully read {data_name} with {encoding} encoding")
                    break
            except Exception:
                continue

        if df is None:
            raise RuntimeError(f"Could not read {data_name} with any encoding")

        return df


def _filter_md_lea(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for Maryland LEAs only"""
    state_cols = [c for c in df.columns if c.upper() in ['ST', 'STABBR', 'STATE', 'STATE_ABBR']]
    if state_cols:
        df = df[df[state_cols[0]].astype(str).str.upper() == 'MD'].copy()

    fips_cols = [c for c in df.columns if c.upper() in ['FIPST', 'ST_FIPS', 'STATE_FIPS']]
    if fips_cols:
        df = df[df[fips_cols[0]].astype(str).str.zfill(2) == '24'].copy()

    return df


def _extract_total_enrollment(df: pd.DataFrame) -> pd.Series:
    """
    Extract total enrollment from membership file.

    Looks for common field names:
    - MEMBER (total membership)
    - TOTAL / TOTMEM (total count)
    - Grade sums (if individual grades present)
    """
    # Common total membership fields
    total_fields = ['MEMBER', 'TOTAL', 'TOTMEM', 'TOTAL_STUDENTS',
                   'ENROLLMENT', 'TOTAL_ENROLLMENT', 'MEMBERSHIP']

    for field in total_fields:
        matching = [c for c in df.columns if c.upper() == field.upper()]
        if matching:
            logger.info(f"Using {matching[0]} field for total enrollment")
            return pd.to_numeric(df[matching[0]], errors='coerce').fillna(0)

    # Try summing individual grade fields (PK-12)
    grade_fields = [c for c in df.columns if re.match(r'^(G|GRADE)?(PK|KG|0[1-9]|1[0-2])$', c.upper())]
    if grade_fields:
        logger.info(f"Computing total from {len(grade_fields)} grade fields")
        grade_totals = df[grade_fields].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)
        return grade_totals

    # Fallback: use any numeric column with "total" or "member" in name
    total_cols = [c for c in df.columns if 'total' in c.lower() or 'member' in c.lower()]
    if total_cols:
        logger.warning(f"Using fallback column: {total_cols[0]}")
        return pd.to_numeric(df[total_cols[0]], errors='coerce').fillna(0)

    raise RuntimeError("No enrollment/membership fields found in data")


def _map_lea_to_county(df: pd.DataFrame) -> pd.DataFrame:
    """Map LEAs to Maryland counties using name matching"""
    name_cols = [c for c in df.columns if c.lower() in ['lea_name', 'leaid_name', 'name',
                                                         'agency_name', 'lea', 'leanm']]
    if not name_cols:
        return df

    name_col = name_cols[0]
    county_map = _build_county_name_map()

    def map_name(name: str) -> str:
        n = _normalize_name(name)
        if 'baltimore city' in n:
            return '24510'
        if 'baltimore county' in n or 'baltimore co' in n:
            return '24005'
        for county_norm, fips in county_map.items():
            if county_norm and county_norm in n:
                return fips
        return ""

    df = df.copy()
    df['fips_code'] = df[name_col].apply(map_name)
    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]
    return df


def _build_enrollment_timeseries_nces(latest_year: Optional[int] = None) -> pd.DataFrame:
    """
    Build enrollment timeseries for Maryland counties using Student Membership data.

    Enforces MAX 5-year lookback from today (January 29, 2026).
    For school year 2025 (2024-25), this means years 2021-2025.

    Args:
        latest_year: Latest school year end (default: 2025)

    Returns:
        DataFrame with columns: fips_code, data_year, total_enrollment, schools_total
    """
    records = []

    # Enforce 5-year max lookback from today (Jan 29, 2026)
    if latest_year is None:
        current_year = 2025  # School year 2024-25
    else:
        current_year = min(latest_year, 2025)  # Cap at 2025

    # Get last 5 school years
    min_year = current_year - 4
    years_to_try = list(range(min_year, current_year + 1))

    logger.info(f"Fetching Student Membership data for school years: {years_to_try}")

    for year in years_to_try:
        logger.info(f"Processing school year {year-1}-{str(year)[-2:]}...")
        try:
            zip_path = _download_membership_file(year)
            df = _read_membership_zip(zip_path)
            df = _filter_md_lea(df)
        except Exception as e:
            logger.warning(f"Skipping year {year}: {e}")
            continue

        # Try county code mapping first
        cols = {c.lower(): c for c in df.columns}
        county_col = None
        for cand in ['conum', 'county', 'county_code', 'cnty', 'lea_county']:
            if cand in cols:
                county_col = cols[cand]
                break

        if county_col:
            # Direct county code mapping
            state_col = cols.get('fipst') or cols.get('st_fips')
            if state_col:
                df['state_fips'] = df[state_col].astype(str).str.zfill(2)
            else:
                df['state_fips'] = '24'

            df = df[df['state_fips'] == '24'].copy()
            df[county_col] = df[county_col].astype(str).str.zfill(3)
            df['fips_code'] = df['state_fips'] + df[county_col]
            df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]
        else:
            # Fall back to name matching
            df = _map_lea_to_county(df)

        if df.empty:
            logger.warning(f"No Maryland LEAs found for year {year}")
            continue

        # Extract total enrollment
        enrollment = _extract_total_enrollment(df)
        df['total_enrollment'] = enrollment

        # Aggregate to county level
        agg = df.groupby('fips_code', as_index=False).agg(
            total_enrollment=('total_enrollment', 'sum'),
            schools_total=('fips_code', 'size')
        )
        agg['data_year'] = year
        records.append(agg)

        logger.info(f"✓ Loaded {len(agg)} counties for year {year}")

    if not records:
        logger.warning("No enrollment data loaded")
        return pd.DataFrame()

    return pd.concat(records, ignore_index=True)


def _build_enrollment_timeseries_msde(latest_year: Optional[int] = None) -> pd.DataFrame:
    try:
        source_paths = _download_msde_enrollment_files(latest_year=latest_year)
    except Exception as e:
        logger.warning(f"Failed to load MSDE enrollment data: {e}")
        return pd.DataFrame()

    if not source_paths:
        return pd.DataFrame()

    frames = []
    for source_path in source_paths:
        try:
            df_part = _read_msde_enrollment_file(source_path)
        except Exception as e:
            logger.warning(f"Failed to read MSDE file {source_path.name}: {e}")
            continue
        df_part.columns = [_normalize_colname(c) for c in df_part.columns]
        year_guess = _parse_year_from_text(source_path.name)
        if year_guess and "year" not in df_part.columns:
            df_part["year"] = year_guess
        frames.append(df_part)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    cols = list(df.columns)

    def _pick(candidates: List[str]) -> Optional[str]:
        for cand in candidates:
            if cand in cols:
                return cand
        return None

    year_col = _pick(["year", "school_year", "data_year"])
    lea_col = _pick(["lea_name", "lss_name", "lea", "local_school_system", "school_system", "district"])
    student_col = _pick(["enrolled_count", "student_count", "enrollment_count", "enrollment", "membership", "member"])
    race_col = _pick(["race", "race_ethnicity"])
    school_col = _pick(["school", "school_number", "school_id"])

    if not year_col or not lea_col or not student_col:
        logger.warning("MSDE enrollment file missing required columns (year/lea/student count)")
        return pd.DataFrame()

    # Prefer aggregate race rows when present; keep all rows otherwise.
    if race_col:
        race = df[race_col].astype(str).str.lower().str.strip()
        if race.eq("all").any():
            df = df[race.eq("all")].copy()

    df["data_year"] = pd.to_numeric(df[year_col], errors="coerce")
    df = df[df["data_year"].notna()].copy()
    if latest_year is not None:
        df = df[df["data_year"] <= latest_year]
    if df.empty:
        return pd.DataFrame()

    df["total_enrollment"] = pd.to_numeric(df[student_col], errors="coerce")
    df = df[df["total_enrollment"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    county_map = _build_county_name_map()

    def _map_lea_name(name: str) -> str:
        normalized = _normalize_name(name)
        if "baltimore city" in normalized:
            return "24510"
        if "baltimore county" in normalized or "baltimore co" in normalized:
            return "24005"
        for county_norm, fips in county_map.items():
            if county_norm and county_norm in normalized:
                return fips
        return ""

    df["fips_code"] = df[lea_col].astype(str).apply(_map_lea_name)
    df = df[df["fips_code"].isin(MD_COUNTY_FIPS.keys())]
    if df.empty:
        return pd.DataFrame()

    if school_col:
        df["_school"] = df[school_col].astype(str).str.strip()
        schools = df.groupby(["fips_code", "data_year"])["_school"].nunique(dropna=True).rename("schools_total")
    else:
        schools = df.groupby(["fips_code", "data_year"]).size().rename("schools_total")

    enroll = df.groupby(["fips_code", "data_year"])["total_enrollment"].sum(min_count=1)
    agg = pd.concat([enroll, schools], axis=1).reset_index()
    agg["schools_total"] = pd.to_numeric(agg["schools_total"], errors="coerce").fillna(0)
    return agg


def _build_enrollment_timeseries(latest_year: Optional[int] = None) -> pd.DataFrame:
    msde = _build_enrollment_timeseries_msde(latest_year=latest_year)
    if not msde.empty:
        logger.info("Using MSDE enrollment source")
        return msde
    logger.info("MSDE unavailable; falling back to NCES membership")
    return _build_enrollment_timeseries_nces(latest_year=latest_year)


def calculate_school_indicators(latest_year: Optional[int] = None) -> pd.DataFrame:
    """
    Calculate school trajectory indicators for Maryland counties.

    Args:
        latest_year: Latest school year to include (default: 2025)

    Returns:
        DataFrame with school indicators by county and year
    """
    # Get enrollment timeseries
    df = _build_enrollment_timeseries(latest_year=latest_year)

    if df.empty:
        return pd.DataFrame()

    # Compute 3-year enrollment change
    df = df.sort_values(['fips_code', 'data_year'])

    def compute_3yr_change(group):
        group = group.sort_values('data_year')

        for idx, row in group.iterrows():
            year = row['data_year']
            baseline_year = year - 3

            baseline_rows = group[group['data_year'] == baseline_year]
            if not baseline_rows.empty:
                baseline_val = baseline_rows.iloc[0]['total_enrollment']
                current_val = row['total_enrollment']

                if baseline_val > 0:
                    pct_change = ((current_val - baseline_val) / baseline_val) * 100
                    group.loc[idx, 'enrollment_3yr_change_pct'] = pct_change

        return group

    df = df.groupby('fips_code', group_keys=False).apply(compute_3yr_change)

    # Compute enrollment momentum score (percentile rank within each year)
    def compute_momentum_score(year_group):
        if 'enrollment_3yr_change_pct' in year_group.columns:
            year_group['enrollment_momentum_score'] = year_group['enrollment_3yr_change_pct'].rank(pct=True)
        return year_group

    df = df.groupby('data_year', group_keys=False).apply(compute_momentum_score)

    # Add NULL columns for unimplemented indicators
    df['capital_investment_score'] = None
    df['capacity_strain_indicator'] = None

    return df


def store_school_data(df: pd.DataFrame):
    """Store school trajectory data in database"""
    logger.info(f"Storing {len(df)} school trajectory records")
    append_mode = is_append_mode()

    with get_db() as db:
        if not append_mode:
            # Bootstrap/overwrite mode replaces current contents.
            db.execute(text("DELETE FROM layer3_school_trajectory"))

        if append_mode:
            insert_sql = text("""
                INSERT INTO layer3_school_trajectory (
                    fips_code, data_year, total_enrollment, schools_total,
                    enrollment_3yr_change_pct, enrollment_momentum_score,
                    capital_investment_score, capacity_strain_indicator
                ) VALUES (
                    :fips_code, :data_year, :total_enrollment, :schools_total,
                    :enrollment_3yr_change_pct, :enrollment_momentum_score,
                    :capital_investment_score, :capacity_strain_indicator
                )
                ON CONFLICT (fips_code, data_year) DO NOTHING
            """)
        else:
            insert_sql = text("""
                INSERT INTO layer3_school_trajectory (
                    fips_code, data_year, total_enrollment, schools_total,
                    enrollment_3yr_change_pct, enrollment_momentum_score,
                    capital_investment_score, capacity_strain_indicator
                ) VALUES (
                    :fips_code, :data_year, :total_enrollment, :schools_total,
                    :enrollment_3yr_change_pct, :enrollment_momentum_score,
                    :capital_investment_score, :capacity_strain_indicator
                )
                ON CONFLICT (fips_code, data_year)
                DO UPDATE SET
                    total_enrollment = EXCLUDED.total_enrollment,
                    schools_total = EXCLUDED.schools_total,
                    enrollment_3yr_change_pct = EXCLUDED.enrollment_3yr_change_pct,
                    enrollment_momentum_score = EXCLUDED.enrollment_momentum_score,
                    capital_investment_score = EXCLUDED.capital_investment_score,
                    capacity_strain_indicator = EXCLUDED.capacity_strain_indicator,
                    updated_at = CURRENT_TIMESTAMP
            """)

        # Insert new records
        for _, row in df.iterrows():
            db.execute(
                insert_sql,
                {
                    'fips_code': row['fips_code'],
                    'data_year': int(row['data_year']),
                    'total_enrollment': float(row['total_enrollment']) if pd.notna(row['total_enrollment']) else None,
                    'schools_total': int(row['schools_total']) if pd.notna(row['schools_total']) else None,
                    'enrollment_3yr_change_pct': float(row['enrollment_3yr_change_pct']) if pd.notna(row.get('enrollment_3yr_change_pct')) else None,
                    'enrollment_momentum_score': float(row['enrollment_momentum_score']) if pd.notna(row.get('enrollment_momentum_score')) else None,
                    'capital_investment_score': None,
                    'capacity_strain_indicator': None,
                }
            )

        db.commit()

    logger.info("✓ School data stored successfully")


def main():
    """Main execution for Layer 3 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 3: SCHOOL TRAJECTORY INGESTION")
        logger.info("=" * 60)

        parser = argparse.ArgumentParser(description='Ingest Layer 3 School Trajectory data')
        parser.add_argument('--year', type=int, default=2025,
                          help='Latest school year to fetch (default: 2025, max: 2025)')
        args = parser.parse_args()

        # Enforce max year cap (5-year lookback from Jan 2026)
        year = min(args.year, 2025)
        logger.info(f"Fetching data for school year ending {year} (last 5 years: {year-4} to {year})")

        df = calculate_school_indicators(latest_year=year)

        if df.empty:
            logger.error("No school data to store")
            log_refresh(
                layer_name="layer3_school_trajectory",
                data_source="NCES CCD Student Membership (052 series)",
                status="failed",
                error_message="No records produced",
            )
            return

        store_school_data(df)

        years_loaded = sorted(df['data_year'].unique().tolist())
        log_refresh(
            layer_name="layer3_school_trajectory",
            data_source="NCES CCD Student Membership (052 series)",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={
                "years": years_loaded,
                "latest_year": year,
                "counties": len(df['fips_code'].unique())
            }
        )

        logger.info("=" * 60)
        logger.info(f"✓ Layer 3 ingestion complete - {len(years_loaded)} years loaded")
        logger.info(f"  Years: {years_loaded}")
        logger.info(f"  Counties: {len(df['fips_code'].unique())}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Layer 3 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer3_school_trajectory",
            data_source="NCES CCD Student Membership (052 series)",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
