"""
Maryland Viability Atlas - Layer 3: School System Trajectory
Ingests school enrollment trends and capacity indicators using real data.

Data Sources:
- NCES CCD LEA files (recent directory/universe releases; enrollment extracted when present)

Signals Produced:
- Total enrollment (LEA -> county)
- Enrollment 3-year change percent
- Enrollment momentum score (percentile across counties)

Note: Capital investment and capacity strain require CIP data and remain NULL.
"""

import sys
import zipfile
import re
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text
import requests

# Ensure project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import get_settings, MD_COUNTY_FIPS
from config.database import get_db, log_refresh
from src.utils.data_sources import download_file
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

CACHE_DIR = Path("data/cache/schools")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# NCES CCD LEA browse page - contains direct download links
# Pattern: ccd_lea_029_{YY}{YY}_w_{version}_{date}.zip
CCD_LEA_BROWSE_URL = "https://nces.ed.gov/ccd/pau_rev.asp"
CCD_BASE_URL = "https://nces.ed.gov/ccd/"


def _normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.lower()
    for token in ["public schools", "public school system", "public school", "school district", "county", "city", "board of education"]:
        n = n.replace(token, " ")
    n = n.replace("'", " ").replace("-", " ")
    n = " ".join(n.split())
    return n


def _build_county_name_map() -> Dict[str, str]:
    mapping = {}
    for fips, name in MD_COUNTY_FIPS.items():
        mapping[_normalize_name(name.replace("County", "").replace("City", ""))] = fips
    return mapping


def _resolve_ccd_lea_zip_url(year: int) -> Optional[str]:
    """
    Resolve the LEA ZIP download URL for a given school year from the browse page.

    Args:
        year: School year (e.g., 2022 for 2021-22)

    Returns:
        Full download URL or None if not found
    """
    try:
        resp = requests.get(CCD_LEA_BROWSE_URL, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to load CCD LEA browse page: {e}")
        return None

    # Convert year to YYZZ format (e.g., 2022 -> 2122 for 2021-22)
    prev_year = year - 1
    yy_format = f"{str(prev_year)[-2:]}{str(year)[-2:]}"

    # Look for patterns like: Data/zip/ccd_lea_029_2122_w_*.zip
    # These are relative paths on the page
    pattern = rf"Data/zip/ccd_lea_029_{yy_format}_[^\"'<>]+\.zip"
    matches = re.findall(pattern, resp.text)

    if not matches:
        logger.warning(f"No LEA ZIP found for year {year} (pattern: {yy_format})")
        return None

    # Take first match and construct full URL
    relative_path = matches[0]
    full_url = CCD_BASE_URL + relative_path

    logger.info(f"Resolved CCD LEA URL for {year}: {full_url}")
    return full_url


def _download_ccd_file(year: int) -> Path:
    """
    Download CCD LEA file for a given year.

    Args:
        year: School year (e.g., 2022 for 2021-22 school year)

    Returns:
        Path to downloaded ZIP file
    """
    target = CACHE_DIR / f"ccd_lea_{year}.zip"
    if not target.exists():
        zip_url = _resolve_ccd_lea_zip_url(year)
        if not zip_url:
            raise RuntimeError(f"No CCD LEA download link found for year {year}")
        logger.info(f"Downloading CCD LEA file for {year}")
        ok = download_file(zip_url, str(target))
        if not ok:
            raise RuntimeError(f"Failed to download CCD LEA file for {year}")
    return target


def _read_membership_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, 'r') as zf:
        candidates = [n for n in zf.namelist() if n.lower().endswith(('.csv', '.txt'))]
        if not candidates:
            raise RuntimeError(f"No data file found in {zip_path.name}")
        # Prefer LEA-level files when available
        preferred = [n for n in candidates if "lea" in n.lower() or "agency" in n.lower()]
        data_name = preferred[0] if preferred else candidates[0]

        # Try multiple encodings (NCES files can use different encodings)
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None

        for encoding in encodings_to_try:
            try:
                with zf.open(data_name) as f:
                    # Try common delimiters
                    try:
                        df = pd.read_csv(f, dtype=str, encoding=encoding)
                        break
                    except Exception:
                        pass

                    # Try pipe delimiter
                    with zf.open(data_name) as f:
                        try:
                            df = pd.read_csv(f, dtype=str, sep='|', encoding=encoding)
                            break
                        except Exception:
                            pass

                    # Try tab delimiter
                    with zf.open(data_name) as f:
                        try:
                            df = pd.read_csv(f, dtype=str, sep='\t', encoding=encoding)
                            break
                        except Exception:
                            pass
            except Exception:
                continue

        if df is None or df.empty:
            raise RuntimeError(f"Failed to parse data file {data_name} with any encoding/delimiter")

    return df


def _filter_md_lea(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    if 'fipst' in cols:
        return df[df[cols['fipst']].astype(str).str.zfill(2) == '24'].copy()
    if 'state' in cols:
        return df[df[cols['state']].astype(str).str.upper() == 'MD'].copy()
    if 'stabbr' in cols:
        return df[df[cols['stabbr']].astype(str).str.upper() == 'MD'].copy()
    return df


def _extract_enrollment(df: pd.DataFrame) -> "pd.Series[float]":
    candidates = ['member', 'total', 'total_membership', 'totmem', 'enrollment']
    for c in candidates:
        if c in [col.lower() for col in df.columns]:
            col = [col for col in df.columns if col.lower() == c][0]
            return pd.to_numeric(df[col], errors='coerce')

    # Fallback: sum numeric columns that look like grade counts (G??)
    grade_cols = [c for c in df.columns if c.lower().startswith('g') and c[1:].isdigit()]
    if grade_cols:
        return df[grade_cols].apply(pd.to_numeric, errors='coerce').sum(axis=1)

    return pd.Series([pd.NA] * len(df))


def _map_lea_to_county(df: pd.DataFrame) -> pd.DataFrame:
    name_cols = [c for c in df.columns if c.lower() in ['lea_name', 'leaid_name', 'name', 'agency_name', 'lea']]
    if not name_cols:
        return df

    name_col = name_cols[0]
    county_map = _build_county_name_map()

    def map_name(name: str) -> str:
        n = _normalize_name(name)
        if 'baltimore city' in n:
            return '24510'
        if 'baltimore county' in n:
            return '24005'
        for county_norm, fips in county_map.items():
            if county_norm and county_norm in n:
                return fips
        return ""

    df = df.copy()
    df['fips_code'] = df[name_col].apply(map_name)
    df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]
    return df


def _build_enrollment_timeseries(latest_year: Optional[int] = None, multi_year: bool = True) -> pd.DataFrame:
    records = []

    # Default to 2025 (NCES data typically lags by 1 year)
    if latest_year is None:
        current_year = 2025
    else:
        current_year = latest_year

    if multi_year:
        min_year = current_year - 4
        years_to_try = list(range(min_year, current_year + 1))
    else:
        years_to_try = [current_year]

    logger.info(f"Attempting to fetch CCD data for years: {years_to_try}")

    for year in years_to_try:
        logger.info(f"Trying year {year}...")
        try:
            zip_path = _download_ccd_file(year)
            df = _read_membership_zip(zip_path)
            df = _filter_md_lea(df)
        except Exception as e:
            logger.warning(f"Skipping CCD {year}: {e}")
            continue

        # Map to county using available county code if present
        cols = {c.lower(): c for c in df.columns}
        county_col = None
        for cand in ['conum', 'county', 'county_code', 'cnty']:
            if cand in cols:
                county_col = cols[cand]
                break

        if county_col:
            state_col = cols.get('fipst')
            if state_col:
                df['state_fips'] = df[state_col].astype(str).str.zfill(2)
            else:
                state_abbr = cols.get('stabbr') or cols.get('state') or cols.get('st')
                if state_abbr:
                    df['state_fips'] = df[state_abbr].astype(str).str.upper().map({'MD': '24'})
                else:
                    df['state_fips'] = '24'

            df = df[df['state_fips'] == '24'].copy()
            df[county_col] = df[county_col].astype(str).str.zfill(3)
            df['fips_code'] = df['state_fips'] + df[county_col]
            df = df[df['fips_code'].isin(MD_COUNTY_FIPS.keys())]
        else:
            df = _map_lea_to_county(df)

        if df.empty:
            logger.warning(f"No Maryland LEA records found for {year}")
            continue

        enrollment = _extract_enrollment(df)
        df['total_enrollment'] = enrollment

        agg = df.groupby('fips_code', as_index=False).agg(
            total_enrollment=('total_enrollment', 'sum'),
            schools_total=('fips_code', 'size')
        )
        agg['data_year'] = year
        records.append(agg)

        logger.info(f"Loaded CCD directory data for {year}: {len(agg)} counties")

    if not records:
        return pd.DataFrame()

    return pd.concat(records, ignore_index=True)


def calculate_school_indicators(latest_year: Optional[int] = None, multi_year: bool = True) -> pd.DataFrame:
    """Compute enrollment trends from CCD LEA membership data."""
    ts = _build_enrollment_timeseries(latest_year=latest_year, multi_year=multi_year)
    if ts.empty:
        return pd.DataFrame()

    ts = ts.sort_values(['fips_code', 'data_year']).reset_index(drop=True)

    # Compute 3-year change percent using year-3 baseline when available
    ts['enrollment_3yr_change_pct'] = pd.NA

    for fips in ts['fips_code'].unique():
        sub = ts[ts['fips_code'] == fips].copy()
        year_to_val = {row['data_year']: row['total_enrollment'] for _, row in sub.iterrows()}
        for idx, row in sub.iterrows():
            year = row['data_year']
            baseline_year = year - 3 if (year - 3) in year_to_val else None
            if baseline_year is None:
                continue
            baseline = year_to_val[baseline_year]
            if pd.notna(baseline) and baseline != 0:
                change = (row['total_enrollment'] - baseline) / baseline * 100
                ts.loc[idx, 'enrollment_3yr_change_pct'] = round(change, 3)

    # Enrollment trend label
    def classify_trend(pct):
        if pd.isna(pct):
            return None
        if pct >= 1.0:
            return 'growing'
        if pct <= -1.0:
            return 'declining'
        return 'stable'

    ts['enrollment_trend'] = ts['enrollment_3yr_change_pct'].apply(classify_trend)

    # Momentum score (percentile rank by year)
    ts['enrollment_momentum_score'] = pd.NA
    for year in ts['data_year'].unique():
        year_mask = ts['data_year'] == year
        values = ts.loc[year_mask, 'enrollment_3yr_change_pct']
        if values.notna().sum() >= 3:
            ranks = values.rank(pct=True)
            ts.loc[year_mask, 'enrollment_momentum_score'] = ranks

    # Fill required columns with NULLs for unavailable data
    if 'schools_total' not in ts.columns:
        ts['schools_total'] = pd.NA
    ts['schools_elementary'] = pd.NA
    ts['schools_middle'] = pd.NA
    ts['schools_high'] = pd.NA
    ts['capital_investment_total'] = pd.NA
    ts['capital_per_student'] = pd.NA
    ts['expansion_projects_count'] = pd.NA
    ts['capacity_strain_indicator'] = pd.NA

    # Ensure expected columns
    columns = [
        'fips_code', 'data_year', 'total_enrollment', 'enrollment_3yr_change_pct',
        'enrollment_trend', 'schools_total', 'schools_elementary', 'schools_middle',
        'schools_high', 'capital_investment_total', 'capital_per_student',
        'expansion_projects_count', 'enrollment_momentum_score', 'capacity_strain_indicator'
    ]

    return ts[columns]


def store_school_data(df: pd.DataFrame):
    """Store school trajectory data in database."""
    logger.info(f"Storing {len(df)} school records")

    with get_db() as db:
        # Delete existing data for affected years
        years = df['data_year'].unique().tolist()
        delete_sql = text("""
            DELETE FROM layer3_school_trajectory
            WHERE data_year = ANY(:years)
        """)
        db.execute(delete_sql, {"years": years})

        insert_sql = text("""
            INSERT INTO layer3_school_trajectory (
                fips_code, data_year,
                total_enrollment, enrollment_3yr_change_pct, enrollment_trend,
                schools_total, schools_elementary, schools_middle, schools_high,
                capital_investment_total, capital_per_student, expansion_projects_count,
                enrollment_momentum_score, capacity_strain_indicator
            ) VALUES (
                :fips_code, :data_year,
                :total_enrollment, :enrollment_3yr_change_pct, :enrollment_trend,
                :schools_total, :schools_elementary, :schools_middle, :schools_high,
                :capital_investment_total, :capital_per_student, :expansion_projects_count,
                :enrollment_momentum_score, :capacity_strain_indicator
            )
        """)

        for _, row in df.iterrows():
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            db.execute(insert_sql, row_dict)

        db.commit()

    logger.info("✓ School data stored successfully")


def main():
    """Main execution for Layer 3 ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("LAYER 3: SCHOOL TRAJECTORY INGESTION")
        logger.info("=" * 60)

        parser = argparse.ArgumentParser(description='Ingest Layer 3 School Trajectory data')
        parser.add_argument('--year', type=int, default=2025, help='Latest year to fetch (default: 2025)')
        parser.add_argument('--single-year', action='store_true', help='Fetch only single year (default: multi-year)')
        args = parser.parse_args()

        logger.info(f"Year: {args.year}, Multi-year: {not args.single_year}")

        df = calculate_school_indicators(latest_year=args.year, multi_year=not args.single_year)

        if df.empty:
            logger.error("No school data to store (real data not available)")
            log_refresh(
                layer_name="layer3_school_trajectory",
                data_source="NCES CCD Directory Files",
                status="failed",
                error_message="No records produced",
            )
            return

        store_school_data(df)

        log_refresh(
            layer_name="layer3_school_trajectory",
            data_source="NCES CCD Directory Files",
            status="success",
            records_processed=len(df),
            records_inserted=len(df),
            metadata={
                "years": sorted(df['data_year'].unique().tolist()),
                "latest_year": args.year,
                "multi_year": not args.single_year
            }
        )

        logger.info("✓ Layer 3 ingestion complete")

    except Exception as e:
        logger.error(f"Layer 3 ingestion failed: {e}", exc_info=True)
        log_refresh(
            layer_name="layer3_school_trajectory",
            data_source="NCES CCD Directory Files",
            status="failed",
            error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
