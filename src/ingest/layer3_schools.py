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

# NCES CCD preliminary directory pages (recent years)
# We resolve the actual ZIP download link at runtime.
CCD_DIRECTORY_PAGES = {
    2025: "https://nces.ed.gov/use-work/resource-library/data/data-file/2024-25-common-core-data-ccd-preliminary-directory-files",
    2024: "https://nces.ed.gov/use-work/resource-library/data/data-file/2023-24-common-core-data-ccd-preliminary-directory-files",
    2023: "https://nces.ed.gov/use-work/resource-library/data/data-file/2022-23-common-core-data-ccd-preliminary-directory-files",
    2022: "https://nces.ed.gov/use-work/resource-library/data/data-file/2021-22-common-core-data-ccd-preliminary-files",
    2021: "https://nces.ed.gov/use-work/resource-library/data/data-file/2020-21-common-core-data-ccd-preliminary-files",
}


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


def _resolve_ccd_zip_url(page_url: str) -> Optional[str]:
    try:
        resp = requests.get(page_url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to load CCD page {page_url}: {e}")
        return None

    links = re.findall(r"https?://[^\"']+\\.zip", resp.text)
    if not links:
        return None

    for link in links:
        if "asset_builder_data" in link or "/ccd/Data/zip/" in link:
            return link
    return links[0]


def _download_ccd_file(year: int, page_url: str) -> Path:
    target = CACHE_DIR / f"ccd_directory_{year}.zip"
    if not target.exists():
        zip_url = _resolve_ccd_zip_url(page_url)
        if not zip_url:
            raise RuntimeError(f"No download link found for CCD page {page_url}")
        logger.info(f"Downloading CCD directory file for {year}")
        ok = download_file(zip_url, str(target))
        if not ok:
            raise RuntimeError(f"Failed to download CCD file for {year}")
    return target


def _read_membership_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, 'r') as zf:
        candidates = [n for n in zf.namelist() if n.lower().endswith(('.csv', '.txt'))]
        if not candidates:
            raise RuntimeError(f"No data file found in {zip_path.name}")
        # Prefer LEA-level files when available
        preferred = [n for n in candidates if "lea" in n.lower() or "agency" in n.lower()]
        data_name = preferred[0] if preferred else candidates[0]
        with zf.open(data_name) as f:
            # Try common delimiters
            try:
                df = pd.read_csv(f, dtype=str)
            except Exception:
                f.seek(0)
                try:
                    df = pd.read_csv(f, dtype=str, sep='|')
                except Exception:
                    f.seek(0)
                    df = pd.read_csv(f, dtype=str, sep='\t')
    return df


def _filter_md_lea(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    if 'fipst' in cols:
        return df[df[cols['fipst']].astype(str).str.zfill(2) == '24']
    if 'state' in cols:
        return df[df[cols['state']].astype(str).str.upper() == 'MD']
    if 'stabbr' in cols:
        return df[df[cols['stabbr']].astype(str).str.upper() == 'MD']
    return df


def _extract_enrollment(df: pd.DataFrame) -> pd.Series:
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


def _build_enrollment_timeseries() -> pd.DataFrame:
    records = []

    current_year = datetime.utcnow().year
    min_year = current_year - 4

    for year, page_url in CCD_DIRECTORY_PAGES.items():
        if year < min_year or year > current_year:
            continue
        try:
            zip_path = _download_ccd_file(year, page_url)
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


def calculate_school_indicators() -> pd.DataFrame:
    """Compute enrollment trends from CCD LEA membership data."""
    ts = _build_enrollment_timeseries()
    if ts.empty:
        return pd.DataFrame()

    ts = ts.sort_values(['fips_code', 'data_year']).reset_index(drop=True)

    # Compute 3-year change percent using year-2 baseline when available
    ts['enrollment_3yr_change_pct'] = pd.NA

    for fips in ts['fips_code'].unique():
        sub = ts[ts['fips_code'] == fips].copy()
        year_to_val = {row['data_year']: row['total_enrollment'] for _, row in sub.iterrows()}
        for idx, row in sub.iterrows():
            year = row['data_year']
            baseline_year = year - 2 if (year - 2) in year_to_val else None
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

        df = calculate_school_indicators()

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
            metadata={"years": sorted(df['data_year'].unique().tolist())}
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
