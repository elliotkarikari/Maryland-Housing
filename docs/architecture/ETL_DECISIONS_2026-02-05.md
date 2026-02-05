# ETL Decision Log (2026-02-05)

This records the final source decisions based on data already present in this repo (`data/cache`) and current ingest code.

## Question 1: School Enrollment Source

**Decision:** Use MSDE enrollment cache/manifest as primary; NCES 052 as fallback only.

- Primary files already present:
  - `data/cache/schools/Enrollment_By_Race_2020.csv`
  - `data/cache/schools/Enrollment_By_Race_2021.csv`
  - `data/cache/schools/Enrollment_By_Race_2022.csv`
  - `data/cache/schools/Enrollment_By_Race_2023.csv`
  - `data/cache/schools/Enrollment_By_Race_2024.csv`
  - `data/cache/schools/Enrollment_By_Race_2025.csv`
- Manifest pinned to local filenames:
  - `config/msde_enrollment_manifest.json`
- Runtime mode:
  - deterministic (no scrape/index dependency)

## Question 2: Spatial Access Ingestion

**Decision:** Use local `access.zip` cache and parse city CSV.GZ files, filtering GEOIDs starting with `24` for Maryland.

- Confirmed source file:
  - `data/cache/mobility_v2/access.zip`
- Confirmed Maryland rows in cache:
  - `access/baltimore.csv.gz` and `access/washington.csv.gz` include MD GEOIDs.
- Runtime mode:
  - local-first, no Figshare discovery during normal runs

## Question 3: Floodplain Source (Layer 6)

**Decision:** Keep FEMA API path available, but default to skip until a Maryland local floodplain dataset is added.

- Current local state:
  - no floodplain shapefile/GeoPackage found under `data/cache`
- Current deterministic default:
  - `FEMA_SKIP_NFHL=true`
- Activation path:
  - set `FEMA_SKIP_NFHL=false` and provide `FEMA_NFHL_LOCAL_PATH` when file is available

## Question 4: Optional Source Strategy (API vs Local File)

**Decision:** Use both.

- Order:
  1. API first when configured
  2. File URL/local path fallback
  3. Graceful no-op if neither is configured
- Applied to:
  - USPS vacancy ingestion
  - floodplain enrichment
  - CIP follow-through fallback

## Question 5: CIP Follow-Through Source

**Decision:** DB evidence table first, optional local file second, no-op otherwise.

- Current local state:
  - no CIP extracted file found under `data/cache`
- Deterministic behavior:
  - no crash; optional enrichment remains blank unless configured

## Applied Runtime Defaults

- `.env` now uses:
  - `MSDE_ENROLLMENT_AUTO_DISCOVER=false`
  - `MSDE_ENROLLMENT_ALLOW_MSDE_INDEX=false`
  - `MSDE_ENROLLMENT_MANIFEST_PATH=config/msde_enrollment_manifest.json`
  - `NCES_MEMBERSHIP_MANIFEST_PATH=config/nces_membership_manifest.json`
  - `USPS_VACANCY_DATA_URL=` (cleared non-direct landing page URL)
  - `FEMA_SKIP_NFHL=true`
  - `FEMA_NFHL_LOCAL_PATH=`
  - `CIP_AI_EXTRACTED_PATH=`

## Outcome

This gives deterministic, root-cause aligned behavior:
- core layers run without brittle discovery logic
- optional enrichments are enabled where real sources exist
- optional enrichments skip cleanly when source files are absent
