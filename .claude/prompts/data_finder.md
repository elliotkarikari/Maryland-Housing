You are an expert data ingestion and ETL engineer building the Maryland Growth & Family Viability Atlas. Use multiple sub-agents (via parallel tool calls) to fetch and verify real data from sources in DATA_SOURCES.md for ALL layers. The current date is January 31, 2026—align to latest available (e.g., 2025 or 2024 vintages). Prioritize real data only; no synthetics. If fetches fail, diagnose using past fixes (e.g., pagination for FEMA, archives for EJScreen) and retry with web_search/browse_page. Output processed MD-filtered data samples (e.g., DataFrame heads via code_execution) for each layer.

Current weak implementations: Various ingest scripts fail (e.g., Layer 6: CDC SVI None, EPA EJScreen failed, FEMA 500; Layer 5: USPS restricted; Layer 3: CIP manual). Past fixes: Use direct CSVs (e.g., SVI https://www.atsdr.cdc.gov/placeandhealth/svi/data/svi_data/2022/SVI2022_US_tract.csv), archives (EJScreen ftp://newftp.epa.gov/EJSCREEN/2023/...), pagination/smaller bbox for FEMA. For partials (e.g., County CIP PDFs), extract via browse_pdf_attachment if accessible.

Task: Overhaul ingestion for all layers using tools. Delegate sub-agents per layer/source. Process with code_execution (e.g., pandas/geopandas for MD filter, spatial joins). Cache skeletons for Parquet/PostGIS. Preserve timeseries compatibility.

Requirements — Real Data Fetch Approach (2025–2026 best practice):

1. Use web_search/browse_page to confirm/locate latest URLs if listed outdated (e.g., "BLS QCEW 2025 Maryland county CSV").
2. Parallel Sub-Agents (one per major source/layer):
   - Layer 1: Sub-agent for LEHD (web_search "LEHD LODES MD WAC 2023 CSV"); BLS QCEW (browse https://data.bls.gov/cew/data/files/2025/...); USASpending (browse https://www.usaspending.gov/api/v2/... with MD filter).
   - Layer 2: OSM (code_execution with osmnx for MD graph); GTFS (browse https://feeds.mta.maryland.gov/gtfs/...); MDOT (web_search "Maryland AADT CSV 2025").
   - Layer 3: MSDE (web_search "MSDE Report Card enrollment CSV 2025"); NCES (browse https://nces.ed.gov/ccd/...); CIP (browse_pdf_attachment for Montgomery PDF, extract budgets).
   - Layer 4: HUD Permits (web_search "HUD SOC permits MD county CSV 2025"); Zoning GIS (browse https://planning.maryland.gov/... for shapefiles); ACS (code_execution with census API for B25077/B19013 MD counties).
   - Layer 5: ACS (same as above for B07001/B01001); IRS Migration (browse https://www.irs.gov/pub/irs-soi/countymigration2223.csv); USPS (browse https://www.huduser.gov/portal/datasets/usps.html, note registration).
   - Layer 6: FEMA (browse https://hazards.fema.gov/... with paginated query/code_execution); NOAA (web_search "NOAA SLR MD data 2050"); EJScreen (web_search "EPA EJScreen 2023 CSV archive"); MD Infrastructure (browse https://data.imap.maryland.gov/... for bridges).
   - Layer 7: USASpending repeats (API for multi-year); others excluded if no API.
3. For each: Download via code_execution (e.g., requests.get, pandas.read_csv/zip); filter MD (ST_ABBR=='MD' or FIPS '24'); compute tract-level if needed (geopandas joins).
4. Handle failures: Retry with refined queries (e.g., smaller bbox for FEMA: -77.5,38.5,-75.0,39.7); log like DATA_SOURCES.md.
5. New fields: source_url, fetch_date, is_real=True.
6. Pipeline: Update src/ingest/layer\*.py skeletons with tool-based code.
7. Output: Layer-by-layer data samples (e.g., df.head(5)); full processed DFs if small.

Deliverables:

- Parallel tool calls for sub-agents.
- Processed MD data samples per layer.
- Updated ingestion skeletons.
- ANALYSIS_METHODS.md note: "All layers use real tool-based fetches; fixes applied per past issues."
