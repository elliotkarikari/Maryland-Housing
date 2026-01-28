# Data Sources Documentation

**Last Updated:** 2026-01-28

This document catalogs all data sources used in the Maryland Growth & Family Viability Atlas. Every source listed is **programmatically accessible** and **verifiable**.

---

## Layer 1: Employment Gravity

### 1.1 US Census LEHD/LODES
**Source:** Longitudinal Employer-Household Dynamics Origin-Destination Employment Statistics
**Agency:** US Census Bureau
**URL:** https://lehd.ces.census.gov/data/
**API/Access:** Direct CSV download via LEHD API
**Geography:** Census block, aggregable to county
**Frequency:** Annual
**Latest Available:** 2021 (as of 2026-01)
**Key Fields:**
- `w_geocode` - Workplace geography code
- `C000` - Total jobs
- `CNS01`-`CNS20` - Jobs by NAICS sector

**Usage:**
```python
# LODES WAC (Workplace Area Characteristics)
url = "https://lehd.ces.census.gov/data/lodes/LODES8/md/wac/md_wac_S000_JT00_2021.csv.gz"
```

**Limitations:**
- 2-year lag from reference year
- Self-employed not included
- Federal government workers excluded

---

### 1.2 BLS QCEW
**Source:** Quarterly Census of Employment and Wages
**Agency:** Bureau of Labor Statistics
**URL:** https://www.bls.gov/cew/
**API:** https://www.bls.gov/cew/api.htm
**Geography:** County
**Frequency:** Quarterly (with annual averages)
**Latest Available:** 2025 Q3
**Key Fields:**
- `area_fips` - County FIPS code
- `own_code` - Ownership (private/government)
- `industry_code` - NAICS code
- `annual_avg_emplvl` - Average employment
- `annual_avg_wkly_wage` - Average weekly wage

**Usage:**
```python
# BLS API endpoint for Maryland county data
url = f"https://data.bls.gov/cew/data/api/2025/3/area/24XXX.csv"
# XXX = county code (e.g., 24001 for Allegany)
```

**API Key:** Optional but recommended (increases rate limit to 500/day)
**Register:** https://data.bls.gov/registrationEngine/

**Limitations:**
- Quarterly data can be volatile
- Suppressed cells for disclosure protection
- Does not include gig economy

---

### 1.3 USASpending.gov
**Source:** Federal Procurement and Grant Awards
**Agency:** US Treasury
**URL:** https://www.usaspending.gov/
**API:** https://api.usaspending.gov/
**Geography:** County (via recipient location)
**Frequency:** Daily updates
**Latest Available:** Current
**Key Fields:**
- `recipient_location_county_code` - FIPS code
- `award_amount` - Dollars obligated
- `award_type` - Contract/grant/loan
- `awarding_agency_name`
- `period_of_performance_start_date`

**Usage:**
```python
# Advanced search API for Maryland counties
url = "https://api.usaspending.gov/api/v2/search/spending_by_geography/"
payload = {
    "scope": "place_of_performance",
    "geo_layer": "county",
    "filters": {
        "time_period": [{"start_date": "2020-01-01", "end_date": "2025-12-31"}],
        "place_of_performance_locations": [{"country": "USA", "state": "MD"}]
    }
}
```

**Rate Limits:** None documented, but implement 1 req/sec to be respectful
**Limitations:**
- Obligations ≠ actual spending
- Sub-county precision limited
- Classification complexity

---

## Layer 2: Mobility Optionality

### 2.1 OpenStreetMap
**Source:** Collaborative map data
**Access:** Overpass API
**URL:** https://overpass-api.de/
**Geography:** Point/line geometries, aggregable to county
**Frequency:** Continuously updated
**Key Features:**
- `highway=motorway|trunk` - Major highways
- `railway=rail` - Rail lines
- `highway=motorway_junction` - Interchanges

**Usage:**
```python
import osmnx as ox
# Get highway network for Maryland
G = ox.graph_from_place("Maryland, USA", network_type="drive")
```

**Limitations:**
- Data quality varies by contributor
- Not authoritative for planning
- Requires local validation

---

### 2.2 GTFS Feeds
**Sources:**
- **MARC** (Maryland Area Regional Commuter): https://www.mta.maryland.gov/gtfs
- **WMATA** (Washington Metro): https://developer.wmata.com/
- **MTA Maryland**: https://www.mta.maryland.gov/developer-resources

**Format:** GTFS (General Transit Feed Specification)
**Frequency:** Updated as schedules change (monthly typical)
**Key Files:**
- `stops.txt` - Station locations
- `routes.txt` - Transit lines
- `trips.txt` - Service patterns
- `stop_times.txt` - Schedules

**Usage:**
```python
import gtfs_kit as gk
# Load MARC GTFS
feed = gk.read_feed("https://www.mta.maryland.gov/gtfs/marc-train", dist_units="mi")
```

**Limitations:**
- Scheduled service ≠ actual reliability
- Does not capture frequency quality
- Rural areas underrepresented

---

### 2.3 Maryland DOT Open Data
**Source:** Maryland Department of Transportation
**Portal:** https://www.roads.maryland.gov/
**Data:** State Highway Administration datasets
**Geography:** Road segments, counties
**Key Datasets:**
- Annual Average Daily Traffic (AADT)
- Highway network shapefiles
- Bridge condition reports

**Access:** Direct download (no API as of 2026-01)
**Format:** Shapefiles, CSV
**Frequency:** Annual

**Limitations:**
- No real-time traffic data in open portal
- Limited modal integration

---

## Layer 3: School System Trajectory

### 3.1 Maryland State Department of Education
**Source:** Maryland Report Card
**URL:** https://reportcard.msde.maryland.gov/
**Access:** Downloadable CSV files
**Geography:** School, district (county equivalent in MD)
**Frequency:** Annual (released September for prior year)
**Latest Available:** 2024-25 school year
**Key Fields:**
- `School Number`
- `LEA` (Local Education Agency / county)
- `Total Enrollment`
- `Grade Range`

**Usage:**
```bash
# Direct download link (example for 2024)
https://reportcard.msde.maryland.gov/Graphs/#/DataDownloads/datadownload/3/17/6/1/XXXX/2024
```

**Limitations:**
- Test score data excluded per project ethos
- Capital data NOT in Report Card (see 3.3)
- Charter/private schools separate reporting

---

### 3.2 NCES (National Center for Education Statistics)
**Source:** Common Core of Data (CCD)
**URL:** https://nces.ed.gov/ccd/
**API:** NCES Data Tools API
**Geography:** School, district
**Frequency:** Annual
**Latest Available:** 2023-24
**Key Fields:**
- `LEAID` - District ID
- `ENROLLMENT` - Total students
- `MEMBER` - Membership (for mobility calc)
- `TITLEI_STATUS`

**Usage:**
```python
# NCES API endpoint
url = "https://nces.ed.gov/ccd/data/txt/psu221alay.txt"  # Example for 2022-23
```

**Limitations:**
- Federal reporting lag (2 years typical)
- Does not include facility data
- Limited financial granularity

---

### 3.3 County Capital Improvement Plans
**Source:** Individual county governments
**Access:** County websites (manual scraping required)
**Geography:** County
**Frequency:** Annual adoption, typically June-July
**Format:** PDF, occasionally Excel

**Known Accessible Examples:**
- **Montgomery County:** https://www.montgomerycountymd.gov/cip/
- **Howard County:** https://www.howardcountymd.gov/budget
- **Anne Arundel County:** https://www.aacounty.org/departments/budget/

**Fields Extracted:**
- Project name (school construction/renovation)
- Appropriated amount
- Projected expenditure year
- Status (planned/approved/funded/under construction)

**Limitations:**
- ⚠️ **V1 EXCLUSION:** No standardized API
- Requires manual PDF scraping per county
- Inconsistent categorization across counties
- Historical data retention varies

**V1 Workaround:** Use MSDE enrollment trends only; exclude capital investment

---

## Layer 4: Housing Elasticity

### 4.1 HUD Building Permits Survey
**Source:** Survey of Construction (SOC)
**Agency:** US Census Bureau for HUD
**URL:** https://www.census.gov/construction/bps/
**API:** Census API
**Geography:** County
**Frequency:** Monthly
**Latest Available:** Current month - 1
**Key Fields:**
- `fips` - County FIPS
- `1-unit` - Single-family permits
- `2-units` - Two-unit permits
- `3-4 units` - Small multifamily
- `5+ units` - Large multifamily

**Usage:**
```python
import census
c = census.Census("YOUR_API_KEY")
# Get permits for Maryland counties
c.acs5.state_county(
    fields=('NAME',),
    state_fips='24',
    county_fips=census.ALL,
    year=2025
)
```

**Limitations:**
- Permits ≠ completions
- Demolitions not tracked
- Seasonal volatility

---

### 4.2 County Zoning GIS Layers
**Source:** County GIS portals
**Access:** Direct download (shapefile/GeoJSON)
**Geography:** Parcel or zoning district
**Frequency:** Updated irregularly

**Accessible Counties (as of 2026-01):**
- **Montgomery:** https://data.montgomerycountymd.gov/
- **Howard:** https://data.howardcountymd.gov/
- **Baltimore County:** https://www.baltimorecountymd.gov/departments/planning/gis

**Limitations:**
- ⚠️ **V1 PARTIAL:** Only ~8 of 24 counties have open GIS
- Zoning codes not standardized
- Density calculations require parcel geometry + allowed FAR
- Overlay zones complicate capacity modeling

**V1 Workaround:** Binary indicator (has open zoning GIS = 1, else 0)

---

### 4.3 ACS Price-to-Income
**Source:** American Community Survey
**API:** Census API
**Geography:** County
**Frequency:** Annual (5-year estimates)
**Latest Available:** 2019-2023 (released Dec 2024)
**Key Tables:**
- `B25077` - Median home value
- `B19013` - Median household income

**Usage:**
```python
import census
c = census.Census("YOUR_API_KEY")
md_data = c.acs5.state_county(
    fields=('NAME', 'B25077_001E', 'B19013_001E'),
    state_fips='24',
    county_fips='*',
    year=2023
)
```

---

## Layer 5: Demographic Momentum

### 5.1 ACS Migration & Age Cohorts
**Source:** American Community Survey
**API:** Census API
**Geography:** County
**Frequency:** Annual (5-year estimates for reliability)
**Key Tables:**
- `B07001` - Geographic mobility by age
- `B01001` - Sex by age
- `B11001` - Household type

**Usage:**
```python
# Prime working age (25-44) by county
c.acs5.state_county(
    fields=('B01001_010E', 'B01001_011E', 'B01001_034E', 'B01001_035E'),
    state_fips='24',
    county_fips='*',
    year=2023
)
```

---

### 5.2 IRS County-to-County Migration
**Source:** Statistics of Income Division
**Agency:** Internal Revenue Service
**URL:** https://www.irs.gov/statistics/soi-tax-stats-migration-data
**Geography:** County-to-county flows
**Frequency:** Annual
**Latest Available:** 2021-2022 (as of 2026-01)
**Key Fields:**
- `y1_statefips`, `y1_countyfips` - Origin
- `y2_statefips`, `y2_countyfips` - Destination
- `n1` - Number of returns (households)
- `n2` - Number of exemptions (persons)

**Usage:**
```python
# Direct download
url = "https://www.irs.gov/pub/irs-soi/countymigration2122.csv"
```

**Limitations:**
- Only captures tax filers
- 2-year reporting lag
- Suppresses small flows for privacy

---

### 5.3 USPS Vacancy Data
**Source:** USPS Delivery Statistics
**Agency:** US Postal Service
**URL:** https://www.huduser.gov/portal/datasets/usps.html (via HUD)
**Geography:** ZIP code (crosswalk to county needed)
**Frequency:** Quarterly
**Latest Available:** 2025 Q3
**Key Fields:**
- `zip` - ZIP code
- `total_units` - Total addresses
- `vacant` - Vacant addresses
- `no_stat` - No-stat (likely demolished)

**Access Status:** ⚠️ **Conditional**
HUD USPS data requires registration; unclear if API access available.

**V1 Decision:** Include if programmatically accessible via HUD API; otherwise exclude.

---

## Layer 6: Risk Drag

### 6.1 FEMA National Flood Hazard Layer
**Source:** Flood Insurance Rate Maps (FIRM)
**Agency:** Federal Emergency Management Agency
**URL:** https://msc.fema.gov/
**API:** FEMA National Flood Hazard Layer (NFHL) Web Services
**Geography:** Polygon geometries
**Frequency:** Updated on rolling basis per county
**Key Features:**
- Special Flood Hazard Areas (SFHA / 100-year floodplain)
- Flood zones (A, AE, VE, X)

**Usage:**
```python
# FEMA Map Service Center API
url = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query"
params = {
    "where": "STATE_CODE='MD'",
    "outFields": "*",
    "f": "geojson"
}
```

**Limitations:**
- Some counties have outdated maps
- Does not reflect future climate change (see 6.2)

---

### 6.2 NOAA Climate Data
**Source:** National Oceanic and Atmospheric Administration
**Portal:** https://www.ncei.noaa.gov/
**Datasets:**
- **Sea Level Rise Viewer:** https://coast.noaa.gov/slr/
- **Climate Normals:** https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals

**Geography:** Station points, gridded datasets
**Frequency:** Climate normals updated every 10 years (1991-2020 current)
**Access:** Direct download, some API access

**V1 Usage:** Sea level rise projections for coastal counties (Anne Arundel, Calvert, etc.)

---

### 6.3 EPA EJScreen
**Source:** Environmental Justice Screening Tool
**Agency:** Environmental Protection Agency
**URL:** https://www.epa.gov/ejscreen
**API:** EJSCREEN API
**Geography:** Census block group, aggregable to county
**Frequency:** Annual
**Latest Available:** 2023 (as of 2026-01)
**Key Indicators:**
- PM2.5 (particulate matter)
- Ozone
- Proximity to hazardous waste sites (TSDF)
- Traffic proximity

**Usage:**
```python
# Download national dataset, filter to Maryland
url = "https://gaftp.epa.gov/EJSCREEN/2023/EJSCREEN_2023_StatePct_with_AS_CNMI_GU_VI.csv.zip"
```

**Limitations:**
- Block group geography requires aggregation
- Demographic weighting available but not used (pure environmental exposure only)

---

### 6.4 Maryland Infrastructure Condition Reports
**Source:** Maryland Department of Transportation
**URL:** https://www.mdot.maryland.gov/
**Datasets:**
- Bridge condition (National Bridge Inventory subset)
- Pavement quality reports

**Access:** Direct download
**Frequency:** Annual
**Geography:** Road segment, bridge point

**V1 Usage:** County-level aggregation of structurally deficient bridges

---

## Policy Persistence Layer

### 7.1 County Capital Improvement Plans (Historical)
**Source:** County government archives
**Access:** Manual download from county websites
**Frequency:** Annual
**Format:** PDF (requires OCR/scraping)

**V1 Status:** ⚠️ **EXCLUDED** - No programmatic access
Requires manual scraping of 24 county websites across 5+ years to establish follow-through rate.

---

### 7.2 State & County Budget Archives
**Source:** Maryland Open Budget Portal
**URL:** https://openbudget.maryland.gov/
**Access:** Web portal, limited CSV export
**Frequency:** Annual (fiscal year)

**V1 Status:** ⚠️ **PARTIAL** - State-level only
County budgets not centralized; would require individual county scraping.

---

### 7.3 USASpending Repeat Awards
**Source:** USASpending.gov (see 1.3)
**Usage:** Track multi-year federal awards to identify persistent funding

**V1 Status:** ✅ **INCLUDED** - Programmatically accessible

---

### 7.4 Leadership Tenure Data
**Source:** Manual assembly required
**Potential Sources:**
- Ballotpedia
- County government websites
- News archives

**V1 Status:** ⚠️ **EXCLUDED** - No single authoritative API

---

## V1 Final Data Inventory

### ✅ Fully Programmatically Accessible
1. Census LEHD/LODES
2. BLS QCEW
3. USASpending.gov
4. OpenStreetMap
5. GTFS feeds
6. Maryland MSDE enrollment
7. NCES
8. HUD Building Permits
9. ACS (all tables)
10. IRS migration
11. FEMA NFHL
12. EPA EJScreen

### ⚠️ Partially Accessible / Manual Required
13. County zoning GIS (8 of 24 counties)
14. Maryland DOT data (direct download only)
15. NOAA climate (some API, some manual)
16. County CIP plans (manual scraping)
17. USPS vacancy (requires HUD registration check)

### ❌ Excluded from V1
18. County budgets (not centralized)
19. Planning commission approvals (no standard format)
20. Leadership tenure (no authoritative source)

---

## Data Access Summary

| Layer | Fully Automated | Requires Manual Steps | V1 Completeness |
|-------|----------------|----------------------|----------------|
| Employment Gravity | Yes | No | 100% |
| Mobility Optionality | Yes | No | 100% |
| School Trajectory | Yes | CIP data excluded | 75% |
| Housing Elasticity | Partial | Zoning excluded | 80% |
| Demographic Momentum | Yes | USPS TBD | 90% |
| Risk Drag | Yes | No | 100% |
| Policy Persistence | Partial | Most excluded | 30% |

---

## API Keys Required

| Service | Required? | Free Tier | Registration |
|---------|-----------|-----------|--------------|
| Census API | Yes | 500/day | https://api.census.gov/data/key_signup.html |
| BLS API | Recommended | 500/day | https://data.bls.gov/registrationEngine/ |
| USASpending | No | Unlimited | N/A |
| WMATA GTFS | No | Unlimited | N/A |
| FEMA | No | Unlimited | N/A |
| EPA | No | Unlimited | N/A |

---

## Data Refresh Schedule (Production)

| Source | Refresh Frequency | System Update Schedule |
|--------|------------------|----------------------|
| BLS QCEW | Quarterly | Day 1 of month after release |
| HUD Permits | Monthly | Day 5 of each month |
| ACS 5-year | Annual (December) | January 15 |
| IRS Migration | Annual (variable) | When released + 1 week |
| LEHD/LODES | Annual (delayed) | March 1 |
| GTFS | As updated | Weekly check |
| FEMA NFHL | Rolling | Quarterly validation |

---

**Maintained by:** Maryland Viability Atlas Team
**Questions?** Open an issue at [GitHub repo URL]
