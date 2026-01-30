-- Migration 009: Layer 4 Housing Affordability Overhaul
-- Adds modern affordability-based metrics using HUD CHAS + ACS data
-- Date: 2026-01-29

-- =============================================================================
-- TRACT-LEVEL TABLE: Primary analysis unit for affordability computation
-- =============================================================================
CREATE TABLE IF NOT EXISTS layer4_housing_affordability_tract (
    id SERIAL PRIMARY KEY,

    -- Geography identifiers
    tract_geoid VARCHAR(11) NOT NULL,           -- 11-digit census tract FIPS
    fips_code VARCHAR(5) NOT NULL,              -- County FIPS for rollup
    data_year INTEGER NOT NULL,

    -- Housing stock metrics
    total_housing_units INTEGER,                 -- Total housing units
    occupied_units INTEGER,                      -- Occupied housing units
    owner_occupied_units INTEGER,                -- Owner-occupied units
    renter_occupied_units INTEGER,               -- Renter-occupied units
    vacant_units INTEGER,                        -- Vacant units
    vacancy_rate NUMERIC(5,4),                   -- Vacancy rate (0-1)

    -- Cost burden metrics (HUD CHAS methodology)
    total_households INTEGER,                    -- Total households
    cost_burdened_households INTEGER,            -- Paying >30% income on housing
    severely_cost_burdened_households INTEGER,   -- Paying >50% income on housing
    cost_burdened_pct NUMERIC(5,4),             -- % cost burdened (0-1)
    severely_cost_burdened_pct NUMERIC(5,4),    -- % severely burdened (0-1)

    -- Cost burden by tenure
    owner_cost_burdened_pct NUMERIC(5,4),       -- % owner cost burdened
    renter_cost_burdened_pct NUMERIC(5,4),      -- % renter cost burdened

    -- AMI bracket analysis (for families at different income levels)
    households_0_30_ami INTEGER,                 -- Extremely low income
    households_30_50_ami INTEGER,                -- Very low income
    households_50_80_ami INTEGER,               -- Low income
    households_80_100_ami INTEGER,              -- Moderate income
    households_100_plus_ami INTEGER,            -- Above moderate

    -- Affordable units by AMI level
    units_affordable_0_30_ami INTEGER,          -- Units affordable at 0-30% AMI
    units_affordable_30_50_ami INTEGER,         -- Units affordable at 30-50% AMI
    units_affordable_50_80_ami INTEGER,         -- Units affordable at 50-80% AMI
    affordable_units_pct NUMERIC(5,4),          -- % units affordable to low-income

    -- Housing + transportation cost (H+T Index concept)
    median_gross_rent INTEGER,                   -- Median gross rent
    median_home_value INTEGER,                   -- Median home value
    median_household_income INTEGER,             -- Median household income
    price_to_income_ratio NUMERIC(6,2),         -- Home value / income
    rent_to_income_ratio NUMERIC(5,4),          -- Monthly rent / monthly income

    -- Estimated H+T burden (housing + commute as % of income)
    avg_commute_time_minutes NUMERIC(5,1),      -- Average commute time
    estimated_commute_cost_monthly INTEGER,     -- Estimated monthly commute cost
    housing_plus_transport_pct NUMERIC(5,4),    -- Combined H+T as % of income

    -- Housing quality indicators
    housing_age_median_year INTEGER,            -- Median year built
    pre_1950_housing_pct NUMERIC(5,4),          -- % housing built before 1950
    crowded_units_pct NUMERIC(5,4),             -- % with >1 person per room
    lacking_complete_plumbing_pct NUMERIC(5,4), -- % lacking complete plumbing
    lacking_complete_kitchen_pct NUMERIC(5,4),  -- % lacking complete kitchen

    -- Normalized scores (0-1 scale)
    affordability_burden_score NUMERIC(5,4),    -- 1 - cost_burdened_pct (higher = better)
    affordable_stock_score NUMERIC(5,4),        -- Percentile of affordable units
    housing_quality_score NUMERIC(5,4),         -- Composite quality (age, crowding)
    housing_affordability_score NUMERIC(5,4),   -- Weighted composite

    -- Area metrics
    land_area_sq_mi NUMERIC(10,4),
    housing_density_per_sq_mi NUMERIC(8,2),

    -- Population for weighting in aggregation
    tract_population INTEGER,

    -- Data provenance
    acs_year INTEGER,                           -- Year of ACS data used
    chas_year INTEGER,                          -- Year of CHAS data used (if different)
    computation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT uq_tract_housing_year UNIQUE (tract_geoid, data_year)
);

-- Indexes for tract table
CREATE INDEX IF NOT EXISTS idx_tract_housing_county ON layer4_housing_affordability_tract(fips_code);
CREATE INDEX IF NOT EXISTS idx_tract_housing_year ON layer4_housing_affordability_tract(data_year);
CREATE INDEX IF NOT EXISTS idx_tract_housing_score ON layer4_housing_affordability_tract(housing_affordability_score);
CREATE INDEX IF NOT EXISTS idx_tract_housing_burden ON layer4_housing_affordability_tract(cost_burdened_pct);

-- =============================================================================
-- ADD NEW COLUMNS TO EXISTING COUNTY TABLE
-- =============================================================================
ALTER TABLE layer4_housing_elasticity
    -- Cost burden metrics
    ADD COLUMN IF NOT EXISTS total_households INTEGER,
    ADD COLUMN IF NOT EXISTS cost_burdened_households INTEGER,
    ADD COLUMN IF NOT EXISTS severely_cost_burdened_households INTEGER,
    ADD COLUMN IF NOT EXISTS cost_burdened_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS severely_cost_burdened_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS owner_cost_burdened_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS renter_cost_burdened_pct NUMERIC(5,4),

    -- AMI analysis
    ADD COLUMN IF NOT EXISTS households_0_30_ami INTEGER,
    ADD COLUMN IF NOT EXISTS households_30_50_ami INTEGER,
    ADD COLUMN IF NOT EXISTS households_50_80_ami INTEGER,
    ADD COLUMN IF NOT EXISTS units_affordable_0_30_ami INTEGER,
    ADD COLUMN IF NOT EXISTS units_affordable_30_50_ami INTEGER,
    ADD COLUMN IF NOT EXISTS units_affordable_50_80_ami INTEGER,
    ADD COLUMN IF NOT EXISTS affordable_units_pct NUMERIC(5,4),

    -- H+T metrics
    ADD COLUMN IF NOT EXISTS rent_to_income_ratio NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS avg_commute_time_minutes NUMERIC(5,1),
    ADD COLUMN IF NOT EXISTS housing_plus_transport_pct NUMERIC(5,4),

    -- Quality metrics
    ADD COLUMN IF NOT EXISTS housing_age_median_year INTEGER,
    ADD COLUMN IF NOT EXISTS pre_1950_housing_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS crowded_units_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS housing_quality_score NUMERIC(5,4),

    -- New composite scores
    ADD COLUMN IF NOT EXISTS affordability_burden_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS affordable_stock_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS housing_affordability_score NUMERIC(5,4),

    -- Composite index (combining v1 elasticity + v2 affordability)
    ADD COLUMN IF NOT EXISTS housing_opportunity_index NUMERIC(5,4),

    -- Data provenance
    ADD COLUMN IF NOT EXISTS acs_year INTEGER,
    ADD COLUMN IF NOT EXISTS chas_year INTEGER,
    ADD COLUMN IF NOT EXISTS affordability_version VARCHAR(20) DEFAULT 'v1-elasticity';

-- Update existing records to mark as v1
UPDATE layer4_housing_elasticity
SET affordability_version = 'v1-elasticity'
WHERE affordability_version IS NULL;

-- =============================================================================
-- CHAS DATA TRACKING TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS housing_chas_data (
    id SERIAL PRIMARY KEY,
    chas_version VARCHAR(20) NOT NULL,          -- e.g., "2016-2020"
    state_fips VARCHAR(2) NOT NULL,
    geography_level VARCHAR(20) NOT NULL,       -- 'tract' or 'county'
    download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_hash VARCHAR(64),
    record_count INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT uq_chas_version UNIQUE (chas_version, state_fips, geography_level)
);

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE layer4_housing_affordability_tract IS
'Tract-level housing affordability metrics computed from ACS + HUD CHAS data. Primary data source for housing scoring.';

COMMENT ON COLUMN layer4_housing_affordability_tract.cost_burdened_pct IS
'Percentage of households paying more than 30% of income on housing costs (HUD standard)';

COMMENT ON COLUMN layer4_housing_affordability_tract.housing_affordability_score IS
'Composite score: 0.5×(1-burden) + 0.3×affordable_stock + 0.2×quality, normalized 0-1';

COMMENT ON COLUMN layer4_housing_affordability_tract.housing_plus_transport_pct IS
'Estimated combined housing and transportation cost as percentage of income (H+T Index concept)';

COMMENT ON COLUMN layer4_housing_elasticity.housing_opportunity_index IS
'v1-v2 composite: 0.4×housing_elasticity_index (supply) + 0.6×housing_affordability_score (affordability)';

COMMENT ON COLUMN layer4_housing_elasticity.affordability_version IS
'v1-elasticity: Original permit/supply metrics. v2-affordability: HUD CHAS-based burden metrics';
