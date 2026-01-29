-- Migration 007: Layer 1 Economic Opportunity Accessibility Overhaul
-- Adds modern accessibility-based economic metrics using LODES wage-filtered data
-- Date: 2026-01-29

-- =============================================================================
-- TRACT-LEVEL TABLE: Primary analysis unit for economic accessibility
-- =============================================================================
CREATE TABLE IF NOT EXISTS layer1_economic_opportunity_tract (
    id SERIAL PRIMARY KEY,

    -- Geography identifiers
    tract_geoid VARCHAR(11) NOT NULL,           -- 11-digit census tract FIPS
    fips_code VARCHAR(5) NOT NULL,              -- County FIPS for rollup
    data_year INTEGER NOT NULL,

    -- Core LODES job counts at tract level
    total_jobs INTEGER,                          -- All jobs (C000)
    high_wage_jobs INTEGER,                      -- Jobs paying > regional median (SE03)
    mid_wage_jobs INTEGER,                       -- Jobs paying mid-range (SE02)
    low_wage_jobs INTEGER,                       -- Jobs paying < $1,250/mo (SE01)

    -- Accessibility metrics (jobs reachable from this tract)
    high_wage_jobs_accessible_45min INTEGER,     -- High-wage jobs within 45 min
    high_wage_jobs_accessible_30min INTEGER,     -- High-wage jobs within 30 min
    total_jobs_accessible_45min INTEGER,         -- All jobs within 45 min
    total_jobs_accessible_30min INTEGER,         -- All jobs within 30 min

    -- Normalized accessibility scores (0-1 scale)
    economic_accessibility_score NUMERIC(5,4),   -- Primary metric: high-wage access
    job_market_reach_score NUMERIC(5,4),         -- Total job market accessibility
    wage_quality_ratio NUMERIC(5,4),             -- High-wage / total accessible

    -- Relative accessibility (vs regional max)
    pct_regional_high_wage_accessible NUMERIC(5,4),  -- % of region's high-wage jobs
    pct_regional_jobs_accessible NUMERIC(5,4),       -- % of region's total jobs

    -- Industry diversity at tract level
    sector_diversity_entropy NUMERIC(6,4),       -- Shannon entropy
    high_wage_sector_concentration NUMERIC(5,4), -- HHI of high-wage sectors

    -- Economic opportunity indicators
    upward_mobility_score NUMERIC(5,4),          -- Composite opportunity metric
    job_quality_index NUMERIC(5,4),              -- Wage-weighted job quality
    entrepreneurship_density NUMERIC(8,2),       -- Establishments per 1000 pop

    -- Tract characteristics
    tract_population INTEGER,
    tract_working_age_pop INTEGER,               -- 25-64 population
    labor_force_participation NUMERIC(5,4),      -- LFP rate

    -- Data provenance
    lodes_year INTEGER,                          -- Year of LODES data
    acs_year INTEGER,                            -- Year of ACS data
    computation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT uq_econ_tract_year UNIQUE (tract_geoid, data_year)
);

-- Indexes for tract table
CREATE INDEX IF NOT EXISTS idx_econ_tract_county ON layer1_economic_opportunity_tract(fips_code);
CREATE INDEX IF NOT EXISTS idx_econ_tract_year ON layer1_economic_opportunity_tract(data_year);
CREATE INDEX IF NOT EXISTS idx_econ_tract_score ON layer1_economic_opportunity_tract(economic_accessibility_score);

-- =============================================================================
-- ADD NEW COLUMNS TO EXISTING COUNTY TABLE
-- =============================================================================
ALTER TABLE layer1_employment_gravity
    ADD COLUMN IF NOT EXISTS high_wage_jobs INTEGER,
    ADD COLUMN IF NOT EXISTS mid_wage_jobs INTEGER,
    ADD COLUMN IF NOT EXISTS low_wage_jobs INTEGER,
    ADD COLUMN IF NOT EXISTS high_wage_jobs_accessible_45min INTEGER,
    ADD COLUMN IF NOT EXISTS high_wage_jobs_accessible_30min INTEGER,
    ADD COLUMN IF NOT EXISTS total_jobs_accessible_45min INTEGER,
    ADD COLUMN IF NOT EXISTS total_jobs_accessible_30min INTEGER,
    ADD COLUMN IF NOT EXISTS economic_accessibility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS job_market_reach_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS wage_quality_ratio NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS pct_regional_high_wage_accessible NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS pct_regional_jobs_accessible NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS high_wage_sector_concentration NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS upward_mobility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS job_quality_index NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS entrepreneurship_density NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS working_age_pop INTEGER,
    ADD COLUMN IF NOT EXISTS labor_force_participation NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS lodes_year INTEGER,
    ADD COLUMN IF NOT EXISTS acs_year INTEGER,
    ADD COLUMN IF NOT EXISTS accessibility_version VARCHAR(20) DEFAULT 'v1-counts';

-- Update existing records to mark as v1
UPDATE layer1_employment_gravity
SET accessibility_version = 'v1-counts'
WHERE accessibility_version IS NULL;

-- =============================================================================
-- OPPORTUNITY INSIGHTS DATA TABLE (optional enhancement)
-- =============================================================================
CREATE TABLE IF NOT EXISTS opportunity_insights_tract (
    id SERIAL PRIMARY KEY,
    tract_geoid VARCHAR(11) NOT NULL,
    fips_code VARCHAR(5) NOT NULL,

    -- Mobility metrics from Opportunity Atlas
    kfr_pooled_pooled_p25 NUMERIC(8,4),          -- Expected income rank for children from 25th percentile parents
    kfr_pooled_pooled_mean NUMERIC(8,4),         -- Mean expected income rank
    has_mom_pooled_pooled_p25 NUMERIC(5,4),      -- Probability of single mother
    jail_pooled_pooled_p25 NUMERIC(5,4),         -- Incarceration rate

    -- Computed mobility predictor
    upward_mobility_predictor NUMERIC(5,4),      -- Composite from OI data

    data_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_oi_tract UNIQUE (tract_geoid)
);

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE layer1_economic_opportunity_tract IS
'Tract-level economic opportunity metrics using LODES wage-filtered data. Primary source for employment scoring.';

COMMENT ON COLUMN layer1_economic_opportunity_tract.high_wage_jobs_accessible_45min IS
'High-wage jobs (LODES SE03: >$3,333/mo) reachable within 45 minutes from tract centroid';

COMMENT ON COLUMN layer1_economic_opportunity_tract.economic_accessibility_score IS
'Normalized score: high_wage_jobs_accessible_45min / regional_max, 0-1 scale';

COMMENT ON COLUMN layer1_economic_opportunity_tract.upward_mobility_score IS
'Composite: 0.7×economic_accessibility + 0.3×upward_mobility_predictor (if available)';
