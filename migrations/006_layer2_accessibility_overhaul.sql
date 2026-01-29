-- Migration 006: Layer 2 Mobility Accessibility Overhaul
-- Adds modern accessibility-based metrics using r5py travel time analysis
-- Date: 2026-01-29

-- =============================================================================
-- TRACT-LEVEL TABLE: Primary analysis unit for accessibility computation
-- =============================================================================
CREATE TABLE IF NOT EXISTS layer2_mobility_accessibility_tract (
    id SERIAL PRIMARY KEY,

    -- Geography identifiers
    tract_geoid VARCHAR(11) NOT NULL,           -- 11-digit census tract FIPS
    fips_code VARCHAR(5) NOT NULL,              -- County FIPS for rollup
    data_year INTEGER NOT NULL,

    -- Core R5 accessibility metrics (jobs reachable within time threshold)
    jobs_accessible_transit_45min INTEGER,       -- Jobs within 45 min by transit
    jobs_accessible_transit_30min INTEGER,       -- Jobs within 30 min by transit
    jobs_accessible_walk_30min INTEGER,          -- Jobs within 30 min walking
    jobs_accessible_bike_30min INTEGER,          -- Jobs within 30 min cycling
    jobs_accessible_car_30min INTEGER,           -- Jobs within 30 min driving (baseline)

    -- Normalized accessibility scores (0-1 scale)
    transit_accessibility_score NUMERIC(5,4),    -- Normalized transit access
    walk_accessibility_score NUMERIC(5,4),       -- Normalized walk access
    bike_accessibility_score NUMERIC(5,4),       -- Normalized bike access
    multimodal_accessibility_score NUMERIC(5,4), -- Weighted composite

    -- Relative accessibility (vs regional max)
    pct_regional_jobs_by_transit NUMERIC(5,4),   -- % of region's jobs reachable
    transit_car_accessibility_ratio NUMERIC(5,4), -- Transit jobs / Car jobs ratio

    -- Transit service quality metrics
    transit_stop_density NUMERIC(8,2),           -- Stops per sq mile
    frequent_transit_area_pct NUMERIC(5,4),      -- % area with <15 min headways
    average_headway_minutes NUMERIC(6,2),        -- Avg service frequency

    -- Infrastructure metrics (retained from v1)
    highway_access_score NUMERIC(5,4),           -- Highway proximity score
    mode_diversity_score NUMERIC(5,4),           -- Modes available (0-1)

    -- Population for weighting in aggregation
    tract_population INTEGER,

    -- Data provenance
    gtfs_feed_date DATE,                         -- Date of GTFS snapshot used
    osm_extract_date DATE,                       -- Date of OSM data used
    lodes_year INTEGER,                          -- Year of LODES job data
    computation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT uq_tract_mobility_year UNIQUE (tract_geoid, data_year)
);

-- Indexes for tract table
CREATE INDEX IF NOT EXISTS idx_tract_mobility_county ON layer2_mobility_accessibility_tract(fips_code);
CREATE INDEX IF NOT EXISTS idx_tract_mobility_year ON layer2_mobility_accessibility_tract(data_year);
CREATE INDEX IF NOT EXISTS idx_tract_mobility_score ON layer2_mobility_accessibility_tract(multimodal_accessibility_score);

-- =============================================================================
-- ADD NEW COLUMNS TO EXISTING COUNTY TABLE
-- =============================================================================
-- Add accessibility metrics to existing county table
ALTER TABLE layer2_mobility_optionality
    ADD COLUMN IF NOT EXISTS jobs_accessible_transit_45min INTEGER,
    ADD COLUMN IF NOT EXISTS jobs_accessible_transit_30min INTEGER,
    ADD COLUMN IF NOT EXISTS jobs_accessible_walk_30min INTEGER,
    ADD COLUMN IF NOT EXISTS jobs_accessible_bike_30min INTEGER,
    ADD COLUMN IF NOT EXISTS jobs_accessible_car_30min INTEGER,
    ADD COLUMN IF NOT EXISTS transit_accessibility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS walk_accessibility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS bike_accessibility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS multimodal_accessibility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS pct_regional_jobs_by_transit NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS transit_car_accessibility_ratio NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS transit_stop_density NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS frequent_transit_area_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS average_headway_minutes NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS highway_access_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS mode_diversity_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS gtfs_feed_date DATE,
    ADD COLUMN IF NOT EXISTS osm_extract_date DATE,
    ADD COLUMN IF NOT EXISTS lodes_year INTEGER,
    ADD COLUMN IF NOT EXISTS accessibility_version VARCHAR(20) DEFAULT 'v1-infrastructure';

-- Update existing records to mark as v1
UPDATE layer2_mobility_optionality
SET accessibility_version = 'v1-infrastructure'
WHERE accessibility_version IS NULL;

-- =============================================================================
-- GTFS FEED TRACKING TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS mobility_gtfs_feeds (
    id SERIAL PRIMARY KEY,
    feed_name VARCHAR(100) NOT NULL,
    feed_url VARCHAR(500) NOT NULL,
    agency_name VARCHAR(200),
    feed_date DATE,
    download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_hash VARCHAR(64),
    is_active BOOLEAN DEFAULT TRUE,
    coverage_area VARCHAR(100),
    CONSTRAINT uq_gtfs_feed UNIQUE (feed_name, feed_date)
);

-- =============================================================================
-- ACCESSIBILITY COMPUTATION CACHE
-- =============================================================================
CREATE TABLE IF NOT EXISTS mobility_computation_cache (
    id SERIAL PRIMARY KEY,
    computation_id VARCHAR(64) NOT NULL UNIQUE,
    data_year INTEGER NOT NULL,
    gtfs_date DATE NOT NULL,
    osm_date DATE NOT NULL,
    lodes_year INTEGER NOT NULL,
    tract_count INTEGER,
    computation_duration_seconds INTEGER,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE layer2_mobility_accessibility_tract IS
'Tract-level accessibility metrics computed via R5 routing engine. Primary data source for mobility scoring.';

COMMENT ON COLUMN layer2_mobility_accessibility_tract.jobs_accessible_transit_45min IS
'Total jobs (LODES C000) reachable within 45 minutes by public transit from tract centroid';

COMMENT ON COLUMN layer2_mobility_accessibility_tract.multimodal_accessibility_score IS
'Composite score: 0.6×transit_45 + 0.25×walk_30 + 0.15×bike_30, normalized 0-1';

COMMENT ON COLUMN layer2_mobility_accessibility_tract.transit_car_accessibility_ratio IS
'Ratio of jobs accessible by transit vs car - measures transit competitiveness';

COMMENT ON COLUMN layer2_mobility_optionality.accessibility_version IS
'v1-infrastructure: Original count-based metrics. v2-accessibility: R5-based travel time metrics';
