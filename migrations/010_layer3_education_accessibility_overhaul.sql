-- Migration 010: Layer 3 Education Accessibility Overhaul
-- Adds modern accessibility-based metrics using school quality + travel time analysis
-- Date: 2026-01-29

-- =============================================================================
-- TRACT-LEVEL TABLE: Primary analysis unit for education accessibility
-- =============================================================================
CREATE TABLE IF NOT EXISTS layer3_education_accessibility_tract (
    id SERIAL PRIMARY KEY,

    -- Geography identifiers
    tract_geoid VARCHAR(11) NOT NULL,           -- 11-digit census tract FIPS
    fips_code VARCHAR(5) NOT NULL,              -- County FIPS for rollup
    data_year INTEGER NOT NULL,

    -- School-age population (for weighting)
    school_age_pop_5_17 INTEGER,                -- Population ages 5-17
    school_age_pop_under_5 INTEGER,             -- Population under 5 (pre-K)
    tract_population INTEGER,                   -- Total tract population

    -- v1 Supply Metrics (school counts and capacity)
    total_schools_in_tract INTEGER,             -- Schools physically in tract
    elementary_schools INTEGER,
    middle_schools INTEGER,
    high_schools INTEGER,
    has_prek_program BOOLEAN,                   -- Any pre-K within tract

    -- v2 Accessibility Metrics (schools reachable within travel time)
    schools_accessible_15min INTEGER,           -- All schools within 15 min
    schools_accessible_30min INTEGER,           -- All schools within 30 min
    high_quality_schools_15min INTEGER,         -- Above-median proficiency, 15 min
    high_quality_schools_30min INTEGER,         -- Above-median proficiency, 30 min
    top_quartile_schools_30min INTEGER,         -- Top 25% proficiency, 30 min
    prek_programs_accessible_20min INTEGER,     -- Pre-K programs within 20 min

    -- Quality-Weighted Accessibility
    avg_proficiency_accessible_30min NUMERIC(5,2),  -- Avg proficiency of accessible schools
    avg_grad_rate_accessible_30min NUMERIC(5,4),    -- Avg grad rate of accessible HS
    best_school_proficiency_15min NUMERIC(5,2),     -- Best school within 15 min

    -- Relative accessibility (vs county/region)
    pct_county_schools_accessible NUMERIC(5,4),     -- % of county schools reachable
    pct_high_quality_accessible NUMERIC(5,4),       -- % of high-quality schools reachable
    school_choice_diversity INTEGER,                -- Distinct schools reachable

    -- Equity Metrics
    frl_proficiency_gap NUMERIC(5,2),               -- FRL vs non-FRL proficiency gap
    equity_adjusted_score NUMERIC(5,4),             -- Accessibility adjusted for equity

    -- v1 Normalized Scores (0-1 scale)
    school_supply_score NUMERIC(5,4),               -- v1: density + counts normalized
    school_density_per_sq_mi NUMERIC(8,4),          -- Schools per square mile

    -- v2 Normalized Scores (0-1 scale)
    education_accessibility_score NUMERIC(5,4),     -- v2: accessibility normalized
    school_quality_score NUMERIC(5,4),              -- Quality of accessible schools
    prek_accessibility_score NUMERIC(5,4),          -- Pre-K access normalized

    -- Composite Scores
    education_opportunity_score NUMERIC(5,4),       -- Weighted composite v1+v2

    -- Land area for density calculations
    land_area_sq_mi NUMERIC(10,4),

    -- Data provenance
    msde_year INTEGER,                              -- MSDE Report Card year
    nces_year INTEGER,                              -- NCES directory year
    acs_year INTEGER,                               -- ACS population year
    computation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT uq_tract_education_year UNIQUE (tract_geoid, data_year)
);

-- Indexes for tract table
CREATE INDEX IF NOT EXISTS idx_tract_education_county ON layer3_education_accessibility_tract(fips_code);
CREATE INDEX IF NOT EXISTS idx_tract_education_year ON layer3_education_accessibility_tract(data_year);
CREATE INDEX IF NOT EXISTS idx_tract_education_score ON layer3_education_accessibility_tract(education_opportunity_score);
CREATE INDEX IF NOT EXISTS idx_tract_education_quality ON layer3_education_accessibility_tract(school_quality_score);

-- =============================================================================
-- ADD NEW COLUMNS TO EXISTING COUNTY TABLE
-- =============================================================================
ALTER TABLE layer3_school_trajectory
    -- v1 Supply metrics (enhanced)
    ADD COLUMN IF NOT EXISTS total_schools INTEGER,
    ADD COLUMN IF NOT EXISTS elementary_schools INTEGER,
    ADD COLUMN IF NOT EXISTS middle_schools INTEGER,
    ADD COLUMN IF NOT EXISTS high_schools INTEGER,
    ADD COLUMN IF NOT EXISTS schools_with_prek INTEGER,
    ADD COLUMN IF NOT EXISTS school_density_per_sq_mi NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS school_supply_score NUMERIC(5,4),

    -- v2 Accessibility metrics
    ADD COLUMN IF NOT EXISTS high_quality_schools_count INTEGER,
    ADD COLUMN IF NOT EXISTS top_quartile_schools_count INTEGER,
    ADD COLUMN IF NOT EXISTS avg_schools_accessible_15min NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS avg_schools_accessible_30min NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS avg_high_quality_accessible_30min NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS pct_pop_near_high_quality NUMERIC(5,4),

    -- Quality metrics (MSDE Report Card)
    ADD COLUMN IF NOT EXISTS avg_ela_proficiency NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS avg_math_proficiency NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS avg_proficiency NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS avg_graduation_rate NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS frl_proficiency_gap NUMERIC(5,2),

    -- Normalized scores
    ADD COLUMN IF NOT EXISTS education_accessibility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS school_quality_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS prek_accessibility_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS equity_score NUMERIC(5,4),

    -- Composite index (v1 + v2)
    ADD COLUMN IF NOT EXISTS education_opportunity_index NUMERIC(5,4),

    -- Data provenance
    ADD COLUMN IF NOT EXISTS msde_year INTEGER,
    ADD COLUMN IF NOT EXISTS nces_year INTEGER,
    ADD COLUMN IF NOT EXISTS acs_year INTEGER,
    ADD COLUMN IF NOT EXISTS education_version VARCHAR(20) DEFAULT 'v1-enrollment';

-- Update existing records to mark as v1
UPDATE layer3_school_trajectory
SET education_version = 'v1-enrollment'
WHERE education_version IS NULL;

-- =============================================================================
-- SCHOOL DIRECTORY TABLE (Maryland schools with quality metrics)
-- =============================================================================
CREATE TABLE IF NOT EXISTS education_school_directory (
    id SERIAL PRIMARY KEY,
    nces_school_id VARCHAR(15) NOT NULL,        -- NCES school ID
    school_name VARCHAR(200) NOT NULL,
    school_type VARCHAR(50),                    -- Elementary, Middle, High, Combined
    school_level VARCHAR(20),                   -- Primary, Middle, High
    grade_low VARCHAR(10),
    grade_high VARCHAR(10),

    -- Location
    fips_code VARCHAR(5) NOT NULL,              -- County FIPS
    tract_geoid VARCHAR(11),                    -- Census tract
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    address VARCHAR(300),
    city VARCHAR(100),
    zip_code VARCHAR(10),

    -- School characteristics
    is_public BOOLEAN DEFAULT TRUE,
    is_charter BOOLEAN DEFAULT FALSE,
    is_magnet BOOLEAN DEFAULT FALSE,
    has_prek BOOLEAN DEFAULT FALSE,
    total_enrollment INTEGER,
    student_teacher_ratio NUMERIC(5,2),

    -- Quality metrics (from MSDE Report Card)
    ela_proficiency_pct NUMERIC(5,2),           -- ELA % proficient/advanced
    math_proficiency_pct NUMERIC(5,2),          -- Math % proficient/advanced
    avg_proficiency_pct NUMERIC(5,2),           -- Average of ELA + Math
    graduation_rate NUMERIC(5,4),               -- 4-year graduation rate (HS only)

    -- Subgroup metrics for equity analysis
    frl_ela_proficiency NUMERIC(5,2),           -- FRL students ELA proficiency
    frl_math_proficiency NUMERIC(5,2),          -- FRL students Math proficiency
    frl_proficiency_gap NUMERIC(5,2),           -- Gap between FRL and non-FRL

    -- Quality tier (computed)
    quality_tier VARCHAR(20),                   -- 'top_quartile', 'above_median', 'below_median', 'bottom_quartile'
    quality_score NUMERIC(5,4),                 -- Normalized 0-1 quality score

    -- Metadata
    data_year INTEGER NOT NULL,
    msde_school_number VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_school_year UNIQUE (nces_school_id, data_year)
);

-- Indexes for school directory
CREATE INDEX IF NOT EXISTS idx_school_county ON education_school_directory(fips_code);
CREATE INDEX IF NOT EXISTS idx_school_tract ON education_school_directory(tract_geoid);
CREATE INDEX IF NOT EXISTS idx_school_quality ON education_school_directory(quality_tier);
CREATE INDEX IF NOT EXISTS idx_school_location ON education_school_directory(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_school_year ON education_school_directory(data_year);

-- =============================================================================
-- MSDE DATA TRACKING TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS education_msde_data (
    id SERIAL PRIMARY KEY,
    report_year INTEGER NOT NULL,               -- School year (e.g., 2024 for 2023-24)
    data_type VARCHAR(50) NOT NULL,             -- 'report_card', 'graduation', 'enrollment'
    download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_hash VARCHAR(64),
    record_count INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT uq_msde_data UNIQUE (report_year, data_type)
);

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE layer3_education_accessibility_tract IS
'Tract-level education accessibility metrics combining school supply (v1) with quality-weighted accessibility (v2).';

COMMENT ON COLUMN layer3_education_accessibility_tract.high_quality_schools_30min IS
'Count of schools with above-median proficiency reachable within 30 minutes (walking/transit/driving blend)';

COMMENT ON COLUMN layer3_education_accessibility_tract.education_opportunity_score IS
'Composite score: 0.4×school_supply_score (v1) + 0.6×education_accessibility_score (v2), normalized 0-1';

COMMENT ON COLUMN layer3_education_accessibility_tract.equity_adjusted_score IS
'Accessibility score adjusted for FRL proficiency gap - rewards access to schools that serve all students well';

COMMENT ON TABLE education_school_directory IS
'Maryland school directory with NCES identifiers, locations, and MSDE quality metrics for accessibility analysis';

COMMENT ON COLUMN layer3_school_trajectory.education_opportunity_index IS
'v1-v2 composite: 0.4×school_supply (enrollment/density) + 0.6×education_accessibility (quality-weighted access)';

COMMENT ON COLUMN layer3_school_trajectory.education_version IS
'v1-enrollment: Original enrollment-only metrics. v2-accessibility: Quality-weighted accessibility metrics';
