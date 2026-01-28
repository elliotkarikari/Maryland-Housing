-- Maryland Growth & Family Viability Atlas
-- PostgreSQL + PostGIS Schema
-- Version: 1.0
-- Last Updated: 2026-01-28

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Drop existing tables if rebuilding
DROP TABLE IF EXISTS layer_scores CASCADE;
DROP TABLE IF EXISTS county_classifications CASCADE;
DROP TABLE IF EXISTS layer6_risk_drag CASCADE;
DROP TABLE IF EXISTS layer5_demographic_momentum CASCADE;
DROP TABLE IF EXISTS layer4_housing_elasticity CASCADE;
DROP TABLE IF EXISTS layer3_school_trajectory CASCADE;
DROP TABLE IF EXISTS layer2_mobility_optionality CASCADE;
DROP TABLE IF EXISTS layer1_employment_gravity CASCADE;
DROP TABLE IF EXISTS policy_persistence CASCADE;
DROP TABLE IF EXISTS md_counties CASCADE;
DROP TABLE IF EXISTS data_refresh_log CASCADE;
DROP TABLE IF EXISTS export_versions CASCADE;

-- ============================================================================
-- REFERENCE TABLES
-- ============================================================================

-- Maryland Counties Reference
CREATE TABLE md_counties (
    fips_code VARCHAR(5) PRIMARY KEY,
    county_name VARCHAR(100) NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    land_area_sq_mi NUMERIC(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_counties_geom ON md_counties USING GIST(geometry);

COMMENT ON TABLE md_counties IS 'Maryland county boundaries and reference data';

-- ============================================================================
-- DATA REFRESH TRACKING
-- ============================================================================

CREATE TABLE data_refresh_log (
    id SERIAL PRIMARY KEY,
    layer_name VARCHAR(50) NOT NULL,
    data_source VARCHAR(100) NOT NULL,
    refresh_date TIMESTAMP NOT NULL,
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    status VARCHAR(20) CHECK (status IN ('success', 'partial', 'failed')),
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_refresh_log_layer ON data_refresh_log(layer_name, refresh_date DESC);

COMMENT ON TABLE data_refresh_log IS 'Tracks all data ingestion operations for auditing and monitoring';

-- ============================================================================
-- LAYER 1: EMPLOYMENT GRAVITY
-- ============================================================================

CREATE TABLE layer1_employment_gravity (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- LEHD/LODES metrics
    total_jobs INTEGER,
    sector_diversity_entropy NUMERIC(10,6),  -- Shannon entropy of employment across NAICS sectors

    -- Sector breakdown (CNS codes from LODES)
    jobs_goods_producing INTEGER,  -- CNS01-03 (ag, mining, construction)
    jobs_trade_transport INTEGER,  -- CNS07-08
    jobs_information INTEGER,       -- CNS09
    jobs_financial INTEGER,         -- CNS10
    jobs_professional INTEGER,      -- CNS11
    jobs_education_health INTEGER,  -- CNS12-13
    jobs_leisure INTEGER,           -- CNS14
    jobs_other_services INTEGER,    -- CNS15
    jobs_public_admin INTEGER,      -- CNS16

    -- BLS QCEW metrics
    avg_weekly_wage NUMERIC(10,2),
    qcew_total_establishments INTEGER,

    -- USASpending metrics
    federal_awards_total NUMERIC(15,2),
    federal_awards_5yr_avg NUMERIC(15,2),
    federal_awards_volatility NUMERIC(10,6),  -- Coefficient of variation

    -- Derived indicators
    stable_sector_share NUMERIC(5,4),  -- Share in education, health, public admin
    employment_diversification_score NUMERIC(10,6),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_l1_fips_year ON layer1_employment_gravity(fips_code, data_year DESC);

COMMENT ON TABLE layer1_employment_gravity IS 'Employment structure, sector diversity, and federal spending persistence';

-- ============================================================================
-- LAYER 2: MOBILITY OPTIONALITY
-- ============================================================================

CREATE TABLE layer2_mobility_optionality (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- OpenStreetMap highway network
    highway_miles_total NUMERIC(10,2),
    interstate_exits INTEGER,
    major_highway_redundancy BOOLEAN,  -- Multiple interstate/US routes

    -- GTFS transit metrics
    has_rail_service BOOLEAN,
    has_frequent_bus BOOLEAN,  -- 15-min or better peak headways
    transit_stations_count INTEGER,

    -- Modal redundancy
    mode_count INTEGER,  -- Number of distinct modes available (highway, rail, bus)
    mobility_optionality_index NUMERIC(5,4),  -- Composite 0-1 score

    -- MD DOT metrics
    aadt_major_corridors INTEGER,  -- Average Annual Daily Traffic

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_l2_fips_year ON layer2_mobility_optionality(fips_code, data_year DESC);

COMMENT ON TABLE layer2_mobility_optionality IS 'Transportation modal redundancy and job-change resilience indicators';

-- ============================================================================
-- LAYER 3: SCHOOL SYSTEM TRAJECTORY
-- ============================================================================

CREATE TABLE layer3_school_trajectory (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- MSDE enrollment
    total_enrollment INTEGER,
    enrollment_3yr_change_pct NUMERIC(6,3),
    enrollment_trend VARCHAR(20) CHECK (enrollment_trend IN ('growing', 'stable', 'declining')),

    -- NCES data
    schools_total INTEGER,
    schools_elementary INTEGER,
    schools_middle INTEGER,
    schools_high INTEGER,

    -- Capacity indicators (if CIP data available)
    capital_investment_total NUMERIC(15,2),  -- Total school construction spending
    capital_per_student NUMERIC(10,2),
    expansion_projects_count INTEGER,

    -- Derived indicators
    enrollment_momentum_score NUMERIC(10,6),
    capacity_strain_indicator NUMERIC(5,4),  -- Enrollment growth vs capacity investment

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_l3_fips_year ON layer3_school_trajectory(fips_code, data_year DESC);

COMMENT ON TABLE layer3_school_trajectory IS 'School enrollment trends and capital investment patterns';

-- ============================================================================
-- LAYER 4: HOUSING ELASTICITY
-- ============================================================================

CREATE TABLE layer4_housing_elasticity (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- HUD Building Permits
    permits_total INTEGER,
    permits_single_family INTEGER,
    permits_multifamily INTEGER,
    permits_per_1000_households NUMERIC(8,4),
    permits_3yr_trend VARCHAR(20) CHECK (permits_3yr_trend IN ('increasing', 'stable', 'decreasing')),

    -- ACS housing metrics
    median_home_value NUMERIC(12,2),
    median_household_income NUMERIC(12,2),
    price_to_income_ratio NUMERIC(6,3),
    price_to_income_5yr_change NUMERIC(6,3),

    -- Zoning capacity (if available)
    has_open_zoning_gis BOOLEAN,
    zoning_capacity_indicator INTEGER CHECK (zoning_capacity_indicator IN (0, 1)),  -- Binary for V1

    -- Derived indicators
    supply_responsiveness_score NUMERIC(10,6),
    housing_elasticity_index NUMERIC(5,4),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_l4_fips_year ON layer4_housing_elasticity(fips_code, data_year DESC);

COMMENT ON TABLE layer4_housing_elasticity IS 'Housing supply responsiveness and growth absorption capacity';

-- ============================================================================
-- LAYER 5: DEMOGRAPHIC MOMENTUM
-- ============================================================================

CREATE TABLE layer5_demographic_momentum (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- ACS age cohorts
    pop_total INTEGER,
    pop_age_25_44 INTEGER,  -- Prime working age
    pop_age_25_44_pct NUMERIC(5,4),

    -- ACS household types
    households_total INTEGER,
    households_family INTEGER,
    households_family_with_children INTEGER,

    -- IRS migration
    inflow_households INTEGER,
    outflow_households INTEGER,
    net_migration_households INTEGER,
    inflow_exemptions INTEGER,  -- Proxy for persons
    outflow_exemptions INTEGER,
    net_migration_persons INTEGER,

    -- USPS vacancy (if available)
    total_addresses INTEGER,
    vacant_addresses INTEGER,
    vacancy_rate NUMERIC(5,4),

    -- Derived indicators
    family_household_inflow_rate NUMERIC(6,5),
    working_age_momentum NUMERIC(10,6),
    household_formation_change NUMERIC(6,3),
    demographic_momentum_score NUMERIC(10,6),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_l5_fips_year ON layer5_demographic_momentum(fips_code, data_year DESC);

COMMENT ON TABLE layer5_demographic_momentum IS 'Migration patterns and household formation demand signals';

-- ============================================================================
-- LAYER 6: RISK DRAG
-- ============================================================================

CREATE TABLE layer6_risk_drag (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- FEMA flood risk
    sfha_area_sq_mi NUMERIC(10,4),  -- Special Flood Hazard Area
    sfha_pct_of_county NUMERIC(5,4),

    -- NOAA climate
    sea_level_rise_exposure BOOLEAN,  -- Coastal county with SLR risk
    extreme_heat_days_annual INTEGER,  -- Days >95°F

    -- EPA EJScreen
    pm25_avg NUMERIC(8,4),
    ozone_avg NUMERIC(8,4),
    proximity_hazwaste_score NUMERIC(5,4),
    traffic_proximity_score NUMERIC(5,4),

    -- MD infrastructure
    bridges_total INTEGER,
    bridges_structurally_deficient INTEGER,
    bridges_deficient_pct NUMERIC(5,4),

    -- Derived risk index (subtractive only)
    risk_drag_index NUMERIC(5,4),  -- 0 = no drag, 1 = maximum drag

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_l6_fips_year ON layer6_risk_drag(fips_code, data_year DESC);

COMMENT ON TABLE layer6_risk_drag IS 'Long-term environmental and infrastructure constraints';

-- ============================================================================
-- POLICY PERSISTENCE LAYER
-- ============================================================================

CREATE TABLE policy_persistence (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- Federal spending consistency (from USASpending)
    federal_awards_yoy_consistency NUMERIC(5,4),  -- 1 - CV across 5 years
    repeat_awards_count INTEGER,

    -- County CIP follow-through (if available - likely NULL for V1)
    cip_projects_planned INTEGER,
    cip_projects_completed INTEGER,
    cip_follow_through_rate NUMERIC(5,4),

    -- State budget consistency (if available)
    state_funding_volatility NUMERIC(5,4),

    -- Derived confidence classification
    confidence_class VARCHAR(20) CHECK (confidence_class IN ('strong', 'conditional', 'fragile')),
    confidence_score NUMERIC(5,4),  -- 0-1, does NOT multiply layer scores

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_policy_fips_year ON policy_persistence(fips_code, data_year DESC);

COMMENT ON TABLE policy_persistence IS 'Policy delivery reliability - modifies confidence, not scores';

-- ============================================================================
-- PROCESSED OUTPUTS
-- ============================================================================

-- Layer scores (normalized within Maryland)
CREATE TABLE layer_scores (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    employment_gravity_score NUMERIC(5,4),  -- 0-1, normalized
    mobility_optionality_score NUMERIC(5,4),
    school_trajectory_score NUMERIC(5,4),
    housing_elasticity_score NUMERIC(5,4),
    demographic_momentum_score NUMERIC(5,4),
    risk_drag_score NUMERIC(5,4),  -- 0-1, where 1 = high drag (bad)

    -- Composite (risk drag is subtractive)
    composite_raw NUMERIC(10,6),
    composite_normalized NUMERIC(5,4),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_layer_scores_fips_year ON layer_scores(fips_code, data_year DESC);

COMMENT ON TABLE layer_scores IS 'Normalized layer scores and composite index';

-- Final classifications
CREATE TABLE county_classifications (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) REFERENCES md_counties(fips_code),
    data_year INTEGER NOT NULL,

    -- Directional pressure classification
    directional_class VARCHAR(20) CHECK (directional_class IN ('improving', 'stable', 'at_risk')),
    composite_score NUMERIC(5,4),

    -- Confidence overlay (from policy persistence)
    confidence_class VARCHAR(20) CHECK (confidence_class IN ('strong', 'conditional', 'fragile')),

    -- Explanation fields for map tooltips
    primary_strengths TEXT[],
    primary_weaknesses TEXT[],
    key_trends TEXT[],

    -- Metadata
    classification_method VARCHAR(50),
    version VARCHAR(20),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips_code, data_year)
);

CREATE INDEX idx_classifications_fips_year ON county_classifications(fips_code, data_year DESC);

COMMENT ON TABLE county_classifications IS 'Final directional classifications with confidence modifiers';

-- ============================================================================
-- EXPORT VERSIONING
-- ============================================================================

CREATE TABLE export_versions (
    id SERIAL PRIMARY KEY,
    version VARCHAR(20) UNIQUE NOT NULL,
    export_date TIMESTAMP NOT NULL,
    data_year INTEGER NOT NULL,
    geojson_path TEXT,
    record_count INTEGER,
    checksum VARCHAR(64),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE export_versions IS 'Tracks GeoJSON export versions for reproducibility';

-- ============================================================================
-- VIEWS FOR API CONSUMPTION
-- ============================================================================

-- Latest county data view
CREATE OR REPLACE VIEW v_latest_county_data AS
SELECT
    c.fips_code,
    c.county_name,
    c.geometry,
    cs.directional_class,
    cs.confidence_class,
    cs.composite_score,
    cs.primary_strengths,
    cs.primary_weaknesses,
    ls.employment_gravity_score,
    ls.mobility_optionality_score,
    ls.school_trajectory_score,
    ls.housing_elasticity_score,
    ls.demographic_momentum_score,
    ls.risk_drag_score,
    cs.data_year
FROM md_counties c
LEFT JOIN county_classifications cs ON c.fips_code = cs.fips_code
LEFT JOIN layer_scores ls ON c.fips_code = ls.fips_code AND cs.data_year = ls.data_year
WHERE cs.data_year = (SELECT MAX(data_year) FROM county_classifications);

COMMENT ON VIEW v_latest_county_data IS 'Latest classifications and scores for API consumption';

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Function to calculate Shannon entropy for sector diversity
CREATE OR REPLACE FUNCTION calculate_entropy(sector_jobs INTEGER[])
RETURNS NUMERIC AS $$
DECLARE
    total_jobs INTEGER;
    prob NUMERIC;
    entropy NUMERIC := 0;
    job_count INTEGER;
BEGIN
    total_jobs := (SELECT SUM(j) FROM UNNEST(sector_jobs) AS j);
    IF total_jobs = 0 THEN
        RETURN 0;
    END IF;

    FOREACH job_count IN ARRAY sector_jobs LOOP
        IF job_count > 0 THEN
            prob := job_count::NUMERIC / total_jobs;
            entropy := entropy - (prob * LOG(2, prob));
        END IF;
    END LOOP;

    RETURN entropy;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply timestamp trigger to all main tables
CREATE TRIGGER update_md_counties_updated_at BEFORE UPDATE ON md_counties
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_l1_updated_at BEFORE UPDATE ON layer1_employment_gravity
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_l2_updated_at BEFORE UPDATE ON layer2_mobility_optionality
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_l3_updated_at BEFORE UPDATE ON layer3_school_trajectory
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_l4_updated_at BEFORE UPDATE ON layer4_housing_elasticity
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_l5_updated_at BEFORE UPDATE ON layer5_demographic_momentum
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_l6_updated_at BEFORE UPDATE ON layer6_risk_drag
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_policy_updated_at BEFORE UPDATE ON policy_persistence
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- GRANTS (adjust for your user)
-- ============================================================================

-- Grant read-only access to API user (create this user separately)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO api_user;

-- ============================================================================
-- INITIAL DATA: Maryland Counties
-- ============================================================================

-- Counties will be populated by ingestion script using Census TIGER/Line
-- Placeholder insert to establish structure:

INSERT INTO md_counties (fips_code, county_name, land_area_sq_mi) VALUES
('24001', 'Allegany County', 424.0),
('24003', 'Anne Arundel County', 415.0),
('24005', 'Baltimore County', 598.0),
('24009', 'Calvert County', 213.0),
('24011', 'Caroline County', 320.0),
('24013', 'Carroll County', 449.0),
('24015', 'Cecil County', 346.0),
('24017', 'Charles County', 458.0),
('24019', 'Dorchester County', 541.0),
('24021', 'Frederick County', 660.0),
('24023', 'Garrett County', 647.0),
('24025', 'Harford County', 437.0),
('24027', 'Howard County', 251.0),
('24029', 'Kent County', 277.0),
('24031', 'Montgomery County', 491.0),
('24033', 'Prince Georges County', 483.0),
('24035', 'Queen Annes County', 372.0),
('24037', 'St. Marys County', 357.0),
('24039', 'Somerset County', 320.0),
('24041', 'Talbot County', 269.0),
('24043', 'Washington County', 458.0),
('24045', 'Wicomico County', 374.0),
('24047', 'Worcester County', 468.0),
('24510', 'Baltimore City', 81.0)
ON CONFLICT (fips_code) DO NOTHING;

-- Geometries will be added by ingestion script using pygris/Census TIGER

COMMENT ON DATABASE maryland_atlas IS 'Maryland Growth & Family Viability Atlas - Production Database';
