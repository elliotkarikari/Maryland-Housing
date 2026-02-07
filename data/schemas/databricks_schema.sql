-- ============================================================================
-- Maryland Growth & Family Viability Atlas
-- Azure Databricks / Delta Lake Schema
-- Mirrors PostgreSQL schema.sql + schema_timeseries.sql
-- ============================================================================

-- ============================================================================
-- REFERENCE TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS md_counties (
    fips_code STRING NOT NULL,
    county_name STRING NOT NULL,
    geometry_geojson STRING,          -- GeoJSON string (replaces PostGIS geometry)
    land_area_sq_mi DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Maryland county boundaries and reference data';

-- ============================================================================
-- DATA REFRESH TRACKING
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_refresh_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    layer_name STRING NOT NULL,
    data_source STRING NOT NULL,
    refresh_date TIMESTAMP NOT NULL,
    records_processed INT,
    records_inserted INT,
    records_updated INT,
    status STRING,
    error_message STRING,
    metadata STRING,                  -- JSON stored as STRING
    computation_type STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Tracks all data ingestion operations for auditing and monitoring';

-- ============================================================================
-- LAYER 1: EMPLOYMENT GRAVITY
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer1_employment_gravity (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    total_jobs INT,
    sector_diversity_entropy DECIMAL(10,6),
    jobs_goods_producing INT,
    jobs_trade_transport INT,
    jobs_information INT,
    jobs_financial INT,
    jobs_professional INT,
    jobs_education_health INT,
    jobs_leisure INT,
    jobs_other_services INT,
    jobs_public_admin INT,
    avg_weekly_wage DECIMAL(10,2),
    qcew_total_establishments INT,
    federal_awards_total DECIMAL(15,2),
    federal_awards_5yr_avg DECIMAL(15,2),
    federal_awards_volatility DECIMAL(10,6),
    stable_sector_share DECIMAL(5,4),
    employment_diversification_score DECIMAL(10,6),
    -- V2 composite columns
    economic_opportunity_index DECIMAL(10,6),
    high_wage_job_accessibility DECIMAL(10,6),
    qwi_net_job_growth_rate DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Employment structure, sector diversity, and federal spending persistence';

-- ============================================================================
-- LAYER 2: MOBILITY OPTIONALITY
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer2_mobility_optionality (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    highway_miles_total DECIMAL(10,2),
    interstate_exits INT,
    major_highway_redundancy BOOLEAN,
    has_rail_service BOOLEAN,
    has_frequent_bus BOOLEAN,
    transit_stations_count INT,
    mode_count INT,
    mobility_optionality_index DECIMAL(5,4),
    aadt_major_corridors INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Transportation modal redundancy and job-change resilience indicators';

-- ============================================================================
-- LAYER 3: SCHOOL SYSTEM TRAJECTORY
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer3_school_trajectory (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    total_enrollment INT,
    enrollment_3yr_change_pct DECIMAL(6,3),
    enrollment_trend STRING,
    schools_total INT,
    schools_elementary INT,
    schools_middle INT,
    schools_high INT,
    capital_investment_total DECIMAL(15,2),
    capital_per_student DECIMAL(10,2),
    expansion_projects_count INT,
    enrollment_momentum_score DECIMAL(10,6),
    capacity_strain_indicator DECIMAL(5,4),
    -- V2 composite columns
    education_opportunity_index DECIMAL(10,6),
    education_accessibility_score DECIMAL(10,6),
    school_supply_score DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'School enrollment trends and capital investment patterns';

-- ============================================================================
-- LAYER 4: HOUSING ELASTICITY
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer4_housing_elasticity (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    permits_total INT,
    permits_single_family INT,
    permits_multifamily INT,
    permits_per_1000_households DECIMAL(8,4),
    permits_3yr_trend STRING,
    median_home_value DECIMAL(12,2),
    median_household_income DECIMAL(12,2),
    price_to_income_ratio DECIMAL(6,3),
    price_to_income_5yr_change DECIMAL(6,3),
    has_open_zoning_gis BOOLEAN,
    zoning_capacity_indicator INT,
    supply_responsiveness_score DECIMAL(10,6),
    housing_elasticity_index DECIMAL(5,4),
    -- V2 composite columns
    housing_opportunity_index DECIMAL(10,6),
    housing_affordability_score DECIMAL(10,6),
    fmr_2br_to_income DECIMAL(8,4),
    lihtc_units_per_1000_households DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Housing supply responsiveness and growth absorption capacity';

-- ============================================================================
-- LAYER 5: DEMOGRAPHIC MOMENTUM
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer5_demographic_momentum (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    pop_total INT,
    pop_age_25_44 INT,
    pop_age_25_44_pct DECIMAL(5,4),
    households_total INT,
    households_family INT,
    households_family_with_children INT,
    inflow_households INT,
    outflow_households INT,
    net_migration_households INT,
    inflow_exemptions INT,
    outflow_exemptions INT,
    net_migration_persons INT,
    total_addresses INT,
    vacant_addresses INT,
    vacancy_rate DECIMAL(5,4),
    family_household_inflow_rate DECIMAL(6,5),
    working_age_momentum DECIMAL(10,6),
    household_formation_change DECIMAL(6,3),
    demographic_momentum_score DECIMAL(10,6),
    -- V2 composite columns
    demographic_opportunity_index DECIMAL(10,6),
    equity_score DECIMAL(10,6),
    static_demographic_score DECIMAL(10,6),
    migration_dynamics_score DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Migration patterns and household formation demand signals';

-- ============================================================================
-- LAYER 6: RISK DRAG
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer6_risk_drag (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    sfha_area_sq_mi DECIMAL(10,4),
    sfha_pct_of_county DECIMAL(5,4),
    sea_level_rise_exposure BOOLEAN,
    extreme_heat_days_annual INT,
    pm25_avg DECIMAL(8,4),
    ozone_avg DECIMAL(8,4),
    proximity_hazwaste_score DECIMAL(5,4),
    traffic_proximity_score DECIMAL(5,4),
    bridges_total INT,
    bridges_structurally_deficient INT,
    bridges_deficient_pct DECIMAL(5,4),
    risk_drag_index DECIMAL(5,4),
    -- V2 composite columns
    modern_vulnerability_score DECIMAL(10,6),
    static_risk_score DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Long-term environmental and infrastructure constraints';

-- ============================================================================
-- POLICY PERSISTENCE LAYER
-- ============================================================================

CREATE TABLE IF NOT EXISTS policy_persistence (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    federal_awards_yoy_consistency DECIMAL(5,4),
    repeat_awards_count INT,
    cip_projects_planned INT,
    cip_projects_completed INT,
    cip_follow_through_rate DECIMAL(5,4),
    state_funding_volatility DECIMAL(5,4),
    confidence_class STRING,
    confidence_score DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Policy delivery reliability - modifies confidence, not scores';

-- ============================================================================
-- TIMESERIES FEATURES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer_timeseries_features (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    geoid STRING NOT NULL,
    layer_name STRING NOT NULL,
    as_of_year INT NOT NULL,
    level_latest DECIMAL(10,6),
    level_baseline DECIMAL(10,6),
    momentum_slope DECIMAL(10,6),
    momentum_delta DECIMAL(10,6),
    momentum_percent_change DECIMAL(8,4),
    momentum_fit_quality DECIMAL(5,4),
    stability_volatility DECIMAL(10,6),
    stability_cv DECIMAL(8,4),
    stability_consistency DECIMAL(5,4),
    stability_persistence INT,
    coverage_years INT NOT NULL,
    min_year INT,
    max_year INT,
    data_gaps STRING,                 -- JSON array as STRING
    window_size INT DEFAULT 5,
    computation_method STRING,
    notes STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Multi-year derived features (level, momentum, stability) per layer';

-- ============================================================================
-- LAYER SUMMARY SCORES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer_summary_scores (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    geoid STRING NOT NULL,
    layer_name STRING NOT NULL,
    as_of_year INT NOT NULL,
    layer_level_score DECIMAL(5,4),
    layer_momentum_score DECIMAL(5,4),
    layer_stability_score DECIMAL(5,4),
    layer_overall_score DECIMAL(5,4),
    missingness_penalty DECIMAL(5,4),
    has_momentum BOOLEAN DEFAULT FALSE,
    has_stability BOOLEAN DEFAULT FALSE,
    coverage_years INT,
    weights STRING,                   -- JSON as STRING
    normalization_method STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Normalized layer scores incorporating level, momentum, and stability';

-- ============================================================================
-- FINAL SYNTHESIS CURRENT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS final_synthesis_current (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    geoid STRING NOT NULL,
    current_as_of_year INT NOT NULL,
    per_layer_coverage STRING,        -- JSON as STRING
    final_grouping STRING NOT NULL,
    directional_status STRING NOT NULL,
    confidence_level STRING NOT NULL,
    uncertainty_level STRING,
    uncertainty_reasons STRING,       -- JSON as STRING
    composite_score DECIMAL(5,4),
    risk_drag_applied DECIMAL(5,4),
    drivers STRING,                   -- JSON as STRING
    constraints STRING,               -- JSON as STRING (renamed from 'constraints' if reserved)
    coverage_summary STRING,          -- JSON as STRING
    employment_gravity_score DECIMAL(5,4),
    mobility_optionality_score DECIMAL(5,4),
    school_trajectory_score DECIMAL(5,4),
    housing_elasticity_score DECIMAL(5,4),
    demographic_momentum_score DECIMAL(5,4),
    risk_drag_score DECIMAL(5,4),
    classification_version STRING,
    notes STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Current final synthesis grouping per geography (multi-year evidence based)';

-- ============================================================================
-- PROCESSED OUTPUTS (V1 compatibility)
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer_scores (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    employment_gravity_score DECIMAL(5,4),
    mobility_optionality_score DECIMAL(5,4),
    school_trajectory_score DECIMAL(5,4),
    housing_elasticity_score DECIMAL(5,4),
    demographic_momentum_score DECIMAL(5,4),
    risk_drag_score DECIMAL(5,4),
    composite_raw DECIMAL(10,6),
    composite_normalized DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Normalized layer scores and composite index';

CREATE TABLE IF NOT EXISTS county_classifications (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    fips_code STRING,
    data_year INT NOT NULL,
    directional_class STRING,
    composite_score DECIMAL(5,4),
    confidence_class STRING,
    primary_strengths STRING,         -- JSON array as STRING
    primary_weaknesses STRING,        -- JSON array as STRING
    key_trends STRING,                -- JSON array as STRING
    classification_method STRING,
    version STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Final directional classifications with confidence modifiers';

-- ============================================================================
-- EXPORT VERSIONING
-- ============================================================================

CREATE TABLE IF NOT EXISTS export_versions (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    version STRING NOT NULL,
    export_date TIMESTAMP NOT NULL,
    data_year INT NOT NULL,
    geojson_path STRING,
    record_count INT,
    checksum STRING,
    metadata STRING,                  -- JSON as STRING
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Tracks GeoJSON export versions for reproducibility';

-- ============================================================================
-- INITIAL DATA: Maryland Counties
-- ============================================================================

MERGE INTO md_counties AS target
USING (
    SELECT '24001' AS fips_code, 'Allegany County' AS county_name, 424.0 AS land_area_sq_mi
    UNION ALL SELECT '24003', 'Anne Arundel County', 415.0
    UNION ALL SELECT '24005', 'Baltimore County', 598.0
    UNION ALL SELECT '24009', 'Calvert County', 213.0
    UNION ALL SELECT '24011', 'Caroline County', 320.0
    UNION ALL SELECT '24013', 'Carroll County', 449.0
    UNION ALL SELECT '24015', 'Cecil County', 346.0
    UNION ALL SELECT '24017', 'Charles County', 458.0
    UNION ALL SELECT '24019', 'Dorchester County', 541.0
    UNION ALL SELECT '24021', 'Frederick County', 660.0
    UNION ALL SELECT '24023', 'Garrett County', 647.0
    UNION ALL SELECT '24025', 'Harford County', 437.0
    UNION ALL SELECT '24027', 'Howard County', 251.0
    UNION ALL SELECT '24029', 'Kent County', 277.0
    UNION ALL SELECT '24031', 'Montgomery County', 491.0
    UNION ALL SELECT '24033', 'Prince Georges County', 483.0
    UNION ALL SELECT '24035', 'Queen Annes County', 372.0
    UNION ALL SELECT '24037', 'St. Marys County', 357.0
    UNION ALL SELECT '24039', 'Somerset County', 320.0
    UNION ALL SELECT '24041', 'Talbot County', 269.0
    UNION ALL SELECT '24043', 'Washington County', 458.0
    UNION ALL SELECT '24045', 'Wicomico County', 374.0
    UNION ALL SELECT '24047', 'Worcester County', 468.0
    UNION ALL SELECT '24510', 'Baltimore City', 81.0
) AS source
ON target.fips_code = source.fips_code
WHEN NOT MATCHED THEN INSERT (fips_code, county_name, land_area_sq_mi)
VALUES (source.fips_code, source.county_name, source.land_area_sq_mi);
