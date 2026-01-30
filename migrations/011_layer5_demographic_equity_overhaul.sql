-- Migration 011: Layer 5 Demographic Equity Overhaul
-- Adds modern equity-based metrics using segregation indices + migration flow analysis
-- Date: 2026-01-29

-- =============================================================================
-- TRACT-LEVEL TABLE: Primary analysis unit for demographic equity
-- =============================================================================
CREATE TABLE IF NOT EXISTS layer5_demographic_equity_tract (
    id SERIAL PRIMARY KEY,

    -- Geography identifiers
    tract_geoid VARCHAR(11) NOT NULL,           -- 11-digit census tract FIPS
    fips_code VARCHAR(5) NOT NULL,              -- County FIPS for rollup
    data_year INTEGER NOT NULL,

    -- Population metrics
    total_population INTEGER,
    pop_under_18 INTEGER,
    pop_18_24 INTEGER,
    pop_25_44 INTEGER,                          -- Prime working/family-forming age
    pop_45_64 INTEGER,
    pop_65_plus INTEGER,
    working_age_pct NUMERIC(5,4),               -- 25-44 / total

    -- Household composition
    total_households INTEGER,
    family_households INTEGER,
    family_with_children INTEGER,
    single_parent_households INTEGER,
    married_couple_households INTEGER,
    nonfamily_households INTEGER,

    -- Racial/ethnic composition (for diversity/segregation)
    pop_white_alone INTEGER,
    pop_black_alone INTEGER,
    pop_asian_alone INTEGER,
    pop_hispanic INTEGER,
    pop_other_race INTEGER,

    -- v1 Static Metrics (retained)
    racial_diversity_index NUMERIC(5,4),        -- Entropy-based diversity (0-1)
    age_dependency_ratio NUMERIC(6,4),          -- (under18 + 65+) / working-age
    family_household_pct NUMERIC(5,4),          -- Family / total households

    -- v2 Equity Metrics (NEW)
    dissimilarity_index NUMERIC(5,4),           -- Segregation: 0 = integrated, 1 = fully segregated
    exposure_index NUMERIC(5,4),                -- Intergroup contact probability
    isolation_index NUMERIC(5,4),               -- Same-group contact probability
    income_segregation_index NUMERIC(5,4),      -- Income-based segregation

    -- Family viability metrics
    single_parent_pct NUMERIC(5,4),             -- Single parent / families with kids
    median_family_income INTEGER,
    poverty_rate NUMERIC(5,4),
    child_poverty_rate NUMERIC(5,4),
    family_viability_score NUMERIC(5,4),        -- Composite family health

    -- v3 Migration Dynamics (NEW - apportioned from county)
    est_net_migration_rate NUMERIC(6,4),        -- Apportioned from county IRS data
    est_inflow_rate NUMERIC(6,4),
    est_outflow_rate NUMERIC(6,4),
    population_growth_rate NUMERIC(6,4),        -- YoY population change
    household_formation_rate NUMERIC(6,4),      -- YoY household change

    -- Projected growth (from MD State Data Center or derived)
    projected_pop_change_5yr NUMERIC(6,4),      -- Projected 5-year change rate

    -- Normalized Scores (0-1 scale)
    static_demographic_score NUMERIC(5,4),      -- v1: population structure
    equity_score NUMERIC(5,4),                  -- v2: segregation/diversity
    migration_dynamics_score NUMERIC(5,4),      -- v3: flows and growth
    demographic_opportunity_score NUMERIC(5,4), -- Weighted composite

    -- Land area for density
    land_area_sq_mi NUMERIC(10,4),
    population_density NUMERIC(10,2),

    -- Data provenance
    acs_year INTEGER,
    computation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT uq_tract_demo_year UNIQUE (tract_geoid, data_year)
);

-- Indexes for tract table
CREATE INDEX IF NOT EXISTS idx_tract_demo_county ON layer5_demographic_equity_tract(fips_code);
CREATE INDEX IF NOT EXISTS idx_tract_demo_year ON layer5_demographic_equity_tract(data_year);
CREATE INDEX IF NOT EXISTS idx_tract_demo_score ON layer5_demographic_equity_tract(demographic_opportunity_score);
CREATE INDEX IF NOT EXISTS idx_tract_demo_equity ON layer5_demographic_equity_tract(equity_score);

-- =============================================================================
-- ADD NEW COLUMNS TO EXISTING COUNTY TABLE
-- =============================================================================
ALTER TABLE layer5_demographic_momentum
    -- Racial composition
    ADD COLUMN IF NOT EXISTS pop_white_alone INTEGER,
    ADD COLUMN IF NOT EXISTS pop_black_alone INTEGER,
    ADD COLUMN IF NOT EXISTS pop_asian_alone INTEGER,
    ADD COLUMN IF NOT EXISTS pop_hispanic INTEGER,
    ADD COLUMN IF NOT EXISTS pop_other_race INTEGER,

    -- v1 Static (enhanced)
    ADD COLUMN IF NOT EXISTS racial_diversity_index NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS age_dependency_ratio NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS family_household_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS static_demographic_score NUMERIC(5,4),

    -- v2 Equity metrics
    ADD COLUMN IF NOT EXISTS dissimilarity_index NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS exposure_index NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS isolation_index NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS income_segregation_index NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS single_parent_pct NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS median_family_income INTEGER,
    ADD COLUMN IF NOT EXISTS poverty_rate NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS child_poverty_rate NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS family_viability_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS equity_score NUMERIC(5,4),

    -- v3 Migration dynamics (enhanced)
    ADD COLUMN IF NOT EXISTS net_migration_rate NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS inflow_rate NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS outflow_rate NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS family_net_migration INTEGER,
    ADD COLUMN IF NOT EXISTS family_migration_rate NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS population_growth_rate NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS projected_pop_change_5yr NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS migration_dynamics_score NUMERIC(5,4),

    -- Top origin/destination counties
    ADD COLUMN IF NOT EXISTS top_inflow_counties JSONB,
    ADD COLUMN IF NOT EXISTS top_outflow_counties JSONB,

    -- Composite
    ADD COLUMN IF NOT EXISTS demographic_opportunity_index NUMERIC(5,4),

    -- Data provenance
    ADD COLUMN IF NOT EXISTS acs_year INTEGER,
    ADD COLUMN IF NOT EXISTS demographic_version VARCHAR(20) DEFAULT 'v1-momentum';

-- Update existing records to mark as v1
UPDATE layer5_demographic_momentum
SET demographic_version = 'v1-momentum'
WHERE demographic_version IS NULL;

-- =============================================================================
-- COUNTY MIGRATION FLOWS TABLE (detailed flow tracking)
-- =============================================================================
CREATE TABLE IF NOT EXISTS demographic_migration_flows (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) NOT NULL,              -- Maryland county
    data_year INTEGER NOT NULL,
    flow_direction VARCHAR(10) NOT NULL,        -- 'inflow' or 'outflow'

    -- Origin/destination
    other_fips VARCHAR(5),                      -- Other county FIPS (NULL = aggregate)
    other_state VARCHAR(2),                     -- Other state if out-of-state
    other_name VARCHAR(100),                    -- County/state name

    -- Flow metrics (from IRS SOI)
    returns_count INTEGER,                      -- Number of tax returns (households)
    exemptions_count INTEGER,                   -- Number of exemptions (persons)
    agi_total NUMERIC(15,2),                    -- Total adjusted gross income

    -- Derived
    avg_income_per_return NUMERIC(12,2),
    is_top_flow BOOLEAN DEFAULT FALSE,          -- Top 5 origin/destination

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_migration_flow UNIQUE (fips_code, data_year, flow_direction, other_fips)
);

CREATE INDEX IF NOT EXISTS idx_migration_flows_county ON demographic_migration_flows(fips_code);
CREATE INDEX IF NOT EXISTS idx_migration_flows_year ON demographic_migration_flows(data_year);

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE layer5_demographic_equity_tract IS
'Tract-level demographic equity metrics combining static composition (v1), segregation indices (v2), and migration dynamics (v3).';

COMMENT ON COLUMN layer5_demographic_equity_tract.dissimilarity_index IS
'Segregation measure: proportion of group that would need to move for even distribution. 0=integrated, 1=fully segregated.';

COMMENT ON COLUMN layer5_demographic_equity_tract.exposure_index IS
'Probability that minority group member shares tract with majority group member. Higher = more integrated.';

COMMENT ON COLUMN layer5_demographic_equity_tract.family_viability_score IS
'Composite: considers single-parent rate, poverty rate, median income. Higher = better family conditions.';

COMMENT ON COLUMN layer5_demographic_equity_tract.demographic_opportunity_score IS
'Composite: 0.3×static + 0.4×equity + 0.3×migration. Captures population health, integration, and growth dynamics.';

COMMENT ON COLUMN layer5_demographic_momentum.demographic_opportunity_index IS
'v1-v3 composite: 0.3×static (pop structure) + 0.4×equity (segregation/diversity) + 0.3×migration (flows/growth)';

COMMENT ON COLUMN layer5_demographic_momentum.demographic_version IS
'v1-momentum: Original migration-focused metrics. v2-equity: Modern segregation + equity + flow analysis';

COMMENT ON TABLE demographic_migration_flows IS
'Detailed county-to-county migration flows from IRS SOI data for tracking movement patterns.';
