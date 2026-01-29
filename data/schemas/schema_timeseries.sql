-- ============================================================================
-- MULTI-YEAR EVIDENCE ENGINE SCHEMA
-- Supports Level + Momentum + Stability analysis
-- ============================================================================

-- ============================================================================
-- TIMESERIES FEATURES TABLE
-- Stores multi-year derived features for each layer
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer_timeseries_features (
    id SERIAL PRIMARY KEY,
    geoid VARCHAR(5) NOT NULL REFERENCES md_counties(fips_code),
    layer_name VARCHAR(50) NOT NULL,
    as_of_year INTEGER NOT NULL,  -- Latest year used for feature calculation

    -- LEVEL: Current position
    level_latest NUMERIC(10,6),           -- Latest available metric value
    level_baseline NUMERIC(10,6),         -- Earliest value in window

    -- MOMENTUM: Directional trend
    momentum_slope NUMERIC(10,6),         -- Robust slope (Theil-Sen or OLS)
    momentum_delta NUMERIC(10,6),         -- Latest - earliest (absolute change)
    momentum_percent_change NUMERIC(8,4), -- (Latest - earliest) / earliest
    momentum_fit_quality NUMERIC(5,4),    -- R-squared or similar (0-1)

    -- STABILITY: Consistency & volatility
    stability_volatility NUMERIC(10,6),   -- Std dev or IQR
    stability_cv NUMERIC(8,4),            -- Coefficient of variation
    stability_consistency NUMERIC(5,4),   -- Share of year-over-year changes that are positive
    stability_persistence INTEGER,        -- Count of consecutive positive years

    -- COVERAGE: Data availability
    coverage_years INTEGER NOT NULL,      -- Count of years with data
    min_year INTEGER,                     -- First year in window
    max_year INTEGER,                     -- Last year in window
    data_gaps JSONB,                      -- Array of missing years: [2020, 2022]

    -- METADATA
    window_size INTEGER DEFAULT 5,        -- Intended window size (N_YEARS)
    computation_method VARCHAR(50),       -- 'theil_sen', 'ols', 'simple_delta'
    notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(geoid, layer_name, as_of_year)
);

CREATE INDEX idx_timeseries_geoid_layer ON layer_timeseries_features(geoid, layer_name);
CREATE INDEX idx_timeseries_as_of_year ON layer_timeseries_features(as_of_year DESC);

COMMENT ON TABLE layer_timeseries_features IS 'Multi-year derived features (level, momentum, stability) per layer';
COMMENT ON COLUMN layer_timeseries_features.momentum_slope IS 'Robust linear slope across available years in window';
COMMENT ON COLUMN layer_timeseries_features.stability_consistency IS 'Fraction of YoY changes that are positive (0-1)';
COMMENT ON COLUMN layer_timeseries_features.coverage_years IS 'Number of years with actual data (not interpolated)';

-- ============================================================================
-- LAYER SUMMARY SCORES TABLE
-- Normalized 0-1 scores for level, momentum, stability per layer
-- ============================================================================

CREATE TABLE IF NOT EXISTS layer_summary_scores (
    id SERIAL PRIMARY KEY,
    geoid VARCHAR(5) NOT NULL REFERENCES md_counties(fips_code),
    layer_name VARCHAR(50) NOT NULL,
    as_of_year INTEGER NOT NULL,

    -- COMPONENT SCORES (0-1 normalized)
    layer_level_score NUMERIC(5,4),       -- Percentile of level_latest
    layer_momentum_score NUMERIC(5,4),    -- Percentile of momentum_slope (if available)
    layer_stability_score NUMERIC(5,4),   -- Derived from consistency/volatility (if available)

    -- COMPOSITE SCORE
    layer_overall_score NUMERIC(5,4),     -- Weighted: 0.5*level + 0.3*momentum + 0.2*stability

    -- MISSINGNESS & CONFIDENCE
    missingness_penalty NUMERIC(5,4),     -- 0 = full data, 1 = severe missingness
    has_momentum BOOLEAN DEFAULT FALSE,   -- True if momentum could be calculated
    has_stability BOOLEAN DEFAULT FALSE,  -- True if stability could be calculated
    coverage_years INTEGER,               -- Number of years used

    -- SCORING METADATA
    weights JSONB,                        -- {"level": 0.5, "momentum": 0.3, "stability": 0.2}
    normalization_method VARCHAR(50),     -- 'percentile_rank', 'robust_zscore'

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(geoid, layer_name, as_of_year)
);

CREATE INDEX idx_layer_scores_geoid ON layer_summary_scores(geoid, layer_name);
CREATE INDEX idx_layer_scores_as_of_year ON layer_summary_scores(as_of_year DESC);

COMMENT ON TABLE layer_summary_scores IS 'Normalized layer scores incorporating level, momentum, and stability';
COMMENT ON COLUMN layer_summary_scores.layer_overall_score IS 'Deterministic weighted composition of available components';
COMMENT ON COLUMN layer_summary_scores.missingness_penalty IS 'Penalty applied when coverage_years below threshold';

-- ============================================================================
-- FINAL SYNTHESIS CURRENT TABLE
-- Single current grouping per geography (no historical playback)
-- ============================================================================

CREATE TABLE IF NOT EXISTS final_synthesis_current (
    id SERIAL PRIMARY KEY,
    geoid VARCHAR(5) NOT NULL UNIQUE REFERENCES md_counties(fips_code),

    -- TEMPORAL CONTEXT
    current_as_of_year INTEGER NOT NULL,  -- Global "as of" year for this synthesis
    per_layer_coverage JSONB,             -- {"employment": {"min": 2020, "max": 2024, "n": 5}, ...}

    -- FINAL CLASSIFICATION
    final_grouping VARCHAR(50) NOT NULL CHECK (final_grouping IN (
        'emerging_tailwinds',
        'conditional_growth',
        'stable_constrained',
        'at_risk_headwinds',
        'high_uncertainty'
    )),

    directional_status VARCHAR(20) NOT NULL CHECK (directional_status IN (
        'improving', 'stable', 'at_risk'
    )),

    confidence_level VARCHAR(20) NOT NULL CHECK (confidence_level IN (
        'strong', 'conditional', 'fragile'
    )),

    -- UNCERTAINTY
    uncertainty_level VARCHAR(20) CHECK (uncertainty_level IN (
        'low', 'medium', 'high'
    )),
    uncertainty_reasons JSONB,             -- ["sparse_coverage", "volatile_signals", "contested_classification"]

    -- COMPOSITE METRICS
    composite_score NUMERIC(5,4),          -- Aggregate across layers (after risk drag)
    risk_drag_applied NUMERIC(5,4),        -- Final risk drag multiplier

    -- EXPLAINABILITY
    drivers JSONB,                         -- [{"layer": "Employment", "type": "momentum", "reason": "...", "score": 0.85}]
    constraints JSONB,                     -- [{"layer": "Risk", "type": "level", "reason": "...", "score": 0.3}]
    coverage_summary JSONB,                -- Per-layer coverage: {"Employment": {"years": [2020..2024], "n": 5}, ...}

    -- LAYER OVERALL SCORES (for reference)
    employment_gravity_score NUMERIC(5,4),
    mobility_optionality_score NUMERIC(5,4),
    school_trajectory_score NUMERIC(5,4),
    housing_elasticity_score NUMERIC(5,4),
    demographic_momentum_score NUMERIC(5,4),
    risk_drag_score NUMERIC(5,4),

    -- METADATA
    classification_version VARCHAR(20),    -- e.g., "v2.0-multiyear"
    notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_final_synthesis_grouping ON final_synthesis_current(final_grouping);
CREATE INDEX idx_final_synthesis_directional ON final_synthesis_current(directional_status);
CREATE INDEX idx_final_synthesis_confidence ON final_synthesis_current(confidence_level);

COMMENT ON TABLE final_synthesis_current IS 'Current final synthesis grouping per geography (multi-year evidence based)';
COMMENT ON COLUMN final_synthesis_current.per_layer_coverage IS 'JSON documenting year coverage for each layer';
COMMENT ON COLUMN final_synthesis_current.drivers IS 'Top positive drivers with type: level/momentum/stability';
COMMENT ON COLUMN final_synthesis_current.constraints IS 'Top constraints/weaknesses with type: level/momentum/stability';

-- ============================================================================
-- DATA REFRESH LOG UPDATE
-- Extend to track timeseries computation
-- ============================================================================

-- Add column to existing data_refresh_log if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='data_refresh_log'
        AND column_name='computation_type'
    ) THEN
        ALTER TABLE data_refresh_log
        ADD COLUMN computation_type VARCHAR(50);  -- 'raw_ingest', 'timeseries_features', 'scoring', 'classification'
    END IF;
END $$;

COMMENT ON COLUMN data_refresh_log.computation_type IS 'Type of computation: raw_ingest, timeseries_features, scoring, classification';

-- ============================================================================
-- VIEWS FOR CONVENIENCE
-- ============================================================================

-- View: Latest synthesis with explainability
CREATE OR REPLACE VIEW v_latest_synthesis AS
SELECT
    mc.fips_code,
    mc.county_name,
    fsc.*
FROM final_synthesis_current fsc
JOIN md_counties mc ON fsc.geoid = mc.fips_code
ORDER BY mc.county_name;

COMMENT ON VIEW v_latest_synthesis IS 'Latest synthesis groupings with county names for easy querying';

-- View: Timeseries features with metadata
CREATE OR REPLACE VIEW v_timeseries_summary AS
SELECT
    mc.county_name,
    ltf.layer_name,
    ltf.as_of_year,
    ltf.level_latest,
    ltf.momentum_slope,
    ltf.momentum_percent_change,
    ltf.stability_consistency,
    ltf.coverage_years,
    ltf.min_year,
    ltf.max_year,
    CASE
        WHEN ltf.coverage_years >= 5 THEN 'Full'
        WHEN ltf.coverage_years >= 3 THEN 'Partial'
        ELSE 'Sparse'
    END as coverage_quality
FROM layer_timeseries_features ltf
JOIN md_counties mc ON ltf.geoid = mc.fips_code
ORDER BY mc.county_name, ltf.layer_name;

COMMENT ON VIEW v_timeseries_summary IS 'Human-readable timeseries features with coverage quality';

-- ============================================================================
-- FUNCTION: Calculate missingness penalty
-- ============================================================================

CREATE OR REPLACE FUNCTION calculate_missingness_penalty(
    coverage_years INTEGER,
    window_size INTEGER DEFAULT 5
) RETURNS NUMERIC AS $$
BEGIN
    -- Penalty = 1 - (coverage_years / window_size)
    -- Full coverage (5/5) = 0.0 penalty
    -- Partial (3/5) = 0.4 penalty
    -- Sparse (1/5) = 0.8 penalty
    RETURN GREATEST(0, 1.0 - (coverage_years::NUMERIC / window_size::NUMERIC));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION calculate_missingness_penalty IS 'Deterministic penalty based on data coverage (0 = full, 1 = minimal)';

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_timeseries_composite
    ON layer_timeseries_features(geoid, layer_name, as_of_year DESC);

CREATE INDEX IF NOT EXISTS idx_layer_scores_composite
    ON layer_summary_scores(geoid, as_of_year DESC, layer_overall_score DESC);

-- GIN index for JSONB fields (for queries on drivers/constraints)
CREATE INDEX IF NOT EXISTS idx_final_synthesis_drivers
    ON final_synthesis_current USING GIN(drivers);

CREATE INDEX IF NOT EXISTS idx_final_synthesis_constraints
    ON final_synthesis_current USING GIN(constraints);

-- ============================================================================
-- SAMPLE QUERY EXAMPLES (for documentation)
-- ============================================================================

/*
-- Query 1: Get current synthesis for all counties
SELECT county_name, final_grouping, directional_status, confidence_level, uncertainty_level
FROM v_latest_synthesis
ORDER BY final_grouping, county_name;

-- Query 2: Get timeseries features for a specific county and layer
SELECT *
FROM layer_timeseries_features
WHERE geoid = '24031' AND layer_name = 'employment_gravity'
ORDER BY as_of_year DESC
LIMIT 1;

-- Query 3: Find counties with positive momentum in employment
SELECT mc.county_name, ltf.momentum_slope, ltf.momentum_percent_change, ltf.coverage_years
FROM layer_timeseries_features ltf
JOIN md_counties mc ON ltf.geoid = mc.fips_code
WHERE ltf.layer_name = 'employment_gravity'
  AND ltf.momentum_slope > 0
  AND ltf.coverage_years >= 3
ORDER BY ltf.momentum_slope DESC;

-- Query 4: Get counties with high uncertainty
SELECT county_name, uncertainty_level, uncertainty_reasons, per_layer_coverage
FROM v_latest_synthesis
WHERE uncertainty_level = 'high'
ORDER BY county_name;

-- Query 5: Get forecast-ready features for modeling
SELECT
    geoid,
    layer_name,
    level_latest,
    momentum_slope,
    momentum_percent_change,
    stability_consistency,
    stability_volatility,
    coverage_years
FROM layer_timeseries_features
WHERE as_of_year = (SELECT MAX(as_of_year) FROM layer_timeseries_features)
  AND coverage_years >= 3
ORDER BY geoid, layer_name;
*/
