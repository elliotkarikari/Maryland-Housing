-- Migration 013: Add QWI + HUD affordability enrichment columns
-- Date: 2026-01-31

-- =============================================================================
-- LAYER 1: EMPLOYMENT GRAVITY (QWI DYNAMICS)
-- =============================================================================
ALTER TABLE layer1_employment_gravity
    ADD COLUMN IF NOT EXISTS qwi_emp_total INTEGER,
    ADD COLUMN IF NOT EXISTS qwi_hires INTEGER,
    ADD COLUMN IF NOT EXISTS qwi_separations INTEGER,
    ADD COLUMN IF NOT EXISTS qwi_hire_rate NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS qwi_separation_rate NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS qwi_turnover_rate NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS qwi_net_job_growth_rate NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS qwi_year INTEGER;

-- =============================================================================
-- LAYER 4: HOUSING ELASTICITY (HUD FMR + LIHTC)
-- =============================================================================
ALTER TABLE layer4_housing_elasticity
    ADD COLUMN IF NOT EXISTS fmr_2br INTEGER,
    ADD COLUMN IF NOT EXISTS fmr_2br_to_income NUMERIC(6,4),
    ADD COLUMN IF NOT EXISTS hud_fmr_year INTEGER,
    ADD COLUMN IF NOT EXISTS lihtc_units INTEGER,
    ADD COLUMN IF NOT EXISTS lihtc_units_per_1000_households NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS lihtc_year INTEGER;
