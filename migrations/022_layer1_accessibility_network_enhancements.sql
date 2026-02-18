-- Migration 022: Layer 1 accessibility network + county weighted accessibility fields
-- Date: 2026-02-15

ALTER TABLE layer1_employment_gravity
    ADD COLUMN IF NOT EXISTS high_wage_jobs_accessible_45min_weighted_mean NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS high_wage_jobs_accessible_45min_weighted_median NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS total_jobs_accessible_45min_weighted_mean NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS total_jobs_accessible_45min_weighted_median NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS high_wage_jobs_accessible_30min_weighted_mean NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS high_wage_jobs_accessible_30min_weighted_median NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS total_jobs_accessible_30min_weighted_mean NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS total_jobs_accessible_30min_weighted_median NUMERIC(14,2),
    ADD COLUMN IF NOT EXISTS accessibility_method VARCHAR(40),
    ADD COLUMN IF NOT EXISTS accessibility_threshold_30_min INTEGER,
    ADD COLUMN IF NOT EXISTS accessibility_threshold_45_min INTEGER,
    ADD COLUMN IF NOT EXISTS accessibility_proxy_distance_30_km NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS accessibility_proxy_distance_45_km NUMERIC(6,2);

