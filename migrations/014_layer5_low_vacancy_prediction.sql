-- Migration 014: Layer 5 low vacancy indicators + vacancy prediction metadata
-- Date: 2026-01-31

ALTER TABLE layer5_demographic_momentum
    ADD COLUMN IF NOT EXISTS low_vacancy_county_flag BOOLEAN,
    ADD COLUMN IF NOT EXISTS low_vacancy_units INTEGER,
    ADD COLUMN IF NOT EXISTS low_vacancy_occupied_units INTEGER,
    ADD COLUMN IF NOT EXISTS low_vacancy_percent_occupied NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS low_vacancy_fy INTEGER,
    ADD COLUMN IF NOT EXISTS vacancy_rate_pred NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS vacancy_predicted BOOLEAN,
    ADD COLUMN IF NOT EXISTS vacancy_pred_method VARCHAR(40),
    ADD COLUMN IF NOT EXISTS vacancy_pred_years INTEGER,
    ADD COLUMN IF NOT EXISTS vacancy_source VARCHAR(20);
