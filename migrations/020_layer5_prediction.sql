-- Migration 020: Layer 5 prediction columns for demographic opportunity index
-- Date: 2026-01-31

ALTER TABLE layer5_demographic_momentum
    ADD COLUMN IF NOT EXISTS demographic_opportunity_index_pred NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS demographic_opportunity_index_predicted BOOLEAN,
    ADD COLUMN IF NOT EXISTS demographic_opportunity_index_pred_method VARCHAR(40),
    ADD COLUMN IF NOT EXISTS demographic_opportunity_index_pred_years INTEGER,
    ADD COLUMN IF NOT EXISTS demographic_opportunity_index_source VARCHAR(20),
    ADD COLUMN IF NOT EXISTS demographic_opportunity_index_effective NUMERIC(5,4);
