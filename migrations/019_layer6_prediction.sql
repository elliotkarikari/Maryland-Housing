-- Migration 019: Layer 6 prediction columns
-- Date: 2026-01-31

ALTER TABLE layer6_risk_drag
    ADD COLUMN IF NOT EXISTS risk_drag_index_pred NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS risk_drag_index_predicted BOOLEAN,
    ADD COLUMN IF NOT EXISTS risk_drag_index_pred_method VARCHAR(40),
    ADD COLUMN IF NOT EXISTS risk_drag_index_pred_years INTEGER,
    ADD COLUMN IF NOT EXISTS risk_drag_index_source VARCHAR(20),
    ADD COLUMN IF NOT EXISTS risk_drag_index_effective NUMERIC(5,4);
