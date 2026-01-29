-- Migration 008: Add composite economic opportunity index
-- Combines local employment diversification (v1) with regional accessibility (v2)
-- Date: 2026-01-29

ALTER TABLE layer1_employment_gravity
    ADD COLUMN IF NOT EXISTS economic_opportunity_index NUMERIC(5,4);

-- Backfill where both components exist
UPDATE layer1_employment_gravity
SET economic_opportunity_index =
    0.4 * employment_diversification_score +
    0.6 * economic_accessibility_score
WHERE economic_opportunity_index IS NULL
  AND employment_diversification_score IS NOT NULL
  AND economic_accessibility_score IS NOT NULL;
