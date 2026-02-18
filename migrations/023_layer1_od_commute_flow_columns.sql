-- Migration 023: Layer 1 OD commute-flow enrichment columns
-- Date: 2026-02-15

ALTER TABLE layer1_employment_gravity
    ADD COLUMN IF NOT EXISTS total_jobs INTEGER,
    ADD COLUMN IF NOT EXISTS sector_diversity_entropy NUMERIC(10,6),
    ADD COLUMN IF NOT EXISTS stable_sector_share NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS entrepreneurship_density NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS od_year INTEGER,
    ADD COLUMN IF NOT EXISTS od_resident_workers BIGINT,
    ADD COLUMN IF NOT EXISTS od_inbound_workers BIGINT,
    ADD COLUMN IF NOT EXISTS od_outbound_workers BIGINT,
    ADD COLUMN IF NOT EXISTS od_live_work_same_county BIGINT,
    ADD COLUMN IF NOT EXISTS od_net_commuter_flow BIGINT,
    ADD COLUMN IF NOT EXISTS od_local_capture_rate NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS od_working_age_resident_workers BIGINT,
    ADD COLUMN IF NOT EXISTS od_working_age_live_work_same_county BIGINT,
    ADD COLUMN IF NOT EXISTS od_working_age_share NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS od_working_age_local_capture_rate NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS od_high_wage_resident_workers BIGINT,
    ADD COLUMN IF NOT EXISTS od_high_wage_live_work_same_county BIGINT,
    ADD COLUMN IF NOT EXISTS od_high_wage_share NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS od_high_wage_local_capture_rate NUMERIC(8,4);
