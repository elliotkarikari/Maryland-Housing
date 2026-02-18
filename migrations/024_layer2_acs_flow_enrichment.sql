-- Migration 024: Layer 2 ACS general-flow enrichment + medallion flow tables
-- Date: 2026-02-17

-- Add ACS-flow mobility enrichment columns to county serving table
ALTER TABLE layer2_mobility_optionality
    ADD COLUMN IF NOT EXISTS acs_flow_year INTEGER,
    ADD COLUMN IF NOT EXISTS general_nonmovers BIGINT,
    ADD COLUMN IF NOT EXISTS general_inflow_total BIGINT,
    ADD COLUMN IF NOT EXISTS general_outflow_total BIGINT,
    ADD COLUMN IF NOT EXISTS general_net_flow BIGINT,
    ADD COLUMN IF NOT EXISTS general_inflow_rate NUMERIC(8,6),
    ADD COLUMN IF NOT EXISTS general_outflow_rate NUMERIC(8,6),
    ADD COLUMN IF NOT EXISTS general_net_flow_rate NUMERIC(8,6),
    ADD COLUMN IF NOT EXISTS general_flow_score NUMERIC(8,6),
    ADD COLUMN IF NOT EXISTS mobility_optionality_base_score NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS mobility_optionality_method VARCHAR(80);

-- ACS flows raw table for Layer 2 lineage (bronze)
CREATE TABLE IF NOT EXISTS layer2_acs_flows_raw (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) NOT NULL,
    geoid2 VARCHAR(16),
    sumlev2 VARCHAR(8),
    movedin BIGINT,
    movedout BIGINT,
    nonmovers BIGINT,
    fromdiffcty BIGINT,
    fromdiffstate BIGINT,
    fromabroad BIGINT,
    todiffcty BIGINT,
    todiffstate BIGINT,
    flow_year INTEGER NOT NULL,
    source_endpoint VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_l2_acs_flows_raw_year
    ON layer2_acs_flows_raw(flow_year);
CREATE INDEX IF NOT EXISTS idx_l2_acs_flows_raw_fips
    ON layer2_acs_flows_raw(fips_code);

-- Derived county-level ACS movement summary (silver)
CREATE TABLE IF NOT EXISTS layer2_county_general_flows (
    id SERIAL PRIMARY KEY,
    fips_code VARCHAR(5) NOT NULL,
    acs_flow_year INTEGER NOT NULL,
    general_nonmovers BIGINT,
    general_inflow_total BIGINT,
    general_outflow_total BIGINT,
    general_net_flow BIGINT,
    general_inflow_rate NUMERIC(8,6),
    general_outflow_rate NUMERIC(8,6),
    general_net_flow_rate NUMERIC(8,6),
    general_flow_score NUMERIC(8,6),
    mobility_flow_method VARCHAR(64),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_l2_county_general_flows_year
    ON layer2_county_general_flows(acs_flow_year);
CREATE INDEX IF NOT EXISTS idx_l2_county_general_flows_fips
    ON layer2_county_general_flows(fips_code);

-- Layer 2 GTFS raw lineage table (bronze in Databricks routing)
CREATE TABLE IF NOT EXISTS layer2_gtfs_feeds_raw (
    id SERIAL PRIMARY KEY,
    feed_name VARCHAR(100) NOT NULL,
    feed_url VARCHAR(500) NOT NULL,
    agency_name VARCHAR(200),
    feed_date DATE,
    download_timestamp TIMESTAMP,
    file_hash VARCHAR(64),
    is_active BOOLEAN DEFAULT TRUE,
    coverage_area VARCHAR(100),
    data_year INTEGER
);

CREATE INDEX IF NOT EXISTS idx_l2_gtfs_raw_data_year
    ON layer2_gtfs_feeds_raw(data_year);
