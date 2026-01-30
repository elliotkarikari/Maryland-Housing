-- Migration 011: Expand NCES school ID length
-- Date: 2026-01-30

ALTER TABLE education_school_directory
    ALTER COLUMN nces_school_id TYPE VARCHAR(15);
