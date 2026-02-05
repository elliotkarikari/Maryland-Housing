-- Widen pollution/proximity columns to support real EJScreen ranges.
ALTER TABLE layer6_risk_drag
    ALTER COLUMN proximity_hazwaste_score TYPE NUMERIC(14,4),
    ALTER COLUMN traffic_proximity_score TYPE NUMERIC(14,4);
