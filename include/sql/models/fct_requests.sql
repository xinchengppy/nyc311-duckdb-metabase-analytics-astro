-- Dimension table for complaint types
CREATE TABLE IF NOT EXISTS models.fct_requests (
    unique_key VARCHAR,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    agency VARCHAR,
    complaint_type VARCHAR,
    descriptor VARCHAR,
    incident_zip VARCHAR,
    borough VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    resolution_days INTEGER
);

-- Remove old data
DELETE FROM models.fct_requests;

-- Load new data
INSERT INTO models.fct_requests
SELECT
    unique_key,
    created_date,
    closed_date,
    agency,
    complaint_type,
    descriptor,
    incident_zip,
    borough,
    latitude,
    longitude,
    DATEDIFF('day', created_date, closed_date)
FROM staging.nyc_311
WHERE created_date IS NOT NULL 
  AND closed_date IS NOT NULL;