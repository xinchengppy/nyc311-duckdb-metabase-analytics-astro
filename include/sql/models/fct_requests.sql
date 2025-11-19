-- Dimension table for complaint types
DROP TABLE IF EXISTS models.fct_requests;
CREATE TABLE models.fct_requests (
    unique_key VARCHAR,
    created_date TIMESTAMP,
    created_day TIMESTAMP,
    closed_date TIMESTAMP,
    closed_day TIMESTAMP,
    agency VARCHAR,
    complaint_type VARCHAR,
    descriptor VARCHAR,
    incident_zip VARCHAR,
    borough VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    resolution_days INTEGER,
    is_resolved BOOLEAN
);

-- Remove old data
DELETE FROM models.fct_requests;

-- Load new data
INSERT INTO models.fct_requests
SELECT
    unique_key,
    created_date,
    DATE_TRUNC('day', created_date) AS created_day,
    closed_date,
    DATE_TRUNC('day', closed_date) AS closed_day,
    agency,
    complaint_type,
    descriptor,
    incident_zip,
    borough,
    latitude,
    longitude,
    CASE WHEN closed_date IS NOT NULL THEN DATEDIFF('day', created_date, closed_date) ELSE NULL END AS resolution_days,
    CASE WHEN closed_date IS NOT NULL THEN true ELSE false END AS is_resolved
FROM staging.nyc_311
WHERE created_date IS NOT NULL;