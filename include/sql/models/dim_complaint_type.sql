-- Dimension table for complaint types
DROP TABLE IF EXISTS models.dim_complaint_type;
CREATE TABLE models.dim_complaint_type (
    complaint_type VARCHAR,
    descriptor VARCHAR
);

-- Clear previous data
DELETE FROM models.dim_complaint_type;

-- Insert new dimension rows
INSERT INTO models.dim_complaint_type
SELECT DISTINCT
    complaint_type,
    descriptor
FROM staging.nyc_311
WHERE complaint_type IS NOT NULL;