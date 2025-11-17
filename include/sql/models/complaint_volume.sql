-- View for complaint volume over time with breakdowns
CREATE OR REPLACE VIEW models.complaint_volume AS
SELECT
    DATE(created_date) AS date,
    borough,
    complaint_type,
    COUNT(*) AS complaint_count
FROM staging.nyc_311
WHERE created_date IS NOT NULL
GROUP BY DATE(created_date), borough, complaint_type
ORDER BY date, borough, complaint_count DESC;