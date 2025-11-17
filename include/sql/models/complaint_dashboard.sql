-- Comprehensive dashboard view for complaint analysis
CREATE OR REPLACE VIEW models.complaint_dashboard AS
SELECT
    DATE(created_date) AS complaint_date,
    DATE_TRUNC('week', created_date) AS complaint_week,
    DATE_TRUNC('month', created_date) AS complaint_month,
    borough,
    complaint_type,
    status,
    DATEDIFF('day', created_date, closed_date) AS resolution_days,
    CASE
        WHEN closed_date IS NULL THEN 'Open'
        WHEN DATEDIFF('day', created_date, closed_date) <= 1 THEN 'Same Day'
        WHEN DATEDIFF('day', created_date, closed_date) <= 7 THEN 'Within Week'
        WHEN DATEDIFF('day', created_date, closed_date) <= 30 THEN 'Within Month'
        ELSE 'Over Month'
    END AS resolution_category,
    agency,
    latitude,
    longitude
FROM staging.nyc_311
WHERE created_date IS NOT NULL;