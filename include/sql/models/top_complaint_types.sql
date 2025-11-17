-- View for top complaint types over time
CREATE OR REPLACE VIEW models.top_complaint_types AS
SELECT
    DATE_TRUNC('month', created_date) AS month,
    complaint_type,
    COUNT(*) AS monthly_count,
    RANK() OVER (PARTITION BY DATE_TRUNC('month', created_date) ORDER BY COUNT(*) DESC) AS monthly_rank
FROM staging.nyc_311
WHERE created_date IS NOT NULL AND complaint_type IS NOT NULL
GROUP BY DATE_TRUNC('month', created_date), complaint_type
ORDER BY month DESC, monthly_count DESC;