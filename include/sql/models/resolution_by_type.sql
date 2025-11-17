-- View for service level analysis - time to close by complaint type
CREATE OR REPLACE VIEW models.resolution_by_type AS
SELECT
    complaint_type,
    borough,
    COUNT(*) AS total_requests,
    COUNT(CASE WHEN resolution_days IS NOT NULL THEN 1 END) AS resolved_requests,
    AVG(resolution_days) AS avg_resolution_days,
    MEDIAN(resolution_days) AS median_resolution_days,
    MIN(resolution_days) AS min_resolution_days,
    MAX(resolution_days) AS max_resolution_days,
    -- Service level metrics
    COUNT(CASE WHEN resolution_days <= 7 THEN 1 END) AS resolved_within_7_days,
    COUNT(CASE WHEN resolution_days <= 30 THEN 1 END) AS resolved_within_30_days,
    ROUND(100.0 * COUNT(CASE WHEN resolution_days <= 7 THEN 1 END) / COUNT(*), 2) AS pct_resolved_within_7_days,
    ROUND(100.0 * COUNT(CASE WHEN resolution_days <= 30 THEN 1 END) / COUNT(*), 2) AS pct_resolved_within_30_days
FROM models.fct_requests
GROUP BY complaint_type, borough
ORDER BY total_requests DESC, avg_resolution_days DESC;