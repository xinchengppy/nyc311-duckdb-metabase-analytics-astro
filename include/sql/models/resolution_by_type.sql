-- View for service level analysis - time to close by complaint type
CREATE OR REPLACE VIEW models.resolution_by_type AS
SELECT
    complaint_type,
    borough,
    COUNT(*) AS total_requests,
    COUNT(CASE WHEN is_resolved THEN 1 END) AS resolved_requests,
    AVG(CASE WHEN is_resolved THEN resolution_days END) AS avg_resolution_days,
    MEDIAN(CASE WHEN is_resolved THEN resolution_days END) AS median_resolution_days,
    MIN(CASE WHEN is_resolved THEN resolution_days END) AS min_resolution_days,
    MAX(CASE WHEN is_resolved THEN resolution_days END) AS max_resolution_days,
    -- Service level metrics
    COUNT(CASE WHEN is_resolved AND resolution_days <= 1 THEN 1 END) AS resolved_within_1_days,
    COUNT(CASE WHEN is_resolved AND resolution_days <= 3 THEN 1 END) AS resolved_within_3_days,
    COUNT(CASE WHEN is_resolved AND resolution_days <= 7 THEN 1 END) AS resolved_within_7_days,
    COUNT(CASE WHEN is_resolved AND resolution_days <= 30 THEN 1 END) AS resolved_within_30_days,
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND resolution_days <= 1 THEN 1 END) / COUNT(*), 2) AS pct_resolved_within_1_days,
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND resolution_days <= 3 THEN 1 END) / COUNT(*), 2) AS pct_resolved_within_3_days,
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND resolution_days <= 7 THEN 1 END) / COUNT(*), 2) AS pct_resolved_within_7_days,
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND resolution_days <= 30 THEN 1 END) / COUNT(*), 2) AS pct_resolved_within_30_days
FROM models.fct_requests
GROUP BY complaint_type, borough
ORDER BY total_requests DESC, avg_resolution_days DESC;