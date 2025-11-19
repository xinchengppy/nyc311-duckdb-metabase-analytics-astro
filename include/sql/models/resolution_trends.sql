-- View for resolution percentage trends by time frame
CREATE OR REPLACE VIEW models.resolution_trends AS
SELECT
    complaint_type,
    borough,
    '(01) 1 day' AS time_frame,
    1 AS time_sort,
    pct_resolved_within_1_days AS pct_resolved
FROM models.resolution_by_type
UNION ALL
SELECT
    complaint_type,
    borough,
    '(02) 3 days' AS time_frame,
    3 AS time_sort,
    pct_resolved_within_3_days AS pct_resolved
FROM models.resolution_by_type
UNION ALL
SELECT
    complaint_type,
    borough,
    '(03) 7 days' AS time_frame,
    7 AS time_sort,
    pct_resolved_within_7_days AS pct_resolved
FROM models.resolution_by_type
UNION ALL
SELECT
    complaint_type,
    borough,
    '(04) 30 days' AS time_frame,
    30 AS time_sort,
    pct_resolved_within_30_days AS pct_resolved
FROM models.resolution_by_type;