-- View for complaint heatmap by time of day and day of week
CREATE OR REPLACE VIEW models.complaint_heatmap AS
SELECT
    EXTRACT(dow FROM created_date) AS day_order,  -- For sorting: 0=Sun, 6=Sat
    CASE EXTRACT(dow FROM created_date)
        WHEN 1 THEN '01-Monday'
        WHEN 2 THEN '02-Tuesday'
        WHEN 3 THEN '03-Wednesday'
        WHEN 4 THEN '04-Thursday'
        WHEN 5 THEN '05-Friday'
        WHEN 6 THEN '06-Saturday'
        WHEN 0 THEN '07-Sunday'
    END AS day_of_week,
    FLOOR(EXTRACT(hour FROM created_date) / 2) * 2 AS hour_bin_start,  -- 0,2,4,...,22
    LPAD(CAST(FLOOR(EXTRACT(hour FROM created_date) / 2) * 2 AS VARCHAR), 2, '0') || '-' || LPAD(CAST(FLOOR(EXTRACT(hour FROM created_date) / 2) * 2 + 2 AS VARCHAR), 2, '0') AS hour_range,
    complaint_type,
    COUNT(*) AS complaint_count,
    borough
FROM staging.nyc_311
WHERE created_date IS NOT NULL
GROUP BY day_order, day_of_week, hour_bin_start, hour_range, complaint_type, borough
ORDER BY day_order, hour_bin_start;