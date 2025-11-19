-- Dynamic view for top complaint types by borough with date range (use in Metabase with filters)
-- Note: This view is for reference; use native SQL in Metabase for dynamic dates
CREATE OR REPLACE VIEW models.top_complaint_types_dynamic AS
SELECT
    borough,
    complaint_type,
    COUNT(*) AS complaint_count,
    RANK() OVER (PARTITION BY borough ORDER BY COUNT(*) DESC) AS rank
FROM models.fct_requests
WHERE created_date IS NOT NULL
  AND complaint_type IS NOT NULL
  AND borough IS NOT NULL
GROUP BY borough, complaint_type
ORDER BY borough, complaint_count DESC;