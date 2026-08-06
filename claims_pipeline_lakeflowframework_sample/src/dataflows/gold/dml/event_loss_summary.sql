SELECT
  CASE
    WHEN c.event_id IS NOT NULL THEN 'Catastrophe'
    ELSE 'Non-Catastrophe'
  END AS claim_category,
  e.event_id,
  COALESCE(e.event_name, 'Non-Catastrophe') AS event_name,
  e.start_date AS event_start,
  e.end_date AS event_end,
  DATEDIFF(e.end_date, e.start_date) AS event_duration_days,
  p.region_name,
  c.peril_type,
  COUNT(DISTINCT c.claim_id) AS claim_count,
  SUM(c.incurred_amount) AS total_incurred,
  ROUND(AVG(c.incurred_amount), 2) AS avg_claim_severity,
  MAX(c.incurred_amount) AS max_claim_severity,
  SUM(c.paid_to_date) AS total_paid,
  SUM(c.incurred_amount - c.paid_to_date) AS outstanding_reserve,
  SUM(CASE WHEN c.claim_status = 'Open' THEN 1 ELSE 0 END) AS open_claims,
  SUM(CASE WHEN c.claim_status = 'Closed' THEN 1 ELSE 0 END) AS closed_claims,
  SUM(CASE WHEN c.claim_status = 'Reopened' THEN 1 ELSE 0 END) AS reopened_claims
FROM {silver_schema}.claims_current c
LEFT JOIN {silver_schema}.cyclone_events e
  ON c.event_id = e.event_id
LEFT JOIN {silver_schema}.policies p
  ON c.policy_id = p.policy_id
GROUP BY
  CASE
    WHEN c.event_id IS NOT NULL THEN 'Catastrophe'
    ELSE 'Non-Catastrophe'
  END,
  e.event_id,
  COALESCE(e.event_name, 'Non-Catastrophe'),
  e.start_date,
  e.end_date,
  DATEDIFF(e.end_date, e.start_date),
  p.region_name,
  c.peril_type
