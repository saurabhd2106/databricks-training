SELECT
  e.event_id,
  e.event_name,
  e.start_date,
  e.end_date,
  COUNT(DISTINCT c.claim_id) AS claim_count,
  SUM(c.incurred_amount) AS total_incurred,
  SUM(c.paid_to_date) AS total_paid,
  SUM(CASE WHEN c.claim_status = 'Open' THEN 1 ELSE 0 END) AS open_claims,
  SUM(CASE WHEN c.claim_status = 'Closed' THEN 1 ELSE 0 END) AS closed_claims,
  SUM(CASE WHEN c.claim_status = 'Reopened' THEN 1 ELSE 0 END) AS reopened_claims
FROM {silver_schema}.claims_current c
INNER JOIN {silver_schema}.cyclone_events e
  ON c.event_id = e.event_id
GROUP BY
  e.event_id,
  e.event_name,
  e.start_date,
  e.end_date
