SELECT
  c.peril_type,
  c.claim_status,
  p.region_name,
  p.wind_risk_band,
  p.building_type,
  COUNT(DISTINCT c.claim_id) AS claim_count,
  SUM(c.incurred_amount) AS total_incurred,
  SUM(c.paid_to_date) AS total_paid,
  SUM(c.incurred_amount - c.paid_to_date) AS outstanding_reserve,
  ROUND(AVG(c.incurred_amount), 2) AS avg_claim_severity,
  ROUND(
    SUM(c.paid_to_date) / NULLIF(SUM(c.incurred_amount), 0) * 100,
    2
  ) AS settlement_pct
FROM {silver_schema}.claims_current c
LEFT JOIN {silver_schema}.policies p
  ON c.policy_id = p.policy_id
GROUP BY
  c.peril_type,
  c.claim_status,
  p.region_name,
  p.wind_risk_band,
  p.building_type
