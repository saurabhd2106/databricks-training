WITH claims_by_policy AS (
  SELECT
    policy_id,
    COUNT(DISTINCT claim_id) AS claim_count,
    SUM(incurred_amount) AS total_incurred,
    SUM(paid_to_date) AS total_paid
  FROM {silver_schema}.claims_current
  GROUP BY policy_id
)
SELECT
  p.insurer_name,
  p.wind_risk_band,
  p.building_type,
  COUNT(DISTINCT p.policy_id) AS policy_count,
  SUM(p.annual_premium) AS total_premium,
  COALESCE(SUM(c.claim_count), 0) AS claim_count,
  COALESCE(SUM(c.total_incurred), 0) AS total_incurred,
  COALESCE(SUM(c.total_paid), 0) AS total_paid,
  CASE
    WHEN SUM(p.annual_premium) > 0
    THEN COALESCE(SUM(c.total_incurred), 0) / SUM(p.annual_premium)
    ELSE CAST(NULL AS DOUBLE)
  END AS loss_ratio
FROM {silver_schema}.policies p
LEFT JOIN claims_by_policy c
  ON p.policy_id = c.policy_id
GROUP BY
  p.insurer_name,
  p.wind_risk_band,
  p.building_type
