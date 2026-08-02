WITH claims_by_policy AS (
  SELECT
    policy_id,
    COUNT(DISTINCT claim_id) AS claim_count,
    SUM(incurred_amount) AS total_incurred
  FROM {silver_schema}.claims_current
  GROUP BY policy_id
)
SELECT
  p.wind_risk_band,
  p.region_name,
  COUNT(DISTINCT p.policy_id) AS policy_count,
  SUM(p.annual_premium) AS total_premium,
  COALESCE(SUM(c.claim_count), 0) AS claim_count,
  COALESCE(SUM(c.total_incurred), 0) AS total_incurred,
  CASE
    WHEN COUNT(DISTINCT p.policy_id) > 0
    THEN COALESCE(SUM(c.claim_count), 0) / COUNT(DISTINCT p.policy_id)
    ELSE CAST(0.0 AS DOUBLE)
  END AS claim_frequency,
  CASE
    WHEN SUM(p.annual_premium) > 0
    THEN COALESCE(SUM(c.total_incurred), 0) / SUM(p.annual_premium)
    ELSE CAST(NULL AS DOUBLE)
  END AS loss_ratio
FROM {silver_schema}.policies p
LEFT JOIN claims_by_policy c
  ON p.policy_id = c.policy_id
GROUP BY
  p.wind_risk_band,
  p.region_name
