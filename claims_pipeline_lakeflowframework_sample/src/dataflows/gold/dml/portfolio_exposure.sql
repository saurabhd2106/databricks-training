SELECT
  p.insurer_name,
  p.region_name,
  p.wind_risk_band,
  p.building_type,
  p.mitigation_flag,
  COUNT(DISTINCT p.policy_id) AS policy_count,
  SUM(p.sum_insured) AS total_sum_insured,
  SUM(p.annual_premium) AS total_annual_premium,
  ROUND(AVG(p.sum_insured), 2) AS avg_sum_insured,
  ROUND(AVG(p.annual_premium), 2) AS avg_annual_premium,
  ROUND(
    SUM(p.annual_premium) / NULLIF(SUM(p.sum_insured), 0) * 100,
    4
  ) AS premium_rate_pct
FROM {silver_schema}.policies p
GROUP BY
  p.insurer_name,
  p.region_name,
  p.wind_risk_band,
  p.building_type,
  p.mitigation_flag
