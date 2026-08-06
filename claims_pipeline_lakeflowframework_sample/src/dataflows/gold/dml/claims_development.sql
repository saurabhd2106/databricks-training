SELECT
  c.peril_type,
  p.region_name,
  p.wind_risk_band,
  DATE_TRUNC('MONTH', c.date_of_loss) AS loss_month,
  DATE_TRUNC('MONTH', c.reported_date) AS reported_month,
  COUNT(DISTINCT c.claim_id) AS claim_count,
  ROUND(AVG(DATEDIFF(c.reported_date, c.date_of_loss)), 1) AS avg_reporting_lag_days,
  MAX(DATEDIFF(c.reported_date, c.date_of_loss)) AS max_reporting_lag_days,
  SUM(c.incurred_amount) AS total_incurred,
  SUM(c.paid_to_date) AS total_paid,
  SUM(c.incurred_amount - c.paid_to_date) AS outstanding_reserve,
  ROUND(
    SUM(c.paid_to_date) / NULLIF(SUM(c.incurred_amount), 0) * 100,
    2
  ) AS payment_progress_pct
FROM {silver_schema}.claims_current c
LEFT JOIN {silver_schema}.policies p
  ON c.policy_id = p.policy_id
GROUP BY
  c.peril_type,
  p.region_name,
  p.wind_risk_band,
  DATE_TRUNC('MONTH', c.date_of_loss),
  DATE_TRUNC('MONTH', c.reported_date)
