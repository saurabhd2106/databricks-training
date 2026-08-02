SELECT
  claim_id,
  policy_id,
  event_id,
  date_of_loss,
  reported_date,
  peril_type,
  claim_status,
  incurred_amount,
  paid_to_date,
  snapshot_date,
  _ingest_ts
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY snapshot_date DESC) AS _rn
  FROM live.claims_snapshots
)
WHERE _rn = 1
