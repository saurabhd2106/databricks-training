SELECT 'bronze' AS layer, 'claims_bordereau' AS table_name, COUNT(*) AS row_count, MAX(_ingest_ts) AS latest_ingest_ts
FROM {bronze_schema}.claims_bordereau
UNION ALL
SELECT 'bronze', 'premium_bordereau', COUNT(*), MAX(_ingest_ts)
FROM {bronze_schema}.premium_bordereau
UNION ALL
SELECT 'bronze', 'risk_zone_lookup', COUNT(*), MAX(_ingest_ts)
FROM {bronze_schema}.risk_zone_lookup
UNION ALL
SELECT 'bronze', 'bronze_cyclone_events', COUNT(*), MAX(_ingest_ts)
FROM {bronze_schema}.bronze_cyclone_events
UNION ALL
SELECT 'bronze', 'claims_bordereau_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {bronze_schema}.claims_bordereau_quarantine
UNION ALL
SELECT 'bronze', 'premium_bordereau_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {bronze_schema}.premium_bordereau_quarantine
UNION ALL
SELECT 'bronze', 'risk_zone_lookup_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {bronze_schema}.risk_zone_lookup_quarantine
UNION ALL
SELECT 'bronze', 'bronze_cyclone_events_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {bronze_schema}.bronze_cyclone_events_quarantine
UNION ALL
SELECT 'silver', 'claims_snapshots', COUNT(*), MAX(_ingest_ts)
FROM {silver_schema}.claims_snapshots
UNION ALL
SELECT 'silver', 'claims_current', COUNT(*), MAX(_ingest_ts)
FROM {silver_schema}.claims_current
UNION ALL
SELECT 'silver', 'policies', COUNT(*), MAX(_ingest_ts)
FROM {silver_schema}.policies
UNION ALL
SELECT 'silver', 'cyclone_events', COUNT(*), MAX(_ingest_ts)
FROM {silver_schema}.cyclone_events
UNION ALL
SELECT 'silver', 'risk_zones', COUNT(*), MAX(_ingest_ts)
FROM {silver_schema}.risk_zones
UNION ALL
SELECT 'silver', 'claims_snapshots_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {silver_schema}.claims_snapshots_quarantine
UNION ALL
SELECT 'silver', 'policies_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {silver_schema}.policies_quarantine
UNION ALL
SELECT 'silver', 'cyclone_events_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {silver_schema}.cyclone_events_quarantine
UNION ALL
SELECT 'silver', 'risk_zones_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {silver_schema}.risk_zones_quarantine
UNION ALL
SELECT 'gold', 'event_loss_summary', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.event_loss_summary
UNION ALL
SELECT 'gold', 'policy_loss_ratio', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.policy_loss_ratio
UNION ALL
SELECT 'gold', 'risk_band_performance', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.risk_band_performance
UNION ALL
SELECT 'gold', 'claims_summary', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.claims_summary
UNION ALL
SELECT 'gold', 'claims_development', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.claims_development
UNION ALL
SELECT 'gold', 'portfolio_exposure', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.portfolio_exposure
UNION ALL
SELECT 'gold', 'event_loss_summary_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.event_loss_summary_quarantine
UNION ALL
SELECT 'gold', 'policy_loss_ratio_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.policy_loss_ratio_quarantine
UNION ALL
SELECT 'gold', 'risk_band_performance_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.risk_band_performance_quarantine
UNION ALL
SELECT 'gold', 'claims_summary_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.claims_summary_quarantine
UNION ALL
SELECT 'gold', 'claims_development_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.claims_development_quarantine
UNION ALL
SELECT 'gold', 'portfolio_exposure_quarantine', COUNT(*), CAST(NULL AS TIMESTAMP)
FROM {gold_schema}.portfolio_exposure_quarantine
