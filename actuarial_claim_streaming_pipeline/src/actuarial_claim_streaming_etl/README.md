# actuarial_claim_streaming_etl

Lakeflow Declarative Pipeline source for actuarial streaming medallion:

- **Bronze Streaming Tables** — Auto Loader raw → clean + quarantine
- **Temporary views** — `v_claims_typed`, `v_premiums_typed` (pipeline-scoped only)
- **Silver / gold Materialized Views** — typed cleanses and actuarial marts
- Shared helpers: `actuarial_claim_streaming_pipeline.auto_loader`, `.silver`, `.gold`
- Landing path via pipeline configuration `landing_path`
  (default `/Volumes/actuarial/streaming/landing`)

## Datasets (schema `actuarial.streaming`)

| Layer | Names | Type |
|-------|-------|------|
| Bronze raw | `bronze_*_raw` | Streaming Table |
| Bronze clean | `bronze_claims_bordereau`, `bronze_premium_bordereau`, `bronze_risk_zone_lookup`, `bronze_cyclone_events` | Streaming Table |
| Quarantine | `quarantine_bronze_*` | Streaming Table |
| Temp views | `v_claims_typed`, `v_premiums_typed` | Temporary view (not in UC) |
| Silver | `silver_claims_bordereau`, `silver_claims_current`, `silver_premium_bordereau`, `silver_cyclone_events`, `silver_risk_zone_lookup` | Materialized View |
| Gold | `gold_claims_summary`, `gold_loss_ratio_by_risk`, `gold_event_loss_summary`, `gold_portfolio_exposure`, `gold_claims_development` | Materialized View |

## Run

```bash
databricks bundle deploy --target dev
databricks bundle run actuarial_streaming_job
```

Or refresh the pipeline alone after landing data:

```bash
databricks bundle run actuarial_claim_streaming_etl
```
