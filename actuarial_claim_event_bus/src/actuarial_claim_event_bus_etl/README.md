# actuarial_claim_event_bus_etl

Lakeflow Declarative Pipeline source for actuarial **Event Hubs bronze** ingest:

- **Bronze Streaming Tables** — Kafka/Event Hubs raw → clean + quarantine
- Shared helpers: `actuarial_claim_event_bus.kafka_source`, `.schemas`, `.quarantine`
- Event Hubs settings via pipeline configuration (`eh_bootstrap_servers`, `eh_*_topic`, `eh_jaas_config`)

## Datasets (schema `actuarial.event_bus`)

| Layer | Names | Type |
|-------|-------|------|
| Bronze raw | `bronze_*_raw` | Streaming Table |
| Bronze clean | `bronze_claims_bordereau`, `bronze_premium_bordereau`, `bronze_risk_zone_lookup`, `bronze_cyclone_events` | Streaming Table |
| Quarantine | `quarantine_bronze_*` | Streaming Table |

## Run

```bash
databricks bundle deploy --target dev
databricks bundle run actuarial_event_bus_job
```

Or start the continuous pipeline alone after seeding topics:

```bash
databricks bundle run actuarial_claim_event_bus_etl
```
