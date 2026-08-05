# actuarial_claim_event_bus

Databricks Asset Bundle that demonstrates **continuous bronze ingest** from **Azure Event Hubs** using the **Kafka protocol** into Unity Catalog **Streaming Tables**.

Scope matches [`actuarial-ingestion-adls`](../actuarial-ingestion-adls/) (**bronze only**), but the source is a message bus instead of files. Contrast with [`actuarial_claim_streaming_pipeline`](../actuarial_claim_streaming_pipeline/): that project uses Auto Loader + a full medallion (silver/gold).

| Item | Value |
|------|--------|
| Catalog / schema | `actuarial.event_bus` |
| Pipeline | `actuarial_claim_event_bus_etl` (serverless, **continuous**) |
| Job | `actuarial_event_bus_job` (`setup` → `seed_event_hubs` → `start_pipeline`) |
| CI | [Actuarial claim event bus](../.github/workflows/actuarial-claim-event-bus.yml) (`workflow_dispatch`) |

---

## Why this pattern

| | [`actuarial_claim_pipeline`](../actuarial_claim_pipeline/) | [`actuarial_claim_streaming_pipeline`](../actuarial_claim_streaming_pipeline/) | [`actuarial-ingestion-adls`](../actuarial-ingestion-adls/) | This project |
|--|--|--|--|--|
| Source | UC Volume CSVs | UC Volume + Auto Loader | ADLS `abfss://` | **Event Hubs (Kafka)** |
| Ingest | Batch overwrite | Streaming Tables (triggered) | Auto Loader `availableNow` | Streaming Tables (**continuous**) |
| Layers | Full medallion | Full medallion | Bronze only | **Bronze only** (raw / clean / quarantine) |

**Choose this project** when actuarial events arrive as an ongoing stream on Event Hubs (or any Kafka-compatible bus) and you want append-only bronze Streaming Tables with quarantine.

---

## Architecture

```text
fixtures/sample-events/*.jsonl
        │
        ▼
 Job: actuarial_event_bus_job
   setup → seed_event_hubs → start_pipeline
        │
        ▼
 Azure Event Hubs topics (Kafka protocol)
   actuarial.claims | premiums | risk_zones | cyclone_events
        │
        ▼
 Pipeline: actuarial_claim_event_bus_etl  (continuous)
        │
        ├── bronze_*_raw          (Streaming Table, Kafka + JSON parse)
        ├── bronze_*              (Streaming Table, clean)
        └── quarantine_bronze_*   (Streaming Table, bad rows)
```

### Dataset inventory (`actuarial.event_bus`)

| Table | Role |
|-------|------|
| `bronze_claims_bordereau_raw` (+ premiums / risk_zones / cyclone_events `_raw`) | Kafka ingest + JSON parse |
| `bronze_claims_bordereau` (+ three siblings) | Clean stream (`expect_or_drop` on keys) |
| `quarantine_bronze_claims_bordereau` (+ three siblings) | Parse failures or null business keys |

Audit columns on bronze: `_ingest_ts`, `_topic`, `_partition`, `_offset`, `_kafka_timestamp`, `_raw_value`, `_parse_error`.

Business keys: `claim_id`, `policy_id`, `postcode`, `event_id`.

---

## Prerequisites

1. Azure Event Hubs namespace with **Kafka protocol enabled**.
2. Four hubs/topics (defaults in `databricks.yml`):
   - `actuarial.claims`
   - `actuarial.premiums`
   - `actuarial.risk_zones`
   - `actuarial.cyclone_events`
3. Databricks secret scope **`actuarial-event-bus`** with key **`eh-jaas`** containing the JAAS login string, for example:

   ```text
   kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
   ```

4. Update `eh_bootstrap_servers` in `databricks.yml` (replace `YOUR_NAMESPACE`).
5. Databricks CLI authenticated (`databricks auth login` or `DATABRICKS_HOST` / `DATABRICKS_TOKEN`).
6. All-purpose cluster ID in `databricks.yml` (`cluster_id`) for setup/seed notebook tasks.

---

## Deploy and run

```bash
cd actuarial_claim_event_bus
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run actuarial_event_bus_job --target dev
```

The job seeds topics from `fixtures/sample-events/`, then starts the **continuous** pipeline. Stopping the pipeline:

```bash
databricks pipelines stop --pipeline-id <id>
# or stop from the Databricks UI
```

---

## Local tests

```bash
uv sync --dev
uv run pytest tests/ --ignore=tests/test_smoke_integration.py
```

Post-deploy smoke (requires tables created by a successful pipeline update):

```bash
ACTUARIAL_TEST_CATALOG=actuarial ACTUARIAL_TEST_SCHEMA=event_bus \
  uv run pytest tests/test_smoke_integration.py
```

Smoke skips automatically when Event Hubs is not configured or tables are missing.
