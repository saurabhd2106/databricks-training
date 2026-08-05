# actuarial-ingestion-adls

Databricks Asset Bundle that demonstrates **batch incremental bronze ingest** from **ADLS** using **Auto Loader** (`cloudFiles`) with `.trigger(availableNow=True)`.

Each scheduled job run discovers files under an `abfss://` landing path, processes only what is **new since the last checkpoint**, appends to managed Delta tables, then stops.

| Item | Value |
|------|--------|
| Catalog / schema | `actuarial.ingestion` |
| Landing | `abfss://…/actuarial/ingestion/landing` |
| Autoloader state | `abfss://…/actuarial/ingestion/_autoloader` |
| Job | `actuarial_ingestion_job` (`setup` → `land_sample_data` → `bronze_ingest`) |
| Scope | **Bronze only** — four managed Delta tables |

---

## Why this pattern

| | [`actuarial_claim_pipeline`](../actuarial_claim_pipeline/) | [`actuarial_claim_streaming_pipeline`](../actuarial_claim_streaming_pipeline/) | This project |
|--|--|--|--|
| Orchestration | Jobs + notebooks + wheel | Jobs + Lakeflow Declarative Pipeline | Jobs + notebooks + wheel |
| Source | UC Volume (overwrite) | UC Volume + Streaming Tables | **ADLS `abfss://`** |
| Ingest | `spark.read.csv` overwrite | `@dp.table` + cloudFiles | cloudFiles + **availableNow** |
| Checkpoint / schemaLocation | n/a | Lakeflow-owned | **You own** on ADLS |
| Re-run behavior | Full overwrite | Incremental (pipeline state) | Incremental (**your** checkpoint) |

**Choose this project** when files land periodically in ADLS and you want a classic batch job that only picks up new files.

---

## Architecture

```text
fixtures/sample-data/
  claims/claims_batch_0N.csv
  premiums|risk_zones|cyclone_events/*.csv
        │
        ▼
 Job: actuarial_ingestion_job
   1) 01_setup           → CREATE SCHEMA actuarial.ingestion
   2) land_sample_data   → copy into ADLS landing (existing_cluster_id)
   3) bronze_ingest      → Auto Loader availableNow → Delta append
        │
        ▼
 abfss://…/actuarial/ingestion/landing/{claims,premiums,risk_zones,cyclone_events}/
 abfss://…/actuarial/ingestion/_autoloader/{dataset}/{schema,checkpoints}/
        │
        ▼
 actuarial.ingestion.bronze_claims_bordereau
 actuarial.ingestion.bronze_premium_bordereau
 actuarial.ingestion.bronze_risk_zone_lookup
 actuarial.ingestion.bronze_cyclone_events
```

### Output tables

| Table | Source path under landing |
|-------|---------------------------|
| `bronze_claims_bordereau` | `claims/` |
| `bronze_premium_bordereau` | `premiums/` |
| `bronze_risk_zone_lookup` | `risk_zones/` |
| `bronze_cyclone_events` | `cyclone_events/` |

Audit columns: `_ingest_ts`, `_source_file`.

---

## Prerequisites

1. Unity Catalog external location **`actuarial-uc-location`** covering `{uc_storage_root}/actuarial` (from [`deploy-databricks-azure`](../deploy-databricks-azure/)).
2. Job identity can **READ** landing and **WRITE** Autoloader state under that location.
3. Databricks CLI authenticated (`databricks auth login` or `DATABRICKS_HOST` / `DATABRICKS_TOKEN`).
4. All-purpose cluster ID in `databricks.yml` (`cluster_id`) for setup/land tasks.

Default paths (from Terraform `uc_storage_root`):

```text
abfss://metastore@dbxucfc9c48d2meta.dfs.core.windows.net/actuarial/ingestion/landing
abfss://metastore@dbxucfc9c48d2meta.dfs.core.windows.net/actuarial/ingestion/_autoloader
```

Override via bundle variables `landing_path` and `autoloader_state_path` if your storage root differs.

---

## Deploy and run

```bash
cd actuarial-ingestion-adls
uv sync --dev
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run actuarial_ingestion_job --target dev
```

### Incremental demo

1. First run with `claims_batch=01` (default) — lands batch 01 + dimensions, ingests them.
2. Second run with `claims_batch=02` — only the new claims file is appended; dims are skipped by Auto Loader (already checkpointed).
3. Repeat with `03` or `all`.

```bash
databricks bundle run actuarial_ingestion_job --target dev --params claims_batch=02
```

### Reset ingest state

Delete the dataset directories under `autoloader_state_path` (and optionally drop bronze tables), then re-run. Do **not** overwrite already-ingested claim batch filenames mid-demo if you want a clean “only new files” story.

---

## Repository layout

```text
actuarial-ingestion-adls/
├── databricks.yml
├── pyproject.toml
├── README.md
├── fixtures/sample-data/
├── resources/                     # schema + job
├── src/
│   ├── notebooks/                 # setup, land, bronze_ingest
│   └── actuarial_ingestion_adls/
│       ├── auto_loader.py         # cloudFiles + availableNow helpers
│       └── main.py
└── tests/
.github/workflows/actuarial-ingestion-adls.yml
```

---

## How ingest works

```python
(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)  # required outside Lakeflow
    .load(source_path)
    .writeStream.format("delta")
    .option("checkpointLocation", checkpoint_location)      # you own this
    .trigger(availableNow=True)                              # process new files, stop
    .outputMode("append")
    .toTable("actuarial.ingestion.bronze_claims_bordereau")
)
```

Shared helpers live in [`src/actuarial_ingestion_adls/auto_loader.py`](src/actuarial_ingestion_adls/auto_loader.py).

---

## Tests and CI

```bash
uv run pytest tests/
```

GitHub Actions workflow [`.github/workflows/actuarial-ingestion-adls.yml`](../.github/workflows/actuarial-ingestion-adls.yml): pytest → `bundle validate` → deploy → run job (manual `workflow_dispatch`; needs `DATABRICKS_HOST` / `DATABRICKS_TOKEN`).
