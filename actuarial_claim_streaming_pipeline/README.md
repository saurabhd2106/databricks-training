# actuarial_claim_streaming_pipeline

**Learn the bundle:** step-by-step trainer guide covering `databricks.yml`, permissions, schema/volume, job, notebooks, claims path (raw → quarantine → typed → silver → gold), the Python wheel, and hands-on execution labs → [`docs/bundle-training-guide.md`](docs/bundle-training-guide.md).

Databricks Asset Bundle that demonstrates a full actuarial **streaming medallion** on Unity Catalog using a serverless **Lakeflow Declarative Pipeline**.

It showcases **all three Lakeflow dataset types**:

| Type | Persist to UC? | In this project |
|------|----------------|-----------------|
| **Streaming Table** | Yes | Bronze raw / clean / quarantine (Auto Loader) |
| **Temporary view** | No | `v_claims_typed`, `v_premiums_typed` |
| **Materialized View** | Yes | Silver cleanses + gold actuarial marts |

| Item | Value |
|------|--------|
| Catalog / schema | `actuarial.streaming` |
| Landing volume | `/Volumes/actuarial/streaming/landing` |
| Pipeline | `actuarial_claim_streaming_etl` (serverless, triggered) |
| Job | `actuarial_streaming_job` (`setup` → `land_sample_data` → `refresh_pipeline`) |
| CI | [Actuarial claim streaming pipeline](../.github/workflows/actuarial-claim-streaming-pipeline.yml) (`workflow_dispatch`) |

Contrast with [`actuarial_claim_pipeline`](../actuarial_claim_pipeline/): that project uses Jobs + notebooks + `saveAsTable` overwrite. This project uses Lakeflow Streaming Tables / temp views / Materialized Views.

---

## Why streaming (vs the batch job)

| | Batch job | This streaming pipeline |
|--|--|--|
| Bronze | Full overwrite each run | Auto Loader appends **new files only** |
| Intermediate logic | Notebook cells / Python modules | Temporary views in the pipeline DAG |
| Silver / gold | Managed Delta overwrite | Materialized Views refreshed by the pipeline |
| Bad rows | Filtered away silently | Quarantine Streaming Tables |

---

## Core concepts

### Streaming Table vs Temporary View vs Materialized View

| Type | How records are handled | When to use |
|------|-------------------------|-------------|
| Streaming Table | Each input file/row processed once (append-oriented) | Ingest from cloud storage |
| Temporary view | Computed inside the pipeline; **not** a Catalog table | Intermediate typed logic without storage cost |
| Materialized View | Result kept up to date for the defining query | Silver cleanses, joins, gold marts |

**How to see temporary views:** open the pipeline in Databricks → graph / DAG. They will **not** appear under Catalog Explorer → `actuarial.streaming` as tables. Smoke tests assert `v_claims_typed` / `v_premiums_typed` are absent from `SHOW TABLES`.

### Auto Loader

Bronze raw tables use `spark.readStream.format("cloudFiles")`. Lakeflow owns schema location and checkpoints — do not set them in code. Reset ingest state with a **FULL REFRESH**, not by deleting ad-hoc paths.

### Quarantine

Clean bronze tables use `@dp.expect_or_drop` for null business keys. Matching invalid rows from bronze **raw** are written to `quarantine_bronze_*` with `quarantine_reason` and `_quarantine_ts` so failures are auditable.

### Triggered mode

The pipeline runs as **triggered** updates (job / on-demand). Continuous mode is out of scope.

---

## Architecture

```text
fixtures/sample-data/ (claims batches + dims)
        │
        ▼
 Job: actuarial_streaming_job
   setup → land_sample_data → refresh_pipeline
        │
        ▼
 /Volumes/actuarial/streaming/landing/{claims,premiums,risk_zones,cyclone_events}/
        │
        ▼
 Pipeline: actuarial_claim_streaming_etl
        │
        ├── bronze_*_raw          (Streaming Table, Auto Loader)
        ├── bronze_*              (Streaming Table, clean)
        ├── quarantine_bronze_*   (Streaming Table, bad rows)
        ├── v_claims_typed        (Temporary view)
        ├── v_premiums_typed      (Temporary view)
        ├── silver_*              (Materialized View)
        └── gold_*                (Materialized View)
```

### Dataset inventory (`actuarial.streaming`)

**Bronze Streaming Tables**

| Table | Role |
|-------|------|
| `bronze_claims_bordereau_raw` (+ premiums / risk_zones / cyclone_events `_raw`) | Auto Loader ingest |
| `bronze_claims_bordereau` (+ three siblings) | Clean stream (`expect_or_drop` on keys) |
| `quarantine_bronze_claims_bordereau` (+ three siblings) | Failed expectation rows |

**Temporary views (pipeline only)**

| View | Source → consumer |
|------|-------------------|
| `v_claims_typed` | clean claims → `silver_claims_bordereau` |
| `v_premiums_typed` | clean premiums → `silver_premium_bordereau` |

**Silver Materialized Views**

| Table | Notes |
|-------|--------|
| `silver_claims_bordereau` | Typed snapshots + quality filters |
| `silver_claims_current` | Latest row per `claim_id` |
| `silver_premium_bordereau` | Typed policies |
| `silver_cyclone_events` | Direct from clean bronze (no temp view) |
| `silver_risk_zone_lookup` | Postcode dedupe from clean bronze |

**Gold Materialized Views**

| Table | Grain |
|-------|--------|
| `gold_claims_summary` | Peril / status / region dims |
| `gold_loss_ratio_by_risk` | Insurer / risk dims |
| `gold_event_loss_summary` | Cat vs non-cat by event |
| `gold_portfolio_exposure` | Premiums ⋈ risk zones |
| `gold_claims_development` | Snapshot lag / reserve (uses claim snapshots) |

### AI/BI Lakeview dashboards

Deployed as bundle resources against SQL warehouse `${var.warehouse_id}` (Serverless Starter Warehouse).

Dashboard SQL uses **unqualified** table names. Each dashboard resource sets `dataset_catalog: ${var.catalog}` and `dataset_schema: ${resources.schemas.actuarial_streaming.name}`, so deploy resolves the schema correctly (`dev_<user>_streaming` in development, `streaming` in production).

| Dashboard resource | Audience | Sources |
|--------------------|----------|---------|
| `underwriting_portfolio` | Underwriters / portfolio managers | `gold_loss_ratio_by_risk`, `gold_portfolio_exposure` |
| `claims_operations` | Claims ops | `gold_claims_summary` |
| `catastrophe_events` | Cat / reinsurance | `gold_event_loss_summary` |
| `claims_development` | Actuaries / reserving | `gold_claims_development` |
| `pipeline_monitoring` | Data eng / platform ops | Quarantine + bronze / silver / gold row counts |
| `pipeline_event_log` | Data eng / platform ops | Published pipeline event log (`actuarial_claim_streaming_etl_event_log`) |

The pipeline publishes its event log as `actuarial_claim_streaming_etl_event_log` in the same schema as the medallion tables (do not delete that table). Refresh the pipeline after deploy so the table is populated before opening the event log dashboard.

Definitions: `resources/*.dashboard.yml` + `src/dashboards/*.lvdash.json`.

```bash
databricks bundle deploy --target dev
databricks bundle open underwriting_portfolio --target dev
databricks bundle open claims_operations --target dev
databricks bundle open catastrophe_events --target dev
databricks bundle open claims_development --target dev
databricks bundle open pipeline_monitoring --target dev
databricks bundle open pipeline_event_log --target dev
```

After UI edits, sync local JSON with:

```bash
databricks bundle generate dashboard --resource underwriting_portfolio --force
```

---

## Sample data design

Prepared from repo [`sample-data`](../sample-data/) into `fixtures/sample-data/`:

| Path | Contents |
|------|----------|
| `claims/claims_batch_01.csv` … `_03.csv` | 3 × 944-row chunks of claims bordereau |
| `premiums/premium_bordereau.csv` | Full premiums |
| `risk_zones/risk_zone_lookup.csv` | Postcode lookup |
| `cyclone_events/cyclone_events.csv` | Named events |

Job / notebook widget `claims_batch`: `01` (default) \| `02` \| `03` \| `all`.

Land claims as **new filenames** for the incremental Auto Loader demo. Details: [`fixtures/sample-data/README.md`](fixtures/sample-data/README.md).

---

## Repository layout

```text
actuarial_claim_streaming_pipeline/
├── databricks.yml
├── pyproject.toml
├── README.md
├── fixtures/sample-data/
├── resources/                     # schema, volume, pipeline, job, dashboards
├── src/
│   ├── notebooks/                 # setup + land
│   ├── dashboards/                # AI/BI Lakeview .lvdash.json
│   ├── actuarial_claim_streaming_pipeline/
│   │   ├── auto_loader.py         # cloudFiles + quarantine helpers
│   │   ├── silver.py / gold.py    # DataFrame builders (unit-tested)
│   │   └── pipeline_decorators.py # temporary_view / materialized_view fallbacks
│   └── actuarial_claim_streaming_etl/transformations/
│       ├── bronze_*_raw.py / bronze_*.py / quarantine_bronze_*.py
│       ├── v_claims_typed.py / v_premiums_typed.py
│       ├── silver_*.py / gold_*.py
└── tests/
.github/workflows/actuarial-claim-streaming-pipeline.yml
```

---

## How the pipeline is built

### Bundle

[`databricks.yml`](databricks.yml) — variables `catalog`, `cluster_id`, `landing_volume_path`, `warehouse_id`; targets `dev` / `prod`.

### UC resources

- Schema `streaming` + volume `landing`
- Pipeline: serverless, schema `streaming`, `configuration.landing_path`, glob on `transformations/**`, editable package install

### Job

`setup` → `land_sample_data` (`claims_batch`) → `refresh_pipeline` (normal refresh, not full refresh by default).

### Transform patterns

**Bronze raw (Streaming Table + Auto Loader):**

```python
@dp.table(name="bronze_claims_bordereau_raw", cluster_by_auto=True)
def bronze_claims_bordereau_raw():
    return read_landing_csv(spark, "claims", schema_hints=CLAIMS_SCHEMA_HINTS)
```

**Bronze clean + quarantine (dual stream from raw):**

```python
@dp.expect_or_drop("claim_id_not_null", "claim_id IS NOT NULL")
@dp.table(name="bronze_claims_bordereau")
def bronze_claims_bordereau():
    return spark.readStream.table("bronze_claims_bordereau_raw")

@dp.table(name="quarantine_bronze_claims_bordereau")
def quarantine_bronze_claims_bordereau():
    return quarantine_from_raw(spark.readStream.table("bronze_claims_bordereau_raw"), "claim_id")
```

**Temporary view → Materialized View:**

```python
@temporary_view(name="v_claims_typed")
def v_claims_typed():
    return transform_typed_claims(spark.read.table("bronze_claims_bordereau"))

@materialized_view(name="silver_claims_bordereau", cluster_by_auto=True)
def silver_claims_bordereau():
    return apply_claims_quality(spark.read.table("v_claims_typed"))
```

Decorators resolve via `pipeline_decorators.py` (`temporary_view` / `materialized_view` with fallbacks to `view` / `table`).

---

## Best practices applied

Aligned with [Lakeflow pipeline best practices](https://docs.databricks.com/aws/en/ldp/best-practices) and [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/):

| Practice | Implementation |
|----------|----------------|
| Streaming tables for cloud ingest | Bronze raw Auto Loader |
| Temp views for intermediates | `v_claims_typed`, `v_premiums_typed` |
| Materialized views for analytics | Silver + gold |
| Quarantine failed rows | `quarantine_bronze_*` dual flow |
| Pipeline-managed state | No manual schemaLocation / checkpoints |
| Triggered mode | Job-driven refresh |
| Parameterized landing path | Pipeline configuration |
| Expectations | Warn rescued data; drop null keys on clean bronze; silver quality expects |
| Liquid clustering | `cluster_by_auto=True` on persisted datasets |
| Append-only demo landing | Distinct `claims_batch_0N.csv` files |

---

## Prerequisites

- Databricks CLI authenticated to the workspace in `databricks.yml`
- Catalog `actuarial` grants for schemas, volumes, tables, streaming tables, and materialized views
- All-purpose cluster `0730-111218-jwuz715u` for setup/land tasks
- Repo secrets for CI: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
- Locally: [uv](https://docs.astral.sh/uv/) (`uv sync --dev`)

---

## Build, deploy, and run

### Local checks

```bash
cd actuarial_claim_streaming_pipeline
uv sync --dev
uv run pytest tests/ --ignore=tests/test_smoke_integration.py
```

Spark-backed unit tests skip when Databricks Connect is not configured.

### Deploy and first run

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run actuarial_streaming_job --target dev
```

Default `claims_batch=01`.

### Verify

```sql
-- Clean bronze
SELECT COUNT(*), COUNT(DISTINCT _source_file)
FROM actuarial.streaming.bronze_claims_bordereau;

-- Quarantine (usually empty on clean fixtures)
SELECT quarantine_reason, COUNT(*) AS n
FROM actuarial.streaming.quarantine_bronze_claims_bordereau
GROUP BY quarantine_reason;

-- Silver / gold
SELECT COUNT(*) FROM actuarial.streaming.silver_claims_current;
SELECT * FROM actuarial.streaming.gold_loss_ratio_by_risk LIMIT 20;
```

Confirm `v_claims_typed` / `v_premiums_typed` in the **pipeline DAG**, not in Catalog tables.

### Incremental demo

```bash
databricks bundle run actuarial_streaming_job --target dev --params claims_batch=02
```

Expect an additional `_source_file` on claims bronze and refreshed silver/gold MVs. Use **normal refresh** (not FULL REFRESH).

---

## CI (GitHub Actions)

Workflow: [`.github/workflows/actuarial-claim-streaming-pipeline.yml`](../.github/workflows/actuarial-claim-streaming-pipeline.yml)

1. **test** — `uv run pytest` (ignore smoke)
2. **validate** — `databricks bundle validate --target prod`
3. **deploy_and_run** — deploy + `actuarial_streaming_job`
4. **smoke** — post-deploy table checks against `actuarial.streaming`

Trigger: Actions → **Actuarial claim streaming pipeline** → Run workflow.

---

## Operating notes

| Action | Effect |
|--------|--------|
| Refresh | New files only; MVs update from current bronze/silver |
| FULL REFRESH | Rebuilds datasets and resets Auto Loader state |
| Quarantine query | Investigate `quarantine_reason` / `_rescued_data` |

Dev target uses development-mode prefixes and paused schedules; prod uses the fixed workspace root path.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Empty bronze | Land with `claims_batch=01` before refresh |
| Second batch no new rows | Land a **new** filename; avoid FULL REFRESH |
| Temp views “missing” in Catalog | Expected — check pipeline DAG |
| Import errors for package helpers | Redeploy; pipeline env needs `--editable ${workspace.file_path}` |
| Land task stuck | Start all-purpose cluster / update `cluster_id` |
| CI smoke skips | Pipeline/job did not publish expected tables |

---

## What’s next

- Continuous / real-time pipeline mode
- Richer quarantine (silver-rule failures, reprocessing flows)
- Path-filtered PR CI (in addition to `workflow_dispatch`)
- Scheduled dashboard snapshot emails / Genie spaces over gold marts

---

## Related projects

| Project | Role |
|---------|------|
| [`../sample-data`](../sample-data/) | Upstream CSVs |
| [`../actuarial_claim_pipeline`](../actuarial_claim_pipeline/) | Batch medallion job |
| [`../claims_pipeline_saurabh`](../claims_pipeline_saurabh/) | Lakeflow medallion with batch `@dp.table` reads |

---

## Quick reference

```bash
uv run pytest tests/ --ignore=tests/test_smoke_integration.py
databricks bundle deploy --target dev
databricks bundle run actuarial_streaming_job --target dev
databricks bundle run actuarial_streaming_job --target dev --params claims_batch=02
databricks bundle run actuarial_claim_streaming_etl --target dev
```
