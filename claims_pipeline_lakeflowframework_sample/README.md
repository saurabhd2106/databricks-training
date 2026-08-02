# claims_pipeline_lakeflowframework_sample

Databricks Asset Bundle that uses the **Lakeflow Framework** (metadata-driven YAML dataflows) to ingest cyclone/insurance sample CSVs and build an actuarial **bronze → silver → gold** medallion lakehouse in Unity Catalog.

This is the Lakeflow Framework counterpart to [`claims_pipeline_saurabh`](../claims_pipeline_saurabh/) (Python `@dp.table`). Sample files live under `fixtures/sample-data/` (copied from [`sample-data`](../sample-data/)).

## Learn / Build it yourself

To recreate this pipeline from scratch (scaffold, landing, bronze/silver/gold specs, job wiring), follow the step-by-step guide:

**[docs/MANUAL_BUILD_GUIDE.md](docs/MANUAL_BUILD_GUIDE.md)**

This README is the quick start for deploying the finished project.

## What this repo does

1. **Creates** catalog/schemas (notebook) and a UC Volume (`actuarial_lff.bronze.landing`).
2. **Lands** four CSVs into the volume via a Job notebook on the all-purpose cluster.
3. **Runs** three serverless Lakeflow Framework pipelines:
   - **Bronze** — Auto Loader (`cloudFiles`) CSV ingest
   - **Silver** — typed SCD1 tables + `claims_current` materialized view
   - **Gold** — actuarial marts as materialized views

```text
fixtures/sample-data/*.csv
        │
        ▼
 Job: claims_lff_pipeline_job
        ├── create_schemas_and_tables
        ├── land_sample_data  ──►  Volume actuarial_lff.bronze.landing
        ├── claims_bronze_pipeline
        ├── claims_silver_pipeline
        └── claims_gold_pipeline
                │
                ├── actuarial_lff.bronze.*
                ├── actuarial_lff.silver.*
                └── actuarial_lff.gold.*
```

## Prerequisites

1. Deploy the Lakeflow Framework bundle first:

   ```bash
   cd ../lakeflow_framework
   databricks bundle deploy -t dev
   ```

   Pipelines attach `${framework_source_path}/dlt_pipeline` (defaults to your user `.bundle/lakeflow_framework/dev/current/files/src`).

2. Unity Catalog rights to create catalog `actuarial_lff` (or have an admin create it) with schemas `bronze`, `silver`, `gold`, and grants for `CREATE_TABLE` / `CREATE_MATERIALIZED_VIEW` / `CREATE_VOLUME`.

3. Databricks CLI authenticated to `https://adb-7405611775215693.13.azuredatabricks.net`.

4. All-purpose cluster `0730-111218-jwuz715u` startable for setup/land notebook tasks.

**Optional one-time catalog bootstrap** (if you prefer SQL over the job notebook):

```sql
CREATE CATALOG IF NOT EXISTS actuarial_lff;
CREATE SCHEMA IF NOT EXISTS actuarial_lff.bronze;
CREATE SCHEMA IF NOT EXISTS actuarial_lff.silver;
CREATE SCHEMA IF NOT EXISTS actuarial_lff.gold;
```

Create the catalog/schemas **before** `bundle deploy` so the landing volume resource can attach to `actuarial_lff.bronze`.

## Source datasets

| File | Grain / notes | Join keys |
|------|----------------|-----------|
| `claims_bordereau.csv` | SCD-style claim snapshots (`claim_id` + `snapshot_date`) | `policy_id`, `event_id` |
| `premium_bordereau.csv` | One row per policy (~5,000) | `policy_id`, `postcode` |
| `risk_zone_lookup.csv` | Postcode → region / wind risk band | `postcode` |
| `cyclone_events.csv` | Six illustrative cyclone events | `event_id` |

## Output tables

| Layer | Schema | Tables |
|-------|--------|--------|
| Bronze | `actuarial_lff.bronze` | `claims_bordereau`, `premium_bordereau`, `risk_zone_lookup`, `cyclone_events` |
| Silver | `actuarial_lff.silver` | `claims_snapshots`, `claims_current`, `policies`, `risk_zones`, `cyclone_events` |
| Gold | `actuarial_lff.gold` | `event_loss_summary`, `policy_loss_ratio`, `risk_band_performance` |

## Repository layout

```text
claims_pipeline_lakeflowframework_sample/
├── databricks.yml
├── fixtures/sample-data/
├── resources/
│   ├── landing.volume.yml
│   ├── claims_bronze_pipeline.yml
│   ├── claims_silver_pipeline.yml
│   ├── claims_gold_pipeline.yml
│   └── claims_pipeline_job.job.yml
└── src/
    ├── notebooks/                 # initialize, create schemas, land CSVs
    ├── pipeline_configs/          # YAML format + substitutions
    └── dataflows/{bronze,silver,gold}/
```

## Deploy and run

```bash
cd claims_pipeline_lakeflowframework_sample

# Ensure actuarial_lff.bronze exists (see Prerequisites), then:
databricks bundle validate --target dev
databricks bundle deploy --target dev

# Create schemas (if needed), land CSVs, run bronze → silver → gold
databricks bundle run claims_pipeline_job
```

Override the framework path if needed:

```bash
databricks bundle deploy -t dev \
  --var="framework_source_path=/Workspace/Users/<user>/.bundle/lakeflow_framework/dev/current/files/src"
```

## Verify data

| Check | Expected |
|-------|----------|
| Bronze / silver claim snapshots | ~2,829 |
| `actuarial_lff.silver.claims_current` | ~1,533 unique claims |
| `actuarial_lff.silver.policies` | 5,000 |
| `actuarial_lff.silver.risk_zones` | 19 (one postcode deduped) |
| `actuarial_lff.silver.cyclone_events` | 6 |
| `actuarial_lff.gold.event_loss_summary` | 6 |

```sql
SELECT 'bronze_claims' AS t, COUNT(*) AS n FROM actuarial_lff.bronze.claims_bordereau
UNION ALL SELECT 'silver_snapshots', COUNT(*) FROM actuarial_lff.silver.claims_snapshots
UNION ALL SELECT 'silver_current', COUNT(*) FROM actuarial_lff.silver.claims_current
UNION ALL SELECT 'silver_policies', COUNT(*) FROM actuarial_lff.silver.policies
UNION ALL SELECT 'gold_event_loss', COUNT(*) FROM actuarial_lff.gold.event_loss_summary;

SELECT * FROM actuarial_lff.gold.event_loss_summary ORDER BY total_incurred DESC;
```

## Related repos

| Path | Role |
|------|------|
| [`../lakeflow_framework`](../lakeflow_framework/) | Framework runtime (`dlt_pipeline` notebook) — deploy first |
| [`../claims_pipeline_saurabh`](../claims_pipeline_saurabh/) | Same domain model using Python declarative pipelines |
| [`../sample-data`](../sample-data/) | Upstream sample CSVs |
| [`../deploy-databricks-azure`](../deploy-databricks-azure/) | Workspace / cluster provisioning |
