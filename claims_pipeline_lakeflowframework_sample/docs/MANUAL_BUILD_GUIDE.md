# Manual Build Guide: Claims Pipeline with Lakeflow Framework

This guide walks through building the actuarial **bronze → silver → gold** pipeline **from scratch** using the [Lakeflow Framework](../../lakeflow_framework/) (metadata-driven YAML dataflows). Use this repository as the completed reference: every file you create below already exists under [`claims_pipeline_lakeflowframework_sample/`](../).

For a quick deploy of the finished project, see the [README](../README.md).

---

## What you will build

```text
fixtures/sample-data/*.csv
        │
        ▼
 Job notebooks (create schemas + land CSVs)
        │
        ▼
 /Volumes/actuarial_lff/bronze/landing/{claims,premiums,risk_zones,cyclone_events}/
        │
        ▼
 Bronze pipeline (cloudFiles Auto Loader)  →  actuarial_lff.bronze.*
        │
        ▼
 Silver pipeline (SCD1 + claims_current MV) →  actuarial_lff.silver.*
        │
        ▼
 Gold pipeline (materialized view marts)    →  actuarial_lff.gold.*
```

| Approach | Where used |
|----------|------------|
| **Lakeflow Framework YAML** (this guide) | Specs under `src/dataflows/`; runtime is the framework `dlt_pipeline` notebook |
| **Python `@dp.table`** (sibling project) | [`claims_pipeline_saurabh`](../../claims_pipeline_saurabh/) — same domain, different authoring style |

You do **not** write per-table Python transforms. You author YAML/SQL specs; the framework turns them into Lakeflow Declarative Pipeline tables.

**Defaults in this guide** (change for another workspace):

| Setting | Value |
|---------|--------|
| Catalog | `actuarial_lff` |
| Schemas | `bronze`, `silver`, `gold` |
| Landing volume | `/Volumes/actuarial_lff/bronze/landing` |
| Workspace host | `https://adb-7405611775215693.13.azuredatabricks.net` |
| Land/setup cluster | `0730-111218-jwuz715u` |

---

## Prerequisites

1. **Databricks CLI** authenticated to your workspace.
2. **Deploy Lakeflow Framework** first (pipelines attach its `dlt_pipeline` notebook):

   ```bash
   cd ../lakeflow_framework
   databricks bundle deploy -t dev
   ```

   Note the deployed `src` path, typically:

   `/Workspace/Users/<you>/.bundle/lakeflow_framework/dev/current/files/src`

3. **Unity Catalog** rights: create catalog/schemas/volumes/tables/materialized views (or have an admin create `actuarial_lff` with `bronze` / `silver` / `gold`).
4. An **all-purpose cluster** for landing notebooks (Jobs can use `existing_cluster_id`; serverless pipelines cannot).
5. Sample CSVs: `claims_bordereau.csv`, `premium_bordereau.csv`, `risk_zone_lookup.csv`, `cyclone_events.csv` (from [`sample-data`](../../sample-data/)).

---

## Step 1: Scaffold the bundle

Create the project root and folder tree:

```text
claims_pipeline_lakeflowframework_sample/
├── databricks.yml
├── .gitignore
├── fixtures/sample-data/
├── resources/
└── src/
    ├── notebooks/
    ├── pipeline_configs/
    └── dataflows/
        ├── bronze/{dataflowspec,schemas}/
        ├── silver/{dataflowspec,schemas,expectations,dml}/
        └── gold/{dataflowspec,dml}/
```

Copy the four CSVs into `fixtures/sample-data/`.

### `databricks.yml`

Declare the bundle, variables, and targets. Essentials:

- `include: resources/*.yml`
- Variables: `catalog`, `framework_source_path`, `workspace_host`, `bronze_schema` / `silver_schema` / `gold_schema` (short UC schema names for pipeline resources), `cluster_id`, `landing_volume_path`, `logical_env`
- Dev target: `catalog: actuarial_lff`, schemas `bronze` / `silver` / `gold`, and `framework_source_path` pointing at the deployed framework `src/`

See the full file: [`databricks.yml`](../databricks.yml).

Add a `.gitignore` that ignores `.databricks/`, `.venv/`, `scratch/`, etc.

---

## Step 2: Pipeline configs

Lakeflow Framework reads configs from `src/pipeline_configs/`.

### Spec format — `global.json`

Lock the bundle to YAML specs (do not mix JSON and YAML in one bundle):

```json
{
  "pipeline_bundle_spec_format": {
    "format": "yaml"
  }
}
```

Reference: [`src/pipeline_configs/global.json`](../src/pipeline_configs/global.json).

### Substitutions — `dev_substitutions.yaml`

Tokens used inside dataflow specs as `{bronze_schema}`, `{sample_file_location}`, etc. Use **fully qualified** catalog.schema names here (pipeline YAML uses short schema names separately):

```yaml
tokens:
  bronze_schema: actuarial_lff.bronze{logical_env}
  silver_schema: actuarial_lff.silver{logical_env}
  gold_schema: actuarial_lff.gold{logical_env}
  sample_file_location: /Volumes/actuarial_lff/bronze{logical_env}/landing
```

`{logical_env}` is filled from pipeline configuration `logicalEnv` (empty by default).

Reference: [`src/pipeline_configs/dev_substitutions.yaml`](../src/pipeline_configs/dev_substitutions.yaml).

---

## Step 3: Landing zone

Pipelines read files from a Unity Catalog Volume. Create the catalog/schemas **before** the first `bundle deploy` so the volume resource can attach to `actuarial_lff.bronze`.

### Bootstrap SQL (one-time)

```sql
CREATE CATALOG IF NOT EXISTS actuarial_lff;
CREATE SCHEMA IF NOT EXISTS actuarial_lff.bronze;
CREATE SCHEMA IF NOT EXISTS actuarial_lff.silver;
CREATE SCHEMA IF NOT EXISTS actuarial_lff.gold;
```

### Volume resource

Add [`resources/landing.volume.yml`](../resources/landing.volume.yml):

```yaml
resources:
  volumes:
    actuarial_lff_bronze_landing:
      catalog_name: ${var.catalog}
      schema_name: bronze
      name: landing
      volume_type: MANAGED
```

### Notebooks

| Notebook | Purpose |
|----------|---------|
| [`initialize.ipynb`](../src/notebooks/initialize.ipynb) | Widgets + path variables (`catalog`, schema FQNs, volume root) |
| [`create_schemas_and_tables.ipynb`](../src/notebooks/create_schemas_and_tables.ipynb) | `%run initialize` then `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS` |
| [`land_sample_data.ipynb`](../src/notebooks/land_sample_data.ipynb) | Copy fixtures into volume subfolders |

Landing map (filename → subdirectory under the volume):

| File | Subdir |
|------|--------|
| `claims_bordereau.csv` | `claims/` |
| `premium_bordereau.csv` | `premiums/` |
| `risk_zone_lookup.csv` | `risk_zones/` |
| `cyclone_events.csv` | `cyclone_events/` |

After landing, Auto Loader paths look like `{sample_file_location}/claims/`.

---

## Step 4: Bronze dataflows

Work one entity end-to-end, then repeat the pattern for the other three sources.

### 4a. Schemas

Under `src/dataflows/bronze/schemas/`:

1. **File schema** — columns present in the CSV only (all strings for raw ingest), e.g. `claims_bordereau_file_schema.json`.
2. **Target schema** — file columns plus lineage fields `_ingest_ts` (timestamp) and `_source_file` (string), e.g. `claims_bordereau_schema.json`.

Spark struct JSON shape:

```json
{
  "type": "struct",
  "fields": [
    {"name": "claim_id", "type": "string", "nullable": true, "metadata": {}}
  ]
}
```

### 4b. Dataflow YAML

Create `src/dataflows/bronze/dataflowspec/claims_bordereau_main.yaml`:

- `dataFlowGroup: claims_bronze` (must match the pipeline filter later)
- `sourceType: cloudFiles`
- `path: '{sample_file_location}/claims/'`
- `readerOptions`: `cloudFiles.format: csv`, `header: 'true'`
- `selectExp`: pass through CSV columns; add `current_timestamp() AS _ingest_ts` and `_metadata.file_path AS _source_file`
- Target: `database: '{bronze_schema}'`, `table: claims_bordereau`, CDF enabled

Core shape:

```yaml
dataFlowId: claims_bordereau
dataFlowGroup: claims_bronze
dataFlowType: standard
sourceType: cloudFiles
sourceDetails:
  path: '{sample_file_location}/claims/'
  readerOptions:
    cloudFiles.format: csv
    header: 'true'
  schemaPath: claims_bordereau_file_schema.json
  selectExp:
    - claim_id
    # ... other columns ...
    - current_timestamp() AS _ingest_ts
    - _metadata.file_path AS _source_file
mode: stream
targetFormat: delta
targetDetails:
  database: '{bronze_schema}'
  table: claims_bordereau
  tableProperties:
    delta.enableChangeDataFeed: 'true'
  schemaPath: claims_bordereau_schema.json
```

Full file: [`claims_bordereau_main.yaml`](../src/dataflows/bronze/dataflowspec/claims_bordereau_main.yaml).

### 4c. Repeat for the other sources

| Spec | Volume subdir | Bronze table |
|------|---------------|--------------|
| `premium_bordereau_main.yaml` | `premiums/` | `premium_bordereau` |
| `risk_zone_lookup_main.yaml` | `risk_zones/` | `risk_zone_lookup` |
| `cyclone_events_main.yaml` | `cyclone_events/` | `cyclone_events` |

All four share `dataFlowGroup: claims_bronze`.

---

## Step 5: Bronze pipeline resource

Add [`resources/claims_bronze_pipeline.yml`](../resources/claims_bronze_pipeline.yml):

```yaml
resources:
  pipelines:
    claims_bronze_pipeline:
      name: claims_lff_bronze
      channel: CURRENT
      serverless: true
      catalog: ${var.catalog}
      schema: ${var.bronze_schema}
      libraries:
        - notebook:
            path: ${var.framework_source_path}/dlt_pipeline
      configuration:
        bundle.sourcePath: ${workspace.file_path}/src
        bundle.target: ${bundle.target}
        framework.sourcePath: ${var.framework_source_path}
        workspace.host: ${var.workspace_host}
        pipeline.layer: bronze
        logicalEnv: ${var.logical_env}
        pipeline.dataFlowGroupFilter: claims_bronze
      root_path: ${workspace.file_path}/src
```

Key points:

- **Library** is the framework entry notebook, not your dataflow YAML.
- **`bundle.sourcePath`** is *your* bundle `src/` (specs + substitutions).
- **`pipeline.dataFlowGroupFilter`** selects only `claims_bronze` specs.

---

## Step 6: Silver dataflows

Silver cleans and keys bronze tables. Create specs under `src/dataflows/silver/`.

### 6a. SCD1 typed tables

Pattern for each entity:

1. Target schema JSON with typed columns (`date`, `decimal(18,2)`, etc.).
2. YAML with `sourceType: delta`, `cdfEnabled: true`, `selectExp` casts, and `cdcSettings` SCD Type 1.

Example — claims snapshots (`dataFlowGroup: claims_silver`):

```yaml
sourceDetails:
  database: '{bronze_schema}'
  table: claims_bordereau
  cdfEnabled: true
  selectExp:
    - cast(claim_id as string) as claim_id
    - to_date(date_of_loss) as date_of_loss
    - cast(incurred_amount as decimal(18,2)) as incurred_amount
    # ...
cdcSettings:
  keys:
    - claim_id
    - snapshot_date
  sequence_by: _ingest_ts
  scd_type: '1'
```

| Spec | Source bronze table | Keys | Silver table |
|------|---------------------|------|--------------|
| `claims_snapshots_main.yaml` | `claims_bordereau` | `claim_id`, `snapshot_date` | `claims_snapshots` |
| `policies_main.yaml` | `premium_bordereau` | `policy_id` | `policies` |
| `risk_zones_main.yaml` | `risk_zone_lookup` | `postcode` | `risk_zones` |
| `cyclone_events_main.yaml` | `cyclone_events` | `event_id` | `cyclone_events` |

For policies, add `is_active` in `selectExp`:

```text
(to_date(policy_end_date) is null or to_date(policy_end_date) >= current_date()) as is_active
```

Full example: [`claims_snapshots_main.yaml`](../src/dataflows/silver/dataflowspec/claims_snapshots_main.yaml).

### 6b. Data quality expectations

Create [`src/dataflows/silver/expectations/claims_snapshots_dqe.yaml`](../src/dataflows/silver/expectations/claims_snapshots_dqe.yaml):

```yaml
expect_or_drop:
  - name: incurred_gte_paid
    constraint: incurred_amount >= paid_to_date
    tag: Validity
expect:
  - name: reported_on_or_after_loss
    constraint: reported_date >= date_of_loss
    tag: Validity
```

Wire it on the claims snapshots dataflow:

```yaml
dataQualityExpectationsEnabled: true
dataQualityExpectationsPath: ./claims_snapshots_dqe.yaml
```

(The framework resolves that path under the group’s `expectations/` folder.)

### 6c. `claims_current` materialized view

Latest snapshot per `claim_id`. SQL in [`src/dataflows/silver/dml/claims_current.sql`](../src/dataflows/silver/dml/claims_current.sql) uses **`live.claims_snapshots`** so the silver pipeline tracks the dependency inside the same run:

```sql
SELECT ...
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY snapshot_date DESC) AS _rn
  FROM live.claims_snapshots
)
WHERE _rn = 1
```

Spec ([`claims_current_main.yaml`](../src/dataflows/silver/dataflowspec/claims_current_main.yaml)):

```yaml
dataFlowId: claims_current
dataFlowGroup: claims_silver
dataFlowType: materialized_view
materializedViews:
  claims_current:
    sqlPath: ./claims_current.sql
    tableDetails:
      database: '{silver_schema}'
```

---

## Step 7: Gold dataflows

Gold marts are SQL materialized views over silver. Put SQL under `src/dataflows/gold/dml/` and one group spec under `dataflowspec/`.

### SQL marts

| File | Grain / joins |
|------|----------------|
| [`event_loss_summary.sql`](../src/dataflows/gold/dml/event_loss_summary.sql) | `claims_current` ⋈ `cyclone_events` on `event_id`; aggregates by event |
| [`policy_loss_ratio.sql`](../src/dataflows/gold/dml/policy_loss_ratio.sql) | Policies left-join claim rollups; group by insurer / wind risk / building type; `loss_ratio` |
| [`risk_band_performance.sql`](../src/dataflows/gold/dml/risk_band_performance.sql) | Same claim rollup; group by wind risk / region; `claim_frequency` + `loss_ratio` |

Use **FQN tokens** for cross-pipeline reads, e.g. `{silver_schema}.claims_current` (not `live.*`, because gold is a separate pipeline).

### Spec

[`gold_marts_main.yaml`](../src/dataflows/gold/dataflowspec/gold_marts_main.yaml):

```yaml
dataFlowId: gold_marts
dataFlowGroup: claims_gold
dataFlowType: materialized_view
materializedViews:
  event_loss_summary:
    sqlPath: ./event_loss_summary.sql
    tableDetails:
      database: '{gold_schema}'
  policy_loss_ratio:
    sqlPath: ./policy_loss_ratio.sql
    tableDetails:
      database: '{gold_schema}'
  risk_band_performance:
    sqlPath: ./risk_band_performance.sql
    tableDetails:
      database: '{gold_schema}'
```

---

## Step 8: Silver/gold pipelines and orchestrating job

### Pipeline resources

Mirror the bronze pipeline YAML twice:

| Resource file | Filter | Layer / schema var |
|---------------|--------|--------------------|
| [`claims_silver_pipeline.yml`](../resources/claims_silver_pipeline.yml) | `claims_silver` | `pipeline.layer: silver`, `schema: ${var.silver_schema}` |
| [`claims_gold_pipeline.yml`](../resources/claims_gold_pipeline.yml) | `claims_gold` | `pipeline.layer: gold`, `schema: ${var.gold_schema}` |

Same `libraries` notebook path and `bundle.sourcePath` / `framework.sourcePath` configuration.

### Job

Add [`resources/claims_pipeline_job.job.yml`](../resources/claims_pipeline_job.job.yml) with this task chain:

1. `create_schemas_and_tables` — notebook on `existing_cluster_id`
2. `land_sample_data` — notebook; params `catalog`, `landing_path`, `source_path` (default `${workspace.file_path}/fixtures/sample-data`)
3. `bronze_pipeline` → `claims_bronze_pipeline` (`full_refresh: true`)
4. `silver_pipeline` → `claims_silver_pipeline`
5. `gold_pipeline` → `claims_gold_pipeline`

Each pipeline task depends on the previous. Enable the job queue.

---

## Step 9: Deploy and verify

```bash
cd claims_pipeline_lakeflowframework_sample

# Catalog/schemas must exist before volume deploy (Step 3)
databricks bundle validate --target dev
databricks bundle deploy --target dev

databricks bundle run claims_pipeline_job
```

Override framework path if your deploy location differs:

```bash
databricks bundle deploy -t dev \
  --var="framework_source_path=/Workspace/Users/<user>/.bundle/lakeflow_framework/dev/current/files/src"
```

### Expected counts

| Check | Expected |
|-------|----------|
| Bronze / silver claim snapshots | ~2,829 |
| `actuarial_lff.silver.claims_current` | ~1,533 |
| `actuarial_lff.silver.policies` | 5,000 |
| `actuarial_lff.silver.risk_zones` | 19 |
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

Landing files:

```sql
LIST '/Volumes/actuarial_lff/bronze/landing/claims';
```

---

## Reference map

| Concept | Path in this repo |
|---------|-------------------|
| Bundle / targets | [`databricks.yml`](../databricks.yml) |
| Spec format | [`src/pipeline_configs/global.json`](../src/pipeline_configs/global.json) |
| Substitutions | [`src/pipeline_configs/dev_substitutions.yaml`](../src/pipeline_configs/dev_substitutions.yaml) |
| Landing volume | [`resources/landing.volume.yml`](../resources/landing.volume.yml) |
| Setup / land notebooks | [`src/notebooks/`](../src/notebooks/) |
| Bronze cloudFiles specs | [`src/dataflows/bronze/dataflowspec/`](../src/dataflows/bronze/dataflowspec/) |
| Silver SCD1 + MV | [`src/dataflows/silver/`](../src/dataflows/silver/) |
| Gold mart SQL + MV | [`src/dataflows/gold/`](../src/dataflows/gold/) |
| Bronze / silver / gold pipelines | [`resources/claims_*_pipeline.yml`](../resources/) |
| Orchestrating job | [`resources/claims_pipeline_job.job.yml`](../resources/claims_pipeline_job.job.yml) |
| Framework runtime (deploy separately) | [`../../lakeflow_framework`](../../lakeflow_framework/) |

### Common pitfalls

1. **Framework not deployed** — pipeline library path 404s; deploy `lakeflow_framework` first.
2. **Catalog/schema missing at deploy** — volume resource fails; create `actuarial_lff.bronze` before `bundle deploy`.
3. **Wrong `dataFlowGroupFilter`** — specs silently skipped; group names must match (`claims_bronze` / `claims_silver` / `claims_gold`).
4. **`live.` vs FQN** — use `live.` only for tables in the **same** pipeline; gold reads silver via `{silver_schema}.…`.
5. **File vs target schema** — Auto Loader source schema must not require `_ingest_ts` / `_source_file` columns from the CSV.
