# actuarial_streaming_pipeline

Declarative Automation Bundle (DAB) that lands sample actuarial CSVs into a UC Volume and transforms them with a serverless Lakeflow Spark Declarative Pipeline into `actuarial.dev`.

| Item | Value |
|------|--------|
| Catalog / schema | `actuarial.dev` |
| Landing volume | `/Volumes/actuarial/dev/landing` |
| Job | `actuarial_streaming_job` (`setup` → `land_sample_data` → `refresh_pipeline`) |
| Pipeline | `actuarial_streaming_etl` (serverless, triggered) |
| Bronze | Auto Loader `*_raw` → clean Streaming Tables + quarantine |
| Silver | Materialized Views with typed cleanses + SDP expects |

## Layout

* `src/actuarial_streaming_pipeline/`: Shared Python wheel (Auto Loader + silver helpers).
* `src/actuarial_streaming_etl/transformations/`: Lakeflow dataset definitions (one per file).
* `src/notebooks/`: Setup + land notebooks (serverless job tasks).
* `resources/`: Schema, volume, job, and pipeline YAML.
* `fixtures/sample-data/`: Source CSVs copied into the landing volume.
* `tests/`: Unit and bundle config tests.

## Data flow

```text
fixtures/sample-data
        │
        ▼
 Job: actuarial_streaming_job (serverless)
   setup → land_sample_data (claims_batch=all) → refresh_pipeline
        │
        ▼
 /Volumes/actuarial/dev/landing/{claims,premiums,risk_zones,cyclone_events}/
        │
        ▼
 Pipeline: actuarial_streaming_etl (serverless)
   bronze_*_raw          (Streaming Table, Auto Loader)
   bronze_*              (Streaming Table, clean — DQ gate)
   quarantine_bronze_*   (Streaming Table, DQ audit)
   v_claims_typed / v_premiums_typed  (temporary views)
   silver_*              (Materialized View)
```

## Data quality rules

### Non-null

| Entity | Bronze key (drop + quarantine) | Additional silver required fields |
|--------|--------------------------------|-----------------------------------|
| Claims | `claim_id` | `policy_id`, `date_of_loss`, `reported_date`, `peril_type`, `claim_status`, `incurred_amount`, `paid_to_date` |
| Premiums | `policy_id` | `insurer_name`, `postcode`, `region_name`, `wind_risk_band`, `building_type`, `sum_insured`, `mitigation_flag`, `annual_premium`, `policy_start_date`, `policy_end_date` |
| Risk zones | `postcode` | `region_name`, `wind_risk_band` |
| Cyclone events | `event_id` | `event_name`, `start_date`, `end_date` |

### Business checks

| Entity | Rule | Enforcement |
|--------|------|-------------|
| Claims | `date_of_loss <= reported_date` | Python filter + `@dp.expect` warn |
| Claims | `incurred_amount >= 0` | Python filter |
| Claims | `paid_to_date <= incurred_amount` | Python filter + `@dp.expect_or_drop` |
| Premiums | `policy_start_date < policy_end_date` | Python filter |
| Premiums | `sum_insured > 0` / `annual_premium > 0` | Python filter + `@dp.expect` warn |
| Risk zones | One row per `postcode` | Python dedupe |
| Cyclone | `start_date <= end_date` (multi-format parse) | Python filter + `@dp.expect` warn |
| All raw | `_rescued_data IS NULL` | `@dp.expect` on clean bronze |

## Conventions

- Use `from pyspark import pipelines as dp` (not legacy `import dlt`).
- Prefer `@materialized_view` / `@temporary_view` via `pipeline_decorators` fallbacks.
- Do **not** set `cloudFiles.schemaLocation` or checkpoint paths — Lakeflow manages them.
- Claims landing is append-only (`claims_batch_0N.csv`); default job parameter lands **all** batches.
- Job compute: serverless `environments` / `environment_version: "4"`.
- Pipeline compute: `serverless: true` with wheel via `environment.dependencies: dist/*.whl`.

## Getting started

1. Authenticate (if needed):
   ```bash
   databricks configure
   ```

2. Install local deps:
   ```bash
   uv sync --dev
   ```

3. Validate:
   ```bash
   databricks bundle validate --target dev
   ```

4. Deploy:
   ```bash
   databricks bundle deploy --target dev
   ```

5. Run land + ingest:
   ```bash
   databricks bundle run actuarial_streaming_job
   ```

6. Tests:
   ```bash
   uv run pytest
   ```
