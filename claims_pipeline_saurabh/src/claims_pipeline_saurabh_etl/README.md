# claims_pipeline_saurabh_etl

Lakeflow Declarative Pipeline source for the actuarial medallion flow:

- `transformations/`: Bronze CSV ingest, silver cleansed tables, and gold actuarial marts.
- Landing path is injected via pipeline configuration key `landing_path`
  (default `/Volumes/actuarial/bronze/landing`).

## Layers

| Layer | Schema | Contents |
|-------|--------|----------|
| Bronze | `actuarial.bronze` | Raw CSV reads of claims, premiums, risk zones, cyclone events |
| Silver | `actuarial.silver` | Typed, deduped, current-claim views |
| Gold | `actuarial.gold` | Event loss, policy loss ratio, risk-band performance marts |

## Run

```bash
databricks bundle deploy --target dev
databricks bundle run claims_pipeline_job
```

Or refresh the pipeline alone after landing data:

```bash
databricks bundle run claims_pipeline_saurabh_etl
```
