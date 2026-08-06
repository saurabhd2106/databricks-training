# actuarial_streaming_etl

Lakeflow Spark Declarative Pipeline root for bronze → DQ → silver ingest into `actuarial.dev`.

Transformation modules under `transformations/` are discovered via the pipeline `libraries` glob in `resources/actuarial_streaming_etl.pipeline.yml`.

| Layer | Modules | Dataset type |
|-------|---------|--------------|
| Bronze raw | `bronze_*_raw.py` | Streaming Table (Auto Loader) |
| Bronze clean | `bronze_*.py` (non-raw) | Streaming Table (DQ gate) |
| Quarantine | `quarantine_bronze_*.py` | Streaming Table (DQ audit) |
| Typed | `v_*_typed.py` | Temporary view |
| Silver | `silver_*.py` | Materialized View |

Shared helpers live in the `actuarial_streaming_pipeline` wheel (`auto_loader.py`, `silver.py`, `pipeline_decorators.py`).
