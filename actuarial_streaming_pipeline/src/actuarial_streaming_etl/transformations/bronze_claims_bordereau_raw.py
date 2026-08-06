from pyspark import pipelines as dp

from actuarial_streaming_pipeline.auto_loader import CLAIMS_SCHEMA_HINTS, read_landing_csv


@dp.expect("_rescued_data_null", "_rescued_data IS NULL")
@dp.table(
    name="bronze_claims_bordereau_raw",
    comment="Raw Auto Loader claims ingest (no row drops; quarantine captures failures).",
    cluster_by_auto=True,
)
def bronze_claims_bordereau_raw():
    return read_landing_csv(spark, "claims", schema_hints=CLAIMS_SCHEMA_HINTS)
