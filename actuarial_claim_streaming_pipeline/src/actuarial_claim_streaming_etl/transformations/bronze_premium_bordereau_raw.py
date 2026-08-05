from pyspark import pipelines as dp

from actuarial_claim_streaming_pipeline.auto_loader import PREMIUMS_SCHEMA_HINTS, read_landing_csv


@dp.expect("_rescued_data_null", "_rescued_data IS NULL")
@dp.table(
    name="bronze_premium_bordereau_raw",
    comment="Raw Auto Loader premium ingest (no row drops; quarantine captures failures).",
    cluster_by_auto=True,
)
def bronze_premium_bordereau_raw():
    return read_landing_csv(spark, "premiums", schema_hints=PREMIUMS_SCHEMA_HINTS)
