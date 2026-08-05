from pyspark import pipelines as dp

from actuarial_claim_streaming_pipeline.auto_loader import CYCLONE_EVENTS_SCHEMA_HINTS, read_landing_csv


@dp.expect("_rescued_data_null", "_rescued_data IS NULL")
@dp.table(
    name="bronze_cyclone_events_raw",
    comment="Raw Auto Loader cyclone events ingest (no row drops; quarantine captures failures).",
    cluster_by_auto=True,
)
def bronze_cyclone_events_raw():
    return read_landing_csv(spark, "cyclone_events", schema_hints=CYCLONE_EVENTS_SCHEMA_HINTS)
