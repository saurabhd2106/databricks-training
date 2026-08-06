from pyspark import pipelines as dp

from actuarial_streaming_pipeline.auto_loader import quarantine_from_raw


@dp.table(
    name="quarantine_bronze_cyclone_events",
    comment="Cyclone event rows failing bronze expectations (rescued_data or null event_id).",
    cluster_by_auto=True,
)
def quarantine_bronze_cyclone_events():
    return quarantine_from_raw(spark.readStream.table("bronze_cyclone_events_raw"), "event_id")
