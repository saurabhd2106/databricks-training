from pyspark import pipelines as dp


@dp.expect("_parse_error_null", "_parse_error IS NULL")
@dp.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dp.table(
    name="bronze_cyclone_events",
    comment="Clean streaming bronze cyclone events (null event_id dropped; see quarantine_bronze_cyclone_events).",
    cluster_by_auto=True,
)
def bronze_cyclone_events():
    return spark.readStream.table("bronze_cyclone_events_raw")
