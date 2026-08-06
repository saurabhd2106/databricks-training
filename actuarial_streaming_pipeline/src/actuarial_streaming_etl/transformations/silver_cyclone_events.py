from pyspark import pipelines as dp

from actuarial_streaming_pipeline.pipeline_decorators import materialized_view
from actuarial_streaming_pipeline.silver import transform_silver_cyclone_events


@dp.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dp.expect("start_on_or_before_end", "start_date <= end_date")
@materialized_view(
    name="silver_cyclone_events",
    comment="Typed cyclone events (Materialized View from clean bronze).",
    cluster_by_auto=True,
)
def silver_cyclone_events():
    return transform_silver_cyclone_events(spark.read.table("bronze_cyclone_events"))
