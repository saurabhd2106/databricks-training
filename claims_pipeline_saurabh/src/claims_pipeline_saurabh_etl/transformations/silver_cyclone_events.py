from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="actuarial.silver.cyclone_events",
    comment="Typed cyclone event windows.",
)
def cyclone_events():
    return (
        spark.read.table("actuarial.bronze.cyclone_events")
        .select(
            F.col("event_id").cast("string").alias("event_id"),
            F.col("event_name").cast("string").alias("event_name"),
            F.to_date("start_date").alias("start_date"),
            F.to_date("end_date").alias("end_date"),
            F.col("_ingest_ts"),
        )
        .dropDuplicates(["event_id"])
    )
