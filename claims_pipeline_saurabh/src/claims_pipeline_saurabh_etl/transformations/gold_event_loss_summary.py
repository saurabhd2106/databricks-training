from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="actuarial.gold.event_loss_summary",
    comment="Claim counts and loss totals by cyclone event.",
)
def event_loss_summary():
    claims = spark.read.table("actuarial.silver.claims_current")
    events = spark.read.table("actuarial.silver.cyclone_events")

    return (
        claims.alias("c")
        .join(events.alias("e"), on="event_id", how="inner")
        .groupBy(
            F.col("e.event_id"),
            F.col("e.event_name"),
            F.col("e.start_date"),
            F.col("e.end_date"),
        )
        .agg(
            F.countDistinct("c.claim_id").alias("claim_count"),
            F.sum("c.incurred_amount").alias("total_incurred"),
            F.sum("c.paid_to_date").alias("total_paid"),
            F.sum(F.when(F.col("c.claim_status") == "Open", 1).otherwise(0)).alias("open_claims"),
            F.sum(F.when(F.col("c.claim_status") == "Closed", 1).otherwise(0)).alias("closed_claims"),
            F.sum(F.when(F.col("c.claim_status") == "Reopened", 1).otherwise(0)).alias(
                "reopened_claims"
            ),
        )
    )
