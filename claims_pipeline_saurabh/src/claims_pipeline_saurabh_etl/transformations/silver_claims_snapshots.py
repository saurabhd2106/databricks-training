from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_drop("incurred_gte_paid", "incurred_amount >= paid_to_date")
@dp.expect("reported_on_or_after_loss", "reported_date >= date_of_loss")
@dp.table(
    name="actuarial.silver.claims_snapshots",
    comment="Typed claims snapshots at grain (claim_id, snapshot_date).",
)
def claims_snapshots():
    return (
        spark.read.table("actuarial.bronze.claims_bordereau")
        .select(
            F.col("claim_id").cast("string").alias("claim_id"),
            F.col("policy_id").cast("string").alias("policy_id"),
            F.col("event_id").cast("string").alias("event_id"),
            F.to_date("date_of_loss").alias("date_of_loss"),
            F.to_date("reported_date").alias("reported_date"),
            F.col("peril_type").cast("string").alias("peril_type"),
            F.col("claim_status").cast("string").alias("claim_status"),
            F.col("incurred_amount").cast("decimal(18,2)").alias("incurred_amount"),
            F.col("paid_to_date").cast("decimal(18,2)").alias("paid_to_date"),
            F.to_date("snapshot_date").alias("snapshot_date"),
            F.col("_ingest_ts"),
        )
        .dropDuplicates(["claim_id", "snapshot_date"])
    )
