from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="actuarial.gold.policy_loss_ratio",
    comment="Premium vs incurred loss ratio by insurer, wind risk band, and building type.",
)
def policy_loss_ratio():
    policies = spark.read.table("actuarial.silver.policies").alias("p")
    claims = (
        spark.read.table("actuarial.silver.claims_current")
        .groupBy("policy_id")
        .agg(
            F.countDistinct("claim_id").alias("claim_count"),
            F.sum("incurred_amount").alias("total_incurred"),
            F.sum("paid_to_date").alias("total_paid"),
        )
        .alias("c")
    )

    return (
        policies.join(claims, F.col("p.policy_id") == F.col("c.policy_id"), how="left")
        .groupBy(
            F.col("p.insurer_name"),
            F.col("p.wind_risk_band"),
            F.col("p.building_type"),
        )
        .agg(
            F.countDistinct("p.policy_id").alias("policy_count"),
            F.sum("p.annual_premium").alias("total_premium"),
            F.coalesce(F.sum("c.claim_count"), F.lit(0)).alias("claim_count"),
            F.coalesce(F.sum("c.total_incurred"), F.lit(0)).alias("total_incurred"),
            F.coalesce(F.sum("c.total_paid"), F.lit(0)).alias("total_paid"),
        )
        .withColumn(
            "loss_ratio",
            F.when(
                F.col("total_premium") > 0,
                F.col("total_incurred") / F.col("total_premium"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )
