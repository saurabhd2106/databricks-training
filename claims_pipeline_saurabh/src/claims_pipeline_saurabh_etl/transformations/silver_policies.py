from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="actuarial.silver.policies",
    comment="Typed premium / policy bordereau.",
)
def policies():
    return (
        spark.read.table("actuarial.bronze.premium_bordereau")
        .select(
            F.col("policy_id").cast("string").alias("policy_id"),
            F.col("insurer_name").cast("string").alias("insurer_name"),
            F.col("postcode").cast("string").alias("postcode"),
            F.col("region_name").cast("string").alias("region_name"),
            F.col("wind_risk_band").cast("string").alias("wind_risk_band"),
            F.col("building_type").cast("string").alias("building_type"),
            F.col("sum_insured").cast("decimal(18,2)").alias("sum_insured"),
            F.col("mitigation_flag").cast("string").alias("mitigation_flag"),
            F.col("annual_premium").cast("decimal(18,2)").alias("annual_premium"),
            F.to_date("policy_start_date").alias("policy_start_date"),
            F.to_date("policy_end_date").alias("policy_end_date"),
            F.col("_ingest_ts"),
        )
        .withColumn(
            "is_active",
            F.col("policy_end_date").isNull() | (F.col("policy_end_date") >= F.current_date()),
        )
        .dropDuplicates(["policy_id"])
    )
