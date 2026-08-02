from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(
    name="actuarial.silver.risk_zones",
    comment="Deduped risk zone lookup (one row per postcode; keeps first region for dup 4825).",
)
def risk_zones():
    window = Window.partitionBy("postcode").orderBy(F.col("region_name").asc())
    return (
        spark.read.table("actuarial.bronze.risk_zone_lookup")
        .select(
            F.col("postcode").cast("string").alias("postcode"),
            F.col("region_name").cast("string").alias("region_name"),
            F.col("wind_risk_band").cast("string").alias("wind_risk_band"),
            F.col("_ingest_ts"),
        )
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
