from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(
    name="actuarial.silver.claims_current",
    comment="Latest claim snapshot per claim_id.",
)
def claims_current():
    window = Window.partitionBy("claim_id").orderBy(F.col("snapshot_date").desc())
    return (
        spark.read.table("actuarial.silver.claims_snapshots")
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
