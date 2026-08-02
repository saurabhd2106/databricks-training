from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _landing_path() -> str:
    return spark.conf.get("landing_path", "/Volumes/actuarial/bronze/landing").rstrip("/")


@dp.table(
    name="actuarial.bronze.premium_bordereau",
    comment="Raw premium bordereau CSV ingest.",
)
def premium_bordereau():
    path = f"{_landing_path()}/premiums"
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
