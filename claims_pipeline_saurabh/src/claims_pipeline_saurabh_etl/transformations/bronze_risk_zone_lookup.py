from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _landing_path() -> str:
    return spark.conf.get("landing_path", "/Volumes/actuarial/bronze/landing").rstrip("/")


@dp.table(
    name="actuarial.bronze.risk_zone_lookup",
    comment="Raw risk zone lookup CSV ingest.",
)
def risk_zone_lookup():
    path = f"{_landing_path()}/risk_zones"
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
