from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _landing_path() -> str:
    return spark.conf.get("landing_path", "/Volumes/actuarial/bronze/landing").rstrip("/")


@dp.table(
    name="actuarial.bronze.cyclone_events",
    comment="Raw cyclone events CSV ingest.",
)
def cyclone_events():
    path = f"{_landing_path()}/cyclone_events"
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
