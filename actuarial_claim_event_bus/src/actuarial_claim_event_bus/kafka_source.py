"""Shared Event Hubs (Kafka protocol) helpers for streaming bronze ingest.

Lakeflow manages Streaming Table checkpoints — do not set checkpointLocation here.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from actuarial_claim_event_bus.schemas import DATASET_SCHEMAS, TOPIC_CONFIG_KEYS


def _conf(spark: SparkSession, key: str, default: str | None = None) -> str:
    value = spark.conf.get(key, default)
    if value is None or value == "":
        raise ValueError(f"Missing required Spark/pipeline configuration: {key}")
    return value


def read_event_hub_json(
    spark: SparkSession,
    dataset: str,
    *,
    schema: StructType | None = None,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Stream JSON messages from an Event Hubs topic via the Kafka protocol.

    Pipeline configuration keys:
      eh_bootstrap_servers, eh_jaas_config, eh_<dataset>_topic
    """
    if dataset not in TOPIC_CONFIG_KEYS:
        raise ValueError(f"Unknown dataset '{dataset}'. Expected one of {sorted(TOPIC_CONFIG_KEYS)}")

    topic_key = TOPIC_CONFIG_KEYS[dataset]
    payload_schema = schema or DATASET_SCHEMAS[dataset]
    bootstrap = _conf(spark, "eh_bootstrap_servers")
    topic = _conf(spark, topic_key)
    jaas = _conf(spark, "eh_jaas_config")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("failOnDataLoss", "false")
        .load()
    )

    with_meta = (
        kafka_df.select(
            F.col("value").cast("string").alias("_raw_value"),
            F.col("topic").alias("_topic"),
            F.col("partition").alias("_partition"),
            F.col("offset").alias("_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_parsed", F.from_json(F.col("_raw_value"), payload_schema))
    )

    parse_error = (
        F.when(F.col("_raw_value").isNull(), F.lit("null_value"))
        .when(F.col("_parsed").isNull(), F.lit("invalid_json"))
        .otherwise(F.lit(None).cast("string"))
    )

    return with_meta.withColumn("_parse_error", parse_error).select(
        "_parsed.*",
        "_ingest_ts",
        "_topic",
        "_partition",
        "_offset",
        "_kafka_timestamp",
        "_raw_value",
        "_parse_error",
    )


def parse_json_batch(df: DataFrame, schema: StructType) -> DataFrame:
    """Parse a batch DataFrame that already has a `_raw_value` string column.

    Used by unit tests (no live Kafka). Adds the same audit/parse columns as the stream path
    when metadata columns are missing.
    """
    cols = set(df.columns)
    out = df
    if "_raw_value" not in cols:
        raise ValueError("parse_json_batch requires a `_raw_value` column")
    if "_ingest_ts" not in cols:
        out = out.withColumn("_ingest_ts", F.current_timestamp())
    if "_topic" not in cols:
        out = out.withColumn("_topic", F.lit("test"))
    if "_partition" not in cols:
        out = out.withColumn("_partition", F.lit(0))
    if "_offset" not in cols:
        out = out.withColumn("_offset", F.lit(0).cast("long"))
    if "_kafka_timestamp" not in cols:
        out = out.withColumn("_kafka_timestamp", F.current_timestamp())

    out = out.withColumn("_parsed", F.from_json(F.col("_raw_value"), schema))
    parse_error = (
        F.when(F.col("_raw_value").isNull(), F.lit("null_value"))
        .when(F.col("_parsed").isNull(), F.lit("invalid_json"))
        .otherwise(F.lit(None).cast("string"))
    )
    return out.withColumn("_parse_error", parse_error).select(
        "_parsed.*",
        "_ingest_ts",
        "_topic",
        "_partition",
        "_offset",
        "_kafka_timestamp",
        "_raw_value",
        "_parse_error",
    )
