"""Auto Loader helpers for batch incremental bronze ingest from ADLS.

Unlike Lakeflow Streaming Tables, classic jobs own schemaLocation and
checkpointLocation. Each run uses trigger(availableNow=True) so only new
files since the last checkpoint are processed, then the stream stops.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

# Known actuarial CSV contracts (schemaHints). Remaining columns may still be inferred.
CLAIMS_SCHEMA_HINTS = (
    "claim_id STRING, policy_id STRING, event_id STRING, "
    "date_of_loss DATE, reported_date DATE, peril_type STRING, claim_status STRING, "
    "incurred_amount DECIMAL(18,2), paid_to_date DECIMAL(18,2), snapshot_date DATE"
)

PREMIUMS_SCHEMA_HINTS = (
    "policy_id STRING, insurer_name STRING, postcode INT, region_name STRING, "
    "wind_risk_band STRING, building_type STRING, sum_insured DECIMAL(18,2), "
    "mitigation_flag STRING, annual_premium DECIMAL(18,2), "
    "policy_start_date DATE, policy_end_date DATE"
)

RISK_ZONES_SCHEMA_HINTS = "postcode INT, region_name STRING, wind_risk_band STRING"

CYCLONE_EVENTS_SCHEMA_HINTS = (
    "event_id STRING, event_name STRING, start_date DATE, end_date DATE"
)


@dataclass(frozen=True)
class DatasetSpec:
    """One bronze dataset: landing subdir, target table name, schema hints."""

    subdir: str
    table_name: str
    schema_hints: str


DATASETS: Sequence[DatasetSpec] = (
    DatasetSpec("claims", "bronze_claims_bordereau", CLAIMS_SCHEMA_HINTS),
    DatasetSpec("premiums", "bronze_premium_bordereau", PREMIUMS_SCHEMA_HINTS),
    DatasetSpec("risk_zones", "bronze_risk_zone_lookup", RISK_ZONES_SCHEMA_HINTS),
    DatasetSpec("cyclone_events", "bronze_cyclone_events", CYCLONE_EVENTS_SCHEMA_HINTS),
)


def normalize_path(path: str) -> str:
    """Strip trailing slashes from a storage path."""
    return path.rstrip("/")


def dataset_source_path(landing_path: str, subdir: str) -> str:
    return f"{normalize_path(landing_path)}/{subdir}"


def dataset_schema_location(autoloader_state_path: str, subdir: str) -> str:
    return f"{normalize_path(autoloader_state_path)}/{subdir}/schema"


def dataset_checkpoint_location(autoloader_state_path: str, subdir: str) -> str:
    return f"{normalize_path(autoloader_state_path)}/{subdir}/checkpoints"


def full_table_name(catalog: str, schema: str, table_name: str) -> str:
    return f"{catalog}.{schema}.{table_name}"


def read_cloud_files(
    spark: SparkSession,
    source_path: str,
    schema_location: str,
    *,
    schema_hints: str | None = None,
) -> DataFrame:
    """Stream CSV files from cloud storage via Auto Loader."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", schema_location)
    )
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)

    return (
        reader.load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def ingest_available_now(
    spark: SparkSession,
    *,
    source_path: str,
    table: str,
    schema_location: str,
    checkpoint_location: str,
    schema_hints: str | None = None,
) -> StreamingQuery:
    """Process new files once with availableNow, append to a Delta table, then stop."""
    query = (
        read_cloud_files(
            spark,
            source_path,
            schema_location,
            schema_hints=schema_hints,
        )
        .writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .outputMode("append")
        .toTable(table)
    )
    query.awaitTermination()
    return query


def ingest_all_datasets(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    landing_path: str,
    autoloader_state_path: str,
    datasets: Sequence[DatasetSpec] = DATASETS,
) -> list[str]:
    """Ingest every configured dataset; return fully qualified table names written."""
    written: list[str] = []
    for ds in datasets:
        table = full_table_name(catalog, schema, ds.table_name)
        ingest_available_now(
            spark,
            source_path=dataset_source_path(landing_path, ds.subdir),
            table=table,
            schema_location=dataset_schema_location(autoloader_state_path, ds.subdir),
            checkpoint_location=dataset_checkpoint_location(autoloader_state_path, ds.subdir),
            schema_hints=ds.schema_hints,
        )
        written.append(table)
    return written
