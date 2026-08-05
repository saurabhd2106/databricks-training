"""Bronze-layer helpers: CSV discovery, table naming, audit columns, ingest."""

from __future__ import annotations

from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def bronze_table_name(filename: str) -> str:
    """Derive managed table name from a CSV filename.

    Examples:
        claims_bordereau.csv -> bronze_claims_bordereau
        Claims Bordereau.csv -> bronze_claims_bordereau
        claims-bordereau.CSV -> bronze_claims_bordereau
    """
    base_name = filename.rsplit(".", 1)[0]
    return "bronze_" + base_name.lower().replace("-", "_").replace(" ", "_")


def filter_csv_files(file_infos: Iterable[Any]) -> list[Any]:
    """Keep only entries whose name ends with .csv (case-insensitive)."""
    return [f for f in file_infos if getattr(f, "name", "").lower().endswith(".csv")]


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Attach ingestion_timestamp and source_file_name audit columns."""
    return df.withColumns(
        {
            "ingestion_timestamp": F.current_timestamp(),
            "source_file_name": F.col("_metadata.file_name"),
        }
    )


def read_csv_with_audit(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV with header + inferred schema and add bronze audit columns."""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    return add_audit_columns(df)


def write_bronze_table(
    df: DataFrame,
    full_table_name: str,
    *,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> None:
    """Write a DataFrame as a managed Delta table."""
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", str(overwrite_schema).lower())
        .saveAsTable(full_table_name)
    )


def ingest_volume_csvs(
    spark: SparkSession,
    dbutils: Any,
    *,
    catalog: str,
    schema: str,
    volume_path: str,
    bronze_write_mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> list[dict[str, Any]]:
    """Discover CSVs in a volume, write one bronze table per file, return summaries."""
    all_files = dbutils.fs.ls(volume_path)
    csv_files = filter_csv_files(all_files)

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {volume_path}")

    created_tables: list[dict[str, Any]] = []

    for file_info in csv_files:
        table_name = bronze_table_name(file_info.name)
        full_name = f"{catalog}.{schema}.{table_name}"
        df = read_csv_with_audit(spark, file_info.path)
        write_bronze_table(
            df,
            full_name,
            mode=bronze_write_mode,
            overwrite_schema=overwrite_schema,
        )
        col_names = df.columns
        row_count = spark.table(full_name).count()
        created_tables.append(
            {
                "table": full_name,
                "source": file_info.name,
                "rows": row_count,
                "cols": col_names,
            }
        )

    return created_tables


def list_bronze_tables(spark: SparkSession, catalog: str, schema: str) -> list[str]:
    """Return sorted bronze_* table names in a schema."""
    return sorted(
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        if row.tableName.startswith("bronze_")
    )
