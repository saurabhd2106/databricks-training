"""CLI / notebook entrypoint for batch Auto Loader ADLS ingest."""

from __future__ import annotations

from actuarial_ingestion_adls.auto_loader import DATASETS, ingest_all_datasets


def main() -> None:
    """Run ingest when invoked as a wheel entrypoint on a Databricks cluster."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    catalog = spark.conf.get("actuarial.catalog", "actuarial")
    schema = spark.conf.get("actuarial.schema", "ingestion")
    landing_path = spark.conf.get(
        "actuarial.landing_path",
        "abfss://metastore@dbxucfc9c48d2meta.dfs.core.windows.net/actuarial/ingestion/landing",
    )
    autoloader_state_path = spark.conf.get(
        "actuarial.autoloader_state_path",
        "abfss://metastore@dbxucfc9c48d2meta.dfs.core.windows.net/actuarial/ingestion/_autoloader",
    )

    tables = ingest_all_datasets(
        spark,
        catalog=catalog,
        schema=schema,
        landing_path=landing_path,
        autoloader_state_path=autoloader_state_path,
        datasets=DATASETS,
    )
    print(f"Ingested {len(tables)} table(s):")
    for t in tables:
        print(f"  {t}")


if __name__ == "__main__":
    main()
