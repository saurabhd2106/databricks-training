"""Optional post-deploy smoke tests against actuarial.event_bus tables.

Skipped automatically when required tables are missing (e.g. Event Hubs not
configured or continuous pipeline not started yet).

Run after:
    databricks bundle deploy --target prod
    databricks bundle run actuarial_event_bus_job --target prod
"""

from __future__ import annotations

import os

import pytest

CATALOG = os.environ.get("ACTUARIAL_TEST_CATALOG", "actuarial")
SCHEMA = os.environ.get("ACTUARIAL_TEST_SCHEMA", "event_bus")

BRONZE_CLEAN = [
    "bronze_claims_bordereau",
    "bronze_cyclone_events",
    "bronze_premium_bordereau",
    "bronze_risk_zone_lookup",
]
BRONZE_RAW = [f"{t}_raw" for t in BRONZE_CLEAN]
QUARANTINE = [f"quarantine_{t}" for t in BRONZE_CLEAN]


def _table_names(spark) -> set[str]:
    rows = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
    return {r.tableName for r in rows}


def _require_tables(spark, names: list[str]) -> None:
    existing = _table_names(spark)
    missing = [n for n in names if n not in existing]
    if missing:
        pytest.skip(f"Missing tables in {CATALOG}.{SCHEMA}: {missing}")


@pytest.fixture()
def deployed_tables(spark):
    required = BRONZE_CLEAN + BRONZE_RAW + QUARANTINE
    _require_tables(spark, required)
    return required


def test_expected_bronze_tables_exist(spark, deployed_tables):
    existing = _table_names(spark)
    for name in deployed_tables:
        assert name in existing


def test_no_silver_or_gold_tables(spark, deployed_tables):
    """Event bus project is bronze-only."""
    existing = _table_names(spark)
    for name in existing:
        assert not name.startswith("silver_"), name
        assert not name.startswith("gold_"), name


def test_bronze_raw_has_kafka_audit_columns(spark, deployed_tables):
    cols = set(spark.table(f"{CATALOG}.{SCHEMA}.bronze_claims_bordereau_raw").columns)
    for expected in (
        "_ingest_ts",
        "_topic",
        "_partition",
        "_offset",
        "_kafka_timestamp",
        "_raw_value",
        "_parse_error",
    ):
        assert expected in cols
