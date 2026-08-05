"""Optional post-job smoke tests against deployed actuarial.streaming tables.

Skipped automatically when required tables are missing (e.g. job not run yet).

Run after:
    databricks bundle run actuarial_streaming_job --target prod
"""

from __future__ import annotations

import os

import pytest

CATALOG = os.environ.get("ACTUARIAL_TEST_CATALOG", "actuarial")
SCHEMA = os.environ.get("ACTUARIAL_TEST_SCHEMA", "streaming")

BRONZE_CLEAN = [
    "bronze_claims_bordereau",
    "bronze_cyclone_events",
    "bronze_premium_bordereau",
    "bronze_risk_zone_lookup",
]
BRONZE_RAW = [f"{t}_raw" for t in BRONZE_CLEAN]
QUARANTINE = [f"quarantine_{t}" for t in BRONZE_CLEAN]
SILVER_TABLES = [
    "silver_claims_bordereau",
    "silver_claims_current",
    "silver_cyclone_events",
    "silver_premium_bordereau",
    "silver_risk_zone_lookup",
]
GOLD_TABLES = [
    "gold_claims_summary",
    "gold_loss_ratio_by_risk",
    "gold_event_loss_summary",
    "gold_portfolio_exposure",
    "gold_claims_development",
]


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
    required = BRONZE_CLEAN + BRONZE_RAW + QUARANTINE + SILVER_TABLES + GOLD_TABLES
    _require_tables(spark, required)
    return required


def test_expected_tables_exist(spark, deployed_tables):
    existing = _table_names(spark)
    for name in deployed_tables:
        assert name in existing


def test_temp_views_not_in_catalog(spark, deployed_tables):
    """Temporary views are pipeline-scoped and must not appear as UC tables."""
    existing = _table_names(spark)
    assert "v_claims_typed" not in existing
    assert "v_premiums_typed" not in existing


def test_bronze_clean_and_gold_non_empty(spark, deployed_tables):
    for name in BRONZE_CLEAN:
        assert spark.table(f"{CATALOG}.{SCHEMA}.{name}").count() >= 1, f"{name} is empty"
    for name in ("silver_claims_bordereau", "silver_premium_bordereau"):
        assert spark.table(f"{CATALOG}.{SCHEMA}.{name}").count() >= 1, f"{name} is empty"
    for name in GOLD_TABLES:
        assert spark.table(f"{CATALOG}.{SCHEMA}.{name}").count() >= 1, f"{name} is empty"


def test_silver_row_counts_lte_bronze_clean(spark, deployed_tables):
    pairs = [
        ("bronze_claims_bordereau", "silver_claims_bordereau"),
        ("bronze_cyclone_events", "silver_cyclone_events"),
        ("bronze_premium_bordereau", "silver_premium_bordereau"),
        ("bronze_risk_zone_lookup", "silver_risk_zone_lookup"),
    ]
    for bronze, silver in pairs:
        b = spark.table(f"{CATALOG}.{SCHEMA}.{bronze}").count()
        s = spark.table(f"{CATALOG}.{SCHEMA}.{silver}").count()
        assert s <= b, f"{silver} ({s}) > {bronze} ({b})"
