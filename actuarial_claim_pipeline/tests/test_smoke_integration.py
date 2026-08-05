"""Optional post-job smoke tests against a deployed UC schema.

Skipped automatically when required tables are missing (e.g. job not run yet).

Run after:
    databricks bundle run actuarial_claims_job --target dev
"""

from __future__ import annotations

import os

import pytest

CATALOG = os.environ.get("ACTUARIAL_TEST_CATALOG", "actuarial")
SCHEMA = os.environ.get("ACTUARIAL_TEST_SCHEMA", "dev")

BRONZE_TABLES = [
    "bronze_claims_bordereau",
    "bronze_cyclone_events",
    "bronze_premium_bordereau",
    "bronze_risk_zone_lookup",
]
SILVER_TABLES = [
    "silver_claims_bordereau",
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
    required = BRONZE_TABLES + SILVER_TABLES + GOLD_TABLES
    _require_tables(spark, required)
    return required


def test_expected_tables_exist(spark, deployed_tables):
    existing = _table_names(spark)
    for name in deployed_tables:
        assert name in existing


def test_silver_row_counts_lte_bronze(spark, deployed_tables):
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


def test_silver_required_columns_not_null(spark, deployed_tables):
    claims = spark.table(f"{CATALOG}.{SCHEMA}.silver_claims_bordereau")
    for col in ("claim_id", "policy_id", "date_of_loss", "reported_date", "incurred_amount", "paid_to_date"):
        assert claims.filter(f"{col} IS NULL").count() == 0

    premiums = spark.table(f"{CATALOG}.{SCHEMA}.silver_premium_bordereau")
    for col in ("policy_id", "postcode", "sum_insured", "annual_premium"):
        assert premiums.filter(f"{col} IS NULL").count() == 0


def test_silver_risk_zone_unique_postcodes(spark, deployed_tables):
    rz = spark.table(f"{CATALOG}.{SCHEMA}.silver_risk_zone_lookup")
    assert rz.count() == rz.select("postcode").distinct().count()


def test_gold_tables_non_empty(spark, deployed_tables):
    for name in GOLD_TABLES:
        assert spark.table(f"{CATALOG}.{SCHEMA}.{name}").count() >= 1, f"{name} is empty"
