"""Unit tests for gold aggregations with hand-crafted silver inputs."""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from actuarial_claim_pipeline.gold import (
    build_gold_claims_development,
    build_gold_claims_summary,
    build_gold_event_loss_summary,
    build_gold_loss_ratio_by_risk,
    build_gold_portfolio_exposure,
)

SILVER_CLAIMS = StructType(
    [
        StructField("claim_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("date_of_loss", DateType(), True),
        StructField("reported_date", DateType(), True),
        StructField("peril_type", StringType(), True),
        StructField("claim_status", StringType(), True),
        StructField("incurred_amount", DecimalType(18, 2), True),
        StructField("paid_to_date", DecimalType(18, 2), True),
        StructField("snapshot_date", DateType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("bronze_ingestion_timestamp", TimestampType(), True),
        StructField("silver_ingestion_timestamp", TimestampType(), True),
    ]
)

SILVER_PREMIUMS = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("insurer_name", StringType(), True),
        StructField("postcode", IntegerType(), True),
        StructField("region_name", StringType(), True),
        StructField("wind_risk_band", StringType(), True),
        StructField("building_type", StringType(), True),
        StructField("sum_insured", DecimalType(18, 2), True),
        StructField("mitigation_flag", StringType(), True),
        StructField("annual_premium", DecimalType(18, 2), True),
        StructField("policy_start_date", DateType(), True),
        StructField("policy_end_date", DateType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("bronze_ingestion_timestamp", TimestampType(), True),
        StructField("silver_ingestion_timestamp", TimestampType(), True),
    ]
)

SILVER_EVENTS = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("bronze_ingestion_timestamp", TimestampType(), True),
        StructField("silver_ingestion_timestamp", TimestampType(), True),
    ]
)

SILVER_RISK = StructType(
    [
        StructField("postcode", IntegerType(), True),
        StructField("region_name", StringType(), True),
        StructField("wind_risk_band", StringType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("bronze_ingestion_timestamp", TimestampType(), True),
        StructField("silver_ingestion_timestamp", TimestampType(), True),
    ]
)

TS = datetime(2025, 1, 1)


def _claims(spark, rows):
    return spark.createDataFrame(rows, schema=SILVER_CLAIMS)


def _premiums(spark, rows):
    return spark.createDataFrame(rows, schema=SILVER_PREMIUMS)


def _d(v: str) -> Decimal:
    return Decimal(v)


def test_gold_claims_summary_metrics(spark):
    claims = _claims(
        spark,
        [
            (
                "CLM-1",
                "POL-1",
                "CYC-1",
                date(2025, 2, 10),
                date(2025, 2, 17),
                "Wind",
                "Open",
                _d("100.00"),
                _d("40.00"),
                date(2025, 2, 17),
                "c.csv",
                TS,
                TS,
            ),
            (
                "CLM-2",
                "POL-1",
                None,
                date(2025, 3, 1),
                date(2025, 3, 5),
                "Wind",
                "Open",
                _d("50.00"),
                _d("10.00"),
                date(2025, 3, 5),
                "c.csv",
                TS,
                TS,
            ),
            (
                "CLM-3",
                "POL-MISSING",
                None,
                date(2025, 3, 1),
                date(2025, 3, 2),
                "Flood",
                "Closed",
                _d("20.00"),
                _d("20.00"),
                date(2025, 3, 2),
                "c.csv",
                TS,
                TS,
            ),
        ],
    )
    premiums = _premiums(
        spark,
        [
            (
                "POL-1",
                "Insurer A",
                4870,
                "Cairns QLD",
                "T-W",
                "Home",
                _d("500000.00"),
                "Shutters",
                _d("2500.00"),
                date(2024, 1, 1),
                date(2025, 1, 1),
                "p.csv",
                TS,
                TS,
            ),
        ],
    )

    gold = build_gold_claims_summary(claims, premiums)
    wind = gold.filter("peril_type = 'Wind'").collect()[0]
    assert wind.claim_count == 2
    assert float(wind.total_incurred) == 150.0
    assert float(wind.total_paid) == 50.0
    assert float(wind.outstanding_reserve) == 100.0
    assert float(wind.avg_claim_severity) == 75.0
    assert float(wind.settlement_pct) == 33.33
    assert wind.region_name == "Cairns QLD"

    orphan = gold.filter("peril_type = 'Flood'").collect()[0]
    assert orphan.claim_count == 1
    assert orphan.region_name is None


def test_gold_loss_ratio_by_risk(spark):
    premiums = _premiums(
        spark,
        [
            (
                "POL-1",
                "Insurer A",
                4870,
                "Cairns QLD",
                "T-W",
                "Home",
                _d("500000.00"),
                "Shutters",
                _d("1000.00"),
                date(2024, 1, 1),
                date(2025, 1, 1),
                "p.csv",
                TS,
                TS,
            ),
            (
                "POL-2",
                "Insurer A",
                4870,
                "Cairns QLD",
                "T-W",
                "Home",
                _d("400000.00"),
                "Shutters",
                _d("1000.00"),
                date(2024, 1, 1),
                date(2025, 1, 1),
                "p.csv",
                TS,
                TS,
            ),
        ],
    )
    claims = _claims(
        spark,
        [
            (
                "CLM-1",
                "POL-1",
                None,
                date(2025, 2, 10),
                date(2025, 2, 17),
                "Wind",
                "Open",
                _d("500.00"),
                _d("0.00"),
                date(2025, 2, 17),
                "c.csv",
                TS,
                TS,
            ),
            (
                "CLM-2",
                "POL-1",
                None,
                date(2025, 2, 11),
                date(2025, 2, 18),
                "Wind",
                "Open",
                _d("500.00"),
                _d("0.00"),
                date(2025, 2, 18),
                "c.csv",
                TS,
                TS,
            ),
        ],
    )

    gold = build_gold_loss_ratio_by_risk(premiums, claims).collect()[0]
    assert gold.policy_count == 2
    assert gold.claim_count == 2
    # Join grain: premium rows multiply with claims for POL-1 (2 claims => premium summed twice for that policy)
    # total_premium = 1000*2 (POL-1 duplicated) + 1000 (POL-2) = 3000; incurred = 1000
    assert float(gold.total_incurred) == 1000.0
    assert float(gold.total_premium) == 3000.0
    assert float(gold.loss_ratio_pct) == 33.33


def test_gold_event_loss_summary_cat_vs_noncat(spark):
    claims = _claims(
        spark,
        [
            (
                "CLM-1",
                "POL-1",
                "CYC-1",
                date(2025, 2, 10),
                date(2025, 2, 17),
                "Wind",
                "Open",
                _d("100.00"),
                _d("20.00"),
                date(2025, 2, 17),
                "c.csv",
                TS,
                TS,
            ),
            (
                "CLM-2",
                "POL-1",
                None,
                date(2025, 3, 1),
                date(2025, 3, 5),
                "Flood",
                "Closed",
                _d("50.00"),
                _d("50.00"),
                date(2025, 3, 5),
                "c.csv",
                TS,
                TS,
            ),
        ],
    )
    events = spark.createDataFrame(
        [
            ("CYC-1", "Cyclone Alpha", date(2025, 2, 8), date(2025, 2, 14), "e.csv", TS, TS),
        ],
        schema=SILVER_EVENTS,
    )
    premiums = _premiums(
        spark,
        [
            (
                "POL-1",
                "Insurer A",
                4870,
                "Cairns QLD",
                "T-W",
                "Home",
                _d("500000.00"),
                "Shutters",
                _d("2500.00"),
                date(2024, 1, 1),
                date(2025, 1, 1),
                "p.csv",
                TS,
                TS,
            ),
        ],
    )

    gold = build_gold_event_loss_summary(claims, events, premiums)
    cat = gold.filter("claim_category = 'Catastrophe'").collect()[0]
    assert cat.event_name == "Cyclone Alpha"
    assert cat.event_duration_days == 6
    assert cat.claim_count == 1
    assert float(cat.max_claim_severity) == 100.0

    noncat = gold.filter("claim_category = 'Non-Catastrophe'").collect()[0]
    assert noncat.claim_count == 1
    assert noncat.event_name is None


def test_gold_portfolio_exposure_inner_join(spark):
    premiums = _premiums(
        spark,
        [
            (
                "POL-1",
                "Insurer A",
                4870,
                "Cairns QLD",
                "T-W",
                "Home",
                _d("100.00"),
                "Shutters",
                _d("10.00"),
                date(2024, 1, 1),
                date(2025, 1, 1),
                "p.csv",
                TS,
                TS,
            ),
            (
                "POL-2",
                "Insurer A",
                9999,
                "Unknown",
                "B-F",
                "Home",
                _d("200.00"),
                "None",
                _d("20.00"),
                date(2024, 1, 1),
                date(2025, 1, 1),
                "p.csv",
                TS,
                TS,
            ),
        ],
    )
    risk = spark.createDataFrame(
        [(4870, "Cairns QLD", "T-W", "r.csv", TS, TS)],
        schema=SILVER_RISK,
    )
    gold = build_gold_portfolio_exposure(premiums, risk)
    assert gold.count() == 1
    row = gold.collect()[0]
    assert row.policy_count == 1
    assert float(row.total_sum_insured) == 100.0
    assert float(row.premium_rate_pct) == 10.0


def test_gold_claims_development_lags(spark):
    claims = _claims(
        spark,
        [
            (
                "CLM-1",
                "POL-1",
                None,
                date(2025, 2, 1),
                date(2025, 2, 11),
                "Wind",
                "Open",
                _d("100.00"),
                _d("25.00"),
                date(2025, 2, 11),
                "c.csv",
                TS,
                TS,
            ),
            (
                "CLM-2",
                "POL-1",
                None,
                date(2025, 2, 5),
                date(2025, 2, 15),
                "Wind",
                "Open",
                _d("100.00"),
                _d("25.00"),
                date(2025, 2, 15),
                "c.csv",
                TS,
                TS,
            ),
        ],
    )
    premiums = _premiums(
        spark,
        [
            (
                "POL-1",
                "Insurer A",
                4870,
                "Cairns QLD",
                "T-W",
                "Home",
                _d("500000.00"),
                "Shutters",
                _d("2500.00"),
                date(2024, 1, 1),
                date(2025, 1, 1),
                "p.csv",
                TS,
                TS,
            ),
        ],
    )
    gold = build_gold_claims_development(claims, premiums).collect()[0]
    assert gold.claim_count == 2
    assert float(gold.avg_reporting_lag_days) == 10.0
    assert gold.max_reporting_lag_days == 10
    assert float(gold.payment_progress_pct) == 25.0
    assert gold.loss_month.date() == date(2025, 2, 1)
