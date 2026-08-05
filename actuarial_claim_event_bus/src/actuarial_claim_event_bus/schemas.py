"""Spark StructType contracts for Event Hubs JSON payloads."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CLAIMS_SCHEMA = StructType(
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
    ]
)

PREMIUMS_SCHEMA = StructType(
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
    ]
)

RISK_ZONES_SCHEMA = StructType(
    [
        StructField("postcode", IntegerType(), True),
        StructField("region_name", StringType(), True),
        StructField("wind_risk_band", StringType(), True),
    ]
)

CYCLONE_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
    ]
)

# Pipeline configuration keys → business key for quarantine.
TOPIC_CONFIG_KEYS = {
    "claims": "eh_claims_topic",
    "premiums": "eh_premiums_topic",
    "risk_zones": "eh_risk_zones_topic",
    "cyclone_events": "eh_cyclone_events_topic",
}

DATASET_SCHEMAS = {
    "claims": CLAIMS_SCHEMA,
    "premiums": PREMIUMS_SCHEMA,
    "risk_zones": RISK_ZONES_SCHEMA,
    "cyclone_events": CYCLONE_EVENTS_SCHEMA,
}

DATASET_KEYS = {
    "claims": "claim_id",
    "premiums": "policy_id",
    "risk_zones": "postcode",
    "cyclone_events": "event_id",
}
