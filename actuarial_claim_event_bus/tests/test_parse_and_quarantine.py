"""Unit tests for JSON parse + quarantine helpers (Databricks Connect)."""

from actuarial_claim_event_bus.kafka_source import parse_json_batch
from actuarial_claim_event_bus.quarantine import quarantine_from_raw
from actuarial_claim_event_bus.schemas import CLAIMS_SCHEMA
from helpers import claim_json, raw_value_df


def test_parse_valid_claim_json(spark):
    raw = raw_value_df(spark, [claim_json()])
    parsed = parse_json_batch(raw, CLAIMS_SCHEMA)
    rows = parsed.collect()
    assert len(rows) == 1
    assert rows[0].claim_id == "CLM-001"
    assert rows[0]._parse_error is None
    assert rows[0]._raw_value is not None
    assert "_topic" in parsed.columns


def test_parse_invalid_json_sets_parse_error(spark):
    raw = raw_value_df(spark, ["{not valid", claim_json()])
    parsed = parse_json_batch(raw, CLAIMS_SCHEMA)
    by_offset = {r._offset: r for r in parsed.collect()}
    assert by_offset[0]._parse_error == "invalid_json"
    assert by_offset[0].claim_id is None
    assert by_offset[1]._parse_error is None
    assert by_offset[1].claim_id == "CLM-001"


def test_quarantine_captures_null_key_and_parse_error(spark):
    raw = raw_value_df(
        spark,
        [
            claim_json(claim_id="CLM-OK"),
            claim_json(claim_id=None),
            "{bad",
        ],
    )
    parsed = parse_json_batch(raw, CLAIMS_SCHEMA)
    quarantined = quarantine_from_raw(parsed, "claim_id")
    rows = quarantined.collect()
    assert len(rows) == 2
    assert "quarantine_reason" in quarantined.columns
    assert "_quarantine_ts" in quarantined.columns
    reasons = " ".join(r.quarantine_reason or "" for r in rows)
    assert "claim_id_null" in reasons
    assert "invalid_json" in reasons
