"""Unit tests for Auto Loader helpers (no Spark Connect required)."""

from types import SimpleNamespace

from actuarial_streaming_pipeline.auto_loader import (
    CLAIMS_SCHEMA_HINTS,
    CYCLONE_EVENTS_SCHEMA_HINTS,
    DEFAULT_LANDING_PATH,
    PREMIUMS_SCHEMA_HINTS,
    RISK_ZONES_SCHEMA_HINTS,
    landing_path,
)


class _FakeConf:
    def __init__(self, values: dict[str, str]):
        self._values = values

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._values.get(key, default)


def test_landing_path_default():
    spark = SimpleNamespace(conf=_FakeConf({}))
    assert landing_path(spark) == DEFAULT_LANDING_PATH
    assert landing_path(spark) == "/Volumes/actuarial/dev/landing"


def test_landing_path_from_conf_strips_slash():
    spark = SimpleNamespace(conf=_FakeConf({"landing_path": "/Volumes/actuarial/dev/landing/"}))
    assert landing_path(spark) == "/Volumes/actuarial/dev/landing"


def test_schema_hints_cover_core_contracts():
    for fragment in (
        "claim_id STRING",
        "policy_id STRING",
        "event_id STRING",
        "date_of_loss DATE",
        "reported_date DATE",
        "incurred_amount DECIMAL(18,2)",
        "paid_to_date DECIMAL(18,2)",
        "snapshot_date DATE",
    ):
        assert fragment in CLAIMS_SCHEMA_HINTS

    for fragment in (
        "policy_id STRING",
        "postcode INT",
        "sum_insured DECIMAL(18,2)",
        "annual_premium DECIMAL(18,2)",
        "policy_start_date DATE",
        "policy_end_date DATE",
    ):
        assert fragment in PREMIUMS_SCHEMA_HINTS

    for fragment in ("postcode INT", "region_name STRING", "wind_risk_band STRING"):
        assert fragment in RISK_ZONES_SCHEMA_HINTS

    for fragment in ("event_id STRING", "event_name STRING", "start_date DATE", "end_date DATE"):
        assert fragment in CYCLONE_EVENTS_SCHEMA_HINTS
