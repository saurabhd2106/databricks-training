"""Lightweight config sanity checks (no Spark required)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_bundle_name_and_landing_path():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["bundle"]["name"] == "actuarial_streaming_pipeline"
    for target in ("dev", "prod"):
        vars_ = bundle["targets"][target]["variables"]
        assert vars_["catalog"] == "actuarial"
        assert vars_["schema"] == "dev"
        assert vars_["landing_volume_path"] == "/Volumes/actuarial/dev/landing"


def test_pipeline_is_serverless_triggered():
    pipeline = yaml.safe_load(
        (ROOT / "resources" / "actuarial_streaming_etl.pipeline.yml").read_text()
    )
    cfg = pipeline["resources"]["pipelines"]["actuarial_streaming_etl"]
    assert cfg["serverless"] is True
    assert cfg.get("continuous") in (None, False)
    assert cfg["schema"] == "${var.schema}"
    assert cfg["catalog"] == "${var.catalog}"
    assert cfg["configuration"]["landing_path"] == "${var.landing_volume_path}"
    assert "dist/*.whl" in cfg["environment"]["dependencies"]


def test_job_task_order_serverless_and_claims_batch_default():
    job = yaml.safe_load((ROOT / "resources" / "actuarial_streaming_job.job.yml").read_text())
    job_cfg = job["resources"]["jobs"]["actuarial_streaming_job"]
    tasks = job_cfg["tasks"]
    keys = [t["task_key"] for t in tasks]
    assert keys == ["setup", "land_sample_data", "refresh_pipeline"]

    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in tasks}
    assert deps["land_sample_data"] == ["setup"]
    assert deps["refresh_pipeline"] == ["land_sample_data"]

    params = {p["name"]: p["default"] for p in job_cfg["parameters"]}
    assert params["claims_batch"] == "all"
    assert params["landing_path"] == "${var.landing_volume_path}"
    assert params["schema"] == "${var.schema}"

    for task_key in ("setup", "land_sample_data"):
        task = next(t for t in tasks if t["task_key"] == task_key)
        assert task["environment_key"] == "default"
        assert "existing_cluster_id" not in task

    env = job_cfg["environments"][0]
    assert env["environment_key"] == "default"
    assert env["spec"]["environment_version"] == "4"

    refresh = next(t for t in tasks if t["task_key"] == "refresh_pipeline")
    assert "pipeline_id" in refresh["pipeline_task"]
    assert refresh["pipeline_task"].get("full_refresh") in (None, False)


def test_volume_and_schema_target_dev():
    volume = yaml.safe_load((ROOT / "resources" / "landing.volume.yml").read_text())
    vol = volume["resources"]["volumes"]["actuarial_dev_landing"]
    assert vol["schema_name"] == "${var.schema}"
    assert vol["catalog_name"] == "${var.catalog}"
    assert vol["name"] == "landing"
    assert vol["volume_type"] == "MANAGED"

    schema = yaml.safe_load((ROOT / "resources" / "dev.schema.yml").read_text())
    sch = schema["resources"]["schemas"]["actuarial_dev"]
    assert sch["catalog_name"] == "${var.catalog}"
    assert sch["name"] == "${var.schema}"


def test_transform_modules_cover_medallion_and_quarantine():
    tx = ROOT / "src" / "actuarial_streaming_etl" / "transformations"
    expected = [
        # Bronze raw
        "bronze_claims_bordereau_raw.py",
        "bronze_premium_bordereau_raw.py",
        "bronze_risk_zone_lookup_raw.py",
        "bronze_cyclone_events_raw.py",
        # Bronze clean (DQ gate)
        "bronze_claims_bordereau.py",
        "bronze_premium_bordereau.py",
        "bronze_risk_zone_lookup.py",
        "bronze_cyclone_events.py",
        # Quarantine (DQ audit)
        "quarantine_bronze_claims_bordereau.py",
        "quarantine_bronze_premium_bordereau.py",
        "quarantine_bronze_risk_zone_lookup.py",
        "quarantine_bronze_cyclone_events.py",
        # Typed views
        "v_claims_typed.py",
        "v_premiums_typed.py",
        # Silver Materialized Views
        "silver_claims_bordereau.py",
        "silver_claims_current.py",
        "silver_premium_bordereau.py",
        "silver_risk_zone_lookup.py",
        "silver_cyclone_events.py",
    ]
    for name in expected:
        assert (tx / name).is_file(), f"missing transform module: {name}"


def test_bronze_clean_expect_or_drop_key_predicates():
    tx = ROOT / "src" / "actuarial_streaming_etl" / "transformations"
    expectations = {
        "bronze_claims_bordereau.py": ('expect_or_drop("claim_id_not_null", "claim_id IS NOT NULL")'),
        "bronze_premium_bordereau.py": (
            'expect_or_drop("policy_id_not_null", "policy_id IS NOT NULL")'
        ),
        "bronze_risk_zone_lookup.py": ('expect_or_drop("postcode_not_null", "postcode IS NOT NULL")'),
        "bronze_cyclone_events.py": ('expect_or_drop("event_id_not_null", "event_id IS NOT NULL")'),
    }
    for name, predicate in expectations.items():
        source = (tx / name).read_text()
        assert predicate in source, f"{name} missing expect_or_drop: {predicate}"
